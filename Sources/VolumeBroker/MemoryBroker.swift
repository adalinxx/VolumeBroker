import Foundation

public final class MemoryBroker: @unchecked Sendable, RetainedRootMergeBroker {
    public let near: (any VolumeBroker)?
    public let far: (any VolumeBroker)?

    private struct State {
        var contentByCID: [String: Data] = [:]
        var membersByRoot: [String: Set<String>] = [:]
        var ownersByCID: [String: Set<String>] = [:]
        var insertedAt: [String: ContinuousClock.Instant] = [:]
        var pins: [String: [String: PinEntry]] = [:]
        var retainedRoots: [String: Set<String>] = [:]
        var lru = LRUOrder()
    }

    private let lock = RWLock()
    private var state = State()
    private let capacity: Int?
    private let byteBudget: Int?
    private let evictUnpinnedGrace: Duration

    public init(
        capacity: Int? = nil,
        evictUnpinnedGrace: Duration = .seconds(600),
        near: (any VolumeBroker)? = nil,
        far: (any VolumeBroker)? = nil
    ) {
        self.capacity = capacity
        self.byteBudget = nil
        self.evictUnpinnedGrace = evictUnpinnedGrace
        self.near = near
        self.far = far
    }

    /// Bound resident memory by total volume payload bytes rather than by
    /// volume count. Unpinned LRU volumes are evicted until resident bytes
    /// fall back to `byteBudget`; pinned volumes are never evicted.
    public init(
        byteBudget: Int,
        evictUnpinnedGrace: Duration = .seconds(600),
        near: (any VolumeBroker)? = nil,
        far: (any VolumeBroker)? = nil
    ) {
        self.capacity = nil
        self.byteBudget = byteBudget
        self.evictUnpinnedGrace = evictUnpinnedGrace
        self.near = near
        self.far = far
    }

    /// Sum of unique content payload sizes currently resident.
    public func residentBytes() async -> Int {
        lock.withReadLock { Self.residentBytes(state: state) }
    }

    private static func residentBytes(state: State) -> Int {
        state.contentByCID.values.reduce(0) { $0 + $1.count }
    }

    public func hasVolume(root: String) async -> Bool {
        lock.withReadLock { Self.isComplete(root: root, state: state) }
    }

    public func fetchVolumeLocal(root: String) async -> SerializedVolume? {
        // A successful fetch is a use: refresh LRU recency so byte-budget
        // eviction is genuinely least-recently-used, not insertion-order, for
        // read-heavy workloads. Requires the write lock to mutate `state.lru`.
        lock.withWriteLock {
            guard let volume = Self.volume(root: root, state: state) else { return nil }
            state.lru.touch(root)
            return volume
        }
    }

    public func fetchDataLocal(cid: String) async -> Data? {
        lock.withWriteLock {
            guard let data = state.contentByCID[cid],
                  let owners = state.ownersByCID[cid],
                  !owners.isEmpty else { return nil }
            for root in owners { state.lru.touch(root) }
            return data
        }
    }

    public func storeVolumesLocal(_ volumes: [SerializedVolume]) async throws {
        let volumes = volumes.map { $0.ownedCopy() }
        for volume in volumes { try volume.validate() }
        guard !volumes.isEmpty else { return }
        let insertedAt = ContinuousClock.Instant.now
        try lock.withWriteLock {
            let submittedRoots = Set(volumes.map(\.root))
            let newVolumes = try Self.preflight(volumes, state: state)
            try ensureBatchFits(volumes, state: state)
            for volume in newVolumes {
                let members = Set(volume.entries.keys)
                state.membersByRoot[volume.root] = members
                state.insertedAt[volume.root] = insertedAt
                for (cid, data) in volume.entries {
                    state.contentByCID[cid] = data
                    state.ownersByCID[cid, default: []].insert(volume.root)
                }
            }
            for volume in volumes {
                state.lru.touch(volume.root)
            }
            evictIfOverCapacity(protecting: submittedRoots, state: &state)
            evictIfOverByteBudget(protecting: submittedRoots, state: &state)
        }
    }

    private func ensureBatchFits(_ volumes: [SerializedVolume], state: State) throws {
        let submittedRoots = Set(volumes.map(\.root))
        let existingRoots = Set(state.membersByRoot.keys)
        let protectedRoots = Self.protectedRoots(state: state, now: .now)
            .intersection(existingRoots)
        let requiredRoots = submittedRoots.union(protectedRoots)

        if let capacity, requiredRoots.count > capacity {
            throw BrokerError.capacityExceeded
        }
        guard let byteBudget else { return }

        var requiredCIDs = Set<String>()
        for root in requiredRoots {
            requiredCIDs.formUnion(state.membersByRoot[root] ?? [])
        }
        var submittedContent: [String: Data] = [:]
        for volume in volumes {
            requiredCIDs.formUnion(volume.entries.keys)
            submittedContent.merge(volume.entries) { current, _ in current }
        }

        var requiredBytes = 0
        for cid in requiredCIDs {
            guard let data = submittedContent[cid] ?? state.contentByCID[cid] else {
                throw BrokerError.inconsistentState("protected CID \(cid) has no resident bytes")
            }
            let (sum, overflow) = requiredBytes.addingReportingOverflow(data.count)
            if overflow { throw BrokerError.capacityExceeded }
            requiredBytes = sum
        }
        if requiredBytes > byteBudget { throw BrokerError.capacityExceeded }
    }

    private static func preflight(_ volumes: [SerializedVolume], state: State) throws -> [SerializedVolume] {
        var pendingMemberships: [String: Set<String>] = [:]
        var pendingContent: [String: Data] = [:]
        var newVolumes: [SerializedVolume] = []

        for volume in volumes {
            let members = Set(volume.entries.keys)
            if let existing = pendingMemberships[volume.root] ?? state.membersByRoot[volume.root],
               existing != members {
                throw BrokerError.conflictingVolume(volume.root)
            }
            if pendingMemberships[volume.root] == nil {
                pendingMemberships[volume.root] = members
                if state.membersByRoot[volume.root] == nil {
                    newVolumes.append(volume)
                }
            }

            for (cid, data) in volume.entries {
                if let existing = pendingContent[cid] ?? state.contentByCID[cid],
                   existing != data {
                    throw BrokerError.conflictingContent(cid)
                }
                pendingContent[cid] = data
            }
        }
        return newVolumes
    }

    private static func isComplete(root: String, state: State) -> Bool {
        guard let members = state.membersByRoot[root],
              !members.isEmpty,
              members.contains(root) else { return false }
        return members.allSatisfy { cid in
            state.contentByCID[cid] != nil && state.ownersByCID[cid]?.contains(root) == true
        }
    }

    private static func volume(root: String, state: State) -> SerializedVolume? {
        guard isComplete(root: root, state: state),
              let members = state.membersByRoot[root] else { return nil }
        var entries: [String: Data] = [:]
        entries.reserveCapacity(members.count)
        for cid in members {
            guard let data = state.contentByCID[cid] else { return nil }
            entries[cid] = data
        }
        return SerializedVolume(root: root, entries: entries)
    }

    @discardableResult
    private static func removeVolume(root: String, state: inout State) -> Int {
        guard let members = state.membersByRoot.removeValue(forKey: root) else { return 0 }
        var freedBytes = 0
        for cid in members {
            var owners = state.ownersByCID[cid] ?? []
            owners.remove(root)
            if owners.isEmpty {
                state.ownersByCID.removeValue(forKey: cid)
                freedBytes += state.contentByCID.removeValue(forKey: cid)?.count ?? 0
            } else {
                state.ownersByCID[cid] = owners
            }
        }
        state.insertedAt.removeValue(forKey: root)
        state.lru.remove(root)
        return freedBytes
    }

    public func pin(root: String, owner: String, count: Int, ttl: Duration?) async throws {
        guard count > 0 else { throw BrokerError.invalidPinCount }
        if let ttl, ttl < .zero { throw BrokerError.invalidPinTTL }
        let now = ContinuousClock.Instant.now
        let expiresAt = ttl.map { now + $0 }
        try lock.withWriteLock {
            guard Self.isComplete(root: root, state: state) else {
                throw BrokerError.notFound
            }
            if var entry = state.pins[root, default: [:]][owner],
               entry.expiresAt.map({ now < $0 }) ?? true {
                let (updated, overflow) = entry.count.addingReportingOverflow(count)
                guard !overflow else { throw BrokerError.invalidPinCount }
                entry.count = updated
                if expiresAt == nil || entry.expiresAt == nil {
                    entry.expiresAt = nil
                } else if let new = expiresAt, let old = entry.expiresAt, new > old {
                    entry.expiresAt = new
                }
                state.pins[root, default: [:]][owner] = entry
            } else {
                state.pins[root, default: [:]][owner] = PinEntry(count: count, expiresAt: expiresAt)
            }
        }
    }

    public func unpin(root: String, owner: String, count: Int) async throws {
        guard count > 0 else { throw BrokerError.invalidPinCount }
        lock.withWriteLock {
            guard var entry = state.pins[root]?[owner] else { return }
            entry.count -= count
            if entry.count <= 0 {
                state.pins[root]?.removeValue(forKey: owner)
                if state.pins[root]?.isEmpty == true { state.pins.removeValue(forKey: root) }
            } else {
                state.pins[root]?[owner] = entry
            }
        }
    }

    public func unpinAll(owner: String) async throws {
        lock.withWriteLock {
            for root in state.pins.keys {
                state.pins[root]?.removeValue(forKey: owner)
                if state.pins[root]?.isEmpty == true { state.pins.removeValue(forKey: root) }
            }
        }
    }

    public func owners(root: String) async -> Set<String> {
        let now = ContinuousClock.Instant.now
        return lock.withReadLock {
            guard let ownerMap = state.pins[root] else { return [] }
            return Set(ownerMap.compactMap { owner, entry in
                if let expiresAt = entry.expiresAt, now >= expiresAt { return nil }
                if entry.count <= 0 { return nil }
                return owner
            })
        }
    }

    public func pinnedRoots(owners: [String] = [], ownerPrefixes: [String] = []) async -> [String] {
        let exactOwners = Set(owners.filter { !$0.isEmpty })
        let prefixes = ownerPrefixes.filter { !$0.isEmpty }
        guard !exactOwners.isEmpty || !prefixes.isEmpty else { return [] }

        let now = ContinuousClock.Instant.now
        return lock.withReadLock {
            state.pins.compactMap { root, ownerMap in
                let hasMatchingOwner = ownerMap.contains { owner, entry in
                    if entry.count <= 0 { return false }
                    if let expiresAt = entry.expiresAt, now >= expiresAt { return false }
                    return exactOwners.contains(owner) || prefixes.contains { owner.hasPrefix($0) }
                }
                return hasMatchingOwner ? root : nil
            }
        }
    }

    public func advanceRetainedRoots(scope: String, roots: [String]) async throws {
        let canonicalRoots = try Self.canonicalRetainedRoots(roots)
        guard !scope.isEmpty else {
            throw BrokerError.invalidRetainedRoots("scope must not be empty")
        }

        try lock.withWriteLock {
            for root in canonicalRoots {
                try Self.validateRetainedVolume(root: root, state: state)
            }
            state.retainedRoots[scope] = Set(canonicalRoots)
        }
    }

    public func mergeRetainedRoots(scope: String, roots: [String]) async throws {
        let canonicalRoots = try Self.canonicalRetainedRoots(roots)
        guard !scope.isEmpty else {
            throw BrokerError.invalidRetainedRoots("scope must not be empty")
        }

        try lock.withWriteLock {
            for root in canonicalRoots {
                try Self.validateRetainedVolume(root: root, state: state)
            }
            state.retainedRoots[scope, default: []].formUnion(canonicalRoots)
        }
    }

    public func retainedRoots(scope: String) async throws -> [String] {
        guard !scope.isEmpty else { return [] }
        return lock.withReadLock {
            Array(state.retainedRoots[scope] ?? []).sorted()
        }
    }

    public func evictUnpinned() async throws -> Int {
        let now = ContinuousClock.Instant.now
        return lock.withWriteLock {
            for (root, ownerMap) in state.pins {
                let expired = ownerMap.filter { _, entry in
                    guard let expiresAt = entry.expiresAt else { return false }
                    return now >= expiresAt
                }.keys
                for owner in expired { state.pins[root]?.removeValue(forKey: owner) }
                if state.pins[root]?.isEmpty == true { state.pins.removeValue(forKey: root) }
            }
            let protected = Self.protectedRoots(state: state, now: now)
            let unpinned = state.membersByRoot.keys.filter { root in
                if protected.contains(root) { return false }
                guard let insertedAt = state.insertedAt[root] else { return true }
                return insertedAt + evictUnpinnedGrace <= now
            }
            for root in unpinned {
                Self.removeVolume(root: root, state: &state)
            }
            return unpinned.count
        }
    }

    private func evictIfOverCapacity(protecting submittedRoots: Set<String>, state: inout State) {
        guard let capacity else { return }
        let now = ContinuousClock.Instant.now
        guard state.membersByRoot.count > capacity else { return }
        let protected = Self.protectedRoots(state: state, now: now).union(submittedRoots)
        var node = state.lru.oldest
        while state.membersByRoot.count > capacity, let current = node {
            let key = current.key
            let next = current.next
            if !protected.contains(key) {
                Self.removeVolume(root: key, state: &state)
                state.pins.removeValue(forKey: key)
            }
            node = next
        }
    }

    private func evictIfOverByteBudget(protecting submittedRoots: Set<String>, state: inout State) {
        guard let byteBudget else { return }
        let now = ContinuousClock.Instant.now
        var resident = Self.residentBytes(state: state)
        guard resident > byteBudget else { return }
        let protected = Self.protectedRoots(state: state, now: now).union(submittedRoots)
        var node = state.lru.oldest
        while resident > byteBudget, let current = node {
            let key = current.key
            let next = current.next
            if !protected.contains(key), state.membersByRoot[key] != nil {
                resident -= Self.removeVolume(root: key, state: &state)
                state.pins.removeValue(forKey: key)
            }
            node = next
        }
    }

    private static func canonicalRetainedRoots(_ roots: [String]) throws -> [String] {
        let unique = Array(Set(roots))
        if unique.contains(where: { $0.isEmpty }) {
            throw BrokerError.invalidRetainedRoots("roots must not contain empty strings")
        }
        return unique.sorted()
    }

    private static func validateRetainedVolume(root: String, state: State) throws {
        guard isComplete(root: root, state: state) else {
            throw BrokerError.missingRetainedRoot(root)
        }
    }

    private static func protectedRoots(state: State, now: ContinuousClock.Instant) -> Set<String> {
        var protected = Set<String>()
        for (root, ownerMap) in state.pins {
            let hasLivePin = ownerMap.contains { _, entry in
                if entry.count <= 0 { return false }
                guard let expiresAt = entry.expiresAt else { return true }
                return now < expiresAt
            }
            if hasLivePin { protected.insert(root) }
        }
        for roots in state.retainedRoots.values {
            protected.formUnion(roots)
        }

        return protected
    }
}

struct PinEntry {
    var count: Int
    var expiresAt: ContinuousClock.Instant?
}

// MARK: - O(1) LRU tracking

private struct LRUOrder {
    final class Node {
        let key: String
        var prev: Node?
        var next: Node?
        init(_ key: String) { self.key = key }
    }

    private var map: [String: Node] = [:]
    private var head: Node?
    private var tail: Node?

    var oldest: Node? { head }

    mutating func touch(_ key: String) {
        if let existing = map[key] {
            detach(existing)
            appendTail(existing)
        } else {
            let node = Node(key)
            map[key] = node
            appendTail(node)
        }
    }

    mutating func remove(_ key: String) {
        guard let node = map.removeValue(forKey: key) else { return }
        detach(node)
    }

    private mutating func detach(_ node: Node) {
        let p = node.prev
        let n = node.next
        p?.next = n
        n?.prev = p
        if head === node { head = n }
        if tail === node { tail = p }
        node.prev = nil
        node.next = nil
    }

    private mutating func appendTail(_ node: Node) {
        node.prev = tail
        node.next = nil
        tail?.next = node
        tail = node
        if head == nil { head = node }
    }
}

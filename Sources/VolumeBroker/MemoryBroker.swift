import Foundation

public final class MemoryBroker: @unchecked Sendable, VolumeBroker, RetainedRootBroker, RetainedRootMergeBroker {
    public var near: (any VolumeBroker)?
    public var far: (any VolumeBroker)?

    private struct State {
        var volumes: [String: SerializedVolume] = [:]
        var insertedAt: [String: ContinuousClock.Instant] = [:]
        var pins: [String: [String: PinEntry]] = [:]
        var retainedRoots: [String: Set<String>] = [:]
        var retainedRootOperations: [String: (scope: String, payload: String)] = [:]
        var lru = LRUOrder()
    }

    private let lock = RWLock()
    private var state = State()
    private let capacity: Int?
    private let byteBudget: Int?
    private let evictUnpinnedGrace: Duration

    public init(capacity: Int? = nil, evictUnpinnedGrace: Duration = .seconds(600)) {
        self.capacity = capacity
        self.byteBudget = nil
        self.evictUnpinnedGrace = evictUnpinnedGrace
    }

    /// Bound resident memory by total volume payload bytes rather than by
    /// volume count. Unpinned LRU volumes are evicted until resident bytes
    /// fall back to `byteBudget`; pinned volumes are never evicted.
    public init(byteBudget: Int, evictUnpinnedGrace: Duration = .seconds(600)) {
        self.capacity = nil
        self.byteBudget = byteBudget
        self.evictUnpinnedGrace = evictUnpinnedGrace
    }

    /// Sum of stored `SerializedVolume.entries` payload sizes currently resident.
    public func residentBytes() async -> Int {
        lock.withReadLock { Self.residentBytes(state.volumes) }
    }

    private static func payloadBytes(_ volume: SerializedVolume) -> Int {
        volume.entries.values.reduce(0) { $0 + $1.count }
    }

    private static func residentBytes(_ volumes: [String: SerializedVolume]) -> Int {
        volumes.values.reduce(0) { $0 + payloadBytes($1) }
    }

    public func hasVolume(root: String) async -> Bool {
        lock.withReadLock { state.volumes[root] != nil }
    }

    public func fetchVolumeLocal(root: String) async -> SerializedVolume? {
        // A successful fetch is a use: refresh LRU recency so byte-budget
        // eviction is genuinely least-recently-used, not insertion-order, for
        // read-heavy workloads. Requires the write lock to mutate `state.lru`.
        lock.withWriteLock {
            guard let volume = state.volumes[root] else { return nil }
            state.lru.touch(root)
            return volume
        }
    }

    public func fetchDataLocal(cid: String) async -> Data? {
        // The memory tier groups by volume root; resolve a content CID by
        // checking the volume keyed by it (common case) then scanning entries.
        lock.withReadLock {
            if let data = state.volumes[cid]?.entries[cid] { return data }
            for volume in state.volumes.values {
                if let data = volume.entries[cid] { return data }
            }
            return nil
        }
    }

    public func storeVolumeLocal(_ volume: SerializedVolume) async throws {
        try volume.validate()
        let insertedAt = ContinuousClock.Instant.now
        try lock.withWriteLock {
            try Self.validateMemberships([volume], against: state.volumes)
            let isNewRoot = state.volumes[volume.root] == nil
            state.volumes[volume.root] = volume
            if isNewRoot { state.insertedAt[volume.root] = insertedAt }
            state.lru.touch(volume.root)
        }
        evictIfOverCapacity()
        evictIfOverByteBudget()
    }

    public func storeVolumesLocal(_ volumes: [SerializedVolume]) async throws {
        for volume in volumes { try volume.validate() }
        let insertedAt = ContinuousClock.Instant.now
        try lock.withWriteLock {
            try Self.validateMemberships(volumes, against: state.volumes)
            for volume in volumes {
                let isNewRoot = state.volumes[volume.root] == nil
                state.volumes[volume.root] = volume
                if isNewRoot { state.insertedAt[volume.root] = insertedAt }
                state.lru.touch(volume.root)
            }
        }
        evictIfOverCapacity()
        evictIfOverByteBudget()
    }

    private static func validateMemberships(
        _ volumes: [SerializedVolume],
        against stored: [String: SerializedVolume]
    ) throws {
        var pending: [String: Set<String>] = [:]
        for volume in volumes {
            let entries = Set(volume.entries.keys)
            let existing = pending[volume.root]
                ?? stored[volume.root].map { Set($0.entries.keys) }
                ?? entries
            if existing != entries {
                throw BrokerError.conflictingVolume(volume.root)
            }
            pending[volume.root] = entries
        }
    }

    public func pin(root: String, owner: String, count: Int, ttl: Duration?) async throws {
        let expiresAt = ttl.map { ContinuousClock.Instant.now + $0 }
        lock.withWriteLock {
            if var entry = state.pins[root, default: [:]][owner] {
                entry.count += count
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

    public func advanceRetainedRoots(scope: String, roots: [String], operationID: String) async throws {
        let canonicalRoots = try Self.canonicalRetainedRoots(roots)
        guard !scope.isEmpty else {
            throw BrokerError.invalidRetainedRootOperation("scope must not be empty")
        }
        guard !operationID.isEmpty else {
            throw BrokerError.invalidRetainedRootOperation("operationID must not be empty")
        }

        try lock.withWriteLock {
            if let existing = state.retainedRootOperations[operationID] {
                guard existing.scope == scope && existing.payload == Self.operationPayload(kind: "replace", roots: canonicalRoots) else {
                    throw BrokerError.conflictingRetainedRootOperation(operationID)
                }
                return
            }
            for root in canonicalRoots {
                try Self.validateRetainedVolume(root: root, state: state)
            }
            state.retainedRoots[scope] = Set(canonicalRoots)
            state.retainedRootOperations[operationID] = (scope, Self.operationPayload(kind: "replace", roots: canonicalRoots))
        }
    }

    public func mergeRetainedRoots(scope: String, roots: [String], operationID: String) async throws {
        let canonicalRoots = try Self.canonicalRetainedRoots(roots)
        guard !scope.isEmpty else {
            throw BrokerError.invalidRetainedRootOperation("scope must not be empty")
        }
        guard !operationID.isEmpty else {
            throw BrokerError.invalidRetainedRootOperation("operationID must not be empty")
        }

        try lock.withWriteLock {
            if let existing = state.retainedRootOperations[operationID] {
                guard existing.scope == scope && existing.payload == Self.operationPayload(kind: "merge", roots: canonicalRoots) else {
                    throw BrokerError.conflictingRetainedRootOperation(operationID)
                }
                return
            }
            for root in canonicalRoots {
                try Self.validateRetainedVolume(root: root, state: state)
            }
            state.retainedRoots[scope, default: []].formUnion(canonicalRoots)
            state.retainedRootOperations[operationID] = (scope, Self.operationPayload(kind: "merge", roots: canonicalRoots))
        }
    }

    public func retainedRoots(scope: String) async -> [String] {
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
            let unpinned = state.volumes.keys.filter { root in
                if protected.contains(root) { return false }
                guard let insertedAt = state.insertedAt[root] else { return true }
                return insertedAt + evictUnpinnedGrace <= now
            }
            for root in unpinned {
                state.volumes.removeValue(forKey: root)
                state.insertedAt.removeValue(forKey: root)
                state.lru.remove(root)
            }
            return unpinned.count
        }
    }

    private func evictIfOverCapacity() {
        guard let capacity else { return }
        let now = ContinuousClock.Instant.now
        lock.withWriteLock {
            guard state.volumes.count > capacity else { return }
            let protected = Self.protectedRoots(state: state, now: now)
            var node = state.lru.oldest
            while state.volumes.count > capacity, let current = node {
                let key = current.key
                let next = current.next
                if !protected.contains(key) {
                    state.volumes.removeValue(forKey: key)
                    state.insertedAt.removeValue(forKey: key)
                    state.pins.removeValue(forKey: key)
                    state.lru.remove(key)
                }
                node = next
            }
        }
    }

    private func evictIfOverByteBudget() {
        guard let byteBudget else { return }
        let now = ContinuousClock.Instant.now
        lock.withWriteLock {
            var resident = Self.residentBytes(state.volumes)
            guard resident > byteBudget else { return }
            let protected = Self.protectedRoots(state: state, now: now)
            var node = state.lru.oldest
            while resident > byteBudget, let current = node {
                let key = current.key
                let next = current.next
                if !protected.contains(key), let volume = state.volumes[key] {
                    resident -= Self.payloadBytes(volume)
                    state.volumes.removeValue(forKey: key)
                    state.insertedAt.removeValue(forKey: key)
                    state.pins.removeValue(forKey: key)
                    state.lru.remove(key)
                }
                node = next
            }
        }
    }

    private static func canonicalRetainedRoots(_ roots: [String]) throws -> [String] {
        let unique = Array(Set(roots))
        if unique.contains(where: { $0.isEmpty }) {
            throw BrokerError.invalidRetainedRootOperation("roots must not contain empty strings")
        }
        return unique.sorted()
    }

    private static func operationPayload(kind: String, roots: [String]) -> String {
        "\(kind):\(roots.joined(separator: "\n"))"
    }

    private static func validateRetainedVolume(root: String, state: State) throws {
        guard let volume = state.volumes[root], volume.entries[root] != nil else {
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

import Foundation

/// SQLite-backed `VolumeBroker`.
///
/// `DiskBroker` composes the layered storage components and delegates to them;
/// it owns no SQL itself. The layers are:
///   - `SQLiteConnection` — connection/PRAGMA/schema + serialised read/write access.
///   - `CASVolumeStore`    — content-addressed volume store (store/fetch/has).
///   - `PinIndex`          — ref-counted pins, TTL, and batched updates.
///   - `RetainedRootIndex` — named durable retained-root sets.
///   - `EvictionEngine`    — TTL prune + unpinned CAS/entry/metadata reclaim.
public final class DiskBroker: @unchecked Sendable, RetainedRootMergeBroker {
    public let near: (any VolumeBroker)?
    public let far: (any VolumeBroker)?

    private let connection: SQLiteConnection
    private let volumes: CASVolumeStore
    private let pins: PinIndex
    private let retainedRoots: RetainedRootIndex
    private let eviction: EvictionEngine
    private let evictUnpinnedGraceSeconds: Int

    public init(
        path: String,
        evictUnpinnedGraceSeconds: Int = 600,
        near: (any VolumeBroker)? = nil,
        far: (any VolumeBroker)? = nil
    ) throws {
        let connection = try SQLiteConnection(path: path)
        self.connection = connection
        self.volumes = CASVolumeStore(connection: connection)
        self.pins = PinIndex(connection: connection)
        self.retainedRoots = RetainedRootIndex(connection: connection)
        self.eviction = EvictionEngine(connection: connection)
        self.evictUnpinnedGraceSeconds = evictUnpinnedGraceSeconds
        self.near = near
        self.far = far
    }

    // MARK: - VolumeBroker

    public func hasVolume(root: String) async -> Bool {
        await volumes.hasVolume(root: root)
    }

    public func fetchVolumeLocal(root: String) async -> SerializedVolume? {
        await volumes.fetchVolumeLocal(root: root)
    }

    public func fetchDataLocal(cid: String) async -> Data? {
        await volumes.fetchDataLocal(cid: cid)
    }

    public func fetchDataLocal(cids: Set<String>) async -> [String: Data] {
        await volumes.fetchDataLocal(cids: cids)
    }

    public func storeVolumesLocal(_ volumes: [SerializedVolume]) async throws {
        try await self.volumes.storeVolumesLocal(volumes)
    }

    // MARK: - Pins

    /// Applies the complete batch in one pin-index transaction.
    public func pinBatch(roots: [String], owner: String) async throws {
        try await pins.pinBatch(roots: roots, owner: owner)
    }

    public func pin(root: String, owner: String, count: Int, ttl: Duration?) async throws {
        try await pins.pin(root: root, owner: owner, count: count, ttl: ttl)
    }

    /// Applies the complete batch in one pin-index transaction.
    public func unpinBatch(items: [(root: String, owner: String, count: Int)]) async throws {
        try await pins.unpinBatch(items: items)
    }

    public func unpin(root: String, owner: String, count: Int) async throws {
        try await pins.unpin(root: root, owner: owner, count: count)
    }

    public func unpinAll(owner: String) async throws {
        try await pins.unpinAll(owner: owner)
    }

    public func unpinAllBatch(owners: [String]) async throws {
        try await pins.unpinAllBatch(owners: owners)
    }

    public func owners(root: String) async -> Set<String> {
        await pins.owners(root: root)
    }

    /// True iff `cid` is a pinned Volume root or a direct entry of one.
    public func isPinReachable(cid: String) async -> Bool {
        await pins.isPinReachable(cid: cid)
    }

    public func pinnedRoots() async -> [String] {
        await pins.pinnedRoots()
    }

    public func pinnedRoots(owners: [String] = [], ownerPrefixes: [String] = []) async -> [String] {
        await pins.pinnedRoots(owners: owners, ownerPrefixes: ownerPrefixes)
    }

    public func pinnedOwners(prefix: String) async -> [String] {
        await pins.pinnedOwners(prefix: prefix)
    }

    // MARK: - Retained Roots

    public func advanceRetainedRoots(scope: String, roots: [String]) async throws {
        try await retainedRoots.advanceRetainedRoots(scope: scope, roots: roots)
    }

    public func mergeRetainedRoots(scope: String, roots: [String]) async throws {
        try await retainedRoots.mergeRetainedRoots(scope: scope, roots: roots)
    }

    public func retainedRoots(scope: String) async throws -> [String] {
        try await retainedRoots.retainedRoots(scope: scope)
    }

    // MARK: - Eviction

    public func evictUnpinned() async throws -> Int {
        try await eviction.evictUnpinned(graceSeconds: evictUnpinnedGraceSeconds)
    }

    public func evictUnpinned(graceSeconds: Int) async throws -> Int {
        try await eviction.evictUnpinned(graceSeconds: graceSeconds)
    }

    public func checkpoint() async {
        await connection.checkpoint()
    }

}

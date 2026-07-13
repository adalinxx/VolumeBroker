import Foundation

public protocol VolumeBroker: AnyObject, Sendable {
    var near: (any VolumeBroker)? { get set }
    var far: (any VolumeBroker)? { get set }

    func hasVolume(root: String) async -> Bool
    func fetchVolumeLocal(root: String) async -> SerializedVolume?
    /// Fetch a single node's bytes by its CID, regardless of which volume it
    /// belongs to. Unlike `fetchVolumeLocal(root:)` (keyed by a *volume* root),
    /// this resolves any content CID — including an entry that is not itself a
    /// volume root — which the object-grain store needs. Default falls back to
    /// the volume-keyed lookup (works when the CID *is* a root); CAS-backed
    /// brokers override with a direct content lookup.
    func fetchDataLocal(cid: String) async -> Data?
    func storeVolumeLocal(_ volume: SerializedVolume) async throws
    func storeVolumesLocal(_ volumes: [SerializedVolume]) async throws

    func pin(root: String, owner: String, count: Int, ttl: Duration?) async throws
    func unpin(root: String, owner: String, count: Int) async throws
    func unpinAll(owner: String) async throws
    func owners(root: String) async -> Set<String>
    func evictUnpinned() async throws -> Int
}

/// Optional durable-retention surface for brokers that can advance a named set
/// of roots atomically. Retained roots are independent from owner/count pins and
/// protect the same per-Volume entries from serving and eviction.
public protocol RetainedRootBroker: VolumeBroker {
    func advanceRetainedRoots(scope: String, roots: [String], operationID: String) async throws
    func retainedRoots(scope: String) async -> [String]
}

/// Optional retained-root surface for brokers that can atomically add roots to
/// an existing scope without replacing or retransmitting the full scope.
public protocol RetainedRootMergeBroker: RetainedRootBroker {
    func mergeRetainedRoots(scope: String, roots: [String], operationID: String) async throws
}

public extension VolumeBroker {
    func pin(root: String, owner: String) async throws {
        try await pin(root: root, owner: owner, count: 1, ttl: nil)
    }

    func pin(root: String, owner: String, ttl: Duration?) async throws {
        try await pin(root: root, owner: owner, count: 1, ttl: ttl)
    }

    func pin(root: String, owner: String, count: Int) async throws {
        try await pin(root: root, owner: owner, count: count, ttl: nil)
    }

    func unpin(root: String, owner: String) async throws {
        try await unpin(root: root, owner: owner, count: 1)
    }

    func storeVolumesLocal(_ volumes: [SerializedVolume]) async throws {
        for volume in volumes {
            try await storeVolumeLocal(volume)
        }
    }

    func fetchVolume(root: String) async -> SerializedVolume? {
        if let local = await fetchVolumeLocal(root: root) { return local }
        if let near, let volume = await near.fetchVolume(root: root) { return volume }
        if let far, let volume = await far.fetchVolume(root: root) { return volume }
        return nil
    }

    /// Default content-by-CID lookup: a volume keyed by the CID itself (works
    /// when the CID is a volume root). CAS-backed brokers override this to read
    /// `cas_data` directly so non-root entries resolve too.
    func fetchDataLocal(cid: String) async -> Data? {
        await fetchVolumeLocal(root: cid)?.entries[cid]
    }

    /// Content-by-CID across the tier chain (memory → disk → network).
    func fetchData(cid: String) async -> Data? {
        if let local = await fetchDataLocal(cid: cid) { return local }
        if let near, let data = await near.fetchData(cid: cid) { return data }
        if let far, let data = await far.fetchData(cid: cid) { return data }
        return nil
    }
}

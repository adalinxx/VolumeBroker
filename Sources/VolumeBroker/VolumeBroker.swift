import Foundation

public protocol VolumeBroker: AnyObject, Sendable {
    /// Optional storage tiers in this broker's domain. Cross-chain sources use
    /// separate brokers and are coordinated by the node.
    var near: (any VolumeBroker)? { get }
    var far: (any VolumeBroker)? { get }

    func hasVolume(root: String) async -> Bool
    func fetchVolumeLocal(root: String) async -> SerializedVolume?
    /// Fetch a single node's bytes by its CID when at least one complete local
    /// Volume owns it. The default handles the case where the CID is a root;
    /// CAS-backed brokers also resolve non-root members.
    func fetchDataLocal(cid: String) async -> Data?
    /// Fetch multiple locally owned entries in one backend pass.
    func fetchDataLocal(cids: Set<String>) async -> [String: Data]
    func storeVolumesLocal(_ volumes: [SerializedVolume]) async throws

    func pin(root: String, owner: String, count: Int, ttl: Duration?) async throws
    func pinBatch(roots: [String], owner: String) async throws
    func unpin(root: String, owner: String, count: Int) async throws
    func unpinBatch(items: [(root: String, owner: String, count: Int)]) async throws
    func unpinAll(owner: String) async throws
    func owners(root: String) async -> Set<String>
    func evictUnpinned() async throws -> Int
}

/// Optional retention surface for brokers that can atomically advance a named
/// set of roots. Retained roots are independent from owner/count pins and
/// protect the named Volumes from eviction.
public protocol RetainedRootBroker: VolumeBroker {
    func advanceRetainedRoots(scope: String, roots: [String]) async throws
    func retainedRoots(scope: String) async throws -> [String]
}

/// Optional retained-root surface for brokers that can atomically add roots to
/// an existing scope without replacing or retransmitting the full scope.
public protocol RetainedRootMergeBroker: RetainedRootBroker {
    func mergeRetainedRoots(scope: String, roots: [String]) async throws
}

public extension VolumeBroker {
    func pinBatch(roots: [String], owner: String) async throws {
        for root in roots { try await pin(root: root, owner: owner) }
    }

    func unpinBatch(
        items: [(root: String, owner: String, count: Int)]
    ) async throws {
        for item in items {
            try await unpin(
                root: item.root,
                owner: item.owner,
                count: item.count
            )
        }
    }

    func storeVolumeLocal(_ volume: SerializedVolume) async throws {
        try await storeVolumesLocal([volume])
    }

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

    func fetchVolume(root: String) async -> SerializedVolume? {
        if let local = await fetchVolumeLocal(root: root) { return local }
        if let near, let volume = await near.fetchVolume(root: root) { return volume }
        if let far, let volume = await far.fetchVolume(root: root) { return volume }
        return nil
    }

    /// Default content-by-CID lookup for a complete Volume keyed by the CID.
    func fetchDataLocal(cid: String) async -> Data? {
        await fetchVolumeLocal(root: cid)?.entries[cid]
    }

    /// Correct fallback for brokers without a native batch read.
    func fetchDataLocal(cids: Set<String>) async -> [String: Data] {
        var found: [String: Data] = [:]
        found.reserveCapacity(cids.count)
        for cid in cids {
            if let data = await fetchDataLocal(cid: cid) { found[cid] = data }
        }
        return found
    }

    /// Content-by-CID across the tier chain (memory -> disk -> network).
    func fetchData(cid: String) async -> Data? {
        if let local = await fetchDataLocal(cid: cid) { return local }
        if let near, let data = await near.fetchData(cid: cid) { return data }
        if let far, let data = await far.fetchData(cid: cid) { return data }
        return nil
    }

    /// Content-by-CID across the tier chain, querying each tier once for only
    /// the entries still missing from preceding tiers.
    func fetchData(cids: Set<String>) async -> [String: Data] {
        var found = await fetchDataLocal(cids: cids)
        var missing = cids.subtracting(found.keys)
        if !missing.isEmpty, let near {
            found.merge(await near.fetchData(cids: missing)) { current, _ in current }
            missing.subtract(found.keys)
        }
        if !missing.isEmpty, let far {
            found.merge(await far.fetchData(cids: missing)) { current, _ in current }
        }
        return found
    }
}

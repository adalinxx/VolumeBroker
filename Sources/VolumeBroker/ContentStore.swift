import Foundation
import ArrayTrie
import cashew

/// Object-level façade over a `VolumeBroker` tier chain: lattice-node deals in
/// whole content **objects by root CID** and never touches `SerializedVolume`,
/// individual entries, or cashew fetch/resolution mechanics directly.
public actor ContentStore {
    private let broker: any VolumeBroker
    private let source: BrokerFetcher

    public init(broker: any VolumeBroker) {
        self.broker = broker
        self.source = BrokerFetcher(broker: broker)
    }

    // MARK: - Read

    /// Resolve a whole object by root CID using the type's resolution paths
    /// (e.g. a Block's content-package policy). Returns nil if unresolvable.
    public func get<T: Node>(_ type: T.Type, _ rootCID: String, resolving paths: ArrayTrie<ResolutionStrategy>) async throws -> T? {
        try await VolumeImpl<T>(rawCID: rootCID, node: nil, encryptionInfo: nil)
            .resolve(paths: paths, source: source).node
    }

    /// Resolve a whole object and its entire reachable subtree.
    public func getRecursive<T: Node>(_ type: T.Type, _ rootCID: String) async throws -> T? {
        try await VolumeImpl<T>(rawCID: rootCID, node: nil, encryptionInfo: nil)
            .resolveRecursive(source: source).node
    }

    public func has(_ rootCID: String) async -> Bool {
        await broker.fetchData(cid: rootCID) != nil
    }

    public func hasDurable(_ rootCID: String) async -> Bool {
        await broker.hasVolume(root: rootCID)
    }

    // MARK: - Write

    /// Store a whole object (recursively grouping it into volumes via the
    /// store-side `VolumeAwareStorer`); returns its root CID.
    @discardableResult
    public func put<T: Node>(_ object: T) async throws -> String {
        let header = try VolumeImpl(node: object)
        let storer = BrokerStorer(broker: broker)
        try header.storeRecursively(storer: storer)
        try await storer.flush(root: header.rawCID)
        return header.rawCID
    }

    // MARK: - Retention

    /// Retain an object under a reason (`owner`), refcounted. Pinning the object
    /// root protects its whole reachable closure: eviction keeps the transitive
    /// set of cids reachable from a pinned root, so a multi-volume object (e.g.
    /// per-node state) is retained in full, not just the root node.
    public func retain(_ rootCID: String, owner: String) async throws {
        try await broker.pin(root: rootCID, owner: owner)
    }

    /// Release one retention reason; the closure is evictable once no reason
    /// (and no other pinned object's closure) keeps it.
    public func release(_ rootCID: String, owner: String) async throws {
        try await broker.unpin(root: rootCID, owner: owner)
    }
}

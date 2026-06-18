import Foundation
import cashew

public final class BrokerStorer: VolumeAwareStorer, @unchecked Sendable {
    private let broker: any VolumeBroker
    // Stack of volume scopes pushed by enterVolume. Each scope accumulates
    // data for one Volume boundary. exitVolume pops and queues it for flush.
    private var scopeStack: [(root: String, buffer: [String: Data])] = []
    // Volumes completed via exitVolume, waiting for async flush.
    private var pendingVolumes: [(root: String, entries: [String: Data])] = []
    // Fallback buffer for callers that use store() without enterVolume.
    private var flatBuffer: [String: Data] = [:]

    public private(set) var storedRoots: [String] = []

    /// Collect all pending serialized volumes without flushing them to storage.
    /// Use this when the caller wants to accumulate volumes from multiple
    /// storers and write them all in one `storeVolumesLocal` call.
    public func collectVolumes(root: String) -> [SerializedVolume] {
        var volumes: [SerializedVolume] = []
        for pending in pendingVolumes {
            volumes.append(SerializedVolume(root: pending.root, entries: pending.entries))
            storedRoots.append(pending.root)
        }
        if !flatBuffer.isEmpty {
            volumes.append(SerializedVolume(root: root, entries: flatBuffer))
            storedRoots.append(root)
        }
        pendingVolumes = []
        flatBuffer = [:]
        return volumes
    }

    public init(broker: any VolumeBroker) {
        self.broker = broker
    }

    // MARK: - VolumeAwareStorer

    public func enterVolume(rootCID: String) throws {
        scopeStack.append((root: rootCID, buffer: [:]))
    }

    public func exitVolume(rootCID: String) throws {
        guard let idx = scopeStack.indices.last, scopeStack[idx].root == rootCID else { return }
        let scope = scopeStack.removeLast()
        pendingVolumes.append((root: scope.root, entries: scope.buffer))
        // Record the reachability edge parent → child: copy the child's own
        // root node up into the parent scope so flush writes a
        // `volume_entries(parent, child)` row. This makes `volume_entries` a
        // true reachability graph over the volume boundaries the caller
        // bracketed, so transitive eviction protects a pinned root's whole
        // bracketed closure in one pin — no per-node enumeration.
        //
        // The graph spans exactly what cashew's `storeRecursively` brackets with
        // enter/exit, i.e. the caller's *owned* children. To keep the graph from
        // climbing backward into unrelated history (e.g. a block's parent/prev
        // state), the CALLER must model such back/shared links as a cashew
        // `Reference` (not a child Header) so they are never bracketed and never
        // edged here. VolumeBroker edges whatever is bracketed; it cannot tell
        // owned from referenced — that distinction lives in the consumer's types.
        //
        // Child bytes dedup against the child's own volume via `INSERT OR IGNORE`.
        if let parentIdx = scopeStack.indices.last,
           let childBytes = scope.buffer[scope.root] {
            scopeStack[parentIdx].buffer[scope.root] = childBytes
        }
    }

    // MARK: - Storer

    public func store(rawCid: String, data: Data) throws {
        if scopeStack.isEmpty {
            flatBuffer[rawCid] = data
        } else {
            scopeStack[scopeStack.count - 1].buffer[rawCid] = data
        }
    }

    public func contains(rawCid: String) -> Bool {
        if flatBuffer[rawCid] != nil { return true }
        return scopeStack.contains { $0.buffer[rawCid] != nil }
    }

    /// Flush all pending volumes to the broker in a single call.
    /// Internal entries are only resolvable after entering their volume root;
    /// they are not stored as independently fetchable volumes.
    /// Collects every volume boundary buffer into one slice and calls
    /// `storeVolumesLocal` once, allowing the broker (DiskBroker) to commit
    /// all writes in a single SQLite transaction instead of one per Volume.
    public func flush(root: String) async throws {
        var allVolumes: [SerializedVolume] = []
        allVolumes.reserveCapacity(pendingVolumes.count + 1)
        for pending in pendingVolumes {
            allVolumes.append(SerializedVolume(root: pending.root, entries: pending.entries))
            storedRoots.append(pending.root)
        }
        if !flatBuffer.isEmpty {
            allVolumes.append(SerializedVolume(root: root, entries: flatBuffer))
            storedRoots.append(root)
        }
        if !allVolumes.isEmpty {
            try await broker.storeVolumesLocal(allVolumes)
        }
        pendingVolumes = []
        flatBuffer = [:]
    }
}

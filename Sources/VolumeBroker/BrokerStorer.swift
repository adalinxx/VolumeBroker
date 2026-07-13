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

    /// Active scopes are exposed read-only for diagnostics and invariant tests.
    public var openVolumeRoots: [String] { scopeStack.map(\.root) }

    /// Collect completed serialized Volumes without flushing them to storage.
    /// Throws if a traversal is still open: returning pending data in that state
    /// would let a caller publish a partially traversed outer Volume.
    public func collectCompleteVolumes(root: String) throws -> [SerializedVolume] {
        guard scopeStack.isEmpty else {
            throw BrokerError.incompleteVolumeScopes(scopeStack.map(\.root))
        }
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

    /// Compatibility wrapper. It fails closed (returns no Volumes) while a scope
    /// remains open; new code should use ``collectCompleteVolumes(root:)`` so the
    /// lifecycle error is explicit.
    public func collectVolumes(root: String) -> [SerializedVolume] {
        (try? collectCompleteVolumes(root: root)) ?? []
    }

    public init(broker: any VolumeBroker) {
        self.broker = broker
    }

    // MARK: - VolumeAwareStorer

    public func enterVolume(rootCID: String) throws {
        scopeStack.append((root: rootCID, buffer: [:]))
    }

    public func exitVolume(rootCID: String) throws {
        let expected = scopeStack.last?.root
        guard expected == rootCID else {
            throw BrokerError.unbalancedVolumeScope(expected: expected, actual: rootCID)
        }
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
        // enter/exit, i.e. the caller's owned children. Back/shared links must be
        // represented as cashew `Reference` values so they are never edged here.
        if let parentIdx = scopeStack.indices.last,
           let childBytes = scope.buffer[scope.root] {
            scopeStack[parentIdx].buffer[scope.root] = childBytes
        }
    }

    /// Discard a failed scope and any still-open nested scopes. Completed child
    /// Volumes remain pending because each completed Volume is independently
    /// atomic; only the incomplete traversal is abandoned.
    public func abortVolume(rootCID: String) {
        guard let index = scopeStack.lastIndex(where: { $0.root == rootCID }) else { return }
        scopeStack.removeSubrange(index...)
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

    /// Flush all completed Volumes in one broker transaction. An open scope is a
    /// hard lifecycle error: no partial outer Volume is persisted.
    public func flush(root: String) async throws {
        guard scopeStack.isEmpty else {
            throw BrokerError.incompleteVolumeScopes(scopeStack.map(\.root))
        }
        let allVolumes = try collectCompleteVolumes(root: root)
        if !allVolumes.isEmpty {
            try await broker.storeVolumesLocal(allVolumes)
        }
    }
}

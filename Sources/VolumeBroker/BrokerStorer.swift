import Foundation
import cashew

public final class BrokerStorer: VolumeAwareStorer, @unchecked Sendable {
    private struct Scope {
        let root: String
        var buffer: [String: Data]
        var nestedVolumeRoots: Set<String>
    }

    private struct PendingVolume {
        let root: String
        let entries: [String: Data]
        let nestedVolumeRoots: Set<String>
    }

    private let broker: any VolumeBroker
    // Stack of Volume scopes pushed by enterVolume. Each scope accumulates the
    // ordinary bytes and explicit nested-boundary edges for one complete Volume.
    private var scopeStack: [Scope] = []
    // Volumes completed via exitVolume, waiting for async flush.
    private var pendingVolumes: [PendingVolume] = []
    // Fallback state for callers that use store() without entering a Volume.
    private var flatBuffer: [String: Data] = [:]
    private var flatNestedVolumeRoots = Set<String>()

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
            volumes.append(SerializedVolume(
                root: pending.root,
                entries: pending.entries,
                nestedVolumeRoots: pending.nestedVolumeRoots
            ))
            storedRoots.append(pending.root)
        }

        if !flatBuffer.isEmpty || !flatNestedVolumeRoots.isEmpty {
            volumes.append(SerializedVolume(
                root: root,
                entries: flatBuffer,
                nestedVolumeRoots: flatNestedVolumeRoots
            ))
            storedRoots.append(root)
        }

        pendingVolumes = []
        flatBuffer = [:]
        flatNestedVolumeRoots = []
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
        scopeStack.append(Scope(
            root: rootCID,
            buffer: [:],
            nestedVolumeRoots: []
        ))
    }

    public func includeNestedVolume(rootCID: String) throws {
        if scopeStack.isEmpty {
            flatNestedVolumeRoots.insert(rootCID)
        } else {
            scopeStack[scopeStack.count - 1].nestedVolumeRoots.insert(rootCID)
        }
    }

    public func exitVolume(rootCID: String) throws {
        let expected = scopeStack.last?.root
        guard expected == rootCID else {
            throw BrokerError.unbalancedVolumeScope(expected: expected, actual: rootCID)
        }

        let scope = scopeStack.removeLast()
        pendingVolumes.append(PendingVolume(
            root: scope.root,
            entries: scope.buffer,
            nestedVolumeRoots: scope.nestedVolumeRoots
        ))
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

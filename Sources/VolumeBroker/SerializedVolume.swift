import Foundation

public enum SerializedVolumeError: Error, Equatable, Sendable {
    case emptyRoot
    case missingRootEntry(String)
}

/// One complete storage/availability unit emitted by a successful cashew Volume
/// traversal. Nested Volumes are represented and stored independently.
public struct SerializedVolume: Sendable {
    public let root: String
    public let entries: [String: Data]

    public init(root: String, entries: [String: Data]) {
        self.root = root
        self.entries = entries
    }

    /// Enforces the generic structural contract a broker can verify without
    /// understanding the application's DAG semantics. Semantic completeness is
    /// guaranteed by the storer that emitted the Volume; the broker verifies that
    /// the declared boundary is non-empty and that its root object is included.
    public func validateStructure() throws {
        guard !root.isEmpty else { throw SerializedVolumeError.emptyRoot }
        guard entries[root] != nil else { throw SerializedVolumeError.missingRootEntry(root) }
    }
}

import CID
import Foundation
import Multihash

public enum SerializedVolumeError: Error, Equatable, Sendable {
    case emptyRoot
    case missingRootEntry(String)
    case invalidCID(String)
    case contentAddressMismatch(String)
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

    /// Enforces only generic storage invariants. The broker does not interpret the
    /// application's DAG or decide which nested Volumes an operation requires.
    ///
    /// A stored Volume must contain its declared root and every `(CID, bytes)` pair
    /// must be self-authenticating. Semantic completeness inside the boundary is
    /// guaranteed by the successful cashew storer lifecycle that emitted it.
    public func validate() throws {
        guard !root.isEmpty else { throw SerializedVolumeError.emptyRoot }
        guard entries[root] != nil else { throw SerializedVolumeError.missingRootEntry(root) }

        for (rawCID, data) in entries {
            let expected: CID
            do {
                expected = try CID(rawCID)
            } catch {
                throw SerializedVolumeError.invalidCID(rawCID)
            }
            guard let algorithm = expected.multihash.algorithm else {
                throw SerializedVolumeError.invalidCID(rawCID)
            }
            let actualMultihash: Multihash
            do {
                actualMultihash = try Multihash(raw: data, hashedWith: algorithm)
            } catch {
                throw SerializedVolumeError.invalidCID(rawCID)
            }
            let actual: CID
            do {
                actual = try CID(version: expected.version, codec: expected.codec, multihash: actualMultihash)
            } catch {
                throw SerializedVolumeError.invalidCID(rawCID)
            }
            guard actual == expected else {
                throw SerializedVolumeError.contentAddressMismatch(rawCID)
            }
        }
    }
}

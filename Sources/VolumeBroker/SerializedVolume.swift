import CID
import Foundation
import Multihash
import cashew

public enum SerializedVolumeError: Error, Equatable, Sendable {
    case emptyRoot
    case missingRootEntry(String)
    case invalidCID(String)
    case contentAddressMismatch(String)
}

public typealias SerializedVolume = cashew.SerializedVolume

public extension SerializedVolume {
    /// Enforces only generic storage invariants. The broker does not interpret the
    /// application's DAG or decide which nested Volumes an operation requires.
    ///
    /// A stored Volume must contain its declared root and every `(CID, bytes)` pair
    /// must be self-authenticating.
    func validate() throws {
        guard !root.isEmpty else { throw SerializedVolumeError.emptyRoot }
        guard entries[root] != nil else { throw SerializedVolumeError.missingRootEntry(root) }

        for (rawCID, data) in entries {
            let expected = try parsedCID(rawCID)
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
                actual = try CID(
                    version: expected.version,
                    codec: expected.codec,
                    multihash: actualMultihash
                )
            } catch {
                throw SerializedVolumeError.invalidCID(rawCID)
            }

            guard actual == expected else {
                throw SerializedVolumeError.contentAddressMismatch(rawCID)
            }
        }
    }

    private func parsedCID(_ rawCID: String) throws -> CID {
        do {
            return try CID(rawCID)
        } catch {
            throw SerializedVolumeError.invalidCID(rawCID)
        }
    }
}

import CID
import Multihash
import XCTest
@testable import VolumeBroker

final class VolumeIntegrityTests: XCTestCase {
    private func cid(for data: Data) throws -> String {
        let multihash = try Multihash(raw: data, hashedWith: .sha2_256)
        return try CID(version: .v1, codec: .dag_cbor, multihash: multihash).toBaseEncodedString
    }

    func testValidCompleteVolumePassesStructuralAndCIDValidation() throws {
        let rootData = Data("root".utf8)
        let childData = Data("child".utf8)
        let root = try cid(for: rootData)
        let child = try cid(for: childData)
        let volume = SerializedVolume(root: root, entries: [root: rootData, child: childData])

        XCTAssertNoThrow(try volume.validate())
    }

    func testVolumeWithoutDeclaredRootIsRejected() throws {
        let rootData = Data("root".utf8)
        let childData = Data("child".utf8)
        let root = try cid(for: rootData)
        let child = try cid(for: childData)
        let volume = SerializedVolume(root: root, entries: [child: childData])

        XCTAssertThrowsError(try volume.validate()) { error in
            XCTAssertEqual(error as? SerializedVolumeError, .missingRootEntry(root))
        }
    }

    func testCIDMismatchIsRejected() throws {
        let expectedBytes = Data("expected".utf8)
        let wrongBytes = Data("wrong".utf8)
        let root = try cid(for: expectedBytes)
        let volume = SerializedVolume(root: root, entries: [root: wrongBytes])

        XCTAssertThrowsError(try volume.validate()) { error in
            XCTAssertEqual(error as? SerializedVolumeError, .contentAddressMismatch(root))
        }
    }

    func testDiskBrokerRejectsMalformedVolumeBeforeWriting() async throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("VolumeIntegrityTests-\(UUID().uuidString)")
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }

        let broker = try DiskBroker(path: directory.appendingPathComponent("volumes.sqlite").path)
        let expectedBytes = Data("expected".utf8)
        let root = try cid(for: expectedBytes)
        let invalid = SerializedVolume(root: root, entries: [root: Data("wrong".utf8)])

        do {
            try await broker.storeVolumeLocal(invalid)
            XCTFail("malformed Volume must not be stored")
        } catch {
            XCTAssertEqual(error as? SerializedVolumeError, .contentAddressMismatch(root))
        }
        XCTAssertFalse(await broker.hasVolume(root: root))
    }

    func testAbortedScopeCannotBeCollectedAsACompleteVolume() throws {
        let storer = BrokerStorer(broker: MemoryBroker())
        try storer.enterVolume(rootCID: "outer")
        try storer.store(rawCid: "outer", data: Data("partial".utf8))

        XCTAssertTrue(storer.collectVolumes(root: "outer").isEmpty)
        XCTAssertEqual(storer.openVolumeRoots, ["outer"])

        storer.abortVolume(rootCID: "outer")
        XCTAssertTrue(storer.openVolumeRoots.isEmpty)
        XCTAssertTrue(try storer.collectCompleteVolumes(root: "outer").isEmpty)
    }

    func testUnbalancedExitFailsClosed() throws {
        let storer = BrokerStorer(broker: MemoryBroker())
        try storer.enterVolume(rootCID: "outer")

        XCTAssertThrowsError(try storer.exitVolume(rootCID: "other")) { error in
            XCTAssertEqual(
                error as? BrokerError,
                .unbalancedVolumeScope(expected: "outer", actual: "other")
            )
        }
        XCTAssertEqual(storer.openVolumeRoots, ["outer"])
    }
}

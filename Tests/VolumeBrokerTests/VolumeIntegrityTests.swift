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
        let present = await broker.hasVolume(root: root)
        XCTAssertFalse(present)
    }

    func testMemoryBrokerRejectsMalformedVolumeBeforeWriting() async throws {
        let broker = MemoryBroker()
        let expectedBytes = Data("expected".utf8)
        let root = try cid(for: expectedBytes)
        let invalid = SerializedVolume(root: root, entries: [root: Data("wrong".utf8)])

        do {
            try await broker.storeVolumeLocal(invalid)
            XCTFail("malformed Volume must not be stored")
        } catch {
            XCTAssertEqual(error as? SerializedVolumeError, .contentAddressMismatch(root))
        }
        let present = await broker.hasVolume(root: root)
        XCTAssertFalse(present)
    }

    func testMemoryBatchValidationIsAtomic() async throws {
        let broker = MemoryBroker()
        let validData = Data("valid".utf8)
        let validRoot = try cid(for: validData)
        let valid = SerializedVolume(root: validRoot, entries: [validRoot: validData])
        let invalidData = Data("invalid".utf8)
        let invalidRoot = try cid(for: invalidData)
        let invalid = SerializedVolume(root: invalidRoot, entries: [invalidRoot: Data("wrong".utf8)])

        do {
            try await broker.storeVolumesLocal([valid, invalid])
            XCTFail("a malformed Volume must reject the whole batch")
        } catch {
            XCTAssertEqual(error as? SerializedVolumeError, .contentAddressMismatch(invalidRoot))
        }
        let validPresent = await broker.hasVolume(root: validRoot)
        XCTAssertFalse(validPresent)
    }

    func testMissingCASDataMakesLegacyVolumeUnavailable() async throws {
        let path = FileManager.default.temporaryDirectory
            .appendingPathComponent("VolumeIntegrityTests-\(UUID().uuidString).sqlite").path
        defer { try? FileManager.default.removeItem(atPath: path) }
        let connection = try SQLiteConnection(path: path)
        let store = CASVolumeStore(connection: connection, negativeCache: NegativeCache())
        let rootData = Data("root".utf8)
        let childData = Data("child".utf8)
        let root = try cid(for: rootData)
        let child = try cid(for: childData)
        try await store.storeVolumeLocal(SerializedVolume(
            root: root,
            entries: [root: rootData, child: childData]
        ))

        try await connection.write {
            try connection.exec("DELETE FROM cas_data WHERE cid='\(child)'")
        }

        let present = await store.hasVolume(root: root)
        let fetched = await store.fetchVolumeLocal(root: root)
        XCTAssertFalse(present)
        XCTAssertNil(fetched)
    }

    func testMissingPublicationMetadataMakesLegacyVolumeUnavailable() async throws {
        let path = FileManager.default.temporaryDirectory
            .appendingPathComponent("VolumeIntegrityTests-\(UUID().uuidString).sqlite").path
        defer { try? FileManager.default.removeItem(atPath: path) }
        let connection = try SQLiteConnection(path: path)
        let store = CASVolumeStore(connection: connection, negativeCache: NegativeCache())
        let rootData = Data("root".utf8)
        let root = try cid(for: rootData)
        try await store.storeVolumeLocal(SerializedVolume(root: root, entries: [root: rootData]))

        try await connection.write {
            try connection.exec("DELETE FROM volume_metadata WHERE root='\(root)'")
        }

        let present = await store.hasVolume(root: root)
        let fetched = await store.fetchVolumeLocal(root: root)
        XCTAssertFalse(present)
        XCTAssertNil(fetched)
    }

}

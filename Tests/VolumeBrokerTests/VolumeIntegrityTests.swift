import CID
import Multihash
import XCTest
@testable import VolumeBroker

final class VolumeIntegrityTests: XCTestCase {
    private func cid(for data: Data, digestLength: Int? = nil) throws -> String {
        let multihash = try Multihash(
            raw: data,
            hashedWith: .sha2_256,
            customByteLength: digestLength
        )
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

    func testValidTruncatedSHAHashUsesDeclaredDigestLength() throws {
        let data = Data("truncated".utf8)
        let root = try cid(for: data, digestLength: 16)

        XCTAssertNoThrow(try SerializedVolume(root: root, entries: [root: data]).validate())
    }

    func testCIDv0RequiresDagPBSHA256WithFullDigest() throws {
        let data = Data("v0".utf8)
        let valid = try Multihash(raw: data, hashedWith: .sha2_256)
            .asString(base: .base58btc)
        let truncated = try Multihash(
            raw: data,
            hashedWith: .sha2_256,
            customByteLength: 15
        ).asString(base: .base58btc)
        let identity = try Multihash(raw: data, hashedWith: .identity)
            .asString(base: .base58btc)

        XCTAssertNoThrow(try SerializedVolume(root: valid, entries: [valid: data]).validate())
        let prefixedAlias = "z\(valid)"
        XCTAssertThrowsError(
            try SerializedVolume(root: prefixedAlias, entries: [prefixedAlias: data]).validate()
        )
        XCTAssertThrowsError(try SerializedVolume(root: truncated, entries: [truncated: data]).validate())
        XCTAssertThrowsError(try SerializedVolume(root: identity, entries: [identity: data]).validate())
    }

    func testTruncatedSHAHashStillRejectsWrongContent() throws {
        let expected = Data("expected".utf8)
        let root = try cid(for: expected, digestLength: 16)

        XCTAssertThrowsError(
            try SerializedVolume(root: root, entries: [root: Data("wrong".utf8)]).validate()
        ) { error in
            XCTAssertEqual(error as? SerializedVolumeError, .contentAddressMismatch(root))
        }
    }

    func testIdentityHashRequiresExactPayloadRatherThanPrefix() throws {
        let digest = Data("identity".utf8)
        let multihash = try Multihash(raw: digest, hashedWith: .identity)
        let root = try CID(version: .v1, codec: .dag_cbor, multihash: multihash).toBaseEncodedString

        XCTAssertNoThrow(try SerializedVolume(root: root, entries: [root: digest]).validate())
        XCTAssertThrowsError(
            try SerializedVolume(root: root, entries: [root: digest + Data("-suffix".utf8)]).validate()
        ) { error in
            XCTAssertEqual(error as? SerializedVolumeError, .contentAddressMismatch(root))
        }
    }

    func testNonidentityCIDAliasIsRejected() throws {
        let data = Data("nonidentity-alias".utf8)
        let multihash = try Multihash(raw: data, hashedWith: .sha2_256)
        let cid = try CID(version: .v1, codec: .dag_cbor, multihash: multihash)
        let alias = cid.toBaseEncodedString.uppercased()

        XCTAssertNotEqual(alias, cid.toBaseEncodedString)
        XCTAssertThrowsError(
            try SerializedVolume(root: alias, entries: [alias: data]).validate()
        ) { error in
            XCTAssertEqual(error as? SerializedVolumeError, .invalidCID(alias))
        }
    }

    func testIdentityCIDAliasIsRejected() throws {
        let data = Data("identity-alias".utf8)
        let multihash = try Multihash(raw: data, hashedWith: .identity)
        let cid = try CID(version: .v1, codec: .dag_cbor, multihash: multihash)
        let alias = cid.toBaseEncodedString.uppercased()

        XCTAssertNotEqual(alias, cid.toBaseEncodedString)
        XCTAssertThrowsError(
            try SerializedVolume(root: alias, entries: [alias: data]).validate()
        ) { error in
            XCTAssertEqual(error as? SerializedVolumeError, .invalidCID(alias))
        }
    }

    func testZeroLengthDigestFailsClosed() throws {
        let data = Data("zero".utf8)
        let root = try cid(for: data, digestLength: 0)

        XCTAssertThrowsError(try SerializedVolume(root: root, entries: [root: data]).validate()) { error in
            XCTAssertEqual(error as? SerializedVolumeError, .invalidCID(root))
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

    func testMalformedBatchValidationIsAtomicInMemoryAndDisk() async throws {
        let path = FileManager.default.temporaryDirectory
            .appendingPathComponent("VolumeIntegrityTests-\(UUID().uuidString).sqlite").path
        defer { try? FileManager.default.removeItem(atPath: path) }
        let brokers: [any VolumeBroker] = [MemoryBroker(), try DiskBroker(path: path)]
        let validData = Data("valid".utf8)
        let validRoot = try cid(for: validData)
        let valid = SerializedVolume(root: validRoot, entries: [validRoot: validData])
        let invalidData = Data("invalid".utf8)
        let invalidRoot = try cid(for: invalidData)
        let invalid = SerializedVolume(root: invalidRoot, entries: [invalidRoot: Data("wrong".utf8)])

        for broker in brokers {
            do {
                try await broker.storeVolumesLocal([valid, invalid])
                XCTFail("a malformed Volume must reject the whole batch")
            } catch {
                XCTAssertEqual(error as? SerializedVolumeError, .contentAddressMismatch(invalidRoot))
            }
            let validPresent = await broker.hasVolume(root: validRoot)
            XCTAssertFalse(validPresent)
        }
    }

    func testBrokersRejectConflictingVolumeMembershipWithoutMutation() async throws {
        let path = FileManager.default.temporaryDirectory
            .appendingPathComponent("VolumeIntegrityTests-\(UUID().uuidString).sqlite").path
        defer { try? FileManager.default.removeItem(atPath: path) }
        let brokers: [any VolumeBroker] = [MemoryBroker(), try DiskBroker(path: path)]
        let rootData = Data("root".utf8)
        let firstChildData = Data("first".utf8)
        let secondChildData = Data("second".utf8)
        let root = try cid(for: rootData)
        let firstChild = try cid(for: firstChildData)
        let secondChild = try cid(for: secondChildData)
        let first = SerializedVolume(root: root, entries: [root: rootData, firstChild: firstChildData])
        let conflicting = SerializedVolume(root: root, entries: [root: rootData, secondChild: secondChildData])

        for broker in brokers {
            try await broker.storeVolumeLocal(first)
            do {
                try await broker.storeVolumeLocal(conflicting)
                XCTFail("a published Volume membership must be immutable")
            } catch {
                XCTAssertEqual(error as? BrokerError, .conflictingVolume(root))
            }
            let fetched = await broker.fetchVolumeLocal(root: root)
            XCTAssertEqual(Set(fetched?.entries.keys.map { $0 } ?? []), [root, firstChild])
        }
    }

    func testConflictingVolumeMembershipAbortsWholeBatch() async throws {
        let path = FileManager.default.temporaryDirectory
            .appendingPathComponent("VolumeIntegrityTests-\(UUID().uuidString).sqlite").path
        defer { try? FileManager.default.removeItem(atPath: path) }
        let brokers: [any VolumeBroker] = [MemoryBroker(), try DiskBroker(path: path)]
        let rootData = Data("root".utf8)
        let childData = Data("child".utf8)
        let root = try cid(for: rootData)
        let child = try cid(for: childData)
        let first = SerializedVolume(root: root, entries: [root: rootData])
        let conflicting = SerializedVolume(root: root, entries: [root: rootData, child: childData])

        for broker in brokers {
            do {
                try await broker.storeVolumesLocal([first, conflicting])
                XCTFail("conflicting batch must fail")
            } catch {
                XCTAssertEqual(error as? BrokerError, .conflictingVolume(root))
            }
            let present = await broker.hasVolume(root: root)
            XCTAssertFalse(present)
        }
    }

    func testExistingConflictRollsBackEarlierNewVolumeInBatch() async throws {
        let path = FileManager.default.temporaryDirectory
            .appendingPathComponent("VolumeIntegrityTests-\(UUID().uuidString).sqlite").path
        defer { try? FileManager.default.removeItem(atPath: path) }
        let brokers: [any VolumeBroker] = [MemoryBroker(), try DiskBroker(path: path)]
        let existingData = Data("existing".utf8)
        let addedData = Data("added".utf8)
        let newData = Data("new".utf8)
        let existingRoot = try cid(for: existingData)
        let added = try cid(for: addedData)
        let newRoot = try cid(for: newData)
        let existing = SerializedVolume(root: existingRoot, entries: [existingRoot: existingData])
        let conflicting = SerializedVolume(
            root: existingRoot,
            entries: [existingRoot: existingData, added: addedData]
        )
        let newVolume = SerializedVolume(root: newRoot, entries: [newRoot: newData])

        for broker in brokers {
            try await broker.storeVolumeLocal(existing)
            do {
                try await broker.storeVolumesLocal([newVolume, conflicting])
                XCTFail("existing conflict must roll back the whole batch")
            } catch {
                XCTAssertEqual(error as? BrokerError, .conflictingVolume(existingRoot))
            }
            let newPresent = await broker.hasVolume(root: newRoot)
            let existingEntries = await broker.fetchVolumeLocal(root: existingRoot)?.entries
            XCTAssertFalse(newPresent)
            XCTAssertEqual(existingEntries, existing.entries)
        }
    }

    func testMissingCASDataMakesManifestUnavailable() async throws {
        let path = FileManager.default.temporaryDirectory
            .appendingPathComponent("VolumeIntegrityTests-\(UUID().uuidString).sqlite").path
        defer { try? FileManager.default.removeItem(atPath: path) }
        let connection = try SQLiteConnection(path: path)
        let store = CASVolumeStore(connection: connection)
        let rootData = Data("root".utf8)
        let childData = Data("child".utf8)
        let root = try cid(for: rootData)
        let child = try cid(for: childData)
        try await store.storeVolumeLocal(SerializedVolume(
            root: root,
            entries: [root: rootData, child: childData]
        ))

        try await connection.write {
            try connection.exec("PRAGMA foreign_keys=OFF")
            do {
                try connection.exec("DELETE FROM cas_data WHERE cid='\(child)'")
                try connection.exec("PRAGMA foreign_keys=ON")
            } catch {
                try? connection.exec("PRAGMA foreign_keys=ON")
                throw error
            }
        }

        let present = await store.hasVolume(root: root)
        let fetched = await store.fetchVolumeLocal(root: root)
        XCTAssertFalse(present)
        XCTAssertNil(fetched)
        let rootBytes = await store.fetchDataLocal(cid: root)
        let childBytes = await store.fetchDataLocal(cid: child)
        XCTAssertNil(rootBytes)
        XCTAssertNil(childBytes)
    }

    func testMissingManifestMembershipFailsAllReadsClosed() async throws {
        let path = FileManager.default.temporaryDirectory
            .appendingPathComponent("VolumeIntegrityTests-\(UUID().uuidString).sqlite").path
        defer { try? FileManager.default.removeItem(atPath: path) }
        let connection = try SQLiteConnection(path: path)
        let store = CASVolumeStore(connection: connection)
        let rootData = Data("root".utf8)
        let childData = Data("child".utf8)
        let root = try cid(for: rootData)
        let child = try cid(for: childData)
        try await store.storeVolumeLocal(SerializedVolume(
            root: root,
            entries: [root: rootData, child: childData]
        ))

        try await connection.write {
            try connection.exec("DELETE FROM volume_entries WHERE root='\(root)' AND cid='\(child)'")
        }

        let present = await store.hasVolume(root: root)
        let fetched = await store.fetchVolumeLocal(root: root)
        XCTAssertFalse(present)
        XCTAssertNil(fetched)
        let rootBytes = await store.fetchDataLocal(cid: root)
        let childBytes = await store.fetchDataLocal(cid: child)
        XCTAssertNil(rootBytes)
        XCTAssertNil(childBytes)
    }

    func testCorruptedCASBytesFailAllReadsClosed() async throws {
        let path = FileManager.default.temporaryDirectory
            .appendingPathComponent("VolumeIntegrityTests-\(UUID().uuidString).sqlite").path
        defer { try? FileManager.default.removeItem(atPath: path) }
        let connection = try SQLiteConnection(path: path)
        let store = CASVolumeStore(connection: connection)
        let rootData = Data("root".utf8)
        let childData = Data("child".utf8)
        let root = try cid(for: rootData)
        let child = try cid(for: childData)
        try await store.storeVolumeLocal(SerializedVolume(
            root: root,
            entries: [root: rootData, child: childData]
        ))

        try await connection.write {
            try connection.exec("UPDATE cas_data SET data=X'00' WHERE cid='\(child)'")
        }

        let present = await store.hasVolume(root: root)
        let volume = await store.fetchVolumeLocal(root: root)
        let rootBytes = await store.fetchDataLocal(cid: root)
        let childBytes = await store.fetchDataLocal(cid: child)
        XCTAssertFalse(present)
        XCTAssertNil(volume)
        XCTAssertNil(rootBytes)
        XCTAssertNil(childBytes)
    }

    func testOwnedCIDIsReadableButLooseCASRowIsInvisible() async throws {
        let path = FileManager.default.temporaryDirectory
            .appendingPathComponent("VolumeIntegrityTests-\(UUID().uuidString).sqlite").path
        defer { try? FileManager.default.removeItem(atPath: path) }
        let connection = try SQLiteConnection(path: path)
        let store = CASVolumeStore(connection: connection)
        let rootData = Data("root".utf8)
        let childData = Data("child".utf8)
        let orphanData = Data("orphan".utf8)
        let root = try cid(for: rootData)
        let child = try cid(for: childData)
        let orphan = try cid(for: orphanData)
        try await store.storeVolumeLocal(SerializedVolume(
            root: root,
            entries: [root: rootData, child: childData]
        ))
        let orphanHex = orphanData.map { String(format: "%02x", $0) }.joined()
        try await connection.write {
            try connection.exec("INSERT INTO cas_data(cid, data) VALUES('\(orphan)', X'\(orphanHex)')")
        }

        let ownedBytes = await store.fetchDataLocal(cid: child)
        let orphanBytes = await store.fetchDataLocal(cid: orphan)
        XCTAssertEqual(ownedBytes, childData)
        XCTAssertNil(orphanBytes)
    }
}

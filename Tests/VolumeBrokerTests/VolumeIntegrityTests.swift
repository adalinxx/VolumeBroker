import CID
import Foundation
import Multihash
import XCTest
#if canImport(SQLite3)
import SQLite3
#else
import VolumeBrokerSQLite
#endif
#if canImport(Darwin)
import Darwin
#endif
@testable import VolumeBroker

final class VolumeIntegrityTests: XCTestCase {
    private enum Corruption: Sendable {
        case missingCASData
        case missingMembership
        case corruptBytes
    }

#if os(macOS)
    private enum CrashPhase: String, CaseIterable, Sendable {
        case metadata = "volume_metadata"
        case content = "cas_data"
        case membership = "volume_entries"
    }

    private struct ChildResult {
        let status: Int32
        let output: String
    }
#endif

    private func cid(for data: Data, digestLength: Int? = nil) throws -> String {
        let multihash = try Multihash(
            raw: data,
            hashedWith: .sha2_256,
            customByteLength: digestLength
        )
        return try CID(version: .v1, codec: .dag_cbor, multihash: multihash).toBaseEncodedString
    }

    private static func scalarInt(_ db: OpaquePointer, _ sql: String) throws -> Int {
        var statement: OpaquePointer?
        defer { sqlite3_finalize(statement) }
        guard sqlite3_prepare_v2(db, sql, -1, &statement, nil) == SQLITE_OK,
              let statement,
              sqlite3_step(statement) == SQLITE_ROW else {
            throw BrokerError.sqlFailed(String(cString: sqlite3_errmsg(db)))
        }
        return Int(sqlite3_column_int64(statement, 0))
    }

    private static func scalarText(_ db: OpaquePointer, _ sql: String) throws -> String {
        var statement: OpaquePointer?
        defer { sqlite3_finalize(statement) }
        guard sqlite3_prepare_v2(db, sql, -1, &statement, nil) == SQLITE_OK,
              let statement,
              sqlite3_step(statement) == SQLITE_ROW,
              let value = sqlite3_column_text(statement, 0) else {
            throw BrokerError.sqlFailed(String(cString: sqlite3_errmsg(db)))
        }
        return String(cString: value)
    }

    private func assertHealthy(_ connection: SQLiteConnection) async throws {
        let result = try await connection.read {
            (
                try Self.scalarText(connection.readDb, "PRAGMA integrity_check"),
                try Self.scalarInt(connection.readDb, "SELECT COUNT(*) FROM pragma_foreign_key_check")
            )
        }
        XCTAssertEqual(result.0, "ok")
        XCTAssertEqual(result.1, 0)
    }

#if canImport(Darwin)
    private func sanitizerIsActive() -> Bool {
        guard let handle = dlopen(nil, RTLD_NOW) else { return false }
        defer { dlclose(handle) }
        return dlsym(handle, "__asan_init") != nil || dlsym(handle, "__tsan_init") != nil
    }
#endif

#if os(macOS)
    private func runChild(
        testName: String,
        environment: [String: String],
        timeout: Duration = .seconds(20)
    ) async throws -> ChildResult {
        let outputURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("VolumeBrokerChild-\(UUID().uuidString).log")
        try Data().write(to: outputURL)
        let outputHandle = try FileHandle(forWritingTo: outputURL)
        let process = Process()
        process.executableURL = URL(fileURLWithPath: "/usr/bin/xcrun")
        process.arguments = [
            "xctest",
            "-XCTest",
            "VolumeBrokerTests.VolumeIntegrityTests/\(testName)",
            Bundle(for: VolumeIntegrityTests.self).bundlePath,
        ]
        process.environment = ProcessInfo.processInfo.environment.merging(environment) { _, child in child }
        process.standardOutput = outputHandle
        process.standardError = outputHandle

        defer {
            if process.isRunning {
                Darwin.kill(process.processIdentifier, SIGKILL)
            }
            try? outputHandle.close()
            try? FileManager.default.removeItem(at: outputURL)
        }

        try process.run()
        let clock = ContinuousClock()
        let deadline = clock.now.advanced(by: timeout)
        while process.isRunning && clock.now < deadline {
            try await Task.sleep(for: .milliseconds(20))
        }
        if process.isRunning {
            Darwin.kill(process.processIdentifier, SIGKILL)
            let killDeadline = clock.now.advanced(by: .seconds(2))
            while process.isRunning && clock.now < killDeadline {
                try await Task.sleep(for: .milliseconds(20))
            }
            try outputHandle.close()
            let output = String(decoding: try Data(contentsOf: outputURL), as: UTF8.self)
            throw NSError(
                domain: "VolumeIntegrityTests",
                code: 1,
                userInfo: [NSLocalizedDescriptionKey: "child test timed out: \(output)"]
            )
        }

        try outputHandle.close()
        return ChildResult(
            status: process.terminationStatus,
            output: String(decoding: try Data(contentsOf: outputURL), as: UTF8.self)
        )
    }
#endif

    private func withCorruptedStore(
        _ corruption: Corruption,
        operation: (SQLiteConnection, CASVolumeStore, String, String) async throws -> Void
    ) async throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("VolumeIntegrityTests-\(UUID().uuidString)")
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }

        let connection = try SQLiteConnection(path: directory.appendingPathComponent("volumes.sqlite").path)
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
            switch corruption {
            case .missingCASData:
                try connection.exec("PRAGMA foreign_keys=OFF")
                do {
                    try connection.exec("DELETE FROM cas_data WHERE cid='\(child)'")
                    try connection.exec("PRAGMA foreign_keys=ON")
                } catch {
                    try? connection.exec("PRAGMA foreign_keys=ON")
                    throw error
                }
            case .missingMembership:
                try connection.exec("DELETE FROM volume_entries WHERE root='\(root)' AND cid='\(child)'")
            case .corruptBytes:
                try connection.exec("UPDATE cas_data SET data=X'00' WHERE cid='\(child)'")
            }
        }

        try await operation(connection, store, root, child)
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

    func testMemoryAndDiskShareTheVolumeLifecycleContract() async throws {
        let path = FileManager.default.temporaryDirectory
            .appendingPathComponent("VolumeIntegrityTests-\(UUID().uuidString).sqlite").path
        defer { try? FileManager.default.removeItem(atPath: path) }
        let brokers: [any RetainedRootBroker] = [
            MemoryBroker(evictUnpinnedGrace: .zero),
            try DiskBroker(path: path, evictUnpinnedGraceSeconds: 0),
        ]
        let rootData = Data("lifecycle-root".utf8)
        let childData = Data("lifecycle-child".utf8)
        let root = try cid(for: rootData)
        let child = try cid(for: childData)
        let volume = SerializedVolume(root: root, entries: [root: rootData, child: childData])

        for broker in brokers {
            let near = MemoryBroker()
            let far = MemoryBroker()
            broker.near = near
            broker.far = far

            try await broker.storeVolumeLocal(volume)
            let present = await broker.hasVolume(root: root)
            let fetched = await broker.fetchVolumeLocal(root: root)
            let childBytes = await broker.fetchDataLocal(cid: child)
            let nearPresent = await near.hasVolume(root: root)
            let farPresent = await far.hasVolume(root: root)
            XCTAssertTrue(present)
            XCTAssertEqual(fetched?.entries, volume.entries)
            XCTAssertEqual(childBytes, childData)
            XCTAssertFalse(nearPresent)
            XCTAssertFalse(farPresent)

            try await broker.pin(root: root, owner: "lifecycle", count: 2)
            let evictedWhilePinned = try await broker.evictUnpinned()
            XCTAssertEqual(evictedWhilePinned, 0)
            try await broker.unpin(root: root, owner: "lifecycle", count: 1)
            let owners = await broker.owners(root: root)
            XCTAssertEqual(owners, ["lifecycle"])

            try await broker.advanceRetainedRoots(scope: "lifecycle", roots: [root])
            try await broker.unpin(root: root, owner: "lifecycle", count: 1)
            let evictedWhileRetained = try await broker.evictUnpinned()
            XCTAssertEqual(evictedWhileRetained, 0)
            try await broker.advanceRetainedRoots(scope: "lifecycle", roots: [])
            let evictedAfterRelease = try await broker.evictUnpinned()
            let presentAfterEviction = await broker.hasVolume(root: root)
            let childAfterEviction = await broker.fetchDataLocal(cid: child)
            XCTAssertEqual(evictedAfterRelease, 1)
            XCTAssertFalse(presentAfterEviction)
            XCTAssertNil(childAfterEviction)
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

    func testConflictingContentAbortsWholeBatchInMemoryAndCASVolumeStore() async throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("VolumeIntegrityTests-\(UUID().uuidString)")
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let memory = MemoryBroker()
        let cas = CASVolumeStore(connection: try SQLiteConnection(
            path: directory.appendingPathComponent("volumes.sqlite").path
        ))
        let stores: [(
            storeVolume: (SerializedVolume) async throws -> Void,
            storeVolumes: ([SerializedVolume]) async throws -> Void,
            fetchVolume: (String) async -> SerializedVolume?,
            fetchData: (String) async -> Data?
        )] = [
            (
                { try await memory.storeVolumeLocal($0) },
                { try await memory.storeVolumesLocal($0) },
                { await memory.fetchVolumeLocal(root: $0) },
                { await memory.fetchDataLocal(cid: $0) }
            ),
            (
                { try await cas.storeVolumeLocal($0) },
                { try await cas.storeVolumesLocal($0) },
                { await cas.fetchVolumeLocal(root: $0) },
                { await cas.fetchDataLocal(cid: $0) }
            ),
        ]
        let firstBytes = Data("collision-1".utf8)
        let secondBytes = Data("collision-5".utf8)
        let collision = try cid(for: firstBytes, digestLength: 1)
        XCTAssertEqual(collision, try cid(for: secondBytes, digestLength: 1))

        let existingRootData = Data("existing-root".utf8)
        let conflictingRootData = Data("conflicting-root".utf8)
        let unrelatedRootData = Data("unrelated-root".utf8)
        let existingRoot = try cid(for: existingRootData)
        let conflictingRoot = try cid(for: conflictingRootData)
        let unrelatedRoot = try cid(for: unrelatedRootData)
        let existing = SerializedVolume(
            root: existingRoot,
            entries: [existingRoot: existingRootData, collision: firstBytes]
        )
        let conflicting = SerializedVolume(
            root: conflictingRoot,
            entries: [conflictingRoot: conflictingRootData, collision: secondBytes]
        )
        let unrelated = SerializedVolume(
            root: unrelatedRoot,
            entries: [unrelatedRoot: unrelatedRootData]
        )

        for store in stores {
            try await store.storeVolume(existing)
            do {
                try await store.storeVolumes([unrelated, conflicting])
                XCTFail("an existing CID must never acquire different bytes")
            } catch {
                XCTAssertEqual(error as? BrokerError, .conflictingContent(collision))
            }

            let stored = await store.fetchVolume(existingRoot)
            let conflictingStored = await store.fetchVolume(conflictingRoot)
            let unrelatedStored = await store.fetchVolume(unrelatedRoot)
            let collisionBytes = await store.fetchData(collision)
            XCTAssertEqual(stored?.entries, existing.entries)
            XCTAssertNil(conflictingStored)
            XCTAssertNil(unrelatedStored)
            XCTAssertEqual(collisionBytes, firstBytes)
        }
    }

    func testSQLiteBatchFailureAfterWritesRollsBackImmediatelyAndAfterReopen() async throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("VolumeIntegrityTests-\(UUID().uuidString)")
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let path = directory.appendingPathComponent("volumes.sqlite").path
        let firstData = Data("rollback-first".utf8)
        let secondData = Data("rollback-second".utf8)
        let firstRoot = try cid(for: firstData)
        let secondRoot = try cid(for: secondData)

        do {
            let connection = try SQLiteConnection(path: path)
            let store = CASVolumeStore(connection: connection)
            try await connection.write {
                try connection.exec("""
                    CREATE TEMP TRIGGER fail_volume_entry
                    BEFORE INSERT ON volume_entries
                    WHEN NEW.root = '\(secondRoot)'
                    BEGIN SELECT RAISE(ABORT, 'injected write failure'); END
                    """)
            }

            do {
                try await store.storeVolumesLocal([
                    SerializedVolume(root: firstRoot, entries: [firstRoot: firstData]),
                    SerializedVolume(root: secondRoot, entries: [secondRoot: secondData]),
                ])
                XCTFail("the injected volume-entry failure must abort the transaction")
            } catch {
                guard let brokerError = error as? BrokerError,
                      case .sqlFailed = brokerError else {
                    XCTFail("unexpected error: \(error)")
                    return
                }
            }

            let rowCounts = try await connection.read {
                try ["volume_metadata", "cas_data", "volume_entries"].map {
                    try Self.scalarInt(connection.readDb, "SELECT COUNT(*) FROM \($0)")
                }
            }
            XCTAssertEqual(rowCounts, [0, 0, 0])
            try await assertHealthy(connection)
        }

        let reopened = try SQLiteConnection(path: path)
        let reopenedCounts = try await reopened.read {
            try ["volume_metadata", "cas_data", "volume_entries"].map {
                try Self.scalarInt(reopened.readDb, "SELECT COUNT(*) FROM \($0)")
            }
        }
        XCTAssertEqual(reopenedCounts, [0, 0, 0])
        let reopenedStore = CASVolumeStore(connection: reopened)
        let reopenedFirst = await reopenedStore.fetchVolumeLocal(root: firstRoot)
        let reopenedSecond = await reopenedStore.fetchVolumeLocal(root: secondRoot)
        XCTAssertNil(reopenedFirst)
        XCTAssertNil(reopenedSecond)
        try await assertHealthy(reopened)
    }

    func testInterruptedStoreRecoversAtomicallyAtEveryWritePhase() async throws {
#if os(macOS)
        if sanitizerIsActive() {
            throw XCTSkip("instrumented xctest bundles cannot be safely re-launched through xcrun")
        }
        let pathKey = "VOLUME_BROKER_CRASH_CHILD_PATH"
        let phaseKey = "VOLUME_BROKER_CRASH_PHASE"
        let data = Data("interrupted-store".utf8)
        let root = try cid(for: data)

        if let path = ProcessInfo.processInfo.environment[pathKey],
           let phaseName = ProcessInfo.processInfo.environment[phaseKey],
           let phase = CrashPhase(rawValue: phaseName) {
            let connection = try SQLiteConnection(path: path)
            let store = CASVolumeStore(connection: connection)
            try await connection.write {
                guard sqlite3_create_function_v2(
                    connection.db,
                    "abrupt_exit",
                    0,
                    SQLITE_UTF8,
                    nil,
                    { _, _, _ in Darwin._exit(86) },
                    nil,
                    nil,
                    nil
                ) == SQLITE_OK else {
                    throw BrokerError.sqlFailed(String(cString: sqlite3_errmsg(connection.db)))
                }
                try connection.exec("""
                    CREATE TEMP TRIGGER crash_store_phase
                    AFTER INSERT ON \(phase.rawValue)
                    BEGIN SELECT abrupt_exit(); END
                    """)
            }
            try await store.storeVolumeLocal(SerializedVolume(root: root, entries: [root: data]))
            XCTFail("the crash trigger did not terminate the child")
            return
        }

        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("VolumeIntegrityTests-\(UUID().uuidString)")
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }

        for phase in CrashPhase.allCases {
            let path = directory.appendingPathComponent("\(phase.rawValue).sqlite").path
            let child = try await runChild(
                testName: "testInterruptedStoreRecoversAtomicallyAtEveryWritePhase",
                environment: [pathKey: path, phaseKey: phase.rawValue]
            )
            XCTAssertEqual(child.status, 86, "\(phase.rawValue): \(child.output)")

            let reopened = try SQLiteConnection(path: path)
            let counts = try await reopened.read {
                try ["volume_metadata", "cas_data", "volume_entries"].map {
                    try Self.scalarInt(reopened.readDb, "SELECT COUNT(*) FROM \($0)")
                }
            }
            let oldState = [0, 0, 0]
            XCTAssertEqual(counts, oldState, "\(phase.rawValue): \(counts)")
            let volume = await CASVolumeStore(connection: reopened).fetchVolumeLocal(root: root)
            XCTAssertNil(volume)
            try await assertHealthy(reopened)
        }
#else
        throw XCTSkip("self-reexecuting an xctest bundle is currently macOS-only")
#endif
    }

    func testCommittedTransactionSurvivesAbruptProcessExit() async throws {
#if os(macOS)
        if sanitizerIsActive() {
            throw XCTSkip("instrumented xctest bundles cannot be safely re-launched through xcrun")
        }
        let childPathKey = "VOLUME_BROKER_CRASH_CHILD_PATH"
        let data = Data("abrupt-recovery".utf8)
        let root = try cid(for: data)
        if let childPath = ProcessInfo.processInfo.environment[childPathKey] {
            let broker = try DiskBroker(path: childPath)
            try await broker.storeVolumeLocal(SerializedVolume(root: root, entries: [root: data]))
            Darwin._exit(0)
        }

        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("VolumeIntegrityTests-\(UUID().uuidString)")
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let path = directory.appendingPathComponent("volumes.sqlite").path
        let child = try await runChild(
            testName: "testCommittedTransactionSurvivesAbruptProcessExit",
            environment: [childPathKey: path]
        )
        XCTAssertEqual(child.status, 0, child.output)

        let reopened = try SQLiteConnection(path: path)
        let reopenedVolume = await CASVolumeStore(connection: reopened).fetchVolumeLocal(root: root)
        XCTAssertEqual(
            reopenedVolume?.entries,
            [root: data]
        )
        try await assertHealthy(reopened)
#else
        throw XCTSkip("self-reexecuting an xctest bundle is currently macOS-only")
#endif
    }

    func testMissingCASDataMakesManifestUnavailable() async throws {
        try await withCorruptedStore(.missingCASData) { _, store, root, _ in
            let present = await store.hasVolume(root: root)
            XCTAssertFalse(present)
        }
        try await withCorruptedStore(.missingCASData) { _, store, root, _ in
            let volume = await store.fetchVolumeLocal(root: root)
            XCTAssertNil(volume)
        }
        try await withCorruptedStore(.missingCASData) { _, store, _, child in
            let data = await store.fetchDataLocal(cid: child)
            XCTAssertNil(data)
        }
    }

    func testMissingManifestMembershipFailsAllReadsClosed() async throws {
        try await withCorruptedStore(.missingMembership) { _, store, root, _ in
            let present = await store.hasVolume(root: root)
            XCTAssertFalse(present)
        }
        try await withCorruptedStore(.missingMembership) { _, store, root, _ in
            let volume = await store.fetchVolumeLocal(root: root)
            XCTAssertNil(volume)
        }
        try await withCorruptedStore(.missingMembership) { _, store, _, child in
            let data = await store.fetchDataLocal(cid: child)
            XCTAssertNil(data)
        }
    }

    func testCorruptedCASBytesAreQuarantinedOnlyAfterReadProof() async throws {
        try await withCorruptedStore(.corruptBytes) { _, store, root, _ in
            let present = await store.hasVolume(root: root)
            XCTAssertTrue(present, "presence is structural and does not hash every entry")
        }
        try await withCorruptedStore(.corruptBytes) { _, store, root, _ in
            let volume = await store.fetchVolumeLocal(root: root)
            XCTAssertNil(volume)
            let present = await store.hasVolume(root: root)
            XCTAssertFalse(present)
        }
        try await withCorruptedStore(.corruptBytes) { _, store, root, child in
            let data = await store.fetchDataLocal(cid: child)
            XCTAssertNil(data)
            let present = await store.hasVolume(root: root)
            XCTAssertFalse(present)
        }
    }

    func testPointReadIgnoresCorruptSiblingUntilSiblingIsRequested() async throws {
        try await withCorruptedStore(.corruptBytes) { connection, store, root, child in
            let validData = await store.fetchDataLocal(cid: root)
            let presentBeforeProof = await store.hasVolume(root: root)
            XCTAssertEqual(validData, Data("root".utf8))
            XCTAssertTrue(presentBeforeProof)

            let corruptData = await store.fetchDataLocal(cid: child)
            let presentAfterProof = await store.hasVolume(root: root)
            XCTAssertNil(corruptData)
            XCTAssertFalse(presentAfterProof)

            let preserved = try await connection.read {
                (
                    try Self.scalarInt(connection.readDb, "SELECT COUNT(*) FROM volume_metadata"),
                    try Self.scalarInt(connection.readDb, "SELECT COUNT(*) FROM volume_entries"),
                    try Self.scalarInt(connection.readDb, "SELECT COUNT(*) FROM cas_data"),
                    try Self.scalarInt(
                        connection.readDb,
                        "SELECT quarantined FROM volume_metadata WHERE root='\(root)'"
                    )
                )
            }
            XCTAssertEqual(preserved.0, 1)
            XCTAssertEqual(preserved.1, 2)
            XCTAssertEqual(preserved.2, 2)
            XCTAssertEqual(preserved.3, 1)
        }
    }

    func testWholeVolumeProofQuarantinesEveryOwnerOfCorruptContent() async throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("VolumeIntegrityTests-\(UUID().uuidString)")
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let connection = try SQLiteConnection(path: directory.appendingPathComponent("volumes.sqlite").path)
        let store = CASVolumeStore(connection: connection)
        let firstData = Data("first".utf8)
        let secondData = Data("second".utf8)
        let sharedData = Data("shared".utf8)
        let first = try cid(for: firstData)
        let second = try cid(for: secondData)
        let shared = try cid(for: sharedData)
        try await store.storeVolumesLocal([
            SerializedVolume(root: first, entries: [first: firstData, shared: sharedData]),
            SerializedVolume(root: second, entries: [second: secondData, shared: sharedData]),
        ])
        try await connection.write {
            try connection.exec("UPDATE cas_data SET data=X'00' WHERE cid='\(shared)'")
        }

        let fetched = await store.fetchVolumeLocal(root: first)
        let firstPresent = await store.hasVolume(root: first)
        let secondPresent = await store.hasVolume(root: second)
        XCTAssertNil(fetched)
        XCTAssertFalse(firstPresent)
        XCTAssertFalse(secondPresent)
        let preservedRoots = try await connection.read {
            try Self.scalarInt(connection.readDb, "SELECT COUNT(*) FROM volume_metadata")
        }
        XCTAssertEqual(preservedRoots, 2)
    }

    func testPinAndRetentionAdmissionUseStructuralCompleteness() async throws {
        try await withCorruptedStore(.corruptBytes) { connection, _, root, _ in
            let pins = PinIndex(connection: connection)
            let retained = RetainedRootIndex(connection: connection)

            try await pins.pin(root: root, owner: "test", count: 1, ttl: nil)
            try await retained.advanceRetainedRoots(scope: "test", roots: [root])

            let owners = await pins.owners(root: root)
            let roots = try await retained.retainedRoots(scope: "test")
            XCTAssertEqual(owners, ["test"])
            XCTAssertEqual(roots, [root])
        }
    }

    func testExactRestorageClearsQuarantineWhenContentStillMatches() async throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("VolumeIntegrityTests-\(UUID().uuidString)")
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let connection = try SQLiteConnection(path: directory.appendingPathComponent("volumes.sqlite").path)
        let store = CASVolumeStore(connection: connection)
        let data = Data("republish".utf8)
        let root = try cid(for: data)
        let volume = SerializedVolume(root: root, entries: [root: data])
        try await store.storeVolumeLocal(volume)
        try await connection.write {
            try connection.exec("UPDATE volume_metadata SET quarantined=1 WHERE root='\(root)'")
        }

        let hidden = await store.hasVolume(root: root)
        XCTAssertFalse(hidden)
        try await store.storeVolumeLocal(volume)
        let restored = await store.hasVolume(root: root)
        XCTAssertTrue(restored)

        let quarantined = try await connection.read {
            try Self.scalarInt(
                connection.readDb,
                "SELECT quarantined FROM volume_metadata WHERE root='\(root)'"
            )
        }
        XCTAssertEqual(quarantined, 0)
    }

    func testAuthenticatedRestorageRepairsCorruptCASBytes() async throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("VolumeIntegrityTests-\(UUID().uuidString)")
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let connection = try SQLiteConnection(path: directory.appendingPathComponent("volumes.sqlite").path)
        let store = CASVolumeStore(connection: connection)
        let firstData = Data("repair-first".utf8)
        let secondData = Data("repair-second".utf8)
        let sharedData = Data("repair-shared".utf8)
        let first = try cid(for: firstData)
        let second = try cid(for: secondData)
        let shared = try cid(for: sharedData)
        let firstVolume = SerializedVolume(
            root: first,
            entries: [first: firstData, shared: sharedData]
        )
        let secondVolume = SerializedVolume(
            root: second,
            entries: [second: secondData, shared: sharedData]
        )
        try await store.storeVolumeLocal(firstVolume)
        try await connection.write {
            try connection.exec("UPDATE cas_data SET data=X'00' WHERE cid='\(shared)'")
        }

        let corruptShared = await store.fetchDataLocal(cid: shared)
        let firstAfterProof = await store.hasVolume(root: first)
        XCTAssertNil(corruptShared)
        XCTAssertFalse(firstAfterProof)

        try await store.storeVolumeLocal(secondVolume)
        let secondAfterRepair = await store.hasVolume(root: second)
        let repairedShared = await store.fetchDataLocal(cid: shared)
        let firstStillQuarantined = await store.hasVolume(root: first)
        XCTAssertTrue(secondAfterRepair)
        XCTAssertEqual(repairedShared, sharedData)
        XCTAssertFalse(firstStillQuarantined)

        try await store.storeVolumeLocal(firstVolume)
        let firstAfterRestorage = await store.hasVolume(root: first)
        XCTAssertTrue(firstAfterRestorage)
        try await assertHealthy(connection)
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

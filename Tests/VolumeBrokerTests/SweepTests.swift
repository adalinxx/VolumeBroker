import Testing
import Foundation
import CID
import Multihash
#if canImport(SQLite3)
import SQLite3
#else
import VolumeBrokerSQLite
#endif
@testable import VolumeBroker

/// SQL-level sweep tests over one shared `SQLiteConnection`, without the
/// `DiskBroker` façade: damaged rows, quarantine, and SQL failures.
@Suite("Sweep")
struct SweepTests {

    private struct Harness {
        let connection: SQLiteConnection
        let store: CASVolumeStore
        let retained: RetainedRootIndex
    }

    private func harness() throws -> Harness {
        let path = NSTemporaryDirectory() + "vb_sweep_\(UUID().uuidString).sqlite"
        let connection = try SQLiteConnection(path: path)
        return Harness(
            connection: connection,
            store: CASVolumeStore(connection: connection),
            retained: RetainedRootIndex(connection: connection)
        )
    }

    private func cid(for data: Data) -> String {
        let multihash = try! Multihash(raw: data, hashedWith: .sha2_256)
        return try! CID(version: .v1, codec: .dag_cbor, multihash: multihash).toBaseEncodedString
    }

    private func cid(_ value: String) -> String {
        cid(for: Data(value.utf8))
    }

    private func volume(_ root: String, _ entries: [String: Data] = [:]) -> SerializedVolume {
        let rootData = Data(root.utf8)
        var encodedEntries = Dictionary(uniqueKeysWithValues: entries.values.map { data in
            (cid(for: data), data)
        })
        encodedEntries[cid(for: rootData)] = rootData
        return SerializedVolume(root: cid(for: rootData), entries: encodedEntries)
    }

    @Test func unretainedRootIsSwept() async throws {
        let h = try harness()
        try await h.store.store(volume: volume("drop"))

        #expect(try await h.retained.sweep() == 1)
        #expect(await h.store.hasVolume(root: cid("drop")) == false)
        #expect(await casRowCount(connection: h.connection, cid: cid("drop")) == 0)
    }

    /// The serve gate and the sweep share one liveness definition.
    @Test func liveRootServesAndSurvives() async throws {
        let h = try harness()
        let root = cid("r1")
        try await h.store.store(volume: volume("r1"))
        try await h.retained.advanceRetainedRoots(scope: "s", roots: [root])

        #expect(await h.retained.isPinReachable(cid: root))
        #expect(try await h.retained.sweep() == 0)
        #expect(await h.store.hasVolume(root: root))
        #expect(await h.retained.isPinReachable(cid: root))
    }

    @Test func sharedBlobIsDeletedAfterLastOwner() async throws {
        let h = try harness()
        let shared = Data("shared".utf8)
        let sharedCID = cid(for: shared)
        let keepRoot = cid("keep")
        try await h.store.store(volume: volume("keep", ["shared": shared]))
        try await h.store.store(volume: volume("drop", ["shared": shared]))
        try await h.retained.advanceRetainedRoots(scope: "s", roots: [keepRoot])

        #expect(try await h.retained.sweep() == 1)
        #expect(await casRowCount(connection: h.connection, cid: sharedCID) == 1)

        try await h.retained.advanceRetainedRoots(scope: "s", roots: [])
        #expect(try await h.retained.sweep() == 1)
        #expect(await casRowCount(connection: h.connection, cid: sharedCID) == 0)
        #expect(await h.store.fetchDataLocal(cid: sharedCID) == nil)
    }

    @Test func danglingMembershipOwnsNothing() async throws {
        let h = try harness()
        let root = cid("dangling")
        try await h.store.store(volume: volume("dangling"))
        try await h.connection.write {
            try h.connection.exec("PRAGMA foreign_keys=OFF")
            do {
                try h.connection.exec("DELETE FROM volume_metadata WHERE root='\(root)'")
                try h.connection.exec("PRAGMA foreign_keys=ON")
            } catch {
                try? h.connection.exec("PRAGMA foreign_keys=ON")
                throw error
            }
        }

        #expect(!(await h.retained.isPinReachable(cid: root)))
        #expect(try await h.retained.sweep() == 0)
        #expect(await casRowCount(connection: h.connection, cid: root) == 0)
    }

    @Test func malformedUnretainedVolumeIsSwept() async throws {
        let h = try harness()
        let root = cid("real-count")
        try await h.store.store(volume: volume("real-count"))
        try await h.connection.write {
            try h.connection.exec("""
                UPDATE volume_metadata
                SET entry_count=CAST(1.5 AS REAL)
                WHERE root='\(root)'
                """)
        }

        #expect(await h.store.hasVolume(root: root) == false)
        #expect(try await h.retained.sweep() == 1)
        #expect(await casRowCount(connection: h.connection, cid: root) == 0)
    }

    @Test func retainedIntentSurvivesContentLoss() async throws {
        let h = try harness()
        let root = cid("lost")
        try await h.store.store(volume: volume("lost"))
        try await h.retained.advanceRetainedRoots(scope: "canonical", roots: [root])
        try await h.connection.write {
            try h.connection.exec("DELETE FROM volume_metadata WHERE root='\(root)'")
        }

        #expect(await h.store.hasVolume(root: root) == false)
        #expect(try await h.retained.retainedRoots(scope: "canonical") == [root])
    }

    /// A quarantined retained root is still live: the sweep keeps its bytes
    /// for repair, but it is not served and its members do not extend
    /// liveness. Valid republication restores both.
    @Test func quarantinedRetainedRootIsKeptButNotServed() async throws {
        let h = try harness()
        let parentLabel = "corrupt"
        let root = cid(parentLabel)
        let child = volume("child")
        try await h.store.storeVolumesLocal([
            volume(parentLabel, ["child": Data("child".utf8)]),
            child,
        ])
        try await h.retained.advanceRetainedRoots(scope: "canonical", roots: [root])
        try await h.connection.write {
            try h.connection.exec("UPDATE cas_data SET data=X'00' WHERE cid='\(root)'")
        }
        #expect(await h.store.fetchVolumeLocal(root: root) == nil)
        #expect(await h.store.hasVolume(root: root) == false)
        #expect(await h.retained.isPinReachable(cid: root) == false)

        do {
            try await h.retained.mergeRetainedRoots(scope: "candidate", roots: [root])
            Issue.record("CID-invalid Volume must not become a retention root")
        } catch {
            #expect(error as? BrokerError == .missingRetainedRoot(root))
        }

        #expect(try await h.retained.sweep() == 1, "only the child loses liveness")
        #expect(await casRowCount(connection: h.connection, cid: root) == 1)
        #expect(await h.store.hasVolume(root: child.root) == false)
        #expect(try await h.retained.retainedRoots(scope: "canonical") == [root])

        try await h.store.store(volume: volume(parentLabel, ["child": Data("child".utf8)]))
        #expect(try await h.retained.sweep() == 0)
        #expect(await h.store.hasVolume(root: root))
        #expect(await h.retained.isPinReachable(cid: root))
    }

    @Test func retainedRootReadPropagatesSQLFailure() async throws {
        let h = try harness()
        #expect(sqlite3_set_authorizer(h.connection.readDb, { _, action, _, _, _, _ in
            action == SQLITE_READ ? SQLITE_DENY : SQLITE_OK
        }, nil) == SQLITE_OK)
        defer { sqlite3_set_authorizer(h.connection.readDb, nil, nil) }

        do {
            _ = try await h.retained.retainedRoots(scope: "canonical")
            Issue.record("retained-root SQL failures must propagate")
        } catch {
            guard let brokerError = error as? BrokerError else {
                Issue.record("unexpected error: \(error)")
                return
            }
            guard case .sqlFailed = brokerError else {
                Issue.record("unexpected broker error: \(brokerError)")
                return
            }
        }
    }

    @Test func sweepSQLFailureRollsBackWholeSweep() async throws {
        let h = try harness()
        let shared = Data("shared".utf8)
        try await h.store.store(volume: volume("drop", ["shared": shared]))
        sqlite3_set_authorizer(h.connection.db, { _, action, table, _, _, _ in
            guard action == SQLITE_DELETE, let table,
                  String(cString: table) == "cas_data" else { return SQLITE_OK }
            return SQLITE_DENY
        }, nil)
        defer { sqlite3_set_authorizer(h.connection.db, nil, nil) }

        await #expect(throws: BrokerError.self) { try await h.retained.sweep() }
        sqlite3_set_authorizer(h.connection.db, nil, nil)
        #expect(await h.store.hasVolume(root: cid("drop")))
        #expect(await casRowCount(connection: h.connection, cid: cid(for: shared)) == 1)
    }

    @Test func quarantineSQLFailureNeverDeletesContent() async throws {
        let h = try harness()
        let root = cid("sql-failure")
        try await h.store.store(volume: volume("sql-failure"))
        try await h.connection.write {
            try h.connection.exec("UPDATE cas_data SET data=X'00' WHERE cid='\(root)'")
        }

        sqlite3_set_authorizer(h.connection.db, { _, action, _, _, _, _ in
            action == SQLITE_READ ? SQLITE_DENY : SQLITE_OK
        }, nil)
        #expect(await h.store.fetchVolumeLocal(root: root) == nil)
        sqlite3_set_authorizer(h.connection.db, nil, nil)

        #expect(await casRowCount(connection: h.connection, cid: root) == 1)
    }

    private func casRowCount(connection: SQLiteConnection, cid: String) async -> Int {
        await connection.read {
            var stmt: OpaquePointer?
            defer { sqlite3_finalize(stmt) }
            guard sqlite3_prepare_v2(
                connection.readDb,
                "SELECT COUNT(*) FROM cas_data WHERE cid = ?1",
                -1,
                &stmt,
                nil
            ) == SQLITE_OK, let stmt else { return -1 }
            sqlite3_bind_text(stmt, 1, cid, -1, SQLITE_TRANSIENT_SHIM)
            guard sqlite3_step(stmt) == SQLITE_ROW else { return -1 }
            return Int(sqlite3_column_int64(stmt, 0))
        }
    }
}

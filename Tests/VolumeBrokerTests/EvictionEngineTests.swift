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

/// Independent unit tests for the `EvictionEngine` collaborator.
///
/// EvictionEngine reads the tables owned by `CASVolumeStore` and `PinIndex`, so
/// these tests wire all three over one shared `SQLiteConnection` — but never
/// through the `DiskBroker` façade. This pins the reclaim contract (pinned roots
/// survive, unpinned roots and their exclusive CAS blobs are reclaimed, shared
/// blobs are retained, TTL-expired pins are pruned) to the engine itself.
@Suite("EvictionEngine")
struct EvictionEngineTests {

    private struct Harness {
        let connection: SQLiteConnection
        let store: CASVolumeStore
        let pins: PinIndex
        let eviction: EvictionEngine
    }

    private func harness() throws -> Harness {
        let path = NSTemporaryDirectory() + "vb_eviction_\(UUID().uuidString).sqlite"
        let connection = try SQLiteConnection(path: path)
        return Harness(
            connection: connection,
            store: CASVolumeStore(connection: connection),
            pins: PinIndex(connection: connection),
            eviction: EvictionEngine(connection: connection)
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

    /// An unpinned root and its data are reclaimed; the return value counts it.
    @Test func unpinnedRootIsReclaimed() async throws {
        let h = try harness()
        try await h.store.store(volume: volume("drop"))

        let evicted = try await h.eviction.evictUnpinned(graceSeconds: 0)
        #expect(evicted == 1)
        #expect(await h.store.hasVolume(root: cid("drop")) == false)
        #expect(await h.store.fetchVolumeLocal(root: cid("drop")) == nil)
    }

    /// A pinned root survives; an unpinned sibling is reclaimed in the same pass.
    @Test func pinnedRootSurvivesWhileSiblingIsReclaimed() async throws {
        let h = try harness()
        try await h.store.store(volume: volume("keep"))
        try await h.store.store(volume: volume("drop"))
        try await h.pins.pin(root: cid("keep"), owner: "owner", count: 1, ttl: nil)

        let evicted = try await h.eviction.evictUnpinned(graceSeconds: 0)
        #expect(evicted == 1, "only the unpinned root counts")
        #expect(await h.store.hasVolume(root: cid("keep")))
        #expect(await h.store.hasVolume(root: cid("drop")) == false)
    }

    /// A CAS blob shared between a pinned and an unpinned root is retained even
    /// after the unpinned root is reclaimed.
    @Test func sharedBlobSurvivesPartialEviction() async throws {
        let h = try harness()
        let shared = Data([0xAB, 0xCD])
        try await h.store.store(volume: volume("keep", ["shared": shared, "onlyKeep": Data([1])]))
        try await h.store.store(volume: volume("drop", ["shared": shared, "onlyDrop": Data([2])]))
        try await h.pins.pin(root: cid("keep"), owner: "owner", count: 1, ttl: nil)

        _ = try await h.eviction.evictUnpinned(graceSeconds: 0)
        #expect(await h.store.fetchVolumeLocal(root: cid("keep"))?.entries[cid(for: shared)] == shared,
                "the shared blob must remain available to the pinned root")
        #expect(await h.store.hasVolume(root: cid("drop")) == false)
    }

    /// A root with a remaining live pin is never reclaimed (returns 0).
    @Test func livePinBlocksEviction() async throws {
        let h = try harness()
        let root = cid("r1")
        try await h.store.store(volume: volume("r1"))
        try await h.pins.pin(root: root, owner: "owner-a", count: 1, ttl: nil)
        try await h.pins.pin(root: root, owner: "owner-b", count: 1, ttl: nil)

        try await h.pins.unpin(root: root, owner: "owner-a", count: 1)
        let evicted = try await h.eviction.evictUnpinned(graceSeconds: 0)
        #expect(evicted == 0, "owner-b still pins the root")
        #expect(await h.store.hasVolume(root: root))
    }

    /// Eviction first prunes TTL-expired pins, then reclaims the now-unpinned root.
    @Test func ttlExpiredPinIsPrunedThenReclaimed() async throws {
        let h = try harness()
        let root = cid("r1")
        try await h.store.store(volume: volume("r1"))
        try await h.pins.pin(root: root, owner: "owner-a", count: 1, ttl: .zero)

        let evicted = try await h.eviction.evictUnpinned(graceSeconds: 0)
        #expect(evicted == 1)
        #expect(await h.store.hasVolume(root: root) == false)
    }

    /// Serve-gate / eviction predicate alignment: an expired-TTL pin is dead on
    /// BOTH sides — `isPinReachable` refuses to serve it, and `evictUnpinned`
    /// does not protect it (the root is reclaimed).
    @Test func expiredPinNeitherServesNorProtects() async throws {
        let h = try harness()
        let root = cid("r1")
        try await h.store.store(volume: volume("r1"))
        try await h.pins.pin(root: root, owner: "owner-a", count: 1, ttl: .zero)

        #expect(await h.pins.isPinReachable(cid: root) == false,
                "the serve gate must not serve an expired pin")
        let evicted = try await h.eviction.evictUnpinned(graceSeconds: 0)
        #expect(evicted == 1, "an expired pin must not protect from eviction")
        #expect(await h.store.hasVolume(root: root) == false)
    }

    /// Serve-gate / eviction predicate alignment: a live pin is live on BOTH
    /// sides — `isPinReachable` serves it, and `evictUnpinned` protects it.
    @Test func livePinServesAndProtects() async throws {
        let h = try harness()
        let root = cid("r1")
        try await h.store.store(volume: volume("r1"))
        try await h.pins.pin(root: root, owner: "owner-a", count: 1, ttl: nil)

        #expect(await h.pins.isPinReachable(cid: root))
        let evicted = try await h.eviction.evictUnpinned(graceSeconds: 0)
        #expect(evicted == 0)
        #expect(await h.store.hasVolume(root: root))
        #expect(await h.pins.isPinReachable(cid: root), "still served after the sweep")
    }

    @Test func danglingMembershipAndPinOwnNothing() async throws {
        let h = try harness()
        let root = cid("dangling")
        try await h.store.store(volume: volume("dangling"))
        try await h.pins.pin(root: root, owner: "owner", count: 1, ttl: nil)
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

        #expect(!(await h.pins.isPinReachable(cid: root)))
        #expect(try await h.eviction.evictUnpinned(graceSeconds: 0) == 0)
        #expect(await casRowCount(connection: h.connection, cid: root) == 0)
        #expect(await h.pins.owners(root: root).isEmpty)
    }

    @Test func realEntryCountOwnsNothing() async throws {
        let h = try harness()
        let root = cid("real-count")
        try await h.store.store(volume: volume("real-count"))
        try await h.pins.pin(root: root, owner: "owner", count: 1, ttl: nil)
        try await h.connection.write {
            try h.connection.exec("""
                UPDATE volume_metadata
                SET entry_count=CAST(1.5 AS REAL)
                WHERE root='\(root)'
                """)
        }

        #expect(await h.store.hasVolume(root: root) == false)
        #expect(try await h.eviction.evictUnpinned(graceSeconds: 0) == 1)
        #expect(await casRowCount(connection: h.connection, cid: root) == 0)
        #expect(await h.pins.owners(root: root).isEmpty)
    }

    @Test func retainedIntentSurvivesContentLoss() async throws {
        let h = try harness()
        let retained = RetainedRootIndex(connection: h.connection)
        let root = cid("lost")
        try await h.store.store(volume: volume("lost"))
        try await retained.advanceRetainedRoots(scope: "canonical", roots: [root])
        try await h.connection.write {
            try h.connection.exec("DELETE FROM volume_metadata WHERE root='\(root)'")
        }

        #expect(await h.store.hasVolume(root: root) == false)
        #expect(try await retained.retainedRoots(scope: "canonical") == [root])
    }

    @Test func quarantinePreservesIntentUntilExplicitEvictionAndRepublication() async throws {
        let h = try harness()
        let retained = RetainedRootIndex(connection: h.connection)
        let root = cid("corrupt")
        try await h.store.store(volume: volume("corrupt"))
        try await retained.advanceRetainedRoots(scope: "canonical", roots: [root])
        try await h.pins.pin(root: root, owner: "owner", count: 1, ttl: nil)
        try await h.connection.write {
            try h.connection.exec("UPDATE cas_data SET data=X'00' WHERE cid='\(root)'")
        }
        #expect(await h.store.fetchVolumeLocal(root: root) == nil)
        #expect(await h.store.hasVolume(root: root) == false)
        #expect(await h.pins.isPinReachable(cid: root) == false)
        #expect(await casRowCount(connection: h.connection, cid: root) == 1)
        #expect(await h.pins.owners(root: root) == ["owner"])
        #expect(try await retained.retainedRoots(scope: "canonical") == [root])

        do {
            try await retained.mergeRetainedRoots(scope: "candidate", roots: [root])
            Issue.record("CID-invalid Volume must not become a retention root")
        } catch {
            #expect(error as? BrokerError == .missingRetainedRoot(root))
        }
        #expect(try await retained.retainedRoots(scope: "canonical") == [root])
        #expect(try await retained.retainedRoots(scope: "candidate").isEmpty)

        #expect(try await h.eviction.evictUnpinned(graceSeconds: 60 * 60) == 1)
        #expect(await casRowCount(connection: h.connection, cid: root) == 0)
        #expect(await h.pins.owners(root: root).isEmpty)
        #expect(try await retained.retainedRoots(scope: "canonical") == [root])

        try await h.store.store(volume: volume("corrupt"))
        #expect(try await retained.retainedRoots(scope: "canonical") == [root])
        #expect(try await h.eviction.evictUnpinned(graceSeconds: 0) == 0)
        #expect(await h.store.hasVolume(root: root))
        #expect(await h.pins.isPinReachable(cid: root))
    }

    @Test func retainedRootReadPropagatesSQLFailure() async throws {
        let h = try harness()
        let retained = RetainedRootIndex(connection: h.connection)
        #expect(sqlite3_set_authorizer(h.connection.readDb, { _, action, _, _, _, _ in
            action == SQLITE_READ ? SQLITE_DENY : SQLITE_OK
        }, nil) == SQLITE_OK)
        defer { sqlite3_set_authorizer(h.connection.readDb, nil, nil) }

        do {
            _ = try await retained.retainedRoots(scope: "canonical")
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

    @Test func nonpositivePinCountIsRejected() async throws {
        let h = try harness()
        let root = cid("r1")
        try await h.store.store(volume: volume("r1"))
        await #expect(throws: BrokerError.invalidPinCount) {
            try await h.pins.pin(root: root, owner: "owner-a", count: 0, ttl: nil)
        }
        #expect(await h.pins.owners(root: root).isEmpty)
        let evicted = try await h.eviction.evictUnpinned(graceSeconds: 0)
        #expect(evicted == 1)
        #expect(await h.store.hasVolume(root: root) == false)
    }

    /// Freshly stored unpinned content survives the periodic sweep long enough
    /// for a follow-up pin intent, then evicts normally once grace is disabled.
    @Test func evictRespectsStoreThenPinGrace() async throws {
        let h = try harness()
        let root = cid("V")
        try await h.store.store(volume: volume("V"))

        let graceEvicted = try await h.eviction.evictUnpinned(graceSeconds: 60 * 60)
        #expect(graceEvicted == 0)
        #expect(await h.store.hasVolume(root: root))

        try await h.pins.pin(root: root, owner: "owner", count: 1, ttl: nil)
        let pinnedEvicted = try await h.eviction.evictUnpinned(graceSeconds: 0)
        #expect(pinnedEvicted == 0)
        #expect(await h.store.hasVolume(root: root))

        try await h.pins.unpin(root: root, owner: "owner", count: 1)
        let finalEvicted = try await h.eviction.evictUnpinned(graceSeconds: 0)
        #expect(finalEvicted == 1)
        #expect(await h.store.hasVolume(root: root) == false)
    }

    @Test func sharedBlobSurvivesGraceProtectedOwner() async throws {
        let h = try harness()
        let shared = Data("shared".utf8)
        let oldRoot = cid("old")
        let freshRoot = cid("fresh")
        try await h.store.store(volume: volume("old", ["shared": shared]))
        try await h.store.store(volume: volume("fresh", ["shared": shared]))
        try await h.connection.write {
            try h.connection.exec("""
                UPDATE volume_metadata
                SET stored_at = datetime('now', '-2 hours')
                WHERE root = '\(oldRoot)'
                """)
        }

        let evicted = try await h.eviction.evictUnpinned(graceSeconds: 60 * 60)
        #expect(evicted == 1)
        #expect(await h.store.hasVolume(root: oldRoot) == false)
        #expect(await h.store.hasVolume(root: freshRoot))
        #expect(await h.store.fetchDataLocal(cid: cid(for: shared)) == shared)
    }

    @Test func sharedBlobIsDeletedAfterLastOwner() async throws {
        let h = try harness()
        let shared = Data("shared".utf8)
        let sharedCID = cid(for: shared)
        let keepRoot = cid("keep")
        try await h.store.store(volume: volume("keep", ["shared": shared]))
        try await h.store.store(volume: volume("drop", ["shared": shared]))
        try await h.pins.pin(root: keepRoot, owner: "owner", count: 1, ttl: nil)

        _ = try await h.eviction.evictUnpinned(graceSeconds: 0)
        #expect(await casRowCount(connection: h.connection, cid: sharedCID) == 1)

        try await h.pins.unpin(root: keepRoot, owner: "owner", count: 1)
        _ = try await h.eviction.evictUnpinned(graceSeconds: 0)
        #expect(await casRowCount(connection: h.connection, cid: sharedCID) == 0)
        #expect(await h.store.fetchDataLocal(cid: sharedCID) == nil)
    }

    private func storeVolumes(_ h: Harness, _ volumes: [SerializedVolume]) async throws {
        try await h.store.storeVolumesLocal(volumes)
    }

    @Test func pinnedRootDoesNotProtectAnotherVolume() async throws {
        let h = try harness()
        let obj = cid("obj")
        let nested = cid("nested")
        let deep = cid("deep")
        try await storeVolumes(h, [
            SerializedVolume(root: obj, entries: [obj: Data("obj".utf8)]),
            SerializedVolume(
                root: nested,
                entries: [nested: Data("nested".utf8), deep: Data("deep".utf8)]
            ),
        ])
        try await h.pins.pin(root: obj, owner: "o", count: 1, ttl: nil)

        let evicted = try await h.eviction.evictUnpinned(graceSeconds: 0)
        #expect(evicted == 1)
        #expect(await h.store.hasVolume(root: obj))
        #expect(await h.store.hasVolume(root: nested) == false)
        #expect(await h.store.fetchDataLocal(cid: deep) == nil)
    }

    @Test func relatedVolumesCanBePinnedExplicitly() async throws {
        let h = try harness()
        let obj = cid("obj")
        let nested = cid("nested")
        let deep = cid("deep")
        try await storeVolumes(h, [
            SerializedVolume(root: obj, entries: [obj: Data("obj".utf8)]),
            SerializedVolume(
                root: nested,
                entries: [nested: Data("nested".utf8), deep: Data("deep".utf8)]
            ),
        ])
        try await h.pins.pin(root: obj, owner: "o", count: 1, ttl: nil)
        try await h.pins.pin(root: nested, owner: "o", count: 1, ttl: nil)

        #expect(await h.pins.isPinReachable(cid: deep))
        let evicted = try await h.eviction.evictUnpinned(graceSeconds: 0)
        #expect(evicted == 0)
        #expect(await h.store.hasVolume(root: obj))
        #expect(await h.store.hasVolume(root: nested))
        #expect(await h.store.fetchDataLocal(cid: deep) != nil)
    }

    @Test func retainingOneVolumeDoesNotRetainSiblingVolumes() async throws {
        let h = try harness()
        let block = cid("block")
        let txDict = cid("txDict")
        let txBody = cid("txBody")
        let postState = cid("postState")
        try await storeVolumes(h, [
            SerializedVolume(
                root: block,
                entries: [block: Data("block".utf8), txDict: Data("txDict".utf8)]
            ),
            SerializedVolume(root: txBody, entries: [txBody: Data("txBody".utf8)]),
            SerializedVolume(root: postState, entries: [postState: Data("postState".utf8)]),
        ])
        try await h.pins.pin(root: block, owner: "h:1", count: 1, ttl: nil)

        let evicted = try await h.eviction.evictUnpinned(graceSeconds: 0)
        #expect(evicted == 2)
        #expect(await h.store.fetchDataLocal(cid: block) != nil)
        #expect(await h.store.fetchDataLocal(cid: txDict) != nil)
        #expect(await h.store.hasVolume(root: txBody) == false)
        #expect(await h.store.hasVolume(root: postState) == false)
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

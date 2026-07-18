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

@Suite("DiskBroker")
struct DiskBrokerTests {

    private actor StartBarrier {
        private var remaining: Int
        private var waiters: [CheckedContinuation<Void, Never>] = []

        init(participants: Int) {
            remaining = participants
        }

        func wait() async {
            remaining -= 1
            guard remaining > 0 else {
                let waiting = waiters
                waiters.removeAll()
                for waiter in waiting { waiter.resume() }
                return
            }
            await withCheckedContinuation { waiters.append($0) }
        }
    }

    private struct StorePinObservation: Equatable {
        let volumeExists: Bool
        let pinned: Bool
        let pinSucceeded: Bool
    }

    private struct EvictPinObservation: Equatable {
        let evicted: Int
        let volumeExists: Bool
        let pinned: Bool
        let pinSucceeded: Bool
    }

    private static func capture<Value: Sendable>(
        _ operation: @escaping @Sendable () async throws -> Value
    ) async -> Result<Value, BrokerError> {
        do {
            return .success(try await operation())
        } catch let error as BrokerError {
            return .failure(error)
        } catch {
            return .failure(.sqlFailed("unexpected test error: \(error)"))
        }
    }

    private static func race<Left: Sendable, Right: Sendable>(
        _ left: @escaping @Sendable () async -> Left,
        _ right: @escaping @Sendable () async -> Right
    ) async -> (Left, Right) {
        let barrier = StartBarrier(participants: 2)
        async let leftResult: Left = {
            await barrier.wait()
            return await left()
        }()
        async let rightResult: Right = {
            await barrier.wait()
            return await right()
        }()
        return await (leftResult, rightResult)
    }

    private func tempDB(evictUnpinnedGraceSeconds: Int = 0) throws -> DiskBroker {
        let path = NSTemporaryDirectory() + "vb_test_\(UUID().uuidString).sqlite"
        return try DiskBroker(path: path, evictUnpinnedGraceSeconds: evictUnpinnedGraceSeconds)
    }

    private func temporaryDatabase() throws -> (directory: URL, path: String) {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("VolumeBrokerDisk-\(UUID().uuidString)")
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        return (directory, directory.appendingPathComponent("volumes.sqlite").path)
    }

    private func databaseHealth(at path: String) throws -> (integrity: String, foreignKeyViolations: Int) {
        var database: OpaquePointer?
        guard sqlite3_open_v2(path, &database, SQLITE_OPEN_READONLY | SQLITE_OPEN_FULLMUTEX, nil) == SQLITE_OK,
              let database else {
            throw BrokerError.openFailed("health-check open failed")
        }
        defer { sqlite3_close(database) }

        func text(_ sql: String) throws -> String {
            var statement: OpaquePointer?
            defer { sqlite3_finalize(statement) }
            guard sqlite3_prepare_v2(database, sql, -1, &statement, nil) == SQLITE_OK,
                  let statement,
                  sqlite3_step(statement) == SQLITE_ROW,
                  let value = sqlite3_column_text(statement, 0) else {
                throw BrokerError.sqlFailed(String(cString: sqlite3_errmsg(database)))
            }
            return String(cString: value)
        }

        func count(_ sql: String) throws -> Int {
            var statement: OpaquePointer?
            defer { sqlite3_finalize(statement) }
            guard sqlite3_prepare_v2(database, sql, -1, &statement, nil) == SQLITE_OK,
                  let statement,
                  sqlite3_step(statement) == SQLITE_ROW else {
                throw BrokerError.sqlFailed(String(cString: sqlite3_errmsg(database)))
            }
            return Int(sqlite3_column_int64(statement, 0))
        }

        return (
            try text("PRAGMA integrity_check"),
            try count("SELECT COUNT(*) FROM pragma_foreign_key_check")
        )
    }

    private func pinCount(at path: String, root: String, owner: String) throws -> Int {
        var database: OpaquePointer?
        guard sqlite3_open_v2(path, &database, SQLITE_OPEN_READONLY | SQLITE_OPEN_FULLMUTEX, nil) == SQLITE_OK,
              let database else {
            throw BrokerError.openFailed("pin-count open failed")
        }
        defer { sqlite3_close(database) }

        var statement: OpaquePointer?
        defer { sqlite3_finalize(statement) }
        guard sqlite3_prepare_v2(
            database,
            "SELECT COALESCE(MAX(count), 0) FROM volume_pins WHERE root=?1 AND owner=?2",
            -1,
            &statement,
            nil
        ) == SQLITE_OK, let statement else {
            throw BrokerError.sqlFailed(String(cString: sqlite3_errmsg(database)))
        }
        sqlite3_bind_text(statement, 1, root, -1, SQLITE_TRANSIENT_SHIM)
        sqlite3_bind_text(statement, 2, owner, -1, SQLITE_TRANSIENT_SHIM)
        guard sqlite3_step(statement) == SQLITE_ROW else {
            throw BrokerError.sqlFailed(String(cString: sqlite3_errmsg(database)))
        }
        return Int(sqlite3_column_int64(statement, 0))
    }

    private func cid(for data: Data) -> String {
        let multihash = try! Multihash(raw: data, hashedWith: .sha2_256)
        return try! CID(version: .v1, codec: .dag_cbor, multihash: multihash).toBaseEncodedString
    }

    private func cid(_ value: String) -> String {
        cid(for: Data(value.utf8))
    }

    private func payload(_ root: String, _ entries: [String: Data] = [:]) -> SerializedVolume {
        let rootData = Data(root.utf8)
        var encodedEntries = Dictionary(uniqueKeysWithValues: entries.values.map { data in
            (cid(for: data), data)
        })
        encodedEntries[cid(for: rootData)] = rootData
        return SerializedVolume(root: cid(for: rootData), entries: encodedEntries)
    }

    @Test func storeAndFetch() async throws {
        let broker = try tempDB()
        let p = payload("r1", ["c1": Data([1, 2, 3]), "c2": Data([4, 5])])
        try await broker.storeVolumeLocal(p)

        #expect(await broker.hasVolume(root: p.root))
        let fetched = await broker.fetchVolumeLocal(root: p.root)
        #expect(fetched?.entries.count == 3)
        #expect(fetched?.entries[cid(for: Data([1, 2, 3]))] == Data([1, 2, 3]))
    }

    // MARK: - Extracted-layer boundaries

    /// CASVolumeStore boundary: a stored volume round-trips byte-for-byte.
    @Test func casStoreRoundTrip() async throws {
        let broker = try tempDB()
        let entries = ["a": Data([0, 1, 2, 3]), "b": Data(repeating: 7, count: 256)]
        let stored = payload("round", entries)
        try await broker.storeVolumeLocal(stored)

        let fetched = await broker.fetchVolumeLocal(root: stored.root)
        #expect(fetched?.entries == stored.entries)
    }

    /// EvictionEngine boundary: a pinned root survives eviction; the unpinned
    /// sibling is reclaimed.
    @Test func pinSurvivesEviction() async throws {
        let broker = try tempDB()
        let keep = cid("keep")
        let drop = cid("drop")
        try await broker.storeVolumeLocal(payload("keep"))
        try await broker.storeVolumeLocal(payload("drop"))
        try await broker.pin(root: keep, owner: "owner")

        let evicted = try await broker.evictUnpinned()
        #expect(evicted == 1)
        #expect(await broker.hasVolume(root: keep))
        #expect(await broker.hasVolume(root: drop) == false)
        #expect(await broker.fetchVolumeLocal(root: keep) != nil)
    }

    /// PinIndex + EvictionEngine boundary: unpinning the last owner makes the
    /// root eligible for eviction.
    @Test func unpinnedRootIsEvicted() async throws {
        let broker = try tempDB()
        let root = cid("r1")
        try await broker.storeVolumeLocal(payload("r1"))
        try await broker.pin(root: root, owner: "owner")
        try await broker.unpin(root: root, owner: "owner")

        let evicted = try await broker.evictUnpinned()
        #expect(evicted == 1)
        #expect(await broker.hasVolume(root: root) == false)
    }

    @Test func configuredGraceProtectsFreshUnpinnedVolume() async throws {
        let broker = try tempDB(evictUnpinnedGraceSeconds: 60 * 60)
        let root = cid("fresh")
        try await broker.storeVolumeLocal(payload("fresh"))

        let protected = try await broker.evictUnpinned()
        #expect(protected == 0)
        #expect(await broker.hasVolume(root: root))

        let evicted = try await broker.evictUnpinned(graceSeconds: 0)
        #expect(evicted == 1)
        #expect(await broker.hasVolume(root: root) == false)
    }

    @Test func pinAndEvict() async throws {
        let broker = try tempDB()
        let r1 = cid("r1")
        let r2 = cid("r2")
        try await broker.storeVolumeLocal(payload("r1"))
        try await broker.storeVolumeLocal(payload("r2"))
        try await broker.pin(root: r1, owner: "chain-a")

        let evicted = try await broker.evictUnpinned()
        #expect(evicted == 1)
        #expect(await broker.hasVolume(root: r1))
        #expect(await broker.hasVolume(root: r2) == false)
    }

    @Test func sharedCIDSurvivesPartialEviction() async throws {
        let broker = try tempDB()
        let shared = Data([99])
        let r1 = cid("r1")
        try await broker.storeVolumeLocal(payload("r1", ["shared": shared, "only1": Data([1])]))
        try await broker.storeVolumeLocal(payload("r2", ["shared": shared, "only2": Data([2])]))
        try await broker.pin(root: r1, owner: "chain-a")

        _ = try await broker.evictUnpinned()
        let fetched = await broker.fetchVolumeLocal(root: r1)
        #expect(fetched?.entries[cid(for: shared)] == shared)
    }

    @Test func retainedRootServesAndProtectsWithoutPinOwner() async throws {
        let broker = try tempDB()
        let keep = cid("keep")
        let drop = cid("drop")
        try await broker.storeVolumeLocal(payload("keep"))
        try await broker.storeVolumeLocal(payload("drop"))

        try await broker.advanceRetainedRoots(scope: "chain-a:state", roots: [keep])

        #expect(await broker.owners(root: keep).isEmpty)
        #expect(await broker.isPinReachable(cid: keep))
        #expect(try await broker.retainedRoots(scope: "chain-a:state") == [keep])
        let evicted = try await broker.evictUnpinned()
        #expect(evicted == 1)
        #expect(await broker.hasVolume(root: keep))
        #expect(await broker.hasVolume(root: drop) == false)
    }

    @Test func retainedRootAdvanceReplacesOnlyItsScope() async throws {
        let broker = try tempDB()
        let old = cid("old")
        let new = cid("new")
        let other = cid("other")
        try await broker.storeVolumeLocal(payload("old"))
        try await broker.storeVolumeLocal(payload("new"))
        try await broker.storeVolumeLocal(payload("other"))

        try await broker.advanceRetainedRoots(scope: "chain-a:state", roots: [old])
        try await broker.advanceRetainedRoots(scope: "chain-b:state", roots: [other])
        try await broker.advanceRetainedRoots(scope: "chain-a:state", roots: [new])

        #expect(try await broker.retainedRoots(scope: "chain-a:state") == [new])
        #expect(try await broker.retainedRoots(scope: "chain-b:state") == [other])
        let evicted = try await broker.evictUnpinned()
        #expect(evicted == 1)
        #expect(await broker.hasVolume(root: old) == false)
        #expect(await broker.hasVolume(root: new))
        #expect(await broker.hasVolume(root: other))
    }

    @Test func retainedRootMergeAddsWithoutReplacingScope() async throws {
        let broker = try tempDB()
        let old = cid("old")
        let new = cid("new")
        let drop = cid("drop")
        try await broker.storeVolumeLocal(payload("old"))
        try await broker.storeVolumeLocal(payload("new"))
        try await broker.storeVolumeLocal(payload("drop"))

        try await broker.advanceRetainedRoots(scope: "chain-a:state", roots: [old])
        try await broker.mergeRetainedRoots(scope: "chain-a:state", roots: [new])
        try await broker.mergeRetainedRoots(scope: "chain-a:state", roots: [new])

        #expect(try await broker.retainedRoots(scope: "chain-a:state") == [new, old].sorted())
        let evicted = try await broker.evictUnpinned()
        #expect(evicted == 1)
        #expect(await broker.hasVolume(root: old))
        #expect(await broker.hasVolume(root: new))
        #expect(await broker.hasVolume(root: drop) == false)
    }

    @Test func persistedRetentionSurvivesReopenThenControlsEviction() async throws {
        let location = try temporaryDatabase()
        defer { try? FileManager.default.removeItem(at: location.directory) }
        let permanent = cid("permanent")
        let retained = cid("retained")
        let expired = cid("expired")
        let unprotected = cid("unprotected")
        let missing = cid("missing")

        do {
            let broker = try DiskBroker(path: location.path, evictUnpinnedGraceSeconds: 0)
            try await broker.storeVolumesLocal([
                payload("permanent"), payload("retained"), payload("expired"), payload("unprotected"),
            ])
            try await broker.pinBatch(roots: [permanent], owner: "owner")
            try await broker.pin(root: expired, owner: "expired", ttl: .zero)
            try await broker.advanceRetainedRoots(scope: "canonical", roots: [retained])

            await #expect(throws: BrokerError.missingRetainedRoot(missing)) {
                try await broker.advanceRetainedRoots(
                    scope: "canonical",
                    roots: [permanent, missing]
                )
            }
            await #expect(throws: BrokerError.missingRetainedRoot(missing)) {
                try await broker.mergeRetainedRoots(
                    scope: "canonical",
                    roots: [unprotected, missing]
                )
            }
            #expect(try await broker.retainedRoots(scope: "canonical") == [retained])
        }

        let reopened = try DiskBroker(path: location.path, evictUnpinnedGraceSeconds: 0)
        #expect(Set(await reopened.pinnedRoots()) == [permanent])
        #expect(await reopened.owners(root: permanent) == ["owner"])
        #expect(await reopened.owners(root: expired).isEmpty)
        #expect(try await reopened.retainedRoots(scope: "canonical") == [retained])

        #expect(try await reopened.evictUnpinned() == 2)
        #expect(await reopened.hasVolume(root: permanent))
        #expect(await reopened.hasVolume(root: retained))
        #expect(await reopened.hasVolume(root: expired) == false)
        #expect(await reopened.hasVolume(root: unprotected) == false)

        try await reopened.unpinBatch(items: [(root: permanent, owner: "owner", count: 1)])
        try await reopened.advanceRetainedRoots(scope: "canonical", roots: [])
        #expect(try await reopened.evictUnpinned() == 2)
        #expect(await reopened.hasVolume(root: permanent) == false)
        #expect(await reopened.hasVolume(root: retained) == false)

        let health = try databaseHealth(at: location.path)
        #expect(health.integrity == "ok")
        #expect(health.foreignKeyViolations == 0)
    }

    @Test func retainedRootMergeRequiresStoredRoots() async throws {
        let broker = try tempDB()
        let missing = cid("missing")

        do {
            try await broker.mergeRetainedRoots(scope: "chain-a:state", roots: [missing])
            #expect(Bool(false), "merging a retained root before storing it must fail")
        } catch BrokerError.missingRetainedRoot(let root) {
            #expect(root == missing)
        }
        #expect(try await broker.retainedRoots(scope: "chain-a:state").isEmpty)
    }

    @Test func retainedRootReplaceIsNaturallyIdempotent() async throws {
        let broker = try tempDB()
        let r1 = cid("r1")
        try await broker.storeVolumeLocal(payload("r1"))

        try await broker.advanceRetainedRoots(scope: "chain-a:state", roots: [r1])
        try await broker.advanceRetainedRoots(scope: "chain-a:state", roots: [r1])

        #expect(try await broker.retainedRoots(scope: "chain-a:state") == [r1])
    }

    @Test func retainedRootAdvanceRequiresStoredRoots() async throws {
        let broker = try tempDB()
        let missing = cid("missing")

        do {
            try await broker.advanceRetainedRoots(scope: "chain-a:state", roots: [missing])
            #expect(Bool(false), "retaining a root before storing it must fail")
        } catch BrokerError.missingRetainedRoot(let root) {
            #expect(root == missing)
        }
        #expect(try await broker.retainedRoots(scope: "chain-a:state").isEmpty)
    }

    @Test func retainedRootAdvanceValidatesOnlyRequestedVolume() async throws {
        let broker = try tempDB()
        let root = cid("root")
        try await broker.storeVolumeLocal(payload("root", [
            "root": Data("root".utf8),
            "child": Data("child".utf8),
        ]))

        try await broker.advanceRetainedRoots(scope: "chain-a:state", roots: [root])

        #expect(try await broker.retainedRoots(scope: "chain-a:state") == [root])
    }

    @Test func retainedRootDoesNotProtectAnotherVolume() async throws {
        let broker = try tempDB()
        let object = cid("object")
        let child = cid("child")
        let drop = cid("drop")
        let leaf = cid("leaf")
        try await broker.storeVolumesLocal([
            payload("object", ["object": Data("object".utf8), "child": Data("child".utf8)]),
            payload("child", ["child": Data("child".utf8), "leaf": Data("leaf".utf8)]),
            payload("drop")
        ])

        try await broker.advanceRetainedRoots(scope: "chain-a:state", roots: [object])
        #expect(await broker.isPinReachable(cid: leaf) == false)
        let evicted = try await broker.evictUnpinned()

        #expect(evicted == 2)
        #expect(await broker.hasVolume(root: object))
        #expect(await broker.hasVolume(root: child) == false)
        #expect(await broker.hasVolume(root: drop) == false)
        #expect(await broker.fetchDataLocal(cid: leaf) == nil)
    }

    @Test func multiOwnerPins() async throws {
        let broker = try tempDB()
        let root = cid("r1")
        try await broker.storeVolumeLocal(payload("r1"))
        try await broker.pin(root: root, owner: "chain-a")
        try await broker.pin(root: root, owner: "chain-b")

        #expect(await broker.owners(root: root) == ["chain-a", "chain-b"])

        try await broker.unpin(root: root, owner: "chain-a")
        let evicted = try await broker.evictUnpinned()
        #expect(evicted == 0)
        #expect(await broker.hasVolume(root: root))
    }

    @Test func duplicatePinAddsToCount() async throws {
        let broker = try tempDB()
        let root = cid("r1")
        try await broker.storeVolumeLocal(payload("r1"))
        try await broker.pin(root: root, owner: "chain-a")
        try await broker.pin(root: root, owner: "chain-a")
        #expect(await broker.owners(root: root).count == 1)

        try await broker.unpin(root: root, owner: "chain-a")
        #expect(await broker.owners(root: root).count == 1, "one unpin should leave count=1")

        let evicted = try await broker.evictUnpinned()
        #expect(evicted == 0, "still pinned with count=1")
        #expect(await broker.hasVolume(root: root))

        try await broker.unpin(root: root, owner: "chain-a")
        #expect(await broker.owners(root: root).isEmpty)
        let evicted2 = try await broker.evictUnpinned()
        #expect(evicted2 == 1)
    }

    @Test func pinWithExplicitCount() async throws {
        let broker = try tempDB()
        let root = cid("r1")
        try await broker.storeVolumeLocal(payload("r1"))
        try await broker.pin(root: root, owner: "chain-a", count: 3)

        try await broker.unpin(root: root, owner: "chain-a", count: 2)
        #expect(await broker.owners(root: root).count == 1, "count=1 remaining")

        try await broker.unpin(root: root, owner: "chain-a", count: 1)
        #expect(await broker.owners(root: root).isEmpty)
    }

    @Test func unpinMoreThanCountRemovesPin() async throws {
        let broker = try tempDB()
        let root = cid("r1")
        try await broker.storeVolumeLocal(payload("r1"))
        try await broker.pin(root: root, owner: "chain-a", count: 2)
        try await broker.unpin(root: root, owner: "chain-a", count: 5)
        #expect(await broker.owners(root: root).isEmpty)
    }

    @Test func ttlExpiredOwnerPrunedOnEvict() async throws {
        let broker = try tempDB()
        let root = cid("r1")
        try await broker.storeVolumeLocal(payload("r1"))
        try await broker.pin(root: root, owner: "chain-a:42", ttl: .zero)

        let evicted = try await broker.evictUnpinned()
        #expect(evicted == 1)
        #expect(await broker.hasVolume(root: root) == false)
    }

    @Test func mixedTTLAndPermanentOwners() async throws {
        let broker = try tempDB()
        let root = cid("r1")
        try await broker.storeVolumeLocal(payload("r1"))
        try await broker.pin(root: root, owner: "chain-a:42", ttl: .zero)
        try await broker.pin(root: root, owner: "chain-b:tip")

        let evicted = try await broker.evictUnpinned()
        #expect(evicted == 0)
        #expect(await broker.hasVolume(root: root))
        #expect(await broker.owners(root: root) == ["chain-b:tip"])
    }

    @Test func pinnedRootsByOwnerAndPrefix() async throws {
        let broker = try tempDB()
        let labels = ["exact-root", "height-root", "candidate-root", "expired-root", "foreign-root", "leaf-root"]
        try await broker.storeVolumesLocal(labels.map { payload($0) })
        try await broker.pin(root: cid("exact-root"), owner: "account:Nexus/A/Child")
        try await broker.pin(root: cid("height-root"), owner: "Nexus/A/Child:42")
        try await broker.pin(root: cid("candidate-root"), owner: "candidate:Nexus/A/Child:43")
        try await broker.pin(root: cid("expired-root"), owner: "Nexus/A/Child:44", ttl: .zero)
        try await broker.pin(root: cid("foreign-root"), owner: "Nexus/B/Child:42")
        try await broker.pin(root: cid("leaf-root"), owner: "Child:42")

        let roots = Set(await broker.pinnedRoots(
            owners: ["account:Nexus/A/Child"],
            ownerPrefixes: ["Nexus/A/Child:", "candidate:Nexus/A/Child:"]
        ))

        #expect(roots == [cid("exact-root"), cid("height-root"), cid("candidate-root")])
    }

    @Test func simultaneousStartSharedStateLinearizability() async throws {
        do {
            let location = try temporaryDatabase()
            defer { try? FileManager.default.removeItem(at: location.directory) }
            let broker = try DiskBroker(path: location.path)
            let volume = payload("pin-unpin")
            let owner = "shared-owner"
            try await broker.storeVolumeLocal(volume)
            try await broker.pin(root: volume.root, owner: owner)

            let (pin, unpin) = await Self.race(
                { await Self.capture { try await broker.pin(root: volume.root, owner: owner, count: 2) } },
                { await Self.capture { try await broker.unpin(root: volume.root, owner: owner, count: 2) } }
            )
            _ = try pin.get()
            _ = try unpin.get()

            // pin -> unpin leaves 1; unpin -> pin leaves 2.
            let count = try pinCount(at: location.path, root: volume.root, owner: owner)
            #expect([1, 2].contains(count), "pin/unpin count: \(count)")
            let health = try databaseHealth(at: location.path)
            #expect(health.integrity == "ok" && health.foreignKeyViolations == 0)
        }

        do {
            let location = try temporaryDatabase()
            defer { try? FileManager.default.removeItem(at: location.directory) }
            let broker = try DiskBroker(path: location.path)
            let volume = payload("store-pin")
            let owner = "shared-owner"

            let (store, pin) = await Self.race(
                { await Self.capture { try await broker.storeVolumeLocal(volume); return true } },
                { await Self.capture { try await broker.pin(root: volume.root, owner: owner); return true } }
            )
            _ = try store.get()
            let pinSucceeded: Bool
            switch pin {
            case .success:
                pinSucceeded = true
            case .failure(.notFound):
                pinSucceeded = false
            case .failure(let error):
                throw error
            }

            let observed = StorePinObservation(
                volumeExists: await broker.hasVolume(root: volume.root),
                pinned: await broker.owners(root: volume.root).contains(owner),
                pinSucceeded: pinSucceeded
            )
            let serialOutcomes = [
                StorePinObservation(volumeExists: true, pinned: true, pinSucceeded: true),
                StorePinObservation(volumeExists: true, pinned: false, pinSucceeded: false),
            ]
            #expect(serialOutcomes.contains(observed), "store/pin observed: \(observed)")
            let health = try databaseHealth(at: location.path)
            #expect(health.integrity == "ok" && health.foreignKeyViolations == 0)
        }

        do {
            let location = try temporaryDatabase()
            defer { try? FileManager.default.removeItem(at: location.directory) }
            let broker = try DiskBroker(path: location.path)
            let volumes = ["retained-a", "retained-b", "retained-c"].map { payload($0) }
            let scope = "shared-scope"
            try await broker.storeVolumesLocal(volumes)
            try await broker.advanceRetainedRoots(scope: scope, roots: [volumes[0].root])

            let (replace, merge) = await Self.race(
                { await Self.capture { try await broker.advanceRetainedRoots(scope: scope, roots: [volumes[1].root]); return true } },
                { await Self.capture { try await broker.mergeRetainedRoots(scope: scope, roots: [volumes[2].root]); return true } }
            )
            _ = try replace.get()
            _ = try merge.get()

            let observed = Set(try await broker.retainedRoots(scope: scope))
            let serialOutcomes: [Set<String>] = [
                [volumes[1].root],
                [volumes[1].root, volumes[2].root],
            ]
            #expect(serialOutcomes.contains(observed), "replace/merge observed: \(observed)")
            let health = try databaseHealth(at: location.path)
            #expect(health.integrity == "ok" && health.foreignKeyViolations == 0)
        }

        do {
            let location = try temporaryDatabase()
            defer { try? FileManager.default.removeItem(at: location.directory) }
            let broker = try DiskBroker(path: location.path, evictUnpinnedGraceSeconds: 0)
            let volume = payload("evict-pin")
            let owner = "shared-owner"
            try await broker.storeVolumeLocal(volume)

            let (eviction, pin) = await Self.race(
                { await Self.capture { try await broker.evictUnpinned() } },
                { await Self.capture { try await broker.pin(root: volume.root, owner: owner); return true } }
            )
            let evicted = try eviction.get()
            let pinSucceeded: Bool
            switch pin {
            case .success:
                pinSucceeded = true
            case .failure(.notFound):
                pinSucceeded = false
            case .failure(let error):
                throw error
            }

            let observed = EvictPinObservation(
                evicted: evicted,
                volumeExists: await broker.hasVolume(root: volume.root),
                pinned: await broker.owners(root: volume.root).contains(owner),
                pinSucceeded: pinSucceeded
            )
            let serialOutcomes = [
                EvictPinObservation(evicted: 0, volumeExists: true, pinned: true, pinSucceeded: true),
                EvictPinObservation(evicted: 1, volumeExists: false, pinned: false, pinSucceeded: false),
            ]
            #expect(serialOutcomes.contains(observed), "eviction/pin observed: \(observed)")
            let health = try databaseHealth(at: location.path)
            #expect(health.integrity == "ok" && health.foreignKeyViolations == 0)
        }
    }

    /// Regression: DiskBroker is shared across multiple ChainNetwork actors.
    /// Without serialising complete write transactions, two actors calling
    /// storeVolumeLocal concurrently can both pass SQLITE_OPEN_FULLMUTEX's
    /// per-call serialisation and then both issue BEGIN IMMEDIATE, causing the
    /// second to fail with "cannot start a transaction within a transaction".
    @Test("Concurrent writes from multiple actors do not produce nested-transaction errors")
    func testConcurrentWritesFromMultipleActors() async throws {
        let broker = try tempDB()

        // Simulate multiple ChainNetwork actors sharing the same DiskBroker.
        // Each actor calls storeVolumeLocal concurrently; the write executor
        // must serialise transactions so none overlap.
        let writeCount = 20
        try await withThrowingTaskGroup(of: Void.self) { group in
            for i in 0..<writeCount {
                group.addTask {
                    let p = self.payload("root-\(i)", ["cid-\(i)": Data("data-\(i)".utf8)])
                    try await broker.storeVolumeLocal(p)
                }
            }
            try await group.waitForAll()
        }

        // All writes must have committed — no "nested transaction" errors swallowed.
        for i in 0..<writeCount {
            let hasVolume = await broker.hasVolume(root: cid("root-\(i)"))
            #expect(hasVolume, "root-\(i) missing — write was lost")
        }
    }

    /// Same scenario for the batched write path used during block storage.
    @Test("Concurrent storeBatch calls do not produce nested-transaction errors")
    func testConcurrentStoreBatch() async throws {
        let broker = try tempDB()

        try await withThrowingTaskGroup(of: Void.self) { group in
            for i in 0..<20 {
                group.addTask {
                    let payloads = (0..<5).map { j in
                        self.payload("batch-\(i)-\(j)", ["c-\(i)-\(j)": Data()])
                    }
                    try await broker.storeVolumesLocal(payloads)
                }
            }
            try await group.waitForAll()
        }

        for i in 0..<20 {
            for j in 0..<5 {
                let hasVolume = await broker.hasVolume(root: cid("batch-\(i)-\(j)"))
                #expect(hasVolume, "batch-\(i)-\(j) missing")
            }
        }
    }
}

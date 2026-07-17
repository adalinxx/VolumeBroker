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

/// Independent unit tests for the `PinIndex` collaborator.
///
/// These construct `PinIndex` directly from a bare `SQLiteConnection` rather
/// than going through the `DiskBroker` façade, so pin-count and TTL semantics
/// are pinned to the collaborator's own surface.
@Suite("PinIndex")
struct PinIndexTests {

    private func tempIndex(_ labels: [String] = ["r1"]) async throws -> (pins: PinIndex, roots: [String: String]) {
        let path = NSTemporaryDirectory() + "vb_pinindex_\(UUID().uuidString).sqlite"
        let connection = try SQLiteConnection(path: path)
        var roots: [String: String] = [:]
        let volumes = try labels.map { label in
            let data = Data(label.utf8)
            let multihash = try Multihash(raw: data, hashedWith: .sha2_256)
            let root = try CID(
                version: .v1,
                codec: .dag_cbor,
                multihash: multihash
            ).toBaseEncodedString
            roots[label] = root
            return SerializedVolume(root: root, entries: [root: data])
        }
        try await CASVolumeStore(connection: connection).storeVolumesLocal(volumes)
        return (PinIndex(connection: connection), roots)
    }

    /// A single pin establishes exactly one owner.
    @Test func pinEstablishesOwner() async throws {
        let h = try await tempIndex()
        let root = try #require(h.roots["r1"])
        try await h.pins.pin(root: root, owner: "owner-a", count: 1, ttl: nil)
        #expect(await h.pins.owners(root: root) == ["owner-a"])
    }

    @Test func pinRequiresACompleteLocalVolume() async throws {
        let pins = try await tempIndex([]).pins
        do {
            try await pins.pin(root: "missing", owner: "owner-a", count: 1, ttl: nil)
            Issue.record("a pin must not create ownership for a missing Volume")
        } catch {
            #expect(error as? BrokerError == .notFound)
        }
        #expect(await pins.owners(root: "missing").isEmpty)
    }

    @Test func pinCountsMustBePositiveAndCannotOverflow() async throws {
        let h = try await tempIndex()
        let root = try #require(h.roots["r1"])
        for count in [0, -1] {
            do {
                try await h.pins.pin(root: root, owner: "owner-a", count: count, ttl: nil)
                Issue.record("nonpositive pin count must fail")
            } catch {
                #expect(error as? BrokerError == .invalidPinCount)
            }
        }

        try await h.pins.pin(root: root, owner: "owner-a", count: .max, ttl: nil)
        await #expect(throws: (any Error).self) {
            try await h.pins.pin(root: root, owner: "owner-a", count: 1, ttl: nil)
        }
        for count in [0, -1] {
            do {
                try await h.pins.unpin(root: root, owner: "owner-a", count: count)
                Issue.record("nonpositive unpin count must fail")
            } catch {
                #expect(error as? BrokerError == .invalidPinCount)
            }
        }
        #expect(await h.pins.owners(root: root) == ["owner-a"])
    }

    @Test func fractionalTTLIsPersisted() async throws {
        let h = try await tempIndex()
        let root = try #require(h.roots["r1"])
        let before = Date.now
        try await h.pins.pin(
            root: root,
            owner: "owner-a",
            count: 1,
            ttl: .milliseconds(500)
        )
        let after = Date.now
        let persisted = await h.pins.connection.read {
            var statement: OpaquePointer?
            defer { sqlite3_finalize(statement) }
            guard sqlite3_prepare_v2(
                h.pins.connection.readDb,
                "SELECT expires_at FROM volume_pins WHERE root=?1 AND owner='owner-a'",
                -1,
                &statement,
                nil
            ) == SQLITE_OK, let statement else { return nil as String? }
            sqlite3_bind_text(statement, 1, root, -1, SQLITE_TRANSIENT_SHIM)
            guard sqlite3_step(statement) == SQLITE_ROW,
                  let value = sqlite3_column_text(statement, 0) else { return nil }
            return String(cString: value)
        }
        let expiration = try #require(persisted.flatMap(SQLiteConnection.isoFormatter.date(from:)))
        #expect(expiration.timeIntervalSince(before) >= 0.49)
        #expect(expiration.timeIntervalSince(after) <= 0.51)
    }

    /// Pin counts accumulate per (root, owner); the pin only clears after an
    /// equal number of single unpins.
    @Test func pinCountAccumulatesAndDrains() async throws {
        let h = try await tempIndex()
        let root = try #require(h.roots["r1"])
        try await h.pins.pin(root: root, owner: "owner-a", count: 1, ttl: nil)
        try await h.pins.pin(root: root, owner: "owner-a", count: 1, ttl: nil)
        #expect(await h.pins.owners(root: root) == ["owner-a"], "two pins, one owner")

        try await h.pins.unpin(root: root, owner: "owner-a", count: 1)
        #expect(await h.pins.owners(root: root) == ["owner-a"], "count=1 remains after one unpin")

        try await h.pins.unpin(root: root, owner: "owner-a", count: 1)
        #expect(await h.pins.owners(root: root).isEmpty, "second unpin clears the pin")
    }

    /// An explicit count pins N references in one call and drains in counted steps.
    @Test func explicitCountPinsAndDrains() async throws {
        let h = try await tempIndex()
        let root = try #require(h.roots["r1"])
        try await h.pins.pin(root: root, owner: "owner-a", count: 3, ttl: nil)

        try await h.pins.unpin(root: root, owner: "owner-a", count: 2)
        #expect(await h.pins.owners(root: root) == ["owner-a"], "count=1 remaining")

        try await h.pins.unpin(root: root, owner: "owner-a", count: 1)
        #expect(await h.pins.owners(root: root).isEmpty)
    }

    /// Unpinning more than the live count removes the pin (count clamps at 0,
    /// never negative).
    @Test func overUnpinRemovesPin() async throws {
        let h = try await tempIndex()
        let root = try #require(h.roots["r1"])
        try await h.pins.pin(root: root, owner: "owner-a", count: 2, ttl: nil)
        try await h.pins.unpin(root: root, owner: "owner-a", count: 5)
        #expect(await h.pins.owners(root: root).isEmpty)
    }

    /// Distinct owners hold independent ref-counts on the same root.
    @Test func ownersAreIndependent() async throws {
        let h = try await tempIndex()
        let root = try #require(h.roots["r1"])
        try await h.pins.pin(root: root, owner: "owner-a", count: 1, ttl: nil)
        try await h.pins.pin(root: root, owner: "owner-b", count: 1, ttl: nil)
        #expect(await h.pins.owners(root: root) == ["owner-a", "owner-b"])

        try await h.pins.unpin(root: root, owner: "owner-a", count: 1)
        #expect(await h.pins.owners(root: root) == ["owner-b"], "owner-b's pin is untouched")
    }

    /// A TTL-expired owner is hidden from live-owner queries even before
    /// eviction runs.
    @Test func expiredOwnerIsNotLive() async throws {
        let h = try await tempIndex()
        let root = try #require(h.roots["r1"])
        try await h.pins.pin(root: root, owner: "owner-a", count: 1, ttl: .zero)
        #expect(await h.pins.owners(root: root).isEmpty, "a zero-TTL pin is immediately stale")
    }

    @Test func pinnedOwnersByPrefix() async throws {
        let h = try await tempIndex(["r1", "r2", "r3", "r4"])
        try await h.pins.pin(root: try #require(h.roots["r1"]), owner: "candidate:ns:5", count: 1, ttl: nil)
        try await h.pins.pin(root: try #require(h.roots["r2"]), owner: "candidate:ns:6", count: 1, ttl: nil)
        try await h.pins.pin(root: try #require(h.roots["r3"]), owner: "ns:5", count: 1, ttl: nil)
        try await h.pins.pin(root: try #require(h.roots["r4"]), owner: "candidate:ns:7", count: 1, ttl: .zero)

        let owners = Set(await h.pins.pinnedOwners(prefix: "candidate:ns:"))
        #expect(owners == ["candidate:ns:5", "candidate:ns:6"])
    }

    @Test func batchAPIsHandleEmptySuccessDuplicatesAndUnfilteredRoots() async throws {
        let h = try await tempIndex(["r1", "r2"])
        let r1 = try #require(h.roots["r1"])
        let r2 = try #require(h.roots["r2"])

        try await h.pins.pinBatch(roots: [], owner: "owner-a")
        try await h.pins.unpinBatch(items: [])
        try await h.pins.unpinAllBatch(owners: [])
        #expect(await h.pins.pinnedRoots().isEmpty)

        try await h.pins.pinBatch(roots: [r1, r1, r2], owner: "owner-a")
        #expect(Set(await h.pins.pinnedRoots()) == [r1, r2])

        try await h.pins.unpinBatch(items: [
            (root: r1, owner: "owner-a", count: 1),
            (root: r2, owner: "owner-a", count: 1),
        ])
        #expect(await h.pins.owners(root: r1) == ["owner-a"])
        #expect(await h.pins.owners(root: r2).isEmpty)

        try await h.pins.unpinBatch(items: [(root: r1, owner: "owner-a", count: 1)])
        #expect(await h.pins.pinnedRoots().isEmpty)
    }

    @Test func batchValidationIsAtomic() async throws {
        let h = try await tempIndex(["r1", "r2"])
        let r1 = try #require(h.roots["r1"])
        let r2 = try #require(h.roots["r2"])
        try await h.pins.pin(root: r1, owner: "existing", count: 2, ttl: nil)
        try await h.pins.pin(root: r2, owner: "existing", count: 2, ttl: nil)

        await #expect(throws: BrokerError.notFound) {
            try await h.pins.pinBatch(roots: [r1, "missing"], owner: "batch")
        }
        #expect(await h.pins.owners(root: r1) == ["existing"])

        await #expect(throws: BrokerError.invalidPinCount) {
            try await h.pins.unpinBatch(items: [
                (root: r1, owner: "existing", count: 1),
                (root: r2, owner: "existing", count: 0),
            ])
        }
        try await h.pins.unpinBatch(items: [
            (root: r1, owner: "existing", count: 1),
            (root: r2, owner: "existing", count: 1),
        ])
        #expect(await h.pins.owners(root: r1) == ["existing"])
        #expect(await h.pins.owners(root: r2) == ["existing"])
    }

    @Test func pinBatchReplacesExpiredCountAndOwnersRemainIndependent() async throws {
        let h = try await tempIndex(["r1", "r2"])
        let r1 = try #require(h.roots["r1"])
        let r2 = try #require(h.roots["r2"])
        try await h.pins.pin(root: r1, owner: "owner-a", count: 3, ttl: .zero)
        try await h.pins.pinBatch(roots: [r1, r2], owner: "owner-a")
        try await h.pins.pinBatch(roots: [r1, r2], owner: "owner-b")

        try await h.pins.unpin(root: r1, owner: "owner-a", count: 1)
        #expect(await h.pins.owners(root: r1) == ["owner-b"])
        #expect(await h.pins.owners(root: r2) == ["owner-a", "owner-b"])

        try await h.pins.unpinAllBatch(owners: ["owner-a", "owner-a"])
        #expect(await h.pins.owners(root: r2) == ["owner-b"])
        try await h.pins.unpinAllBatch(owners: ["owner-b"])
        #expect(await h.pins.pinnedRoots().isEmpty)
    }

    @Test func batchPinsPersistAcrossReopen() async throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("VolumeBrokerPinIndex-\(UUID().uuidString)")
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let path = directory.appendingPathComponent("volumes.sqlite").path
        let labels = ["r1", "r2", "expired"]
        let roots = Dictionary(uniqueKeysWithValues: try labels.map { label in
            let data = Data(label.utf8)
            return (label, try CID(
                version: .v1,
                codec: .dag_cbor,
                multihash: try Multihash(raw: data, hashedWith: .sha2_256)
            ).toBaseEncodedString)
        })
        let r1 = try #require(roots["r1"])
        let r2 = try #require(roots["r2"])
        let expired = try #require(roots["expired"])

        do {
            let connection = try SQLiteConnection(path: path)
            let store = CASVolumeStore(connection: connection)
            try await store.storeVolumesLocal(labels.map { label in
                let root = roots[label]!
                return SerializedVolume(root: root, entries: [root: Data(label.utf8)])
            })
            let pins = PinIndex(connection: connection)
            try await pins.pinBatch(
                roots: [r1, r1, r2],
                owner: "owner-a"
            )
            try await pins.pin(root: expired, owner: "expired", count: 1, ttl: .zero)
        }

        let reopened = PinIndex(connection: try SQLiteConnection(path: path))
        #expect(Set(await reopened.pinnedRoots()) == [r1, r2])
        try await reopened.unpinBatch(items: [
            (root: r1, owner: "owner-a", count: 1),
            (root: r2, owner: "owner-a", count: 1),
        ])
        #expect(await reopened.owners(root: r1) == ["owner-a"])
        #expect(await reopened.owners(root: r2).isEmpty)
        try await reopened.unpinAllBatch(owners: ["owner-a"])
        #expect(await reopened.pinnedRoots().isEmpty)
    }
}

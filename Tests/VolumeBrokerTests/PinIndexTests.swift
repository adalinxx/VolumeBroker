import Testing
import Foundation
import CID
import Multihash
@testable import VolumeBroker

/// Independent unit tests for the `PinIndex` collaborator.
///
/// These construct `PinIndex` directly from a bare `SQLiteConnection` rather
/// than going through the `DiskBroker` façade, so pin-count / TTL / idempotency
/// semantics are pinned to the collaborator's own surface.
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

    @Test func fractionalTTLDoesNotExpireImmediately() async throws {
        let h = try await tempIndex()
        let root = try #require(h.roots["r1"])
        try await h.pins.pin(
            root: root,
            owner: "owner-a",
            count: 1,
            ttl: .milliseconds(500)
        )
        #expect(await h.pins.owners(root: root) == ["owner-a"])
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

    /// `unpinBatchOnce` is idempotent per operation id: replaying the same id
    /// must not decrement twice, but a fresh id still applies.
    @Test func unpinBatchOnceIsIdempotent() async throws {
        let h = try await tempIndex()
        let root = try #require(h.roots["r1"])
        try await h.pins.pin(root: root, owner: "owner-a", count: 2, ttl: nil)

        let items = [(root: root, owner: "owner-a", count: 1)]
        try await h.pins.unpinBatchOnce(operationID: "op-1", items: items)
        #expect(await h.pins.owners(root: root) == ["owner-a"], "first apply leaves count=1")

        try await h.pins.unpinBatchOnce(operationID: "op-1", items: items)
        #expect(await h.pins.owners(root: root) == ["owner-a"], "replay of op-1 is a no-op")

        try await h.pins.unpinBatchOnce(operationID: "op-2", items: items)
        #expect(await h.pins.owners(root: root).isEmpty, "a distinct op id still decrements")
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

    @Test func deleteUnpinOperationsBelowHeightByPrefix() async throws {
        let pins = try await tempIndex([]).pins
        let item = (root: "missing", owner: "owner", count: 1)
        try await pins.unpinBatchOnce(operationID: "prune:candidate:ns:5", items: [item])
        try await pins.unpinBatchOnce(operationID: "prune:candidate:ns:6", items: [item])
        try await pins.unpinBatchOnce(operationID: "prune:candidate:ns:not-a-height", items: [item])
        try await pins.unpinBatchOnce(operationID: "prune:other:4", items: [item])

        let deleted = try await pins.deleteUnpinOperations(belowHeight: 6, prefix: "prune:candidate:ns:")
        #expect(deleted == 1)
        #expect(await pins.unpinOperationCount(prefix: "prune:candidate:ns:") == 2)
        #expect(await pins.unpinOperationCount(prefix: "prune:other:") == 1)

        let bulkDeleted = try await pins.deleteUnpinOperations(prefix: "prune:candidate:ns:")
        #expect(bulkDeleted == 2)
        #expect(await pins.unpinOperationCount(prefix: "prune:candidate:ns:") == 0)
    }
}

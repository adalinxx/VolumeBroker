import Testing
import Foundation
@testable import VolumeBroker

/// Independent unit tests for the `PinIndex` collaborator.
///
/// These construct `PinIndex` directly from a bare `SQLiteConnection` rather
/// than going through the `DiskBroker` façade, so pin-count / TTL / idempotency
/// semantics are pinned to the collaborator's own surface.
@Suite("PinIndex")
struct PinIndexTests {

    private func tempIndex() throws -> PinIndex {
        let path = NSTemporaryDirectory() + "vb_pinindex_\(UUID().uuidString).sqlite"
        return PinIndex(connection: try SQLiteConnection(path: path))
    }

    /// A single pin establishes exactly one owner.
    @Test func pinEstablishesOwner() async throws {
        let pins = try tempIndex()
        try await pins.pin(root: "r1", owner: "owner-a", count: 1, ttl: nil)
        #expect(await pins.owners(root: "r1") == ["owner-a"])
    }

    /// Pin counts accumulate per (root, owner); the pin only clears after an
    /// equal number of single unpins.
    @Test func pinCountAccumulatesAndDrains() async throws {
        let pins = try tempIndex()
        try await pins.pin(root: "r1", owner: "owner-a", count: 1, ttl: nil)
        try await pins.pin(root: "r1", owner: "owner-a", count: 1, ttl: nil)
        #expect(await pins.owners(root: "r1") == ["owner-a"], "two pins, one owner")

        try await pins.unpin(root: "r1", owner: "owner-a", count: 1)
        #expect(await pins.owners(root: "r1") == ["owner-a"], "count=1 remains after one unpin")

        try await pins.unpin(root: "r1", owner: "owner-a", count: 1)
        #expect(await pins.owners(root: "r1").isEmpty, "second unpin clears the pin")
    }

    /// An explicit count pins N references in one call and drains in counted steps.
    @Test func explicitCountPinsAndDrains() async throws {
        let pins = try tempIndex()
        try await pins.pin(root: "r1", owner: "owner-a", count: 3, ttl: nil)

        try await pins.unpin(root: "r1", owner: "owner-a", count: 2)
        #expect(await pins.owners(root: "r1") == ["owner-a"], "count=1 remaining")

        try await pins.unpin(root: "r1", owner: "owner-a", count: 1)
        #expect(await pins.owners(root: "r1").isEmpty)
    }

    /// Unpinning more than the live count removes the pin (count clamps at 0,
    /// never negative).
    @Test func overUnpinRemovesPin() async throws {
        let pins = try tempIndex()
        try await pins.pin(root: "r1", owner: "owner-a", count: 2, ttl: nil)
        try await pins.unpin(root: "r1", owner: "owner-a", count: 5)
        #expect(await pins.owners(root: "r1").isEmpty)
    }

    /// Distinct owners hold independent ref-counts on the same root.
    @Test func ownersAreIndependent() async throws {
        let pins = try tempIndex()
        try await pins.pin(root: "r1", owner: "owner-a", count: 1, ttl: nil)
        try await pins.pin(root: "r1", owner: "owner-b", count: 1, ttl: nil)
        #expect(await pins.owners(root: "r1") == ["owner-a", "owner-b"])

        try await pins.unpin(root: "r1", owner: "owner-a", count: 1)
        #expect(await pins.owners(root: "r1") == ["owner-b"], "owner-b's pin is untouched")
    }

    /// A TTL-expired owner is hidden from live-owner queries even before
    /// eviction runs.
    @Test func expiredOwnerIsNotLive() async throws {
        let pins = try tempIndex()
        try await pins.pin(root: "r1", owner: "owner-a", count: 1, ttl: .zero)
        #expect(await pins.owners(root: "r1").isEmpty, "a zero-TTL pin is immediately stale")
    }

    /// `unpinBatchOnce` is idempotent per operation id: replaying the same id
    /// must not decrement twice, but a fresh id still applies.
    @Test func unpinBatchOnceIsIdempotent() async throws {
        let pins = try tempIndex()
        try await pins.pin(root: "r1", owner: "owner-a", count: 2, ttl: nil)

        let items = [(root: "r1", owner: "owner-a", count: 1)]
        try await pins.unpinBatchOnce(operationID: "op-1", items: items)
        #expect(await pins.owners(root: "r1") == ["owner-a"], "first apply leaves count=1")

        try await pins.unpinBatchOnce(operationID: "op-1", items: items)
        #expect(await pins.owners(root: "r1") == ["owner-a"], "replay of op-1 is a no-op")

        try await pins.unpinBatchOnce(operationID: "op-2", items: items)
        #expect(await pins.owners(root: "r1").isEmpty, "a distinct op id still decrements")
    }

    @Test func pinnedOwnersByPrefix() async throws {
        let pins = try tempIndex()
        try await pins.pin(root: "r1", owner: "candidate:ns:5", count: 1, ttl: nil)
        try await pins.pin(root: "r2", owner: "candidate:ns:6", count: 1, ttl: nil)
        try await pins.pin(root: "r3", owner: "ns:5", count: 1, ttl: nil)
        try await pins.pin(root: "r4", owner: "candidate:ns:7", count: 1, ttl: .zero)

        let owners = Set(await pins.pinnedOwners(prefix: "candidate:ns:"))
        #expect(owners == ["candidate:ns:5", "candidate:ns:6"])
    }

    @Test func deleteUnpinOperationsBelowHeightByPrefix() async throws {
        let pins = try tempIndex()
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

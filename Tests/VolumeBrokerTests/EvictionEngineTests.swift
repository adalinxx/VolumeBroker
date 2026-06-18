import Testing
import Foundation
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
        let store: CASVolumeStore
        let pins: PinIndex
        let eviction: EvictionEngine
        let negativeCache: NegativeCache
    }

    private func harness() throws -> Harness {
        let path = NSTemporaryDirectory() + "vb_eviction_\(UUID().uuidString).sqlite"
        let connection = try SQLiteConnection(path: path)
        let negativeCache = NegativeCache()
        return Harness(
            store: CASVolumeStore(connection: connection, negativeCache: negativeCache),
            pins: PinIndex(connection: connection),
            eviction: EvictionEngine(connection: connection, negativeCache: negativeCache),
            negativeCache: negativeCache
        )
    }

    private func volume(_ root: String, _ entries: [String: Data] = [:]) -> SerializedVolume {
        SerializedVolume(root: root, entries: entries.isEmpty ? [root: Data(root.utf8)] : entries)
    }

    /// An unpinned root and its data are reclaimed; the return value counts it.
    @Test func unpinnedRootIsReclaimed() async throws {
        let h = try harness()
        try await h.store.storeVolumeLocal(volume("drop"))

        let evicted = try await h.eviction.evictUnpinned(graceSeconds: 0)
        #expect(evicted == 1)
        #expect(await h.store.hasVolume(root: "drop") == false)
        #expect(await h.store.fetchVolumeLocal(root: "drop") == nil)
    }

    /// A pinned root survives; an unpinned sibling is reclaimed in the same pass.
    @Test func pinnedRootSurvivesWhileSiblingIsReclaimed() async throws {
        let h = try harness()
        try await h.store.storeVolumeLocal(volume("keep"))
        try await h.store.storeVolumeLocal(volume("drop"))
        try await h.pins.pin(root: "keep", owner: "owner", count: 1, ttl: nil)

        let evicted = try await h.eviction.evictUnpinned(graceSeconds: 0)
        #expect(evicted == 1, "only the unpinned root counts")
        #expect(await h.store.hasVolume(root: "keep"))
        #expect(await h.store.hasVolume(root: "drop") == false)
    }

    /// A CAS blob shared between a pinned and an unpinned root is retained even
    /// after the unpinned root is reclaimed.
    @Test func sharedBlobSurvivesPartialEviction() async throws {
        let h = try harness()
        let shared = Data([0xAB, 0xCD])
        try await h.store.storeVolumeLocal(volume("keep", ["shared": shared, "onlyKeep": Data([1])]))
        try await h.store.storeVolumeLocal(volume("drop", ["shared": shared, "onlyDrop": Data([2])]))
        try await h.pins.pin(root: "keep", owner: "owner", count: 1, ttl: nil)

        _ = try await h.eviction.evictUnpinned(graceSeconds: 0)
        #expect(await h.store.fetchVolumeLocal(root: "keep")?.entries["shared"] == shared,
                "the shared blob must remain available to the pinned root")
        #expect(await h.store.hasVolume(root: "drop") == false)
    }

    /// A root with a remaining live pin is never reclaimed (returns 0).
    @Test func livePinBlocksEviction() async throws {
        let h = try harness()
        try await h.store.storeVolumeLocal(volume("r1"))
        try await h.pins.pin(root: "r1", owner: "owner-a", count: 1, ttl: nil)
        try await h.pins.pin(root: "r1", owner: "owner-b", count: 1, ttl: nil)

        try await h.pins.unpin(root: "r1", owner: "owner-a", count: 1)
        let evicted = try await h.eviction.evictUnpinned(graceSeconds: 0)
        #expect(evicted == 0, "owner-b still pins the root")
        #expect(await h.store.hasVolume(root: "r1"))
    }

    /// Eviction first prunes TTL-expired pins, then reclaims the now-unpinned root.
    @Test func ttlExpiredPinIsPrunedThenReclaimed() async throws {
        let h = try harness()
        try await h.store.storeVolumeLocal(volume("r1"))
        try await h.pins.pin(root: "r1", owner: "owner-a", count: 1, ttl: .zero)

        let evicted = try await h.eviction.evictUnpinned(graceSeconds: 0)
        #expect(evicted == 1)
        #expect(await h.store.hasVolume(root: "r1") == false)
    }

    /// Serve-gate / eviction predicate alignment: an expired-TTL pin is dead on
    /// BOTH sides — `isPinReachable` refuses to serve it, and `evictUnpinned`
    /// does not protect it (the root is reclaimed).
    @Test func expiredPinNeitherServesNorProtects() async throws {
        let h = try harness()
        try await h.store.storeVolumeLocal(volume("r1"))
        try await h.pins.pin(root: "r1", owner: "owner-a", count: 1, ttl: .zero)

        #expect(await h.pins.isPinReachable(cid: "r1") == false,
                "the serve gate must not serve an expired pin")
        let evicted = try await h.eviction.evictUnpinned(graceSeconds: 0)
        #expect(evicted == 1, "an expired pin must not protect from eviction")
        #expect(await h.store.hasVolume(root: "r1") == false)
    }

    /// Serve-gate / eviction predicate alignment: a live pin is live on BOTH
    /// sides — `isPinReachable` serves it, and `evictUnpinned` protects it.
    @Test func livePinServesAndProtects() async throws {
        let h = try harness()
        try await h.store.storeVolumeLocal(volume("r1"))
        try await h.pins.pin(root: "r1", owner: "owner-a", count: 1, ttl: nil)

        #expect(await h.pins.isPinReachable(cid: "r1"))
        let evicted = try await h.eviction.evictUnpinned(graceSeconds: 0)
        #expect(evicted == 0)
        #expect(await h.store.hasVolume(root: "r1"))
        #expect(await h.pins.isPinReachable(cid: "r1"), "still served after the sweep")
    }

    /// A pin row CAN linger with count <= 0 (`pin(count: 0)` inserts one; only
    /// the unpin paths delete such rows). The serve gate ignores it, so the
    /// eviction seed must too — otherwise eviction would protect content the
    /// gate never serves, and the two "live pin" predicates would drift.
    @Test func zeroCountPinRowNeitherServesNorProtects() async throws {
        let h = try harness()
        try await h.store.storeVolumeLocal(volume("r1"))
        try await h.pins.pin(root: "r1", owner: "owner-a", count: 0, ttl: nil)

        #expect(await h.pins.owners(root: "r1") == ["owner-a"],
                "the count=0 row really lingers (owners() filters TTL only)")
        #expect(await h.pins.isPinReachable(cid: "r1") == false,
                "the serve gate must not serve a count<=0 row")
        let evicted = try await h.eviction.evictUnpinned(graceSeconds: 0)
        #expect(evicted == 1, "a count<=0 row must not protect from eviction")
        #expect(await h.store.hasVolume(root: "r1") == false)
    }

    /// Freshly stored unpinned content survives the periodic sweep long enough
    /// for a follow-up pin intent, then evicts normally once grace is disabled.
    @Test func evictRespectsStoreThenPinGrace() async throws {
        let h = try harness()
        try await h.store.storeVolumeLocal(volume("V"))

        let graceEvicted = try await h.eviction.evictUnpinned(graceSeconds: 60 * 60)
        #expect(graceEvicted == 0)
        #expect(await h.store.hasVolume(root: "V"))

        try await h.pins.pin(root: "V", owner: "owner", count: 1, ttl: nil)
        let pinnedEvicted = try await h.eviction.evictUnpinned(graceSeconds: 0)
        #expect(pinnedEvicted == 0)
        #expect(await h.store.hasVolume(root: "V"))

        try await h.pins.unpin(root: "V", owner: "owner", count: 1)
        let finalEvicted = try await h.eviction.evictUnpinned(graceSeconds: 0)
        #expect(finalEvicted == 1)
        #expect(await h.store.hasVolume(root: "V") == false)
    }

    /// Evicting a root releases its durable known-present negative-cache entry.
    @Test func evictedRootClearsKnownPresent() async throws {
        let h = try harness()
        #expect(await h.store.hasVolume(root: "r1") == false)
        try await h.store.storeVolumeLocal(volume("r1"))
        #expect(h.negativeCache.mightBeAbsent("r1") == false)

        let evicted = try await h.eviction.evictUnpinned(graceSeconds: 0)
        #expect(evicted == 1)
        #expect(h.negativeCache.mightBeAbsent("r1"),
                "after eviction, the prior absent bloom verdict is authoritative again")
    }

    /// Collect the volumes a `BrokerStorer` produces for a nested store, exactly
    /// as cashew's `storeRecursively` drives it (enter/store/exit). The storer
    /// records the owned-child edges (parent volume gains the child root entry),
    /// so this returns the REAL reachability graph — not a hand-built one.
    private func storeReal(_ h: Harness, build: (BrokerStorer) throws -> Void, root: String) async throws {
        let storer = BrokerStorer(broker: MemoryBroker())
        try build(storer)
        for vol in storer.collectVolumes(root: root) {
            try await h.store.storeVolumeLocal(vol)
        }
    }

    /// Pinning an object root transitively protects its NESTED volume closure:
    /// `obj` owns the child volume `nested`, which holds the leaf `deep`. A
    /// one-level pin would protect only `obj`'s direct entries and reclaim
    /// `nested`/`deep`; transitive reachability keeps the whole closure. Releasing
    /// the only pin then makes the whole closure evictable. (The retain/release
    /// correctness point for per-node state.)
    ///
    /// The graph is produced by the REAL `BrokerStorer` (owned-child edges), so
    /// this proves the store path writes the edges transitive eviction walks —
    /// not just that the engine handles a hand-built graph.
    @Test func pinnedRootTransitivelyProtectsNestedVolumeClosure() async throws {
        let h = try harness()
        try await storeReal(h, build: { s in
            try s.enterVolume(rootCID: "obj")
            try s.store(rawCid: "obj", data: Data("obj".utf8))
            try s.enterVolume(rootCID: "nested")
            try s.store(rawCid: "nested", data: Data("nested".utf8))
            try s.store(rawCid: "deep", data: Data("deep".utf8))
            try s.exitVolume(rootCID: "nested")
            try s.exitVolume(rootCID: "obj")
        }, root: "obj")
        try await h.pins.pin(root: "obj", owner: "o", count: 1, ttl: nil)

        let evicted = try await h.eviction.evictUnpinned(graceSeconds: 0)
        #expect(evicted == 0, "nested closure is transitively protected by the pin on obj")
        #expect(await h.store.hasVolume(root: "obj"))
        #expect(await h.store.hasVolume(root: "nested"), "nested root kept transitively")
        #expect(await h.store.fetchDataLocal(cid: "deep") != nil, "deep leaf kept transitively")

        try await h.pins.unpinAll(owner: "o")
        let evicted2 = try await h.eviction.evictUnpinned(graceSeconds: 0)
        #expect(evicted2 == 2, "obj + nested reclaimed once the pin is released")
        #expect(await h.store.fetchDataLocal(cid: "deep") == nil)
    }

    /// Transitive arm of the aligned predicate: a DEAD pin (count <= 0) on a
    /// nested-closure root protects nothing — not the root, not the nested
    /// sub-volume, not the deep leaf — AND the serve gate refuses the deep leaf.
    /// This is the one combination the flat predicate tests don't reach: the new
    /// `count > 0` seed must propagate through the recursive closure walk, so a
    /// dead parent pin leaves the whole subtree both unservable and reclaimable.
    @Test func deadPinDoesNotProtectNestedClosureTransitively() async throws {
        let h = try harness()
        try await storeReal(h, build: { s in
            try s.enterVolume(rootCID: "obj")
            try s.store(rawCid: "obj", data: Data("obj".utf8))
            try s.enterVolume(rootCID: "nested")
            try s.store(rawCid: "nested", data: Data("nested".utf8))
            try s.store(rawCid: "deep", data: Data("deep".utf8))
            try s.exitVolume(rootCID: "nested")
            try s.exitVolume(rootCID: "obj")
        }, root: "obj")
        // A count==0 pin row lingers (only unpin deletes it), but is dead.
        try await h.pins.pin(root: "obj", owner: "o", count: 0, ttl: nil)

        #expect(await h.pins.isPinReachable(cid: "deep") == false,
                "the serve gate must not serve a deep leaf under a dead parent pin")
        let evicted = try await h.eviction.evictUnpinned(graceSeconds: 0)
        #expect(evicted == 2, "obj + nested reclaimed: a dead pin protects no descendant")
        #expect(await h.store.hasVolume(root: "obj") == false)
        #expect(await h.store.hasVolume(root: "nested") == false)
        #expect(await h.store.fetchDataLocal(cid: "deep") == nil)
    }

    /// A block-shaped owned closure, produced by the real storer: the block
    /// volume owns a transactions group (with a tx-body sub-volume), a postState
    /// frontier (with a trie-node sub-volume), and a child block — siblings at
    /// depth, nested boundaries within. Pinning the block root protects the
    /// entire closure; releasing it reclaims every volume in one pass.
    @Test func pinnedBlockRootProtectsWholeOwnedClosure() async throws {
        let h = try harness()
        try await storeReal(h, build: { s in
            try s.enterVolume(rootCID: "block")
            try s.store(rawCid: "block", data: Data("block".utf8))
            try s.store(rawCid: "txDict", data: Data("txDict".utf8))   // in-package header entry
            // a transaction body sub-volume
            try s.enterVolume(rootCID: "txBody")
            try s.store(rawCid: "txBody", data: Data("txBody".utf8))
            try s.exitVolume(rootCID: "txBody")
            // postState frontier with a nested trie node
            try s.enterVolume(rootCID: "postState")
            try s.store(rawCid: "postState", data: Data("postState".utf8))
            try s.enterVolume(rootCID: "trieNode")
            try s.store(rawCid: "trieNode", data: Data("trieNode".utf8))
            try s.exitVolume(rootCID: "trieNode")
            try s.exitVolume(rootCID: "postState")
            // a child block
            try s.enterVolume(rootCID: "childBlock")
            try s.store(rawCid: "childBlock", data: Data("childBlock".utf8))
            try s.exitVolume(rootCID: "childBlock")
            try s.exitVolume(rootCID: "block")
        }, root: "block")
        try await h.pins.pin(root: "block", owner: "h:1", count: 1, ttl: nil)

        let evicted = try await h.eviction.evictUnpinned(graceSeconds: 0)
        #expect(evicted == 0, "entire owned closure protected by the single block-root pin")
        for cid in ["block", "txDict", "txBody", "postState", "trieNode", "childBlock"] {
            #expect(await h.store.fetchDataLocal(cid: cid) != nil, "\(cid) kept transitively")
        }

        try await h.pins.unpinAll(owner: "h:1")
        let evicted2 = try await h.eviction.evictUnpinned(graceSeconds: 0)
        #expect(evicted2 == 5, "block + 4 sub-volumes reclaimed (txDict is in-package, not a root)")
        for cid in ["block", "txBody", "postState", "trieNode", "childBlock"] {
            #expect(await h.store.fetchDataLocal(cid: cid) == nil, "\(cid) reclaimed")
        }
    }
}

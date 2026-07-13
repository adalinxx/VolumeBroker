import Testing
import Foundation
import CID
import Multihash
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
        try await h.store.storeVolumeLocal(volume("drop"))

        let evicted = try await h.eviction.evictUnpinned(graceSeconds: 0)
        #expect(evicted == 1)
        #expect(await h.store.hasVolume(root: cid("drop")) == false)
        #expect(await h.store.fetchVolumeLocal(root: cid("drop")) == nil)
    }

    /// A pinned root survives; an unpinned sibling is reclaimed in the same pass.
    @Test func pinnedRootSurvivesWhileSiblingIsReclaimed() async throws {
        let h = try harness()
        try await h.store.storeVolumeLocal(volume("keep"))
        try await h.store.storeVolumeLocal(volume("drop"))
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
        try await h.store.storeVolumeLocal(volume("keep", ["shared": shared, "onlyKeep": Data([1])]))
        try await h.store.storeVolumeLocal(volume("drop", ["shared": shared, "onlyDrop": Data([2])]))
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
        try await h.store.storeVolumeLocal(volume("r1"))
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
        try await h.store.storeVolumeLocal(volume("r1"))
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
        try await h.store.storeVolumeLocal(volume("r1"))
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
        try await h.store.storeVolumeLocal(volume("r1"))
        try await h.pins.pin(root: root, owner: "owner-a", count: 1, ttl: nil)

        #expect(await h.pins.isPinReachable(cid: root))
        let evicted = try await h.eviction.evictUnpinned(graceSeconds: 0)
        #expect(evicted == 0)
        #expect(await h.store.hasVolume(root: root))
        #expect(await h.pins.isPinReachable(cid: root), "still served after the sweep")
    }

    /// A pin row CAN linger with count <= 0 (`pin(count: 0)` inserts one; only
    /// the unpin paths delete such rows). The serve gate ignores it, so the
    /// eviction seed must too — otherwise eviction would protect content the
    /// gate never serves, and the two "live pin" predicates would drift.
    @Test func zeroCountPinRowNeitherServesNorProtects() async throws {
        let h = try harness()
        let root = cid("r1")
        try await h.store.storeVolumeLocal(volume("r1"))
        try await h.pins.pin(root: root, owner: "owner-a", count: 0, ttl: nil)

        #expect(await h.pins.owners(root: root) == ["owner-a"],
                "the count=0 row really lingers (owners() filters TTL only)")
        #expect(await h.pins.isPinReachable(cid: root) == false,
                "the serve gate must not serve a count<=0 row")
        let evicted = try await h.eviction.evictUnpinned(graceSeconds: 0)
        #expect(evicted == 1, "a count<=0 row must not protect from eviction")
        #expect(await h.store.hasVolume(root: root) == false)
    }

    /// Freshly stored unpinned content survives the periodic sweep long enough
    /// for a follow-up pin intent, then evicts normally once grace is disabled.
    @Test func evictRespectsStoreThenPinGrace() async throws {
        let h = try harness()
        let root = cid("V")
        try await h.store.storeVolumeLocal(volume("V"))

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

    /// Evicting a root releases its durable known-present negative-cache entry.
    @Test func evictedRootClearsKnownPresent() async throws {
        let h = try harness()
        let root = cid("r1")
        #expect(await h.store.hasVolume(root: root) == false)
        try await h.store.storeVolumeLocal(volume("r1"))
        #expect(h.negativeCache.mightBeAbsent(root) == false)

        let evicted = try await h.eviction.evictUnpinned(graceSeconds: 0)
        #expect(evicted == 1)
        #expect(h.negativeCache.mightBeAbsent(root),
                "after eviction, the prior absent bloom verdict is authoritative again")
    }

    /// Collect the independent Volumes a `BrokerStorer` produces, exactly as
    /// cashew's `storeRecursively` drives it (enter/store/exit per boundary).
    private func storeReal(_ h: Harness, build: (BrokerStorer) throws -> Void, root: String) async throws {
        let storer = BrokerStorer(broker: MemoryBroker())
        try build(storer)
        for vol in storer.collectVolumes(root: root) {
            try await h.store.storeVolumeLocal(vol)
        }
    }

    @Test func pinnedRootDoesNotProtectAnotherVolume() async throws {
        let h = try harness()
        let obj = cid("obj")
        let nested = cid("nested")
        let deep = cid("deep")
        try await storeReal(h, build: { s in
            try s.enterVolume(rootCID: obj)
            try s.store(rawCid: obj, data: Data("obj".utf8))
            try s.exitVolume(rootCID: obj)
            try s.enterVolume(rootCID: nested)
            try s.store(rawCid: nested, data: Data("nested".utf8))
            try s.store(rawCid: deep, data: Data("deep".utf8))
            try s.exitVolume(rootCID: nested)
        }, root: obj)
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
        try await storeReal(h, build: { s in
            try s.enterVolume(rootCID: obj)
            try s.store(rawCid: obj, data: Data("obj".utf8))
            try s.exitVolume(rootCID: obj)
            try s.enterVolume(rootCID: nested)
            try s.store(rawCid: nested, data: Data("nested".utf8))
            try s.store(rawCid: deep, data: Data("deep".utf8))
            try s.exitVolume(rootCID: nested)
        }, root: obj)
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
        try await storeReal(h, build: { s in
            try s.enterVolume(rootCID: block)
            try s.store(rawCid: block, data: Data("block".utf8))
            try s.store(rawCid: txDict, data: Data("txDict".utf8))
            try s.exitVolume(rootCID: block)
            try s.enterVolume(rootCID: txBody)
            try s.store(rawCid: txBody, data: Data("txBody".utf8))
            try s.exitVolume(rootCID: txBody)
            try s.enterVolume(rootCID: postState)
            try s.store(rawCid: postState, data: Data("postState".utf8))
            try s.exitVolume(rootCID: postState)
        }, root: block)
        try await h.pins.pin(root: block, owner: "h:1", count: 1, ttl: nil)

        let evicted = try await h.eviction.evictUnpinned(graceSeconds: 0)
        #expect(evicted == 2)
        #expect(await h.store.fetchDataLocal(cid: block) != nil)
        #expect(await h.store.fetchDataLocal(cid: txDict) != nil)
        #expect(await h.store.hasVolume(root: txBody) == false)
        #expect(await h.store.hasVolume(root: postState) == false)
    }
}

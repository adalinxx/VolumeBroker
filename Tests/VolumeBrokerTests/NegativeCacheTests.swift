import Testing
import Foundation
@testable import VolumeBroker

/// Independent unit tests for the `NegativeCache` collaborator and its
/// integration point in `CASVolumeStore`.
///
/// The DiskBroker façade tests never exercise negative-cache *invalidation*:
/// the documented trigger is that storing a root suppresses a prior
/// "confirmed absent" verdict so a freshly-arrived volume is never masked.
@Suite("NegativeCache")
struct NegativeCacheTests {

    private func tempStore() throws -> (CASVolumeStore, NegativeCache) {
        let path = NSTemporaryDirectory() + "vb_negcache_\(UUID().uuidString).sqlite"
        let connection = try SQLiteConnection(path: path)
        let cache = NegativeCache()
        return (CASVolumeStore(connection: connection, negativeCache: cache), cache)
    }

    // MARK: - Unit: NegativeCache surface

    /// A fresh root is not assumed absent: the bloom has never seen it.
    @Test func unseenRootIsNotFlaggedAbsent() {
        let cache = NegativeCache()
        #expect(cache.mightBeAbsent("never-seen") == false)
    }

    /// Recording a confirmed miss makes the bloom report the root as absent.
    @Test func recordAbsentFlagsRoot() {
        let cache = NegativeCache()
        cache.recordAbsent("missing")
        #expect(cache.mightBeAbsent("missing"))
    }

    /// Core invalidation: a root previously confirmed absent is no longer
    /// reported absent once it is stored — the recent-stores set overrides the
    /// bloom so a freshly-arrived volume is never masked as missing.
    @Test func storeInvalidatesNegativeEntry() {
        let cache = NegativeCache()
        cache.recordAbsent("late-arrival")
        #expect(cache.mightBeAbsent("late-arrival"), "precondition: flagged absent")

        cache.recordStored("late-arrival")
        #expect(cache.mightBeAbsent("late-arrival") == false,
                "storing the content must invalidate the negative-cache verdict")
    }

    /// Durable invalidation: stored roots remain known-present until eviction,
    /// even after enough other stores to overflow the old 512-entry ring.
    @Test func storedRootStaysPresentBeyond512Fillers() {
        let cache = NegativeCache()
        cache.recordAbsent("resident-root")
        cache.recordStored("resident-root")
        #expect(cache.mightBeAbsent("resident-root") == false)

        for i in 0..<1_000 {
            cache.recordStored("filler-\(i)")
        }
        #expect(cache.mightBeAbsent("resident-root") == false,
                "store invalidation lasts until a real eviction releases it")
    }

    /// Eviction is the symmetric inverse of store: after release, any prior
    /// bloom verdict is again authoritative.
    @Test func recordEvictedReinstatesAbsentVerdict() {
        let cache = NegativeCache()
        cache.recordAbsent("evicted-root")
        cache.recordStored("evicted-root")
        #expect(cache.mightBeAbsent("evicted-root") == false)

        cache.recordEvicted("evicted-root")
        #expect(cache.mightBeAbsent("evicted-root"),
                "after eviction, the bloom's absent verdict applies again")
    }

    // MARK: - Integration: CASVolumeStore + NegativeCache

    /// `hasVolume` on a miss records the root as absent, priming the negative
    /// cache (the production fast-path during initial sync).
    @Test func missPrimesNegativeCache() async throws {
        let (store, cache) = try tempStore()
        #expect(await store.hasVolume(root: "absent") == false)
        #expect(cache.mightBeAbsent("absent"),
                "a confirmed miss must record the root as absent")
    }

    /// End-to-end invalidation through the store: a root reported absent, then
    /// stored, is correctly visible via `hasVolume` — the negative-cache
    /// fast-path must not mask the now-present volume.
    @Test func storeAfterMissIsVisible() async throws {
        let (store, cache) = try tempStore()

        // Confirmed miss primes the negative cache.
        #expect(await store.hasVolume(root: "r1") == false)
        #expect(cache.mightBeAbsent("r1"))

        // Content later appears.
        try await store.storeVolumeLocal(SerializedVolume(root: "r1", entries: ["c1": Data([1, 2, 3])]))

        // The negative-cache verdict must be invalidated...
        #expect(cache.mightBeAbsent("r1") == false)
        // ...and the volume must be visible despite the earlier miss.
        #expect(await store.hasVolume(root: "r1"))
        #expect(await store.fetchVolumeLocal(root: "r1")?.entries["c1"] == Data([1, 2, 3]))
    }

    /// End-to-end regression: a stored root must not become a
    /// permanent false negative after many later stores.
    @Test func storeAfterMiss_survivesManyStores() async throws {
        let (store, _) = try tempStore()
        #expect(await store.hasVolume(root: "r1") == false)

        try await store.storeVolumeLocal(SerializedVolume(root: "r1", entries: ["c1": Data([1])]))
        for i in 0..<1_000 {
            try await store.storeVolumeLocal(SerializedVolume(root: "filler-\(i)", entries: ["c\(i)": Data([UInt8(i & 0xff)])]))
        }

        #expect(await store.hasVolume(root: "r1"))
    }
}

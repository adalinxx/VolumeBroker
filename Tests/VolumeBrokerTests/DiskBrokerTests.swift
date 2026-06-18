import Testing
import Foundation
@testable import VolumeBroker

@Suite("DiskBroker")
struct DiskBrokerTests {

    private func tempDB(evictUnpinnedGraceSeconds: Int = 0) throws -> DiskBroker {
        let path = NSTemporaryDirectory() + "vb_test_\(UUID().uuidString).sqlite"
        return try DiskBroker(path: path, evictUnpinnedGraceSeconds: evictUnpinnedGraceSeconds)
    }

    private func payload(_ root: String, _ entries: [String: Data] = [:]) -> SerializedVolume {
        var e = entries
        if e.isEmpty { e = [root: Data(root.utf8)] }
        return SerializedVolume(root: root, entries: e)
    }

    @Test func storeAndFetch() async throws {
        let broker = try tempDB()
        let p = payload("r1", ["c1": Data([1, 2, 3]), "c2": Data([4, 5])])
        try await broker.storeVolumeLocal(p)

        #expect(await broker.hasVolume(root: "r1"))
        let fetched = await broker.fetchVolumeLocal(root: "r1")
        #expect(fetched?.entries.count == 2)
        #expect(fetched?.entries["c1"] == Data([1, 2, 3]))
    }

    // MARK: - Extracted-layer boundaries

    /// CASVolumeStore boundary: a stored volume round-trips byte-for-byte.
    @Test func casStoreRoundTrip() async throws {
        let broker = try tempDB()
        let entries = ["a": Data([0, 1, 2, 3]), "b": Data(repeating: 7, count: 256)]
        try await broker.storeVolumeLocal(payload("round", entries))

        let fetched = await broker.fetchVolumeLocal(root: "round")
        #expect(fetched?.entries == entries)
    }

    /// EvictionEngine boundary: a pinned root survives eviction; the unpinned
    /// sibling is reclaimed.
    @Test func pinSurvivesEviction() async throws {
        let broker = try tempDB()
        try await broker.storeVolumeLocal(payload("keep"))
        try await broker.storeVolumeLocal(payload("drop"))
        try await broker.pin(root: "keep", owner: "owner")

        let evicted = try await broker.evictUnpinned()
        #expect(evicted == 1)
        #expect(await broker.hasVolume(root: "keep"))
        #expect(await broker.hasVolume(root: "drop") == false)
        #expect(await broker.fetchVolumeLocal(root: "keep") != nil)
    }

    /// PinIndex + EvictionEngine boundary: unpinning the last owner makes the
    /// root eligible for eviction.
    @Test func unpinnedRootIsEvicted() async throws {
        let broker = try tempDB()
        try await broker.storeVolumeLocal(payload("r1"))
        try await broker.pin(root: "r1", owner: "owner")
        try await broker.unpin(root: "r1", owner: "owner")

        let evicted = try await broker.evictUnpinned()
        #expect(evicted == 1)
        #expect(await broker.hasVolume(root: "r1") == false)
    }

    @Test func configuredGraceProtectsFreshUnpinnedVolume() async throws {
        let broker = try tempDB(evictUnpinnedGraceSeconds: 60 * 60)
        try await broker.storeVolumeLocal(payload("fresh"))

        let protected = try await broker.evictUnpinned()
        #expect(protected == 0)
        #expect(await broker.hasVolume(root: "fresh"))

        let evicted = try await broker.evictUnpinned(graceSeconds: 0)
        #expect(evicted == 1)
        #expect(await broker.hasVolume(root: "fresh") == false)
    }

    @Test func pinAndEvict() async throws {
        let broker = try tempDB()
        try await broker.storeVolumeLocal(payload("r1"))
        try await broker.storeVolumeLocal(payload("r2"))
        try await broker.pin(root: "r1", owner: "chain-a")

        let evicted = try await broker.evictUnpinned()
        #expect(evicted == 1)
        #expect(await broker.hasVolume(root: "r1"))
        #expect(await broker.hasVolume(root: "r2") == false)
    }

    @Test func sharedCIDSurvivesPartialEviction() async throws {
        let broker = try tempDB()
        let shared = Data([99])
        try await broker.storeVolumeLocal(payload("r1", ["shared": shared, "only1": Data([1])]))
        try await broker.storeVolumeLocal(payload("r2", ["shared": shared, "only2": Data([2])]))
        try await broker.pin(root: "r1", owner: "chain-a")

        _ = try await broker.evictUnpinned()
        let fetched = await broker.fetchVolumeLocal(root: "r1")
        #expect(fetched?.entries["shared"] == shared)
    }

    @Test func retainedRootServesAndProtectsWithoutPinOwner() async throws {
        let broker = try tempDB()
        try await broker.storeVolumeLocal(payload("keep"))
        try await broker.storeVolumeLocal(payload("drop"))

        try await broker.advanceRetainedRoots(scope: "chain-a:state", roots: ["keep"], operationID: "op-1")

        #expect(await broker.owners(root: "keep").isEmpty)
        #expect(await broker.isPinReachable(cid: "keep"))
        #expect(await broker.retainedRoots(scope: "chain-a:state") == ["keep"])
        let evicted = try await broker.evictUnpinned()
        #expect(evicted == 1)
        #expect(await broker.hasVolume(root: "keep"))
        #expect(await broker.hasVolume(root: "drop") == false)
    }

    @Test func retainedRootAdvanceReplacesOnlyItsScope() async throws {
        let broker = try tempDB()
        try await broker.storeVolumeLocal(payload("old"))
        try await broker.storeVolumeLocal(payload("new"))
        try await broker.storeVolumeLocal(payload("other"))

        try await broker.advanceRetainedRoots(scope: "chain-a:state", roots: ["old"], operationID: "op-1")
        try await broker.advanceRetainedRoots(scope: "chain-b:state", roots: ["other"], operationID: "op-2")
        try await broker.advanceRetainedRoots(scope: "chain-a:state", roots: ["new"], operationID: "op-3")

        #expect(await broker.retainedRoots(scope: "chain-a:state") == ["new"])
        #expect(await broker.retainedRoots(scope: "chain-b:state") == ["other"])
        let evicted = try await broker.evictUnpinned()
        #expect(evicted == 1)
        #expect(await broker.hasVolume(root: "old") == false)
        #expect(await broker.hasVolume(root: "new"))
        #expect(await broker.hasVolume(root: "other"))
    }

    @Test func retainedRootMergeAddsWithoutReplacingScope() async throws {
        let broker = try tempDB()
        try await broker.storeVolumeLocal(payload("old"))
        try await broker.storeVolumeLocal(payload("new"))
        try await broker.storeVolumeLocal(payload("drop"))

        try await broker.advanceRetainedRoots(scope: "chain-a:state", roots: ["old"], operationID: "op-1")
        try await broker.mergeRetainedRoots(scope: "chain-a:state", roots: ["new"], operationID: "op-2")
        try await broker.mergeRetainedRoots(scope: "chain-a:state", roots: ["new"], operationID: "op-2")

        #expect(await broker.retainedRoots(scope: "chain-a:state") == ["new", "old"])
        let evicted = try await broker.evictUnpinned()
        #expect(evicted == 1)
        #expect(await broker.hasVolume(root: "old"))
        #expect(await broker.hasVolume(root: "new"))
        #expect(await broker.hasVolume(root: "drop") == false)
    }

    @Test func retainedRootMergeOperationIDIsPayloadAndKindBound() async throws {
        let broker = try tempDB()
        try await broker.storeVolumeLocal(payload("r1"))
        try await broker.storeVolumeLocal(payload("r2"))

        try await broker.mergeRetainedRoots(scope: "chain-a:state", roots: ["r1"], operationID: "op-1")

        do {
            try await broker.mergeRetainedRoots(scope: "chain-a:state", roots: ["r2"], operationID: "op-1")
            #expect(Bool(false), "operation id replay with a different payload must fail")
        } catch BrokerError.conflictingRetainedRootOperation(let operationID) {
            #expect(operationID == "op-1")
        }

        do {
            try await broker.advanceRetainedRoots(scope: "chain-a:state", roots: ["r1"], operationID: "op-1")
            #expect(Bool(false), "operation id replay with a different operation kind must fail")
        } catch BrokerError.conflictingRetainedRootOperation(let operationID) {
            #expect(operationID == "op-1")
        }

        #expect(await broker.retainedRoots(scope: "chain-a:state") == ["r1"])
    }

    @Test func retainedRootMergeRequiresStoredRoots() async throws {
        let broker = try tempDB()

        do {
            try await broker.mergeRetainedRoots(scope: "chain-a:state", roots: ["missing"], operationID: "op-1")
            #expect(Bool(false), "merging a retained root before storing it must fail")
        } catch BrokerError.missingRetainedRoot(let root) {
            #expect(root == "missing")
        }
        #expect(await broker.retainedRoots(scope: "chain-a:state").isEmpty)
    }

    @Test func retainedRootAdvanceIsPayloadBoundByOperationID() async throws {
        let broker = try tempDB()
        try await broker.storeVolumeLocal(payload("r1"))
        try await broker.storeVolumeLocal(payload("r2"))

        try await broker.advanceRetainedRoots(scope: "chain-a:state", roots: ["r1"], operationID: "op-1")
        try await broker.advanceRetainedRoots(scope: "chain-a:state", roots: ["r1"], operationID: "op-1")

        do {
            try await broker.advanceRetainedRoots(scope: "chain-a:state", roots: ["r2"], operationID: "op-1")
            #expect(Bool(false), "operation id replay with a different payload must fail")
        } catch BrokerError.conflictingRetainedRootOperation(let operationID) {
            #expect(operationID == "op-1")
        }
        #expect(await broker.retainedRoots(scope: "chain-a:state") == ["r1"])
    }

    @Test func retainedRootAdvanceRequiresStoredRoots() async throws {
        let broker = try tempDB()

        do {
            try await broker.advanceRetainedRoots(scope: "chain-a:state", roots: ["missing"], operationID: "op-1")
            #expect(Bool(false), "retaining a root before storing it must fail")
        } catch BrokerError.missingRetainedRoot(let root) {
            #expect(root == "missing")
        }
        #expect(await broker.retainedRoots(scope: "chain-a:state").isEmpty)
    }

    @Test func retainedRootAdvanceRejectsIncompleteStoredClosure() async throws {
        let broker = try tempDB()
        try await broker.storeVolumesLocal([
            SerializedVolume(root: "root", entries: [
                "root": Data("root".utf8),
                "child": Data("child".utf8),
            ]),
            SerializedVolume(root: "child", entries: [:]),
        ])

        do {
            try await broker.advanceRetainedRoots(scope: "chain-a:state", roots: ["root"], operationID: "op-1")
            #expect(Bool(false), "retaining a root with an incomplete stored closure must fail")
        } catch BrokerError.missingRetainedRoot(let root) {
            #expect(root == "child")
        }
        #expect(await broker.retainedRoots(scope: "chain-a:state").isEmpty)
    }

    @Test func retainedRootTransitivelyProtectsNestedVolumeClosure() async throws {
        let broker = try tempDB()
        try await broker.storeVolumesLocal([
            payload("object", ["object": Data("object".utf8), "child": Data("child".utf8)]),
            payload("child", ["child": Data("child".utf8), "leaf": Data("leaf".utf8)]),
            payload("drop")
        ])

        try await broker.advanceRetainedRoots(scope: "chain-a:state", roots: ["object"], operationID: "op-1")
        #expect(await broker.isPinReachable(cid: "leaf"))
        let evicted = try await broker.evictUnpinned()

        #expect(evicted == 1)
        #expect(await broker.hasVolume(root: "object"))
        #expect(await broker.hasVolume(root: "child"))
        #expect(await broker.hasVolume(root: "drop") == false)
        #expect(await broker.fetchDataLocal(cid: "leaf") == Data("leaf".utf8))
    }

    @Test func multiOwnerPins() async throws {
        let broker = try tempDB()
        try await broker.storeVolumeLocal(payload("r1"))
        try await broker.pin(root: "r1", owner: "chain-a")
        try await broker.pin(root: "r1", owner: "chain-b")

        #expect(await broker.owners(root: "r1") == ["chain-a", "chain-b"])

        try await broker.unpin(root: "r1", owner: "chain-a")
        let evicted = try await broker.evictUnpinned()
        #expect(evicted == 0)
        #expect(await broker.hasVolume(root: "r1"))
    }

    @Test func duplicatePinAddsToCount() async throws {
        let broker = try tempDB()
        try await broker.storeVolumeLocal(payload("r1"))
        try await broker.pin(root: "r1", owner: "chain-a")
        try await broker.pin(root: "r1", owner: "chain-a")
        #expect(await broker.owners(root: "r1").count == 1)

        try await broker.unpin(root: "r1", owner: "chain-a")
        #expect(await broker.owners(root: "r1").count == 1, "one unpin should leave count=1")

        let evicted = try await broker.evictUnpinned()
        #expect(evicted == 0, "still pinned with count=1")
        #expect(await broker.hasVolume(root: "r1"))

        try await broker.unpin(root: "r1", owner: "chain-a")
        #expect(await broker.owners(root: "r1").isEmpty)
        let evicted2 = try await broker.evictUnpinned()
        #expect(evicted2 == 1)
    }

    @Test func pinWithExplicitCount() async throws {
        let broker = try tempDB()
        try await broker.storeVolumeLocal(payload("r1"))
        try await broker.pin(root: "r1", owner: "chain-a", count: 3)

        try await broker.unpin(root: "r1", owner: "chain-a", count: 2)
        #expect(await broker.owners(root: "r1").count == 1, "count=1 remaining")

        try await broker.unpin(root: "r1", owner: "chain-a", count: 1)
        #expect(await broker.owners(root: "r1").isEmpty)
    }

    @Test func unpinBatchOnceDoesNotReplayDecrement() async throws {
        let broker = try tempDB()
        try await broker.storeVolumeLocal(payload("r1"))
        try await broker.pin(root: "r1", owner: "chain-a", count: 2)

        let items = [(root: "r1", owner: "chain-a", count: 1)]
        try await broker.unpinBatchOnce(operationID: "prune:chain-a:1", items: items)
        #expect(await broker.owners(root: "r1") == ["chain-a"], "first prune leaves residual count=1")

        try await broker.unpinBatchOnce(operationID: "prune:chain-a:1", items: items)
        #expect(await broker.owners(root: "r1") == ["chain-a"], "retry must not decrement the same operation twice")

        try await broker.unpinBatchOnce(operationID: "prune:chain-a:2", items: items)
        #expect(await broker.owners(root: "r1").isEmpty, "a different operation id still applies its decrement")
    }

    @Test func unpinMoreThanCountRemovesPin() async throws {
        let broker = try tempDB()
        try await broker.pin(root: "r1", owner: "chain-a", count: 2)
        try await broker.unpin(root: "r1", owner: "chain-a", count: 5)
        #expect(await broker.owners(root: "r1").isEmpty)
    }

    @Test func ttlExpiredOwnerPrunedOnEvict() async throws {
        let broker = try tempDB()
        try await broker.storeVolumeLocal(payload("r1"))
        try await broker.pin(root: "r1", owner: "chain-a:42", ttl: .zero)

        let evicted = try await broker.evictUnpinned()
        #expect(evicted == 1)
        #expect(await broker.hasVolume(root: "r1") == false)
    }

    @Test func mixedTTLAndPermanentOwners() async throws {
        let broker = try tempDB()
        try await broker.storeVolumeLocal(payload("r1"))
        try await broker.pin(root: "r1", owner: "chain-a:42", ttl: .zero)
        try await broker.pin(root: "r1", owner: "chain-b:tip")

        let evicted = try await broker.evictUnpinned()
        #expect(evicted == 0)
        #expect(await broker.hasVolume(root: "r1"))
        #expect(await broker.owners(root: "r1") == ["chain-b:tip"])
    }

    @Test func pinnedRootsByOwnerAndPrefix() async throws {
        let broker = try tempDB()
        try await broker.pin(root: "exact-root", owner: "account:Nexus/A/Child")
        try await broker.pin(root: "height-root", owner: "Nexus/A/Child:42")
        try await broker.pin(root: "candidate-root", owner: "candidate:Nexus/A/Child:43")
        try await broker.pin(root: "expired-root", owner: "Nexus/A/Child:44", ttl: .zero)
        try await broker.pin(root: "foreign-root", owner: "Nexus/B/Child:42")
        try await broker.pin(root: "leaf-root", owner: "Child:42")

        let roots = Set(await broker.pinnedRoots(
            owners: ["account:Nexus/A/Child"],
            ownerPrefixes: ["Nexus/A/Child:", "candidate:Nexus/A/Child:"]
        ))

        #expect(roots == ["exact-root", "height-root", "candidate-root"])
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
            let hasVolume = await broker.hasVolume(root: "root-\(i)")
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
                let hasVolume = await broker.hasVolume(root: "batch-\(i)-\(j)")
                #expect(hasVolume, "batch-\(i)-\(j) missing")
            }
        }
    }
}

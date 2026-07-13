import Testing
import Foundation
import CID
import Multihash
@testable import VolumeBroker

@Suite("MemoryBroker")
struct MemoryBrokerTests {

    private func cid(for data: Data) -> String {
        let multihash = try! Multihash(raw: data, hashedWith: .sha2_256)
        return try! CID(version: .v1, codec: .dag_cbor, multihash: multihash).toBaseEncodedString
    }

    private func cid(_ value: String) -> String {
        cid(for: Data(value.utf8))
    }

    private func payload(_ root: String, _ entries: [String: Data] = [:]) -> SerializedVolume {
        let rootData = Data(root.utf8)
        var encodedEntries = [cid(for: rootData): rootData]
        for data in entries.values {
            encodedEntries[cid(for: data)] = data
        }
        return SerializedVolume(root: cid(for: rootData), entries: encodedEntries)
    }

    @Test func byteBudgetEvictionRespectsReadRecency() async throws {
        // Each payload carries ~1 KiB; the byte budget fits two volumes, not three.
        let oneKiB = Data(count: 1024)
        let budget = 2300
        let broker = MemoryBroker(byteBudget: budget)
        try await broker.storeVolumeLocal(payload("A", ["A": oneKiB]))
        try await broker.storeVolumeLocal(payload("B", ["B": oneKiB]))
        // Refresh A's recency via a read. Without recency-on-read this is a no-op
        // and eviction is insertion-order (A oldest), so the hot volume A is lost.
        _ = await broker.fetchVolumeLocal(root: cid("A"))
        // Storing C pushes over budget → the coldest UNPINNED volume is evicted.
        try await broker.storeVolumeLocal(payload("C", ["C": oneKiB]))
        #expect(await broker.fetchVolumeLocal(root: cid("A")) != nil)  // recently read → survives
        #expect(await broker.fetchVolumeLocal(root: cid("B")) == nil)  // coldest → evicted
        #expect(await broker.fetchVolumeLocal(root: cid("C")) != nil)  // newest → present
        #expect(await broker.residentBytes() <= budget)
    }

    @Test func storeAndFetch() async throws {
        let broker = MemoryBroker()
        let p = payload("r1", ["c1": Data([1]), "c2": Data([2])])
        try await broker.storeVolumeLocal(p)

        #expect(await broker.hasVolume(root: p.root))
        let fetched = await broker.fetchVolumeLocal(root: p.root)
        #expect(fetched?.entries.count == 3)
        #expect(fetched?.entries[cid(for: Data([1]))] == Data([1]))
    }

    @Test func missingVolumeReturnsNil() async {
        let broker = MemoryBroker()
        #expect(await broker.fetchVolumeLocal(root: "missing") == nil)
        #expect(await broker.hasVolume(root: "missing") == false)
    }

    @Test func pinAndUnpin() async throws {
        let broker = MemoryBroker()
        try await broker.pin(root: "r1", owner: "chain-a")
        try await broker.pin(root: "r1", owner: "chain-b")
        #expect(await broker.owners(root: "r1") == ["chain-a", "chain-b"])

        try await broker.unpin(root: "r1", owner: "chain-a")
        #expect(await broker.owners(root: "r1") == ["chain-b"])

        try await broker.unpin(root: "r1", owner: "chain-b")
        #expect(await broker.owners(root: "r1").isEmpty)
    }

    @Test func refCountedPins() async throws {
        let broker = MemoryBroker()
        let root = cid("r1")
        try await broker.storeVolumeLocal(payload("r1"))
        try await broker.pin(root: root, owner: "chain-a")
        try await broker.pin(root: root, owner: "chain-a")

        try await broker.unpin(root: root, owner: "chain-a")
        #expect(await broker.owners(root: root) == ["chain-a"], "one unpin should leave count=1")

        let evicted = try await broker.evictUnpinned()
        #expect(evicted == 0)
        #expect(await broker.hasVolume(root: root))

        try await broker.unpin(root: root, owner: "chain-a")
        #expect(await broker.owners(root: root).isEmpty)
    }

    @Test func pinWithExplicitCount() async throws {
        let broker = MemoryBroker()
        try await broker.pin(root: "r1", owner: "chain-a", count: 5)
        try await broker.unpin(root: "r1", owner: "chain-a", count: 3)
        #expect(await broker.owners(root: "r1") == ["chain-a"])

        try await broker.unpin(root: "r1", owner: "chain-a", count: 2)
        #expect(await broker.owners(root: "r1").isEmpty)
    }

    @Test func unpinMoreThanCountRemovesPin() async throws {
        let broker = MemoryBroker()
        try await broker.pin(root: "r1", owner: "chain-a", count: 2)
        try await broker.unpin(root: "r1", owner: "chain-a", count: 10)
        #expect(await broker.owners(root: "r1").isEmpty)
    }

    @Test func unpinAllIgnoresCount() async throws {
        let broker = MemoryBroker()
        try await broker.pin(root: "r1", owner: "chain-a", count: 5)
        try await broker.pin(root: "r2", owner: "chain-a", count: 3)
        try await broker.unpinAll(owner: "chain-a")
        #expect(await broker.owners(root: "r1").isEmpty)
        #expect(await broker.owners(root: "r2").isEmpty)
    }

    @Test func evictUnpinnedRemovesOnlyUnpinned() async throws {
        let broker = MemoryBroker(evictUnpinnedGrace: .zero)
        let keep = cid("r1")
        let drop = cid("r2")
        try await broker.storeVolumeLocal(payload("r1"))
        try await broker.storeVolumeLocal(payload("r2"))
        try await broker.pin(root: keep, owner: "chain-a")

        let evicted = try await broker.evictUnpinned()
        #expect(evicted == 1)
        #expect(await broker.hasVolume(root: keep))
        #expect(await broker.hasVolume(root: drop) == false)
    }

    @Test func retainedRootProtectsWithoutPinOwner() async throws {
        let broker = MemoryBroker(evictUnpinnedGrace: .zero)
        let keep = cid("keep")
        let drop = cid("drop")
        try await broker.storeVolumeLocal(payload("keep"))
        try await broker.storeVolumeLocal(payload("drop"))

        try await broker.advanceRetainedRoots(scope: "chain-a:state", roots: [keep], operationID: "op-1")

        #expect(await broker.owners(root: keep).isEmpty)
        #expect(await broker.retainedRoots(scope: "chain-a:state") == [keep])
        let evicted = try await broker.evictUnpinned()
        #expect(evicted == 1)
        #expect(await broker.hasVolume(root: keep))
        #expect(await broker.hasVolume(root: drop) == false)
    }

    @Test func retainedRootOperationIDIsPayloadBound() async throws {
        let broker = MemoryBroker(evictUnpinnedGrace: .zero)
        let r1 = cid("r1")
        let r2 = cid("r2")
        try await broker.storeVolumeLocal(payload("r1"))
        try await broker.storeVolumeLocal(payload("r2"))

        try await broker.advanceRetainedRoots(scope: "chain-a:state", roots: [r1], operationID: "op-1")
        try await broker.advanceRetainedRoots(scope: "chain-a:state", roots: [r1], operationID: "op-1")

        do {
            try await broker.advanceRetainedRoots(scope: "chain-a:state", roots: [r2], operationID: "op-1")
            #expect(Bool(false), "operation id replay with a different payload must fail")
        } catch BrokerError.conflictingRetainedRootOperation(let operationID) {
            #expect(operationID == "op-1")
        }
        #expect(await broker.retainedRoots(scope: "chain-a:state") == [r1])
    }

    @Test func retainedRootMergeAddsWithoutReplacingScope() async throws {
        let broker = MemoryBroker(evictUnpinnedGrace: .zero)
        let old = cid("old")
        let new = cid("new")
        let drop = cid("drop")
        try await broker.storeVolumeLocal(payload("old"))
        try await broker.storeVolumeLocal(payload("new"))
        try await broker.storeVolumeLocal(payload("drop"))

        try await broker.advanceRetainedRoots(scope: "chain-a:state", roots: [old], operationID: "op-1")
        try await broker.mergeRetainedRoots(scope: "chain-a:state", roots: [new], operationID: "op-2")
        try await broker.mergeRetainedRoots(scope: "chain-a:state", roots: [new], operationID: "op-2")

        #expect(Set(await broker.retainedRoots(scope: "chain-a:state")) == [new, old])
        let evicted = try await broker.evictUnpinned()
        #expect(evicted == 1)
        #expect(await broker.hasVolume(root: old))
        #expect(await broker.hasVolume(root: new))
        #expect(await broker.hasVolume(root: drop) == false)
    }

    @Test func retainedRootMergeOperationIDIsPayloadAndKindBound() async throws {
        let broker = MemoryBroker(evictUnpinnedGrace: .zero)
        let r1 = cid("r1")
        let r2 = cid("r2")
        try await broker.storeVolumeLocal(payload("r1"))
        try await broker.storeVolumeLocal(payload("r2"))

        try await broker.mergeRetainedRoots(scope: "chain-a:state", roots: [r1], operationID: "op-1")

        do {
            try await broker.mergeRetainedRoots(scope: "chain-a:state", roots: [r2], operationID: "op-1")
            #expect(Bool(false), "operation id replay with a different payload must fail")
        } catch BrokerError.conflictingRetainedRootOperation(let operationID) {
            #expect(operationID == "op-1")
        }

        do {
            try await broker.advanceRetainedRoots(scope: "chain-a:state", roots: [r1], operationID: "op-1")
            #expect(Bool(false), "operation id replay with a different operation kind must fail")
        } catch BrokerError.conflictingRetainedRootOperation(let operationID) {
            #expect(operationID == "op-1")
        }

        #expect(await broker.retainedRoots(scope: "chain-a:state") == [r1])
    }

    @Test func retainedRootDoesNotRequireRelatedVolume() async throws {
        let broker = MemoryBroker(evictUnpinnedGrace: .zero)
        let root = cid("root")
        try await broker.storeVolumeLocal(payload("root"))

        try await broker.advanceRetainedRoots(scope: "chain-a:state", roots: [root], operationID: "op-1")

        #expect(await broker.retainedRoots(scope: "chain-a:state") == [root])
    }

    @Test func evictUnpinnedRespectsStoreThenPinGrace() async throws {
        let broker = MemoryBroker(evictUnpinnedGrace: .seconds(600))
        let root = cid("fresh")
        try await broker.storeVolumeLocal(payload("fresh"))

        let evicted = try await broker.evictUnpinned()
        #expect(evicted == 0)
        #expect(await broker.hasVolume(root: root))
    }

    @Test func ttlExpiredOwnerAutoRemoved() async throws {
        let broker = MemoryBroker(evictUnpinnedGrace: .zero)
        let root = cid("r1")
        try await broker.storeVolumeLocal(payload("r1"))
        try await broker.pin(root: root, owner: "chain-a:42", ttl: .zero)

        #expect(await broker.owners(root: root).isEmpty)
        let evicted = try await broker.evictUnpinned()
        #expect(evicted == 1)
    }

    @Test func ttlExpiredButOtherOwnerKeepsAlive() async throws {
        let broker = MemoryBroker()
        let root = cid("r1")
        try await broker.storeVolumeLocal(payload("r1"))
        try await broker.pin(root: root, owner: "chain-a:42", ttl: .zero)
        try await broker.pin(root: root, owner: "chain-b:tip")

        #expect(await broker.owners(root: root) == ["chain-b:tip"])
        let evicted = try await broker.evictUnpinned()
        #expect(evicted == 0)
        #expect(await broker.hasVolume(root: root))
    }

    @Test func noTTLMeansIndefinite() async throws {
        let broker = MemoryBroker()
        let root = cid("r1")
        try await broker.storeVolumeLocal(payload("r1"))
        try await broker.pin(root: root, owner: "chain-a:tip")

        let evicted = try await broker.evictUnpinned()
        #expect(evicted == 0)
        #expect(await broker.hasVolume(root: root))
    }

    @Test func pinnedRootsByOwnerAndPrefix() async throws {
        let broker = MemoryBroker()
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

    @Test func capacityEvictsLRUUnpinned() async throws {
        let broker = MemoryBroker(capacity: 2)
        let r1 = cid("r1")
        let r2 = cid("r2")
        let r3 = cid("r3")
        try await broker.storeVolumeLocal(payload("r1"))
        try await broker.storeVolumeLocal(payload("r2"))
        try await broker.storeVolumeLocal(payload("r3"))

        #expect(await broker.hasVolume(root: r1) == false)
        #expect(await broker.hasVolume(root: r2))
        #expect(await broker.hasVolume(root: r3))
    }

    @Test func pinnedSurvivesCapacityEviction() async throws {
        let broker = MemoryBroker(capacity: 2)
        let r1 = cid("r1")
        try await broker.storeVolumeLocal(payload("r1"))
        try await broker.pin(root: r1, owner: "chain-a")
        try await broker.storeVolumeLocal(payload("r2"))
        try await broker.storeVolumeLocal(payload("r3"))

        #expect(await broker.hasVolume(root: r1))
    }

    @Test func cascadeFetchFallsThrough() async throws {
        let local = MemoryBroker()
        let remote = MemoryBroker()
        let p = payload("r1", ["c1": Data([42])])
        try await remote.storeVolumeLocal(p)

        await local.link(near: remote)
        let fetched = await local.fetchVolume(root: p.root)
        #expect(fetched?.entries[cid(for: Data([42]))] == Data([42]))
    }
}

extension VolumeBroker {
    func link(near: (any VolumeBroker)? = nil, far: (any VolumeBroker)? = nil) {
        self.near = near
        self.far = far
    }
}

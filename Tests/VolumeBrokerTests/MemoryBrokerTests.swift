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

    @Test func capacityRejectsAnUnpublishableBatchWithoutMutation() async throws {
        let broker = MemoryBroker(capacity: 1)
        let first = payload("A")
        let second = payload("B")

        do {
            try await broker.storeVolumesLocal([first, second])
            Issue.record("batch larger than capacity must fail")
        } catch {
            #expect(error as? BrokerError == .capacityExceeded)
        }
        #expect(!(await broker.hasVolume(root: first.root)))
        #expect(!(await broker.hasVolume(root: second.root)))
    }

    @Test func byteBudgetRejectsAnUnpublishableVolumeWithoutMutation() async throws {
        let broker = MemoryBroker(byteBudget: 1)
        let volume = payload("too-large")

        do {
            try await broker.storeVolumeLocal(volume)
            Issue.record("volume larger than byte budget must fail")
        } catch {
            #expect(error as? BrokerError == .capacityExceeded)
        }
        #expect(!(await broker.hasVolume(root: volume.root)))
        #expect(await broker.residentBytes() == 0)
    }

    @Test func storeOwnsItsPublishedBytes() async throws {
        let count = 4_096
        let pointer = UnsafeMutableRawPointer.allocate(byteCount: count, alignment: 1)
        defer { pointer.deallocate() }
        pointer.initializeMemory(as: UInt8.self, repeating: 7, count: count)
        let borrowed = Data(bytesNoCopy: pointer, count: count, deallocator: .none)
        let expected = Data(bytes: pointer, count: count)
        let root = cid(for: borrowed)
        let broker = MemoryBroker()

        try await broker.storeVolumeLocal(SerializedVolume(root: root, entries: [root: borrowed]))
        let bytes = pointer.bindMemory(to: UInt8.self, capacity: count)
        for index in 0..<count { bytes[index] = 9 }

        let fetched = try #require(await broker.fetchVolumeLocal(root: root))
        #expect(await broker.hasVolume(root: root))
        #expect(fetched.entries[root] == expected)
        try fetched.validate()
    }

    @Test func byteBudgetEvictionRespectsPointReadRecency() async throws {
        // Each payload carries ~1 KiB; the byte budget fits two volumes, not three.
        let firstKiB = Data(repeating: 1, count: 1024)
        let secondKiB = Data(repeating: 2, count: 1024)
        let thirdKiB = Data(repeating: 3, count: 1024)
        let budget = 2300
        let broker = MemoryBroker(byteBudget: budget)
        try await broker.storeVolumeLocal(payload("A", ["A": firstKiB]))
        try await broker.storeVolumeLocal(payload("B", ["B": secondKiB]))
        _ = await broker.fetchDataLocal(cid: cid(for: firstKiB))
        // Storing C pushes over budget → the coldest UNPINNED volume is evicted.
        try await broker.storeVolumeLocal(payload("C", ["C": thirdKiB]))
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

    @Test func sharedCIDUsesUniqueBytesAndReleasesAtLastOwner() async throws {
        let broker = MemoryBroker(evictUnpinnedGrace: .zero)
        let firstRootData = Data("first-root".utf8)
        let secondRootData = Data("second-root".utf8)
        let sharedData = Data(repeating: 7, count: 128)
        let firstRoot = cid(for: firstRootData)
        let secondRoot = cid(for: secondRootData)
        let shared = cid(for: sharedData)
        let first = SerializedVolume(
            root: firstRoot,
            entries: [firstRoot: firstRootData, shared: sharedData]
        )
        let second = SerializedVolume(
            root: secondRoot,
            entries: [secondRoot: secondRootData, shared: sharedData]
        )

        try await broker.storeVolumesLocal([first, second])
        let uniqueBytes = firstRootData.count + secondRootData.count + sharedData.count
        #expect(await broker.residentBytes() == uniqueBytes)

        try await broker.storeVolumeLocal(first)
        #expect(await broker.residentBytes() == uniqueBytes)

        try await broker.pin(root: firstRoot, owner: "owner")
        #expect(try await broker.evictUnpinned() == 1)
        #expect(await broker.fetchDataLocal(cid: shared) == sharedData)
        #expect(await broker.residentBytes() == firstRootData.count + sharedData.count)

        try await broker.unpin(root: firstRoot, owner: "owner")
        #expect(try await broker.evictUnpinned() == 1)
        #expect(await broker.fetchDataLocal(cid: shared) == nil)
        #expect(await broker.residentBytes() == 0)
    }

    @Test func missingVolumeReturnsNil() async {
        let broker = MemoryBroker()
        #expect(await broker.fetchVolumeLocal(root: "missing") == nil)
        #expect(await broker.hasVolume(root: "missing") == false)
    }

    @Test func pinRequiresStoredVolumeAndPositiveCount() async throws {
        let broker = MemoryBroker()
        do {
            try await broker.pin(root: "missing", owner: "owner")
            Issue.record("missing Volume must not acquire a pin")
        } catch {
            #expect(error as? BrokerError == .notFound)
        }

        let root = cid("r1")
        try await broker.storeVolumeLocal(payload("r1"))
        for count in [0, -1] {
            do {
                try await broker.pin(root: root, owner: "owner", count: count)
                Issue.record("nonpositive pin count must fail")
            } catch {
                #expect(error as? BrokerError == .invalidPinCount)
            }
        }
        try await broker.pin(root: root, owner: "owner", count: .max)
        do {
            try await broker.pin(root: root, owner: "owner")
            Issue.record("overflowing pin count must fail")
        } catch {
            #expect(error as? BrokerError == .invalidPinCount)
        }
        #expect(await broker.owners(root: root) == ["owner"])
    }

    @Test func pinAndUnpin() async throws {
        let broker = MemoryBroker()
        let root = cid("r1")
        try await broker.storeVolumeLocal(payload("r1"))
        try await broker.pin(root: root, owner: "chain-a")
        try await broker.pin(root: root, owner: "chain-b")
        #expect(await broker.owners(root: root) == ["chain-a", "chain-b"])

        try await broker.unpin(root: root, owner: "chain-a")
        #expect(await broker.owners(root: root) == ["chain-b"])

        try await broker.unpin(root: root, owner: "chain-b")
        #expect(await broker.owners(root: root).isEmpty)
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
        let root = cid("r1")
        try await broker.storeVolumeLocal(payload("r1"))
        try await broker.pin(root: root, owner: "chain-a", count: 5)
        try await broker.unpin(root: root, owner: "chain-a", count: 3)
        #expect(await broker.owners(root: root) == ["chain-a"])

        try await broker.unpin(root: root, owner: "chain-a", count: 2)
        #expect(await broker.owners(root: root).isEmpty)
    }

    @Test func unpinMoreThanCountRemovesPin() async throws {
        let broker = MemoryBroker()
        let root = cid("r1")
        try await broker.storeVolumeLocal(payload("r1"))
        try await broker.pin(root: root, owner: "chain-a", count: 2)
        try await broker.unpin(root: root, owner: "chain-a", count: 10)
        #expect(await broker.owners(root: root).isEmpty)
    }

    @Test func unpinAllIgnoresCount() async throws {
        let broker = MemoryBroker()
        let r1 = cid("r1")
        let r2 = cid("r2")
        try await broker.storeVolumesLocal([payload("r1"), payload("r2")])
        try await broker.pin(root: r1, owner: "chain-a", count: 5)
        try await broker.pin(root: r2, owner: "chain-a", count: 3)
        try await broker.unpinAll(owner: "chain-a")
        #expect(await broker.owners(root: r1).isEmpty)
        #expect(await broker.owners(root: r2).isEmpty)
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

        try await broker.advanceRetainedRoots(scope: "chain-a:state", roots: [keep])

        #expect(await broker.owners(root: keep).isEmpty)
        #expect(try await broker.retainedRoots(scope: "chain-a:state") == [keep])
        let evicted = try await broker.evictUnpinned()
        #expect(evicted == 1)
        #expect(await broker.hasVolume(root: keep))
        #expect(await broker.hasVolume(root: drop) == false)
    }

    @Test func retainedRootReplaceIsNaturallyIdempotent() async throws {
        let broker = MemoryBroker(evictUnpinnedGrace: .zero)
        let r1 = cid("r1")
        try await broker.storeVolumeLocal(payload("r1"))

        try await broker.advanceRetainedRoots(scope: "chain-a:state", roots: [r1])
        try await broker.advanceRetainedRoots(scope: "chain-a:state", roots: [r1])

        #expect(try await broker.retainedRoots(scope: "chain-a:state") == [r1])
    }

    @Test func retainedRootMergeAddsWithoutReplacingScope() async throws {
        let broker = MemoryBroker(evictUnpinnedGrace: .zero)
        let old = cid("old")
        let new = cid("new")
        let drop = cid("drop")
        try await broker.storeVolumeLocal(payload("old"))
        try await broker.storeVolumeLocal(payload("new"))
        try await broker.storeVolumeLocal(payload("drop"))

        try await broker.advanceRetainedRoots(scope: "chain-a:state", roots: [old])
        try await broker.mergeRetainedRoots(scope: "chain-a:state", roots: [new])
        try await broker.mergeRetainedRoots(scope: "chain-a:state", roots: [new])

        #expect(Set(try await broker.retainedRoots(scope: "chain-a:state")) == [new, old])
        let evicted = try await broker.evictUnpinned()
        #expect(evicted == 1)
        #expect(await broker.hasVolume(root: old))
        #expect(await broker.hasVolume(root: new))
        #expect(await broker.hasVolume(root: drop) == false)
    }

    @Test func retainedRootDoesNotRequireRelatedVolume() async throws {
        let broker = MemoryBroker(evictUnpinnedGrace: .zero)
        let root = cid("root")
        try await broker.storeVolumeLocal(payload("root"))

        try await broker.advanceRetainedRoots(scope: "chain-a:state", roots: [root])

        #expect(try await broker.retainedRoots(scope: "chain-a:state") == [root])
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
        let remote = MemoryBroker()
        let local = MemoryBroker(near: remote)
        let p = payload("r1", ["c1": Data([42])])
        try await remote.storeVolumeLocal(p)

        let fetched = await local.fetchVolume(root: p.root)
        #expect(fetched?.entries[cid(for: Data([42]))] == Data([42]))
    }
}

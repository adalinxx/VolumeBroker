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

    @Test func storeOwnsItsPublishedBytes() async throws {
        let count = 4_096
        let pointer = UnsafeMutableRawPointer.allocate(byteCount: count, alignment: 1)
        defer { pointer.deallocate() }
        pointer.initializeMemory(as: UInt8.self, repeating: 7, count: count)
        let borrowed = Data(bytesNoCopy: pointer, count: count, deallocator: .none)
        let expected = Data(bytes: pointer, count: count)
        let root = cid(for: borrowed)
        let broker = MemoryBroker()

        try await broker.store(volume: SerializedVolume(root: root, entries: [root: borrowed]))
        let bytes = pointer.bindMemory(to: UInt8.self, capacity: count)
        for index in 0..<count { bytes[index] = 9 }

        let fetched = try #require(await broker.fetchVolumeLocal(root: root))
        #expect(await broker.hasVolume(root: root))
        #expect(fetched.entries[root] == expected)
        try fetched.validate()
    }

    @Test func storeAndFetch() async throws {
        let broker = MemoryBroker()
        let p = payload("r1", ["c1": Data([1]), "c2": Data([2])])
        try await broker.store(volume: p)

        #expect(await broker.hasVolume(root: p.root))
        let fetched = await broker.fetchVolumeLocal(root: p.root)
        #expect(fetched?.entries.count == 3)
        #expect(fetched?.entries[cid(for: Data([1]))] == Data([1]))
    }

    @Test func sharedCIDUsesUniqueBytesAndReleasesAtLastOwner() async throws {
        let broker = MemoryBroker()
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
        try await broker.store(volume: first)

        try await broker.advanceRetainedRoots(scope: "scope", roots: [firstRoot])
        #expect(try await broker.sweep() == 1)
        #expect(await broker.fetchDataLocal(cid: shared) == sharedData)
        #expect(await broker.fetchDataLocal(cid: secondRoot) == nil)

        try await broker.advanceRetainedRoots(scope: "scope", roots: [])
        #expect(try await broker.sweep() == 1)
        #expect(await broker.fetchDataLocal(cid: shared) == nil)
    }

    @Test func missingVolumeReturnsNil() async {
        let broker = MemoryBroker()
        #expect(await broker.fetchVolumeLocal(root: "missing") == nil)
        #expect(await broker.hasVolume(root: "missing") == false)
    }

    @Test func retainedRootSurvivesSweep() async throws {
        let broker = MemoryBroker()
        let keep = cid("keep")
        let drop = cid("drop")
        try await broker.store(volume: payload("keep"))
        try await broker.store(volume: payload("drop"))

        try await broker.advanceRetainedRoots(scope: "chain-a:state", roots: [keep])

        #expect(try await broker.retainedRoots(scope: "chain-a:state") == [keep])
        let evicted = try await broker.sweep()
        #expect(evicted == 1)
        #expect(await broker.hasVolume(root: keep))
        #expect(await broker.hasVolume(root: drop) == false)
    }

    @Test func retainedRootReplaceIsNaturallyIdempotent() async throws {
        let broker = MemoryBroker()
        let r1 = cid("r1")
        try await broker.store(volume: payload("r1"))

        try await broker.advanceRetainedRoots(scope: "chain-a:state", roots: [r1])
        try await broker.advanceRetainedRoots(scope: "chain-a:state", roots: [r1])

        #expect(try await broker.retainedRoots(scope: "chain-a:state") == [r1])
    }

    @Test func retainedRootMergeAddsWithoutReplacingScope() async throws {
        let broker = MemoryBroker()
        let old = cid("old")
        let new = cid("new")
        let drop = cid("drop")
        try await broker.store(volume: payload("old"))
        try await broker.store(volume: payload("new"))
        try await broker.store(volume: payload("drop"))

        try await broker.advanceRetainedRoots(scope: "chain-a:state", roots: [old])
        try await broker.mergeRetainedRoots(scope: "chain-a:state", roots: [new])
        try await broker.mergeRetainedRoots(scope: "chain-a:state", roots: [new])

        #expect(Set(try await broker.retainedRoots(scope: "chain-a:state")) == [new, old])
        let evicted = try await broker.sweep()
        #expect(evicted == 1)
        #expect(await broker.hasVolume(root: old))
        #expect(await broker.hasVolume(root: new))
        #expect(await broker.hasVolume(root: drop) == false)
    }

    @Test func retainedRootDoesNotRequireRelatedVolume() async throws {
        let broker = MemoryBroker()
        let root = cid("root")
        try await broker.store(volume: payload("root"))

        try await broker.advanceRetainedRoots(scope: "chain-a:state", roots: [root])

        #expect(try await broker.retainedRoots(scope: "chain-a:state") == [root])
    }

    @Test func cascadeFetchFallsThrough() async throws {
        let remote = MemoryBroker()
        let local = MemoryBroker(near: remote)
        let p = payload("r1", ["c1": Data([42])])
        try await remote.store(volume: p)

        let fetched = await local.fetchVolume(root: p.root)
        #expect(fetched?.entries[cid(for: Data([42]))] == Data([42]))
    }

    @Test func batchReadPreservesLocalNearFarPrecedence() async throws {
        let far = MemoryBroker()
        let near = MemoryBroker()
        let local = MemoryBroker(near: near, far: far)
        let localVolume = payload("local")
        let nearVolume = payload("near")
        let farVolume = payload("far")
        try await local.store(volume: localVolume)
        try await near.store(volume: nearVolume)
        try await far.store(volume: farVolume)

        let found = await local.fetchData(cids: [
            localVolume.root, nearVolume.root, farVolume.root, "missing",
        ])

        #expect(found[localVolume.root] == localVolume.entries[localVolume.root])
        #expect(found[nearVolume.root] == nearVolume.entries[nearVolume.root])
        #expect(found[farVolume.root] == farVolume.entries[farVolume.root])
        #expect(found["missing"] == nil)
    }
}

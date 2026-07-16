import CID
import Foundation
import Multihash
import Testing
@testable import VolumeBroker

@Suite("BrokerStorer")
struct BrokerStorerTests {
    private enum InjectedFailure: Error {
        case store
    }

    private final class FlakyBroker: VolumeBroker, @unchecked Sendable {
        var near: (any VolumeBroker)?
        var far: (any VolumeBroker)?
        var failNextStore = true
        let backing = MemoryBroker()

        func hasVolume(root: String) async -> Bool { await backing.hasVolume(root: root) }
        func fetchVolumeLocal(root: String) async -> SerializedVolume? {
            await backing.fetchVolumeLocal(root: root)
        }
        func storeVolumeLocal(_ volume: SerializedVolume) async throws {
            if failNextStore {
                failNextStore = false
                throw InjectedFailure.store
            }
            try await backing.storeVolumeLocal(volume)
        }
        func pin(root: String, owner: String, count: Int, ttl: Duration?) async throws {
            try await backing.pin(root: root, owner: owner, count: count, ttl: ttl)
        }
        func unpin(root: String, owner: String, count: Int) async throws {
            try await backing.unpin(root: root, owner: owner, count: count)
        }
        func unpinAll(owner: String) async throws { try await backing.unpinAll(owner: owner) }
        func owners(root: String) async -> Set<String> { await backing.owners(root: root) }
        func evictUnpinned() async throws -> Int { try await backing.evictUnpinned() }
    }

    private func cid(for data: Data) -> String {
        let multihash = try! Multihash(raw: data, hashedWith: .sha2_256)
        return try! CID(version: .v1, codec: .dag_cbor, multihash: multihash).toBaseEncodedString
    }

    @Test func storesOneCompleteVolumeDirectly() async throws {
        let broker = MemoryBroker()
        let storer = BrokerStorer(broker: broker)
        let rootData = Data("root-data".utf8)
        let childData = Data("child-data".utf8)
        let root = cid(for: rootData)
        let child = cid(for: childData)

        try await storer.store(volume: SerializedVolume(
            root: root,
            entries: [root: rootData, child: childData]
        ))

        let stored = await broker.fetchVolumeLocal(root: root)
        #expect(stored?.entries[root] == rootData)
        #expect(stored?.entries[child] == childData)
        #expect(await broker.fetchVolumeLocal(root: child) == nil)
    }

    @Test func storesSparseEntriesAsIndependentVolumes() async throws {
        let broker = MemoryBroker()
        let storer = BrokerStorer(broker: broker)
        let firstData = Data("first".utf8)
        let secondData = Data("second".utf8)
        let first = cid(for: firstData)
        let second = cid(for: secondData)

        try await storer.store(entries: [first: firstData, second: secondData])

        #expect(await broker.fetchVolumeLocal(root: first)?.entries == [first: firstData])
        #expect(await broker.fetchVolumeLocal(root: second)?.entries == [second: secondData])
    }

    @Test func volumePayloadsRemainIndependent() async throws {
        let broker = MemoryBroker()
        let storer = BrokerStorer(broker: broker)
        let outerData = Data("outer".utf8)
        let nestedData = Data("nested".utf8)
        let deepData = Data("deep".utf8)
        let outer = cid(for: outerData)
        let nested = cid(for: nestedData)
        let deep = cid(for: deepData)

        try await storer.store(volume: SerializedVolume(root: outer, entries: [outer: outerData]))
        try await storer.store(volume: SerializedVolume(
            root: nested,
            entries: [nested: nestedData, deep: deepData]
        ))

        #expect(await broker.fetchVolumeLocal(root: outer)?.entries[nested] == nil)
        #expect(await broker.fetchVolumeLocal(root: nested)?.entries[deep] == deepData)
    }

    @Test func failedStoreHasNoAdapterStateAndCanBeResubmitted() async throws {
        let broker = FlakyBroker()
        let storer = BrokerStorer(broker: broker)
        let data = Data("retry".utf8)
        let root = cid(for: data)
        let volume = SerializedVolume(root: root, entries: [root: data])

        do {
            try await storer.store(volume: volume)
            Issue.record("expected the first store to fail")
        } catch {
            #expect(error as? InjectedFailure == .store)
        }
        #expect(await broker.hasVolume(root: root) == false)

        try await storer.store(volume: volume)
        #expect(await broker.hasVolume(root: root))
    }
}

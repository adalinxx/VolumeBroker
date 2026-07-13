import Testing
import Foundation
import CID
import Multihash
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
            try await storeVolumesLocal([volume])
        }
        func storeVolumesLocal(_ volumes: [SerializedVolume]) async throws {
            if failNextStore {
                failNextStore = false
                throw InjectedFailure.store
            }
            try await backing.storeVolumesLocal(volumes)
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

    @Test func flushStoresOnlyVolumeRoots() async throws {
        let broker = MemoryBroker()
        let storer = BrokerStorer(broker: broker)
        let rootData = Data("root-data".utf8)
        let childData = Data("child-data".utf8)
        let root = cid(for: rootData)
        let child = cid(for: childData)

        try storer.enterVolume(rootCID: root)
        try storer.store(rawCid: root, data: rootData)
        try storer.store(rawCid: child, data: childData)
        try storer.exitVolume(rootCID: root)
        try await storer.flush(root: root)

        let rootVolume = await broker.fetchVolumeLocal(root: root)
        let childVolume = await broker.fetchVolumeLocal(root: child)

        #expect(rootVolume?.entries[root] == rootData)
        #expect(rootVolume?.entries[child] == childData)
        #expect(childVolume == nil)
        #expect(storer.storedRoots == [root])
    }

    @Test func collectVolumesReturnsOnlyVolumeRoots() throws {
        let storer = BrokerStorer(broker: MemoryBroker())
        let rootData = Data("root-data".utf8)
        let childData = Data("child-data".utf8)
        let root = cid(for: rootData)
        let child = cid(for: childData)

        try storer.enterVolume(rootCID: root)
        try storer.store(rawCid: root, data: rootData)
        try storer.store(rawCid: child, data: childData)
        try storer.exitVolume(rootCID: root)

        let volumes = storer.collectVolumes(root: root)

        #expect(volumes.map(\.root) == [root])
        #expect(volumes.first?.entries[child] == childData)
        #expect(storer.storedRoots == [root])
    }

    @Test func relatedVolumesRemainIndependent() async throws {
        let broker = MemoryBroker()
        let storer = BrokerStorer(broker: broker)
        let objData = Data("obj".utf8)
        let nestedData = Data("nested".utf8)
        let deepData = Data("deep".utf8)
        let obj = cid(for: objData)
        let nested = cid(for: nestedData)
        let deep = cid(for: deepData)

        // Cashew completes the parent before storing a materialized nested Volume.
        try storer.enterVolume(rootCID: obj)
        try storer.store(rawCid: obj, data: objData)
        try storer.exitVolume(rootCID: obj)
        try storer.enterVolume(rootCID: nested)
        try storer.store(rawCid: nested, data: nestedData)
        try storer.store(rawCid: deep, data: deepData)
        try storer.exitVolume(rootCID: nested)
        try await storer.flush(root: obj)

        let objVolume = await broker.fetchVolumeLocal(root: obj)
        let nestedVolume = await broker.fetchVolumeLocal(root: nested)

        #expect(objVolume?.entries[obj] == objData)
        #expect(objVolume?.entries[nested] == nil)
        #expect(nestedVolume?.entries[nested] == nestedData)
        #expect(nestedVolume?.entries[deep] == deepData)
        #expect(objVolume?.entries[deep] == nil)
    }

    @Test func exitWithoutRootFailsClosed() throws {
        let broker = MemoryBroker()
        let storer = BrokerStorer(broker: broker)

        try storer.enterVolume(rootCID: "empty")
        do {
            try storer.exitVolume(rootCID: "empty")
            Issue.record("expected a missing-root failure")
        } catch {
            #expect(error as? SerializedVolumeError == .missingRootEntry("empty"))
        }
        #expect(storer.openVolumeRoots == ["empty"])
    }

    @Test func collectVolumesIncludesZeroByteRoot() throws {
        let storer = BrokerStorer(broker: MemoryBroker())
        let rootData = Data()
        let root = cid(for: rootData)

        try storer.enterVolume(rootCID: root)
        try storer.store(rawCid: root, data: rootData)
        try storer.exitVolume(rootCID: root)

        let volumes = storer.collectVolumes(root: root)

        #expect(volumes.count == 1)
        #expect(volumes.first?.root == root)
        #expect(volumes.first?.entries[root] == Data())
        #expect(storer.storedRoots == [root])
    }

    @Test func failedFlushKeepsCompletedVolumesForRetry() async throws {
        let broker = FlakyBroker()
        let storer = BrokerStorer(broker: broker)
        let rootData = Data("retry".utf8)
        let root = cid(for: rootData)
        try storer.enterVolume(rootCID: root)
        try storer.store(rawCid: root, data: rootData)
        try storer.exitVolume(rootCID: root)

        do {
            try await storer.flush(root: root)
            Issue.record("expected the first flush to fail")
        } catch {
            #expect(error as? InjectedFailure == .store)
        }
        #expect(storer.storedRoots.isEmpty)

        try await storer.flush(root: root)
        #expect(await broker.hasVolume(root: root))
        #expect(storer.storedRoots == [root])
    }
}

import Testing
import Foundation
@testable import VolumeBroker

@Suite("BrokerStorer")
struct BrokerStorerTests {
    @Test func flushStoresOnlyVolumeRoots() async throws {
        let broker = MemoryBroker()
        let storer = BrokerStorer(broker: broker)

        try storer.enterVolume(rootCID: "root")
        try storer.store(rawCid: "root", data: Data("root-data".utf8))
        try storer.store(rawCid: "child", data: Data("child-data".utf8))
        try storer.exitVolume(rootCID: "root")
        try await storer.flush(root: "root")

        let rootVolume = await broker.fetchVolumeLocal(root: "root")
        let childVolume = await broker.fetchVolumeLocal(root: "child")

        #expect(rootVolume?.entries["root"] == Data("root-data".utf8))
        #expect(rootVolume?.entries["child"] == Data("child-data".utf8))
        #expect(childVolume == nil)
        #expect(storer.storedRoots == ["root"])
    }

    @Test func collectVolumesReturnsOnlyVolumeRoots() throws {
        let storer = BrokerStorer(broker: MemoryBroker())

        try storer.enterVolume(rootCID: "root")
        try storer.store(rawCid: "root", data: Data("root-data".utf8))
        try storer.store(rawCid: "child", data: Data("child-data".utf8))
        try storer.exitVolume(rootCID: "root")

        let volumes = storer.collectVolumes(root: "root")

        #expect(volumes.map(\.root) == ["root"])
        #expect(volumes.first?.entries["child"] == Data("child-data".utf8))
        #expect(storer.storedRoots == ["root"])
    }

    /// A nested volume boundary records an owned-reachability edge: the child's
    /// root becomes an entry of the parent volume, so `volume_entries(parent,
    /// child)` is written at flush. This is what lets transitive eviction
    /// protect an object's owned closure from a single pin on its root.
    @Test func nestedVolumeRecordsOwnedChildEdge() async throws {
        let broker = MemoryBroker()
        let storer = BrokerStorer(broker: broker)

        // Shape cashew's storeRecursively emits for obj → nested → deep:
        try storer.enterVolume(rootCID: "obj")
        try storer.store(rawCid: "obj", data: Data("obj".utf8))
        try storer.enterVolume(rootCID: "nested")
        try storer.store(rawCid: "nested", data: Data("nested".utf8))
        try storer.store(rawCid: "deep", data: Data("deep".utf8))   // in-package entry of nested
        try storer.exitVolume(rootCID: "nested")
        try storer.exitVolume(rootCID: "obj")
        try await storer.flush(root: "obj")

        let objVolume = await broker.fetchVolumeLocal(root: "obj")
        let nestedVolume = await broker.fetchVolumeLocal(root: "nested")

        // The edge: the parent volume now carries the child's root node.
        #expect(objVolume?.entries["nested"] == Data("nested".utf8), "parent records child root edge")
        #expect(objVolume?.entries["obj"] == Data("obj".utf8))
        // The child volume is unchanged (its own root + in-package entries).
        #expect(nestedVolume?.entries["nested"] == Data("nested".utf8))
        #expect(nestedVolume?.entries["deep"] == Data("deep".utf8))
        // The grandchild is reachable transitively (obj → nested → deep), not as
        // a direct edge of obj.
        #expect(objVolume?.entries["deep"] == nil, "grandchild is not a direct edge of obj")
    }

    @Test func flushStoresEmptyVolumeRoot() async throws {
        let broker = MemoryBroker()
        let storer = BrokerStorer(broker: broker)

        try storer.enterVolume(rootCID: "empty")
        try storer.exitVolume(rootCID: "empty")
        try await storer.flush(root: "empty")

        let emptyVolume = await broker.fetchVolumeLocal(root: "empty")
        #expect(emptyVolume?.entries.isEmpty == true)
        #expect(storer.storedRoots == ["empty"])
    }

    @Test func collectVolumesIncludesEmptyVolumeRoot() throws {
        let storer = BrokerStorer(broker: MemoryBroker())

        try storer.enterVolume(rootCID: "empty")
        try storer.exitVolume(rootCID: "empty")

        let volumes = storer.collectVolumes(root: "empty")

        #expect(volumes.count == 1)
        #expect(volumes.first?.root == "empty")
        #expect(volumes.first?.entries.isEmpty == true)
        #expect(storer.storedRoots == ["empty"])
    }
}

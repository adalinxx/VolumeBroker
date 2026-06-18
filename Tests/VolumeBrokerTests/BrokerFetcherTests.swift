import Foundation
import Testing
import cashew
@testable import VolumeBroker

@Suite("BrokerFetcher")
struct BrokerFetcherTests {
    typealias TestDictionary = VolumeMerkleDictionaryImpl<String>
    typealias TestVolume = VolumeImpl<TestDictionary>

    @Test func resolvesMultiNodeVolumeThroughBatchedSource() async throws {
        let broker = MemoryBroker()
        let storer = BrokerStorer(broker: broker)
        let dictionary = try TestDictionary()
            .inserting(key: "alice", value: "v1")
            .inserting(key: "alicia", value: "v2")
            .inserting(key: "bob", value: "v3")
        let original = try TestVolume(node: dictionary)

        try original.storeRecursively(storer: storer)
        try await storer.flush(root: original.rawCID)

        #expect(storer.storedRoots.count >= 2)

        let source = BrokerFetcher(broker: broker)
        let lazyRoot = TestVolume(rawCID: original.rawCID, node: nil, encryptionInfo: nil)
        let resolved = try await lazyRoot.resolveRecursive(source: source)

        #expect(try resolved.node?.get(key: "alice") == "v1")
        #expect(try resolved.node?.get(key: "alicia") == "v2")
        #expect(try resolved.node?.get(key: "bob") == "v3")
    }

    @Test func batchedFetchReturnsRequestedVolumeRoots() async throws {
        let broker = MemoryBroker()
        let storer = BrokerStorer(broker: broker)
        let a = try TestVolume(node: TestDictionary().inserting(key: "k", value: "va"))
        let b = try TestVolume(node: TestDictionary().inserting(key: "k", value: "vb"))
        try a.storeRecursively(storer: storer); try await storer.flush(root: a.rawCID)
        try b.storeRecursively(storer: storer); try await storer.flush(root: b.rawCID)

        let source = BrokerFetcher(broker: broker)
        let got = await source.fetch([a.rawCID, b.rawCID, "missing"])
        #expect(got[a.rawCID] != nil)
        #expect(got[b.rawCID] != nil)
        #expect(got["missing"] == nil)
    }

    @Test func fetchesInternalEntryByCidWithoutEnteringVolume() async throws {
        // Object-grain storage: one volume "obj" holds multiple entries; the
        // internal entry is NOT its own volume root. With the cas_data CID-blob
        // primitive it resolves directly by CID — no enterVolume needed.
        let broker = MemoryBroker()
        let storer = BrokerStorer(broker: broker)
        try storer.enterVolume(rootCID: "obj")
        try storer.store(rawCid: "obj", data: Data("root".utf8))
        try storer.store(rawCid: "internal", data: Data("internal-data".utf8))
        try storer.exitVolume(rootCID: "obj")
        try await storer.flush(root: "obj")

        // No volume is keyed "internal" — it's only an entry under "obj".
        #expect(await broker.fetchVolumeLocal(root: "internal") == nil)

        let source = BrokerFetcher(broker: broker)
        let data = try await source.fetch(rawCid: "internal")
        #expect(data == Data("internal-data".utf8))

        let batched = await source.fetch(["obj", "internal", "missing"])
        #expect(batched["obj"] == Data("root".utf8))
        #expect(batched["internal"] == Data("internal-data".utf8))
        #expect(batched["missing"] == nil)
    }
}

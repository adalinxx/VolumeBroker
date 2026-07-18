import Foundation
import Testing
import cashew
import CID
import Multihash
@testable import VolumeBroker

@Suite("BrokerFetcher")
struct BrokerFetcherTests {
    typealias TestDictionary = VolumeMerkleDictionaryImpl<String>
    typealias TestVolume = VolumeImpl<TestDictionary>

    private func cid(for data: Data) -> String {
        let multihash = try! Multihash(raw: data, hashedWith: .sha2_256)
        return try! CID(version: .v1, codec: .dag_cbor, multihash: multihash).toBaseEncodedString
    }

    @Test func resolvesMultiNodeVolumeThroughBatchedSource() async throws {
        let broker = MemoryBroker()
        let storer = BrokerStorer(broker: broker)
        let dictionary = try TestDictionary()
            .inserting(key: "alice", value: "v1")
            .inserting(key: "alicia", value: "v2")
            .inserting(key: "bob", value: "v3")
        let original = try TestVolume(node: dictionary)

        try await original.storeRecursively(storer: storer)

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
        try await a.storeRecursively(storer: storer)
        try await b.storeRecursively(storer: storer)

        let source = BrokerFetcher(broker: broker)
        let got = await source.fetch([a.rawCID, b.rawCID, "missing"])
        #expect(got[a.rawCID] != nil)
        #expect(got[b.rawCID] != nil)
        #expect(got["missing"] == nil)
    }

    @Test func fetchesInternalEntryByCidWithoutEnteringVolume() async throws {
        // Object-grain storage: one Volume holds multiple entries; the
        // internal entry is NOT its own volume root. With the cas_data CID-blob
        // primitive it resolves directly by CID.
        let broker = MemoryBroker()
        let storer = BrokerStorer(broker: broker)
        let rootData = Data("root".utf8)
        let internalData = Data("internal-data".utf8)
        let root = cid(for: rootData)
        let internalCID = cid(for: internalData)
        try await storer.store(volume: SerializedVolume(
            root: root,
            entries: [root: rootData, internalCID: internalData]
        ))

        // No Volume is keyed by the internal CID; it is only an entry under root.
        #expect(await broker.fetchVolumeLocal(root: internalCID) == nil)

        let source = BrokerFetcher(broker: broker)
        let data = try await source.fetch(rawCid: internalCID)
        #expect(data == internalData)

        let batched = await source.fetch([root, internalCID, "missing"])
        #expect(batched[root] == rootData)
        #expect(batched[internalCID] == internalData)
        #expect(batched["missing"] == nil)
    }
}

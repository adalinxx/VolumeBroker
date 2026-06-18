import Foundation
import Testing
import ArrayTrie
import cashew
@testable import VolumeBroker

@Suite("ContentStore object facade")
struct ContentStoreTests {
    typealias Dict = VolumeMerkleDictionaryImpl<String>

    @Test func putThenGetRoundTrips() async throws {
        let store = ContentStore(broker: MemoryBroker())
        let dict = try Dict()
            .inserting(key: "alice", value: "v1")
            .inserting(key: "bob", value: "v2")
        let rootCID = try await store.put(dict)

        #expect(await store.has(rootCID))
        #expect(await store.hasDurable(rootCID))

        let got = try await store.getRecursive(Dict.self, rootCID)
        #expect(try got?.get(key: "alice") == "v1")
        #expect(try got?.get(key: "bob") == "v2")
    }

    @Test func getMissingObjectReturnsNil() async throws {
        let store = ContentStore(broker: MemoryBroker())
        #expect(await store.has("nonexistent") == false)
    }
}

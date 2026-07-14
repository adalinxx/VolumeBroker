import Foundation
import Testing
import ArrayTrie
import cashew
@testable import VolumeBroker

@Suite("ContentStore object facade")
struct ContentStoreTests {
    typealias Dict = VolumeMerkleDictionaryImpl<String>

    private struct Leaf: Scalar {
        let value: String
    }

    private enum EncodingFailure: Error {
        case injected
    }

    private struct FailingLeaf: Scalar {
        init() {}

        init(from decoder: Decoder) throws {
            self.init()
        }

        func encode(to encoder: Encoder) throws {
            throw EncodingFailure.injected
        }
    }

    private struct ObjectWithNestedVolume: Node, Sendable {
        let child: VolumeImpl<FailingLeaf>

        func get(property: PathSegment) -> (any Header)? {
            property == "child" ? child : nil
        }

        func properties() -> Set<PathSegment> { ["child"] }

        func set(properties: [PathSegment: any Header]) -> Self {
            guard let child = properties["child"] as? VolumeImpl<FailingLeaf> else { return self }
            return Self(child: child)
        }
    }

    private struct ObjectWithTwoVolumes: Node, Sendable {
        let selected: VolumeImpl<Leaf>
        let sibling: VolumeImpl<Leaf>

        func get(property: PathSegment) -> (any Header)? {
            switch property {
            case "selected": selected
            case "sibling": sibling
            default: nil
            }
        }

        func properties() -> Set<PathSegment> { ["selected", "sibling"] }

        func set(properties: [PathSegment: any Header]) -> Self {
            Self(
                selected: properties["selected"] as? VolumeImpl<Leaf> ?? selected,
                sibling: properties["sibling"] as? VolumeImpl<Leaf> ?? sibling
            )
        }
    }

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

    @Test func completedParentIsStoredBeforeNestedFailureReturns() async throws {
        let placeholder = try VolumeImpl(node: Leaf(value: "placeholder"))
        let failingChild = VolumeImpl<FailingLeaf>(
            rawCID: placeholder.rawCID,
            node: FailingLeaf(),
            encryptionInfo: nil
        )
        let object = ObjectWithNestedVolume(child: failingChild)
        let expectedRoot = try VolumeImpl(node: object).rawCID
        let broker = MemoryBroker()
        let store = ContentStore(broker: broker)

        do {
            _ = try await store.put(object)
            Issue.record("expected nested serialization failure")
        } catch {
            #expect(error as? DataErrors == .serializationFailed)
        }

        #expect(await broker.hasVolume(root: expectedRoot))
        #expect(await broker.hasVolume(root: failingChild.rawCID) == false)
    }

    @Test func targetedPutStoresOnlySelectedNestedVolumes() async throws {
        let selected = try VolumeImpl(node: Leaf(value: "selected"))
        let sibling = try VolumeImpl(node: Leaf(value: "sibling"))
        let object = ObjectWithTwoVolumes(selected: selected, sibling: sibling)
        let broker = MemoryBroker()
        let store = ContentStore(broker: broker)

        let root = try await store.put(
            object,
            storing: [["selected"]: StorageStrategy.targeted]
        )

        #expect(await broker.hasVolume(root: root))
        #expect(await broker.hasVolume(root: selected.rawCID))
        #expect(await broker.hasVolume(root: sibling.rawCID) == false)
    }
}

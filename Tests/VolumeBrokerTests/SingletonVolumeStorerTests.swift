import CID
import Foundation
import Multihash
import Testing
import cashew
@testable import VolumeBroker

@Suite("SingletonVolumeStorer")
struct SingletonVolumeStorerTests {
    private func cid(for data: Data) throws -> String {
        let multihash = try Multihash(raw: data, hashedWith: .sha2_256)
        return try CID(version: .v1, codec: .dag_cbor, multihash: multihash)
            .toBaseEncodedString
    }

    @Test func emptyBatchIsANoOp() async throws {
        try await SingletonVolumeStorer(broker: MemoryBroker(capacity: 0)).store(entries: [:])
    }

    @Test func storesMultiEntryBatchAtomicallyAsSingletonVolumes() async throws {
        let firstData = Data("first".utf8)
        let secondData = Data("second".utf8)
        let first = try cid(for: firstData)
        let second = try cid(for: secondData)
        let entries = [first: firstData, second: secondData]

        let constrained = MemoryBroker(capacity: 1)
        do {
            try await SingletonVolumeStorer(broker: constrained).store(entries: entries)
            Issue.record("the whole batch must be rejected when it cannot fit")
        } catch {
            #expect(error as? BrokerError == .capacityExceeded)
        }
        #expect(await constrained.hasVolume(root: first) == false)
        #expect(await constrained.hasVolume(root: second) == false)

        let broker = MemoryBroker()
        try await SingletonVolumeStorer(broker: broker).store(entries: entries)
        #expect(await broker.fetchVolumeLocal(root: first)?.entries == [first: firstData])
        #expect(await broker.fetchVolumeLocal(root: second)?.entries == [second: secondData])
    }

    @Test func rejectsCIDMismatchWithoutPartialState() async throws {
        let validData = Data("valid".utf8)
        let invalidData = Data("invalid".utf8)
        let valid = try cid(for: validData)
        let invalid = try cid(for: invalidData)
        let broker = MemoryBroker()

        do {
            try await SingletonVolumeStorer(broker: broker).store(entries: [
                valid: validData,
                invalid: Data("wrong".utf8),
            ])
            Issue.record("a CID mismatch must reject the whole batch")
        } catch {
            #expect(error as? SerializedVolumeError == .contentAddressMismatch(invalid))
        }
        #expect(await broker.hasVolume(root: valid) == false)
        #expect(await broker.hasVolume(root: invalid) == false)
    }

    @Test func identicalReplayIsIdempotent() async throws {
        let data = Data("replay".utf8)
        let root = try cid(for: data)
        let broker = MemoryBroker()
        let storer = SingletonVolumeStorer(broker: broker)

        try await storer.store(entries: [root: data])
        try await storer.store(entries: [root: data])

        #expect(await broker.fetchVolumeLocal(root: root)?.entries == [root: data])
    }

    @Test func singletonMembershipCannotBeWidened() async throws {
        let rootData = Data("root".utf8)
        let childData = Data("child".utf8)
        let root = try cid(for: rootData)
        let child = try cid(for: childData)
        let broker = MemoryBroker()

        try await SingletonVolumeStorer(broker: broker).store(entries: [root: rootData])
        do {
            try await broker.storeVolumeLocal(SerializedVolume(
                root: root,
                entries: [root: rootData, child: childData]
            ))
            Issue.record("singleton membership must remain immutable")
        } catch {
            #expect(error as? BrokerError == .conflictingVolume(root))
        }
        #expect(await broker.fetchVolumeLocal(root: root)?.entries == [root: rootData])
    }

    @Test func keepsSparseAndCompleteConformanceBoundariesDistinct() {
        let broker = MemoryBroker()
        let sparse: any Storer = SingletonVolumeStorer(broker: broker)
        let complete: any VolumeStorer = BrokerStorer(broker: broker)

        #expect(!((sparse as Any) is any VolumeStorer))
        #expect(!((complete as Any) is any Storer))
    }
}

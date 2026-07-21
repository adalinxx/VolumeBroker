import CID
import Foundation
import Multihash
import Testing
import cashew
@testable import VolumeBroker

@Suite("BrokerStorer")
struct BrokerStorerTests {
    private enum InjectedFailure: Error {
        case store
    }

    private final class FlakyBroker: VolumeBroker, @unchecked Sendable {
        let near: (any VolumeBroker)? = nil
        let far: (any VolumeBroker)? = nil
        var failNextStore = true
        private(set) var attempts: [(root: String, entries: [String: Data])] = []
        let backing = MemoryBroker()

        func hasVolume(root: String) async -> Bool { await backing.hasVolume(root: root) }
        func fetchVolumeLocal(root: String) async -> SerializedVolume? {
            await backing.fetchVolumeLocal(root: root)
        }
        func storeEntriesLocal(_ entries: [String: Data]) async throws {
            if failNextStore {
                failNextStore = false
                throw InjectedFailure.store
            }
            try await backing.storeEntriesLocal(entries)
        }
        func storeVolumesLocal(_ volumes: [SerializedVolume]) async throws {
            attempts.append(contentsOf: volumes.map { ($0.root, $0.entries) })
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

    @Test func conformsToCompleteAndRawStorer() {
        let broker = MemoryBroker()
        let storer = BrokerStorer(broker: broker)

        #expect((storer as Any) is any VolumeStorer)
        #expect((storer as Any) is any Storer)
    }

    @Test func emptyRawBatchIsANoOp() async throws {
        try await BrokerStorer(broker: MemoryBroker(capacity: 0)).store(entries: [:])
    }

    @Test func storesRawBatchAtomicallyAsLooseCASEntries() async throws {
        let firstData = Data("first".utf8)
        let secondData = Data("second".utf8)
        let first = cid(for: firstData)
        let second = cid(for: secondData)
        let entries = [first: firstData, second: secondData]

        let constrained = MemoryBroker(byteBudget: firstData.count)
        do {
            try await BrokerStorer(broker: constrained).store(entries: entries)
            Issue.record("the whole batch must be rejected when it cannot fit")
        } catch {
            #expect(error as? BrokerError == .capacityExceeded)
        }
        #expect(await constrained.hasVolume(root: first) == false)
        #expect(await constrained.hasVolume(root: second) == false)

        let broker = MemoryBroker()
        try await BrokerStorer(broker: broker).store(entries: entries)
        #expect(await broker.fetchVolumeLocal(root: first) == nil)
        #expect(await broker.fetchVolumeLocal(root: second) == nil)
        #expect(await broker.fetchDataLocal(cid: first) == firstData)
        #expect(await broker.fetchDataLocal(cid: second) == secondData)
    }

    @Test func rawCIDMismatchRejectsWholeBatch() async throws {
        let validData = Data("valid".utf8)
        let invalidData = Data("invalid".utf8)
        let valid = cid(for: validData)
        let invalid = cid(for: invalidData)
        let broker = MemoryBroker()

        do {
            try await BrokerStorer(broker: broker).store(entries: [
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

    @Test func identicalRawReplayIsIdempotent() async throws {
        let data = Data("replay".utf8)
        let root = cid(for: data)
        let broker = MemoryBroker()
        let storer = BrokerStorer(broker: broker)

        try await storer.store(entries: [root: data])
        try await storer.store(entries: [root: data])

        #expect(await broker.fetchVolumeLocal(root: root) == nil)
        #expect(await broker.fetchDataLocal(cid: root) == data)
    }

    @Test func matchingRawRewritePreservesExistingCompleteMembership() async throws {
        let rootData = Data("complete-root".utf8)
        let childData = Data("complete-child".utf8)
        let root = cid(for: rootData)
        let child = cid(for: childData)
        let broker = MemoryBroker()
        let storer = BrokerStorer(broker: broker)
        let complete = SerializedVolume(
            root: root,
            entries: [root: rootData, child: childData]
        )

        try await storer.store(volume: complete)
        try await storer.store(entries: [root: rootData])

        #expect(await broker.fetchVolumeLocal(root: root)?.entries == complete.entries)
    }

    @Test func conflictingRawRewriteRejectsBeforeStoringNewLooseEntries() async throws {
        let rootData = Data("complete-root".utf8)
        let childData = Data("complete-child".utf8)
        let newData = Data("new-singleton".utf8)
        let root = cid(for: rootData)
        let child = cid(for: childData)
        let newRoot = cid(for: newData)
        let broker = MemoryBroker()
        let storer = BrokerStorer(broker: broker)
        let complete = SerializedVolume(
            root: root,
            entries: [root: rootData, child: childData]
        )

        try await storer.store(volume: complete)
        do {
            try await storer.store(entries: [
                root: Data("wrong".utf8),
                newRoot: newData,
            ])
            Issue.record("different bytes for an existing root must fail")
        } catch {
            #expect(error as? BrokerError == .conflictingContent(root))
        }

        #expect(await broker.fetchVolumeLocal(root: root)?.entries == complete.entries)
        #expect(await broker.hasVolume(root: newRoot) == false)
    }

    @Test func rawEntryCanBePublishedAsACompleteVolume() async throws {
        let rootData = Data("root".utf8)
        let childData = Data("child".utf8)
        let root = cid(for: rootData)
        let child = cid(for: childData)
        let broker = MemoryBroker()
        let storer = BrokerStorer(broker: broker)

        try await storer.store(entries: [root: rootData])
        let complete = SerializedVolume(
            root: root,
            entries: [root: rootData, child: childData]
        )
        try await storer.store(volume: complete)
        #expect(await broker.fetchVolumeLocal(root: root)?.entries == complete.entries)
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
        #expect(broker.attempts.count == 2)
        #expect(broker.attempts[0].root == root)
        #expect(broker.attempts[0].entries == volume.entries)
        #expect(broker.attempts[1].root == root)
        #expect(broker.attempts[1].entries == volume.entries)
    }

    @Test func storedParentSurvivesLaterChildFailure() async throws {
        let broker = FlakyBroker()
        let storer = BrokerStorer(broker: broker)
        let parentData = Data("parent".utf8)
        let childData = Data("child".utf8)
        let parent = cid(for: parentData)
        let child = cid(for: childData)

        broker.failNextStore = false
        try await storer.store(volume: SerializedVolume(root: parent, entries: [parent: parentData]))
        broker.failNextStore = true
        do {
            try await storer.store(volume: SerializedVolume(root: child, entries: [child: childData]))
            Issue.record("expected child store to fail")
        } catch {
            #expect(error as? InjectedFailure == .store)
        }

        #expect(await broker.hasVolume(root: parent))
        #expect(await broker.hasVolume(root: child) == false)
    }
}

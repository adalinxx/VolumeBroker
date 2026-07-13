import Testing
import Foundation
import CID
import Multihash
@testable import VolumeBroker

private extension VolumeBroker {
    func link(near: (any VolumeBroker)? = nil) {
        self.near = near
    }
}

@Suite("Benchmarks")
struct Benchmarks {

    // MARK: - Helpers

    private func cid(for data: Data) -> String {
        let multihash = try! Multihash(raw: data, hashedWith: .sha2_256)
        return try! CID(version: .v1, codec: .dag_cbor, multihash: multihash).toBaseEncodedString
    }

    private func cid(_ value: String) -> String {
        cid(for: Data(value.utf8))
    }

    private func payload(_ root: String, entryCount: Int, dataSize: Int = 64) -> SerializedVolume {
        let rootData = Data(root.utf8)
        var entries = [cid(for: rootData): rootData]
        entries.reserveCapacity(entryCount)
        for i in 1..<entryCount {
            var data = Data("\(root):\(i):".utf8)
            if data.count < dataSize {
                data.append(Data(repeating: UInt8(i & 0xFF), count: dataSize - data.count))
            }
            entries[cid(for: data)] = data
        }
        return SerializedVolume(root: cid(for: rootData), entries: entries)
    }

    private func tempDB() throws -> DiskBroker {
        let path = NSTemporaryDirectory() + "vb_bench_\(UUID().uuidString).sqlite"
        return try DiskBroker(path: path)
    }

    private func measure(_ label: String, iterations: Int = 1, _ body: () async throws -> Void) async throws {
        let clock = ContinuousClock()
        let elapsed = try await clock.measure {
            for _ in 0..<iterations {
                try await body()
            }
        }
        let ms = Double(elapsed.components.attoseconds) / 1e15 + Double(elapsed.components.seconds) * 1000
        let perOp = ms / Double(iterations)
        print("  [\(label)] \(iterations) iterations in \(String(format: "%.2f", ms))ms (\(String(format: "%.3f", perOp))ms/op)")
    }

    // MARK: - DiskBroker Store

    @Test func diskStoreSmallVolumes() async throws {
        let broker = try tempDB()
        print("\n--- DiskBroker: Store small volumes (10 entries each) ---")
        try await measure("store 1000 volumes", iterations: 1000) {
            let p = payload("r-\(Int.random(in: 0..<1_000_000))", entryCount: 10)
            try await broker.storeVolumeLocal(p)
        }
    }

    @Test func diskStoreLargeVolume() async throws {
        let broker = try tempDB()
        print("\n--- DiskBroker: Store large volume (1000 entries, 256B each) ---")
        try await measure("store 1 large volume", iterations: 10) {
            let p = payload("large-\(Int.random(in: 0..<1_000_000))", entryCount: 1000, dataSize: 256)
            try await broker.storeVolumeLocal(p)
        }
    }

    @Test func diskBatchStore() async throws {
        let broker = try tempDB()
        print("\n--- DiskBroker: Batch store (50 volumes × 10 entries in 1 txn) ---")
        try await measure("batch store 50 volumes", iterations: 10) {
            let payloads = (0..<50).map { i in
                payload("batch-\(Int.random(in: 0..<1_000_000))-\(i)", entryCount: 10)
            }
            try await broker.storeVolumesLocal(payloads)
        }
    }

    @Test func diskBatchVsIndividualStore() async throws {
        let count = 50
        print("\n--- DiskBroker: Batch vs Individual store (\(count) volumes × 10 entries) ---")

        let brokerIndividual = try tempDB()
        let individualPayloads = (0..<count).map { i in
            payload("ind-\(i)", entryCount: 10)
        }
        try await measure("individual stores", iterations: 1) {
            for p in individualPayloads {
                try await brokerIndividual.storeVolumeLocal(p)
            }
        }

        let brokerBatch = try tempDB()
        let batchPayloads = (0..<count).map { i in
            payload("bat-\(i)", entryCount: 10)
        }
        try await measure("batch store", iterations: 1) {
            try await brokerBatch.storeVolumesLocal(batchPayloads)
        }
    }

    // MARK: - DiskBroker Fetch

    @Test func diskFetch() async throws {
        let broker = try tempDB()
        for i in 0..<100 {
            try await broker.storeVolumeLocal(payload("r-\(i)", entryCount: 20))
        }
        print("\n--- DiskBroker: Fetch volumes (20 entries each) ---")
        try await measure("fetch 1000 times", iterations: 1000) {
            let _ = await broker.fetchVolumeLocal(root: cid("r-\(Int.random(in: 0..<100))"))
        }
    }

    @Test func diskHasVolume() async throws {
        let broker = try tempDB()
        for i in 0..<100 {
            try await broker.storeVolumeLocal(payload("r-\(i)", entryCount: 5))
        }
        print("\n--- DiskBroker: hasVolume checks ---")
        try await measure("hasVolume 10000 checks", iterations: 10000) {
            let _ = await broker.hasVolume(root: cid("r-\(Int.random(in: 0..<200))"))
        }
    }

    // MARK: - DiskBroker Pin/Unpin

    @Test func diskPinUnpin() async throws {
        let broker = try tempDB()
        for i in 0..<100 {
            try await broker.storeVolumeLocal(payload("r-\(i)", entryCount: 5))
        }
        print("\n--- DiskBroker: Pin/unpin operations ---")
        try await measure("pin 1000 times", iterations: 1000) {
            try await broker.pin(root: cid("r-\(Int.random(in: 0..<100))"), owner: "owner-\(Int.random(in: 0..<10))")
        }
        try await measure("unpin 1000 times", iterations: 1000) {
            try await broker.unpin(root: cid("r-\(Int.random(in: 0..<100))"), owner: "owner-\(Int.random(in: 0..<10))")
        }
    }

    // MARK: - DiskBroker Eviction

    @Test func diskEviction() async throws {
        print("\n--- DiskBroker: Eviction (500 volumes, 50 pinned) ---")
        let broker = try tempDB()
        for i in 0..<500 {
            try await broker.storeVolumeLocal(payload("r-\(i)", entryCount: 10))
        }
        for i in 0..<50 {
            try await broker.pin(root: cid("r-\(i)"), owner: "keeper")
        }
        try await measure("evict 450 unpinned volumes", iterations: 1) {
            // graceSeconds: 0 — exercise eviction mechanics, not the store-then-pin grace
            let evicted = try await broker.evictUnpinned(graceSeconds: 0)
            #expect(evicted == 450)
        }
        #expect(await broker.hasVolume(root: cid("r-0")))
        #expect(await broker.hasVolume(root: cid("r-499")) == false)
    }

    // MARK: - MemoryBroker LRU

    @Test func memoryLRUThroughput() async throws {
        let broker = MemoryBroker(capacity: 500)
        print("\n--- MemoryBroker: LRU store+fetch throughput (cap=500) ---")
        try await measure("store 5000 volumes", iterations: 5000) {
            let p = payload("r-\(Int.random(in: 0..<10000))", entryCount: 5)
            try await broker.storeVolumeLocal(p)
        }
        try await measure("fetch 5000 times", iterations: 5000) {
            let _ = await broker.fetchVolumeLocal(root: cid("r-\(Int.random(in: 0..<10000))"))
        }
    }

    @Test func memoryLRUEviction() async throws {
        // grace .zero — exercise eviction mechanics, not the store-then-pin grace
        let broker = MemoryBroker(evictUnpinnedGrace: .zero)
        for i in 0..<1000 {
            try await broker.storeVolumeLocal(payload("r-\(i)", entryCount: 5))
        }
        for i in 0..<100 {
            try await broker.pin(root: cid("r-\(i)"), owner: "keeper")
        }
        print("\n--- MemoryBroker: Evict 900 of 1000 volumes ---")
        try await measure("evictUnpinned", iterations: 1) {
            let evicted = try await broker.evictUnpinned()
            #expect(evicted == 900)
        }
    }

    @Test func memoryBatchStore() async throws {
        let broker = MemoryBroker()
        print("\n--- MemoryBroker: Batch store (100 volumes × 10 entries) ---")
        try await measure("batch store", iterations: 10) {
            let payloads = (0..<100).map { i in
                payload("batch-\(Int.random(in: 0..<1_000_000))-\(i)", entryCount: 10)
            }
            try await broker.storeVolumesLocal(payloads)
        }
    }

    // MARK: - Concurrent Reads

    @Test func diskConcurrentReads() async throws {
        let broker = try tempDB()
        for i in 0..<200 {
            try await broker.storeVolumeLocal(payload("r-\(i)", entryCount: 20))
        }
        print("\n--- DiskBroker: Concurrent reads (4 tasks × 500 fetches) ---")

        let clock = ContinuousClock()
        let elapsed = await clock.measure {
            await withTaskGroup(of: Void.self) { group in
                for _ in 0..<4 {
                    group.addTask {
                        for _ in 0..<500 {
                            let _ = await broker.fetchVolumeLocal(root: cid("r-\(Int.random(in: 0..<200))"))
                        }
                    }
                }
            }
        }
        let ms = Double(elapsed.components.attoseconds) / 1e15 + Double(elapsed.components.seconds) * 1000
        let opsPerSec = 2000.0 / (ms / 1000.0)
        print("  [4-way concurrent fetch] 2000 total fetches in \(String(format: "%.2f", ms))ms (\(String(format: "%.0f", opsPerSec)) ops/sec)")
    }

    @Test func diskConcurrentReadsWhileWriting() async throws {
        let broker = try tempDB()
        for i in 0..<200 {
            try await broker.storeVolumeLocal(payload("r-\(i)", entryCount: 20))
        }
        print("\n--- DiskBroker: Concurrent reads + writes (3 readers + 1 writer) ---")

        let clock = ContinuousClock()
        let elapsed = await clock.measure {
            await withTaskGroup(of: Void.self) { group in
                for _ in 0..<3 {
                    group.addTask {
                        for _ in 0..<500 {
                            let _ = await broker.fetchVolumeLocal(root: cid("r-\(Int.random(in: 0..<200))"))
                        }
                    }
                }
                group.addTask {
                    for i in 200..<400 {
                        try? await broker.storeVolumeLocal(payload("w-\(i)", entryCount: 2))
                    }
                }
            }
        }
        let ms = Double(elapsed.components.attoseconds) / 1e15 + Double(elapsed.components.seconds) * 1000
        print("  [3 readers + 1 writer] completed in \(String(format: "%.2f", ms))ms")
    }

    // MARK: - Cascade Fetch

    @Test func cascadeFetchPerformance() async throws {
        let memory = MemoryBroker(capacity: 50)
        let disk = try tempDB()
        await memory.link(near: disk)

        for i in 0..<200 {
            try await disk.storeVolumeLocal(payload("r-\(i)", entryCount: 10))
        }
        for i in 0..<50 {
            try await memory.storeVolumeLocal(payload("r-\(i)", entryCount: 10))
        }

        print("\n--- Cascade: memory(50) → disk(200) fetch ---")
        try await measure("fetch 1000 (mix hit/miss)", iterations: 1000) {
            let _ = await memory.fetchVolume(root: cid("r-\(Int.random(in: 0..<200))"))
        }
    }
}

import Foundation
import cashew

/// Bridges a `VolumeBroker` tier chain to cashew resolution as a batched
/// `ContentSource` and per-CID `Fetcher`.
///
/// `CoalescingFetcher` batches each resolution wave, so this adapter
/// answers content lookups against the broker chain via `fetchData(cids:)`
/// when a complete published Volume owns the requested CID.
public struct BrokerFetcher: ContentSource, Fetcher {
    private let broker: any VolumeBroker

    public init(broker: any VolumeBroker) {
        self.broker = broker
    }

    /// Batched: resolve each CID against the broker chain in one call.
    public func fetch(_ cids: Set<String>) async -> [String: Data] {
        await broker.fetchData(cids: Set(cids.filter { !$0.isEmpty }))
    }

    /// Per-CID `Fetcher` adapter.
    public func fetch(rawCid: String) async throws -> Data {
        guard let data = await broker.fetchData(cid: rawCid) else { throw BrokerError.notFound }
        return data
    }
}

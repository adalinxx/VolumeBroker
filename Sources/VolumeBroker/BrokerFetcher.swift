import Foundation
import cashew

/// Bridges a `VolumeBroker` tier chain to cashew resolution as a batched
/// `ContentSource` (and a per-CID `Fetcher` for legacy callers).
///
/// `CoalescingFetcher` batches each resolution wave, so this adapter
/// answers content lookups against the broker chain via `fetchData(cid:)`
/// when a complete published Volume owns the requested CID.
public actor BrokerFetcher: ContentSource, Fetcher {
    private let broker: any VolumeBroker

    public init(broker: any VolumeBroker) {
        self.broker = broker
    }

    /// Batched: resolve each CID against the broker chain in one call.
    public func fetch(_ cids: Set<String>) async -> [String: Data] {
        var out: [String: Data] = [:]
        out.reserveCapacity(cids.count)
        for cid in cids where !cid.isEmpty {
            if let data = await broker.fetchData(cid: cid) {
                out[cid] = data
            }
        }
        return out
    }

    /// Per-CID (legacy `Fetcher`).
    public func fetch(rawCid: String) async throws -> Data {
        guard let data = await broker.fetchData(cid: rawCid) else { throw BrokerError.notFound }
        return data
    }
}

import Foundation
import cashew

/// Adapts both complete and sparse cashew writes to a `VolumeBroker`.
///
/// `store(volume:)` preserves an explicit complete Volume boundary.
/// `store(entries:)` preserves any existing complete Volume rooted at an entry's
/// CID when its root bytes match, then atomically publishes every remaining raw
/// entry as a singleton Volume. Raw-first singleton membership is permanent within
/// the broker domain, so a later multi-entry Volume with the same root conflicts.
/// Raw writes are not retained; callers must pin or retain their roots separately
/// when they must survive eviction.
public final class BrokerStorer: VolumeStorer, Storer {
    private let broker: any VolumeBroker

    public init(broker: any VolumeBroker) {
        self.broker = broker
    }

    public func store(volume: SerializedVolume) async throws {
        try await broker.storeVolumeLocal(volume)
    }

    public func store(entries: [String: Data]) async throws {
        guard !entries.isEmpty else { return }
        var singletons: [SerializedVolume] = []
        singletons.reserveCapacity(entries.count)
        for (cid, data) in entries {
            if let existing = await broker.fetchVolumeLocal(root: cid) {
                guard existing.entries[cid] == data else {
                    throw BrokerError.conflictingContent(cid)
                }
            } else {
                singletons.append(SerializedVolume(root: cid, entries: [cid: data]))
            }
        }
        guard !singletons.isEmpty else { return }
        try await broker.storeVolumesLocal(singletons)
    }
}

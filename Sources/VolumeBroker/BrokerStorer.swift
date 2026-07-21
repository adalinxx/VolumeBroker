import Foundation
import cashew

/// Adapts both complete and sparse cashew writes to a `VolumeBroker`.
///
/// `store(volume:)` preserves an explicit complete Volume boundary.
/// `store(entries:)` stores raw CAS bytes without declaring singleton Volumes.
/// A later explicit Volume can reuse those bytes while published Volume membership
/// remains immutable.
/// Raw writes are unretained; callers needing retention must store an explicit
/// Volume boundary and retain that root.
public final class BrokerStorer: VolumeStorer, Storer {
    private let broker: any VolumeBroker

    public init(broker: any VolumeBroker) {
        self.broker = broker
    }

    public func store(volume: SerializedVolume) async throws {
        try await broker.storeVolumeLocal(volume)
    }

    public func store(entries: [String: Data]) async throws {
        try await broker.storeEntriesLocal(entries)
    }
}

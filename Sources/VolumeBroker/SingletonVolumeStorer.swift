import Foundation
import cashew

/// Adapts cashew's sparse `Storer` writes to atomic VolumeBroker batches by
/// publishing every `(CID, bytes)` entry as its own singleton Volume.
///
/// Singleton membership is permanent within a broker domain: this adapter never
/// widens a root beyond `[cid: bytes]`. Because Volume membership is immutable,
/// later publishing a multi-entry Volume with the same root conflicts. Use this
/// adapter only where each CID is itself the complete Volume boundary.
///
/// Storage does not retain the singleton Volumes. Callers must pin or retain the
/// roots separately when they require them to survive eviction.
public final class SingletonVolumeStorer: Storer {
    private let broker: any VolumeBroker

    public init(broker: any VolumeBroker) {
        self.broker = broker
    }

    public func store(entries: [String: Data]) async throws {
        guard !entries.isEmpty else { return }
        try await broker.storeVolumesLocal(entries.map { cid, data in
            SerializedVolume(root: cid, entries: [cid: data])
        })
    }
}

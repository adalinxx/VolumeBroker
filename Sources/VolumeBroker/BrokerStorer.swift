import Foundation
import cashew

/// Adapts Cashew storage plans to a `VolumeBroker`.
public final class BrokerStorer: Storer, VolumeStorer {
    private let broker: any VolumeBroker

    public init(broker: any VolumeBroker) {
        self.broker = broker
    }

    public func store(volume: SerializedVolume) async throws {
        try await broker.storeVolumeLocal(volume)
    }

    public func store(entries: [String: Data]) async throws {
        try await broker.storeVolumesLocal(entries.map {
            SerializedVolume(root: $0.key, entries: [$0.key: $0.value])
        })
    }
}

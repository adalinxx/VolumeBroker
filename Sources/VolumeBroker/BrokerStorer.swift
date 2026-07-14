import cashew

/// Adapts Cashew's complete-Volume storage API to a `VolumeBroker`.
public final class BrokerStorer: VolumeStorer {
    private let broker: any VolumeBroker

    public init(broker: any VolumeBroker) {
        self.broker = broker
    }

    public func store(volume: SerializedVolume) async throws {
        try await broker.storeVolumeLocal(volume)
    }
}

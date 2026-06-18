import Foundation

/// Negative-cache policy for absent volume roots.
///
/// Combines a bloom filter of roots confirmed absent with a durable set of
/// known-present roots. `mightBeAbsent` lets `hasVolume` skip the SQLite
/// round-trip for the overwhelming majority of misses during initial sync.
final class NegativeCache: @unchecked Sendable {
    private let lock = NSLock()
    private var negativeBloom = BloomFilter(bits: 1 << 20, hashCount: 7)
    private var knownPresent = Set<String>()

    /// Bloom says this root was previously confirmed absent AND we haven't
    /// stored it without subsequently evicting it. False-positive rate ≈ 0.1%
    /// at 1M entries.
    func mightBeAbsent(_ root: String) -> Bool {
        lock.withLock { negativeBloom.mightContain(root) && !knownPresent.contains(root) }
    }

    /// Record that a lookup confirmed `root` is absent.
    func recordAbsent(_ root: String) {
        lock.withLock { negativeBloom.insert(root) }
    }

    /// Record that content for `root` is durably present until a matching
    /// eviction removes it.
    func recordStored(_ root: String) {
        lock.withLock { _ = knownPresent.insert(root) }
    }

    /// Record that resident content for `root` was evicted, allowing any prior
    /// absent bloom verdict to apply again.
    func recordEvicted(_ root: String) {
        lock.withLock { _ = knownPresent.remove(root) }
    }
}

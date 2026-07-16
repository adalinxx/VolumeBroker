# VolumeBroker

Volume-granular content-addressed storage for [cashew](https://github.com/adalinxx/cashew) Merkle DAGs. Replaces per-CID storage with atomic serialized Volumes, an owner-based pin ledger, and tiered fetch cascading.

## Why

In a Merkle DAG where every subtree boundary is a [Volume](https://github.com/adalinxx/cashew), the natural unit of storage, pinning, and eviction is the Volume — not the individual CID. VolumeBroker provides:

- **Atomic writes** — a Volume's CIDs are committed as one transaction
- **Ref-counted pinning** — multiple owners (chains, processes) can independently pin the same Volume root; multiple pins to the same (root, owner) pair are additive, and unpin decrements the count. Data is evictable only when all pin counts reach zero.
- **TTL pins** — owners can pin with an expiration; expired owners are pruned automatically during eviction sweeps
- **Domain-local CAS dedup** — CIDs shared by Volumes in one broker domain are stored once
- **Tiered fetch cascade** — memory → disk → network, configurable via `near`/`far` links

## Package layout

```
Sources/VolumeBroker/
  VolumeBroker.swift     Protocol — the core abstraction
  SerializedVolume.swift    {root, entries: [cid: data]}
  MemoryBroker.swift     In-memory LRU with capacity cap
  DiskBroker.swift       SQLite-backed durable storage
  BrokerFetcher.swift    cashew ContentSource (+ Fetcher) adapter
  BrokerStorer.swift     cashew VolumeStorer adapter
  BrokerErrors.swift     Shared error types
```

## Protocol

```swift
public protocol VolumeBroker: AnyObject, Sendable {
    var near: (any VolumeBroker)? { get set }
    var far: (any VolumeBroker)? { get set }

    func hasVolume(root: String) async -> Bool
    func fetchVolumeLocal(root: String) async -> SerializedVolume?
    func fetchDataLocal(cid: String) async -> Data?
    func storeVolumesLocal(_ volumes: [SerializedVolume]) async throws

    func pin(root: String, owner: String, count: Int, ttl: Duration?) async throws
    func unpin(root: String, owner: String, count: Int) async throws
    func unpinAll(owner: String) async throws
    func owners(root: String) async -> Set<String>
    func evictUnpinned() async throws -> Int
}
```

**Fetch cascade** (provided by default extension): `fetchVolume(root:)` and `fetchData(cid:)` each try local, then `near`, then `far`. Those tiers belong to the same storage domain; they are not parent/child chain links. The protocol also ships a volume-keyed fallback for `fetchDataLocal(cid:)`, `storeVolumeLocal(_:)` as a one-item batch, and convenience `pin`/`unpin` overloads. Every broker must implement atomic `storeVolumesLocal(_:)` itself.

**Stores are explicit** — no default cascade. The caller decides which tier to write to (`storeVolumeLocal` on the target broker).

**Content-addressed by CID** — `fetchData(cid:)` resolves a node only when at least one complete published Volume owns that CID. Loose orphan CAS rows are never visible. `fetchVolume(root:)` returns the complete Volume's entries for boundary-grain serving. Volume relationships remain encoded in the application's content-addressed structures; the broker stores and retains each Volume independently.

One broker instance is one storage domain. Its private CAS deduplicates only the
complete Volumes explicitly published to that broker. A node that requires
chain isolation uses a separate broker/path per chain; loose opportunistic
blocks belong in a bounded chain-local transient store, not VolumeBroker.
Pins likewise apply only to complete Volumes already published in that broker;
they cannot create ownership for an arbitrary CID. Chain metadata and canonical
tip records belong to the node, not the storage broker.

A bounded `MemoryBroker` rejects a store with `BrokerError.capacityExceeded`
when the submitted Volumes and already-protected Volumes cannot fit together;
it never reports success after retaining only part of the submitted batch.

## Usage

### Wiring the cascade

```swift
let memory = MemoryBroker(capacity: 10_000)
let disk = try DiskBroker(path: "/path/to/volumes.sqlite")

// Fetch: memory → disk (near/far are settable properties)
memory.near = disk
```

### Storing and pinning

```swift
let volume = SerializedVolume(root: "Qm...", entries: ["Qm...": serializedData])

// Durable write to disk
try await disk.storeVolumeLocal(volume)

// Pin an already-stored Volume; nil TTL = indefinite, count must be positive
try await disk.pin(root: "Qm...", owner: "chain-abc:tip", count: 1)

// Pin again — counts are additive (now count = 2)
try await disk.pin(root: "Qm...", owner: "chain-abc:tip", count: 1)

// Pin with TTL (auto-expires)
try await disk.pin(root: "Qm...", owner: "chain-abc:42", count: 1, ttl: .seconds(3600))
```

### Eviction

```swift
// Decrement pin count for a specific owner (row deleted when count reaches zero)
try await disk.unpin(root: "Qm...", owner: "chain-abc:42", count: 1)

// Remove all pins for an owner across every root
try await disk.unpinAll(owner: "chain-abc:42")

// Sweep: prune expired owners, then evict Volumes with zero remaining pins
let evicted = try await disk.evictUnpinned()
```

### cashew integration

```swift
// Storing a Merkle tree
let storer = BrokerStorer(broker: disk)
try await root.storeRecursively(storer: storer)

// Or store the root plus selected nested Volumes
try await root.store(paths: [["accounts/alice"]: .targeted], storer: storer)

// Resolving a Merkle tree
let fetcher = BrokerFetcher(broker: memory)  // ContentSource/Fetcher; uses fetch cascade
let resolved = try await root.resolveRecursive(fetcher: fetcher)
```

## DiskBroker schema

SQLite tables with WAL journaling:

| Table | Purpose |
|---|---|
| `cas_data(cid, data)` | Content-addressed blobs deduplicated within this broker domain |
| `volume_entries(root, cid)` | Membership index: which CIDs are stored inside each complete Volume |
| `volume_pins(root, owner, count, expires_at)` | Ref-counted pins for stored Volumes; positive integer count and optional TTL |
| `volume_unpin_operations(operation_id)` | Idempotency ledger for `unpinBatchOnce` |
| `volume_metadata(root, entry_count, stored_at)` | Complete-manifest and lifecycle tracking |
| `retained_roots(scope, root)` | Named durable retained-root sets (independent of owner/count pins) |
| `retained_root_operations(operation_id, scope, canonical_roots)` | Idempotency ledger for retained-root advance/merge |

The schema is versioned with `PRAGMA user_version=1` and enables foreign-key enforcement. Concurrent first opens serialize initialization. Reopened databases must match the canonical tables and indexes exactly; nonempty v0, malformed v1, and unsupported future versions fail closed without migration.

Eviction is a single transaction: prune expired pins, delete membership and
metadata for unprotected Volumes older than the grace window
(`evictUnpinnedGraceSeconds`, default 600), then delete CAS rows with no
remaining membership owner. A live pin or retained root protects that Volume
and its direct entries; related Volume roots must be protected explicitly.
Eviction never decrements pin counts; that happens only in `unpin`/`unpinAll`.
Reads revalidate content addresses and transactionally quarantine a discovered
corrupt Volume. Retention intent remains so repairing the same Volume restores
protection; ordinary eviction does not rehash every retained payload.

## Requirements

- Swift 6.0+
- macOS 13+ / iOS 16+
- [cashew](https://github.com/adalinxx/cashew) 4.0.1+
- [ArrayTrie](https://github.com/adalinxx/ArrayTrie) 1.0.0+

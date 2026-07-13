# VolumeBroker

Volume-granular content-addressed storage for [cashew](https://github.com/adalinxx/cashew) Merkle DAGs. Replaces per-CID storage with atomic serialized Volumes, an owner-based pin ledger, and tiered fetch cascading.

## Why

In a Merkle DAG where every subtree boundary is a [Volume](https://github.com/adalinxx/cashew), the natural unit of storage, pinning, and eviction is the Volume — not the individual CID. VolumeBroker provides:

- **Atomic writes** — a Volume's CIDs are committed as one transaction
- **Ref-counted pinning** — multiple owners (chains, processes) can independently pin the same Volume root; multiple pins to the same (root, owner) pair are additive, and unpin decrements the count. Data is evictable only when all pin counts reach zero.
- **TTL pins** — owners can pin with an expiration; expired owners are pruned automatically during eviction sweeps
- **CAS dedup** — CIDs shared across Volumes are stored once (DiskBroker's `cas_data` table)
- **Tiered fetch cascade** — memory → disk → network, configurable via `near`/`far` links

## Package layout

```
Sources/VolumeBroker/
  VolumeBroker.swift     Protocol — the core abstraction
  SerializedVolume.swift    {root, entries: [cid: data]}
  MemoryBroker.swift     In-memory LRU with capacity cap
  DiskBroker.swift       SQLite-backed durable storage
  BrokerFetcher.swift    cashew ContentSource (+ Fetcher) adapter
  BrokerStorer.swift     cashew VolumeAwareStorer adapter
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
    func storeVolumeLocal(_ volume: SerializedVolume) async throws
    func storeVolumesLocal(_ volumes: [SerializedVolume]) async throws

    func pin(root: String, owner: String, count: Int, ttl: Duration?) async throws
    func unpin(root: String, owner: String, count: Int) async throws
    func unpinAll(owner: String) async throws
    func owners(root: String) async -> Set<String>
    func evictUnpinned() async throws -> Int
}
```

**Fetch cascade** (provided by default extension): `fetchVolume(root:)` and `fetchData(cid:)` each try local, then `near`, then `far`. The protocol also ships default implementations of `fetchDataLocal(cid:)` (volume-keyed fallback; CAS-backed brokers override it) and `storeVolumesLocal(_:)` (loops `storeVolumeLocal`), plus convenience `pin`/`unpin` overloads.

**Stores are explicit** — no default cascade. The caller decides which tier to write to (`storeVolumeLocal` on the target broker).

**Content-addressed by CID** — `fetchData(cid:)` resolves any stored node by its CID from `cas_data`, regardless of which Volume it belongs to (cashew 3.x resolves per-CID over a `Fetcher`/`ContentSource`, not by entering a Volume root). `fetchVolume(root:)` still returns a whole Volume's entries for boundary-grain serving. Volume relationships remain encoded in the application's content-addressed structures; the broker stores and retains each Volume independently.

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

// Pin with owner; nil TTL = indefinite, count = 1
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
try root.storeRecursively(storer: storer)
try await storer.flush(root: root.rawCID)  // commits all buffered SerializedVolumes

// Resolving a Merkle tree
let fetcher = BrokerFetcher(broker: memory)  // ContentSource/Fetcher; uses fetch cascade
let resolved = try await root.resolveRecursive(fetcher: fetcher)
```

## DiskBroker schema

SQLite tables with WAL journaling:

| Table | Purpose |
|---|---|
| `cas_data(cid, data)` | Content-addressed blob store; shared across Volumes |
| `volume_entries(root, cid)` | Owned-child reachability graph: which CIDs belong to / are bracketed under which Volume |
| `volume_pins(root, owner, count, expires_at)` | Ref-counted pin ledger with optional TTL; `count INTEGER NOT NULL DEFAULT 1` |
| `volume_unpin_operations(operation_id)` | Idempotency ledger for `unpinBatchOnce` |
| `volume_metadata(root, stored_at)` | Volume lifecycle tracking (drives the eviction grace window) |
| `retained_roots(scope, root)` | Named durable retained-root sets (independent of owner/count pins) |
| `retained_root_operations(operation_id, scope, canonical_roots)` | Idempotency ledger for retained-root advance/merge |
| `chain_meta(key, value)` | Chain metadata key/value store |

A schema migration (`ALTER TABLE volume_pins ADD COLUMN count INTEGER NOT NULL DEFAULT 1`) runs automatically on startup for existing databases.

Eviction is a single transaction: prune pins whose TTL has expired, then delete the CAS data, entries, and metadata for any unprotected Volume older than the grace window (`evictUnpinnedGraceSeconds`, default 600). A live pin or retained root protects that Volume and its direct entries; related Volume roots must be protected explicitly. Shared CAS blobs that are direct entries of a protected Volume are never evicted. Eviction never decrements pin counts — that happens only in `unpin`/`unpinAll`.

## Requirements

- Swift 6.0+
- macOS 13+ / iOS 16+
- [cashew](https://github.com/adalinxx/cashew) 3.0.0+
- [ArrayTrie](https://github.com/adalinxx/ArrayTrie) 1.0.0+

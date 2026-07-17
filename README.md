# VolumeBroker

Atomic, Volume-granular content-addressed storage for
[cashew](https://github.com/adalinxx/cashew).

A `SerializedVolume` names one root CID and the exact `(CID, bytes)` entries
published with that root. VolumeBroker validates and publishes that set as one
unit. It does not follow links, infer child Volumes, or turn one retained root
into a transitive DAG pin.

That separation is the point:

- Cashew decides which DAG boundaries to materialize.
- VolumeBroker stores each selected Volume independently.
- The caller decides where to store it and how long to retain it.

## Mental model

```text
Cashew storage plan
        |
        v
complete SerializedVolume(s)
        |
        v
MemoryBroker or DiskBroker  <---- explicit store target
        |
        +---- local -> near -> far fetch cascade
        |
        +---- pins / retained-root sets -> eviction protection
```

One broker cascade is one storage domain. Its local, `near`, and `far` tiers may
share content, but they do not represent parent and child chains. A host that
needs isolation creates a separate broker or database path for each domain.

## Contract

| Rule | Meaning |
| --- | --- |
| Complete publication | A Volume becomes visible only when its root, membership, and every entry are present and valid. |
| Atomic batches | `storeVolumesLocal(_:)` publishes the whole validated batch or none of it. |
| Immutable content | A CID cannot acquire different bytes, and a Volume root cannot acquire different membership. |
| Explicit storage | Writes go only to the broker the caller selected. There is no write cascade. |
| Tiered reads | `fetchVolume(root:)` and `fetchData(cid:)` try local, then `near`, then `far`. |
| Explicit retention | Pins and retained-root sets protect named Volume roots only. Related roots must be named separately. |
| Local CAS deduplication | Equal CIDs are stored once within each broker tier and released after their last owning Volume there. |

Loose CAS rows are never storage truth. A CID is readable only through at least
one complete, valid, published Volume that contains it.

## API surfaces

Use the highest-level surface that fits the caller:

| Surface | Use it for |
| --- | --- |
| `ContentStore` | Storing and resolving Cashew `Node` values by root CID. |
| `BrokerStorer` / `BrokerFetcher` | Connecting Cashew's storage and resolution plans to a broker. |
| `VolumeBroker` | Direct Volume publication, fetch cascading, pins, and eviction. |
| `RetainedRootBroker` | Atomically replacing a named retained-root set. |
| `RetainedRootMergeBroker` | Idempotently adding roots to a named retained-root set. |

The core protocol is intentionally small:

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

`storeVolumeLocal(_:)`, the read-cascade methods, and common pin/unpin overloads
are protocol extensions. Every implementation supplies its own atomic batch
store.

## Usage

### Build a read cascade

```swift
import VolumeBroker

let memory = MemoryBroker(byteBudget: 64 * 1024 * 1024)
let disk = try DiskBroker(path: "/var/lib/my-app/volumes.sqlite")

memory.near = disk
```

`memory.fetchData(cid:)` now checks memory and then disk. Stores remain explicit:
write to `disk` for durability or to `memory` for transient residency.

### Publish from Cashew

Let Cashew produce valid Volume boundaries instead of constructing placeholder
CIDs by hand:

```swift
let storer = BrokerStorer(broker: disk)

// The root Volume is always selected. This also selects one nested boundary.
try await root.store(
    paths: [["accounts", "alice"]: .targeted],
    storer: storer
)
```

Use `.recursive` when the storage plan should select every nested Volume below a
path. Each emitted Volume is still a separate publication and retention unit.

For object-level calls:

```swift
let objects = ContentStore(broker: disk)
let rootCID = try await objects.put(state)
let loaded = try await objects.getRecursive(State.self, rootCID)
```

### Resolve through the cascade

```swift
let source = BrokerFetcher(broker: memory)
let resolved = try await unresolvedRoot.resolveRecursive(source: source)
```

`BrokerFetcher` serves a CID only when a complete Volume in the cascade owns it.

### Retain and release

Pins are additive per `(root, owner)`. A pin requires an already-published local
Volume.

```swift
try await disk.pin(root: rootCID, owner: "sync:session-42")
try await disk.pin(
    root: rootCID,
    owner: "cache:recent",
    ttl: .seconds(3_600)
)

try await disk.unpin(root: rootCID, owner: "sync:session-42")
try await disk.unpinAll(owner: "cache:recent")
```

On `DiskBroker`, named retained-root sets durably advance policy as one
idempotent operation:

```swift
try await disk.advanceRetainedRoots(
    scope: "state:canonical",
    roots: materializedVolumeRoots,
    operationID: transitionID
)
```

Replaying the same operation ID and payload is a no-op. Reusing an operation ID
for different roots fails. `mergeRetainedRoots` adds roots without replacing the
scope. Pins and retained-root sets are independent mechanisms; both protect only
the roots explicitly named.

### Evict

```swift
let evictedVolumeCount = try await disk.evictUnpinned()
```

An eviction sweep prunes expired pins, removes unprotected Volumes older than the
grace window, then removes CAS rows with no remaining Volume owner. Shared bytes
survive until the last owning Volume is removed.

`MemoryBroker` can be bounded by Volume count or unique resident bytes. A store
that cannot fit alongside already-protected Volumes fails with
`BrokerError.capacityExceeded`; it never reports success after keeping only part
of the submitted batch.

## Disk durability

`DiskBroker` uses SQLite with WAL journaling and foreign-key enforcement.

| Table | Durable fact |
| --- | --- |
| `cas_data` | CID-addressed bytes deduplicated in this broker domain. |
| `volume_metadata` | Published root, declared entry count, and publication time. |
| `volume_entries` | Exact membership of each published Volume. |
| `volume_pins` | Owner/count pins and optional expiration. |
| `volume_unpin_operations` | Idempotency records for counted batch release. |
| `retained_roots` | Named retained-root sets. |
| `retained_root_operations` | Payload-bound idempotency records for retained-root updates. |

Schema v1 is validated on every open. Empty v0 databases initialize atomically;
nonempty v0, malformed v1, and unsupported future schemas fail closed without
automatic migration. Reads revalidate complete Volumes and transactionally
quarantine discovered corruption without deleting named retained-root intent.

## Boundary

| VolumeBroker owns | The caller owns |
| --- | --- |
| Volume validation and atomic publication | DAG traversal and storage-plan selection |
| Local CAS deduplication | Storage-domain and chain isolation |
| Read cascading | Write placement and network retrieval policy |
| Pins, retained-root sets, and eviction | Which materialized roots current policy retains |
| Storage integrity | Canonicity, consensus, and application metadata |

Chain tips, child-chain records, block retention counters, and opportunistic
unvalidated bytes do not belong in VolumeBroker.

## Correctness and verification

- [Correctness invariants](docs/correctness-invariants.md)
- Swift 6.0+
- macOS 13+ or iOS 16+
- Cashew 4.0.1+

```sh
swift build
swift test
```

# VolumeBroker

Atomic, Volume-granular content-addressed storage for
[Cashew](https://github.com/adalinxx/cashew).

A `SerializedVolume` is one root CID plus the exact `(CID, bytes)` entries
published with it. VolumeBroker stores that set as one unit. It never walks DAG
links or infers relationships between Volumes.

## Contract

- A Volume is visible only after its complete, CID-valid entry set commits.
- CID bytes and Volume membership are immutable.
- Reads may fall through `local -> near -> far`; writes target one broker.
- Pins and retained-root sets protect only the Volume roots named by the caller.
- Equal CIDs are deduplicated within each broker tier.

One cascade is one storage domain. Use separate brokers or database paths when
data must be isolated.

## Usage

```swift
import VolumeBroker

let disk = try DiskBroker(path: "/var/lib/my-app/volumes.sqlite")
let memory = MemoryBroker(byteBudget: 64 * 1024 * 1024, near: disk)
```

Reads now try memory and then disk. Stores remain explicit.

Let Cashew produce valid Volume boundaries:

```swift
let storer = BrokerStorer(broker: disk)

// Store the root and one selected nested Volume.
try await root.store(
    paths: [["accounts", "alice"]: .targeted],
    storer: storer
)
```

`.recursive` selects every nested Volume below a path. Each emitted Volume is
still an independent storage and retention unit. Cashew submits one Volume per
storer callback; callers that already hold an all-or-none batch can use
`storeVolumesLocal` to commit it in one transaction.

For structures where every CID is itself a complete Volume boundary, adapt
cashew's sparse `Storer` API explicitly:

```swift
let storer = SingletonVolumeStorer(broker: disk)
try await header.storeRecursively(storer: storer)
```

Each entry is published in one atomic broker batch as
`SerializedVolume(root: cid, entries: [cid: bytes])`. This adapter does not pin
or retain roots. Its singleton membership is permanent within the storage
domain, so a later multi-entry Volume with the same root is a membership
conflict; do not mix those models in one broker domain.

Resolve through the read cascade:

```swift
let source = BrokerFetcher(broker: memory)
let resolved = try await unresolvedRoot.resolveRecursive(source: source)
```

`ContentStore` provides the same integration at the object-by-root-CID level.

## Retention

Pins are additive per `(root, owner)` and require a published local Volume:

```swift
try await disk.pin(root: rootCID, owner: "sync:42")
try await disk.pin(root: rootCID, owner: "cache", ttl: .seconds(3_600))
try await disk.unpin(root: rootCID, owner: "sync:42")
```

`DiskBroker` can also replace a named retained-root set atomically:

```swift
try await disk.advanceRetainedRoots(
    scope: "state:canonical",
    roots: materializedVolumeRoots
)
```

Replacing a scope with the same root set is naturally idempotent, as is merging
roots already in the set. `mergeRetainedRoots` adds roots without replacing the
set. Pin-count mutations are not replay-deduplicated. The node or application
must durably own transition identity, ordering, and replay policy.

```swift
let evicted = try await disk.evictUnpinned()
```

Eviction prunes expired pins, removes old unprotected Volumes, then removes CAS
bytes with no remaining Volume owner. Shared bytes survive until their last
owner is removed.

## Implementations

- `MemoryBroker` supports count or byte limits and LRU eviction.
- `DiskBroker` uses SQLite, WAL, foreign keys, deliberate `synchronous=FULL`
  durability, and schema v1 validation.
- `BrokerStorer`, `SingletonVolumeStorer`, and `BrokerFetcher` connect Cashew
  storage and resolution without combining complete and sparse write ports.
- `ContentStore` stores and resolves Cashew `Node` values by root CID.

`DiskBroker` initializes only an empty v0 database. A nonempty v0 store requires
a new database path or explicit export/rematerialization. Malformed v1 and
future schemas fail closed.

## Boundary

| VolumeBroker owns | The caller owns |
| --- | --- |
| Volume validation and atomic publication | DAG traversal and Volume selection |
| Local CAS, read cascading, and eviction | Storage placement and domain isolation |
| Pins and retained-root sets | Which materialized roots remain live |
| Storage integrity | Application state, canonicity, and consensus |

See [correctness invariants](docs/correctness-invariants.md) for the review
contract.

## Requirements

- Swift 6.0+
- macOS 13+ or iOS 16+
- Cashew 4.0.1+

```sh
swift build
swift test
```

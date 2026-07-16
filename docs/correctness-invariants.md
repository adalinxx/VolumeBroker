# Correctness invariants

## VOLUME-001 — a stored Volume includes its root

A declared root missing from the entry set is rejected before storage.

## VOLUME-002 — every entry is content-address correct

The broker recomputes each CID using its declared version, codec, and multihash algorithm. CIDv0 is accepted only in its canonical dag-pb, 32-byte SHA-256 form. A mismatch is rejected before storage and durable reads revalidate the returned Volume.

## VOLUME-003 — one malformed Volume aborts the batch

All Volumes are validated and copied before a MemoryBroker mutation or SQLite transaction begins. A successful store owns an immutable snapshot of the submitted bytes.

## VOLUME-004 — immutable CID bytes cannot conflict

An existing CID row must be byte-identical. Conflicting bytes are surfaced as corruption rather than hidden by `INSERT OR IGNORE`.

## VOLUME-005 — published Volume membership is immutable

Re-storing a Volume root must provide the same complete entry set. MemoryBroker and DiskBroker reject conflicting memberships before mutating storage.

## VOLUME-006 — incomplete manifests are unavailable

Presence, whole-Volume fetch, and per-CID fetch share one completeness predicate: declared entry count, membership count, owned CAS count, and root membership must all agree.

## VOLUME-007 — incomplete traversal is not publishable

Cashew fully serializes a selected Volume boundary before invoking `VolumeStorer`. A missing or unserializable ordinary Header prevents that Volume from reaching the broker.

## VOLUME-008 — loose CAS rows are not storage truth

A CID is readable only through at least one complete, CID-valid owning Volume. Dangling memberships, pins, and corrupt Volumes own nothing. A read that discovers corruption transactionally quarantines that Volume without mistaking a SQLite error for corruption; ordinary eviction removes structurally unowned CAS bytes without rehashing the entire database.

## VOLUME-009 — schema startup fails closed

Fresh empty v0 databases initialize at schema v1 under one initialization transaction. Reopened v1 databases must match the canonical tables and indexes, including types, defaults, checks, keys, and foreign keys, and must not attach extra schema behavior to owned tables. Nonempty v0, malformed v1, and unsupported future versions fail closed.

## VOLUME-010 — a broker is one storage domain

The local, near, and far tiers of a broker hold complete Volumes for one storage domain. Parent/child chain synchronization uses separate brokers under node policy.

## VOLUME-011 — successful memory publication fits as a whole

A bounded MemoryBroker rejects a batch before mutation when the submitted Volumes and already-protected Volumes cannot coexist within its count or byte limit. Publication and limit enforcement share one critical section.

## VOLUME-012 — pins retain only published local Volumes

A pin is accepted only for a complete Volume already published in the same broker domain. Pin and unpin counts are positive, additive integers; overflow and nonpositive operations fail without mutation. A retained-root intent may outlive quarantined bytes so repairing that same Volume restores the current policy without replaying stale state transitions.

## VOLUME-013 — storage has no chain-control side channel

VolumeBroker stores Volumes and their retention policy only. Canonical tips, child-chain records, and other chain metadata belong to the node and cannot bypass the Volume contract through a broker key/value table.

Established by: `VolumeIntegrityTests`, `SchemaVersionTests`, `PinIndexTests`, `EvictionEngineTests`, and the companion cashew storage-plan tests.

# Correctness invariants

These are the laws a `VolumeBroker` implementation must preserve. "Published"
means the Volume is available to `hasVolume`, whole-Volume fetch, and per-CID
fetch. Retention never substitutes for publication.

## VOLUME-001: the root is an entry

A stored Volume contains bytes for its declared root CID. Missing roots are
rejected before mutation.

## VOLUME-002: every entry authenticates itself

The broker recomputes each CID using its declared version, codec, multihash
algorithm, and digest length. CIDv0 is accepted only in canonical dag-pb,
32-byte SHA-256 form. Writes and durable reads fail closed on mismatch.

## VOLUME-003: publication owns an atomic snapshot

Every Volume is validated and copied before a memory mutation or SQLite
transaction begins. One malformed Volume aborts the whole batch, and later
caller mutation cannot change published bytes.

## VOLUME-004: CID bytes are immutable

If a CID already exists in the broker domain, a new publication must provide
byte-identical content. A conflict is corruption, not an overwrite.

## VOLUME-005: Volume membership is immutable

If a root is already published, republishing it must name the same exact entry
set. Both memory and disk brokers reject conflicting membership before mutation.

## VOLUME-006: incomplete Volumes are unavailable

Presence and fetch agree on one completeness law: the declared entry count,
membership count, owned CAS count, and root membership all match. No partial
Volume is published.

## VOLUME-007: a selected boundary is complete

Cashew serializes an entire selected Volume before calling `VolumeStorer`. A
missing or unserializable ordinary Header prevents that boundary from reaching
the broker.

## VOLUME-008: loose CAS rows own nothing

A CID is readable only through a complete, CID-valid owning Volume. Dangling
memberships, pins, malformed metadata, and corrupt Volumes grant no visibility
or eviction ownership. A read that proves corruption quarantines that Volume in
one transaction; a database error is not treated as proof of corruption.

## VOLUME-009: retention names published local roots

Pins and retained-root updates accept only complete Volumes already published
in the same broker domain. A retained root protects that Volume and its direct
entries; it does not discover or protect linked Volume roots.

Pin counts are positive additive integers. Overflow and nonpositive operations
fail without mutation. Expired pins are not live.

## VOLUME-010: retention operations are replay-safe

An idempotent operation ID is bound to its operation kind, scope, and canonical
payload. Replaying the same operation is a no-op; reusing the ID for a different
operation fails.

A retained-root intent may outlive quarantined bytes. Republishing the same
valid Volume restores protection without replaying stale policy transitions.

## VOLUME-011: successful bounded-memory publication fits as a whole

A bounded `MemoryBroker` rejects a batch before mutation when the submitted
Volumes and already-protected Volumes cannot coexist within its count or byte
limit. Publication and capacity enforcement share one critical section.

## VOLUME-012: eviction follows explicit ownership

A live pin or retained-root entry protects only its named Volume. Shared CAS
bytes remain while any published Volume owns them and disappear only after the
last owner is removed. Eviction never decrements a pin count.

## VOLUME-013: a broker cascade is one storage domain

Local, `near`, and `far` are read tiers in one domain. Reads may fall through
the cascade; writes never do. Cross-domain or cross-chain synchronization uses
separate brokers under caller policy.

## VOLUME-014: schema startup fails closed

Fresh empty v0 databases initialize schema v1 in one transaction. Reopened v1
databases must match canonical tables and indexes, including keys, checks,
defaults, and foreign keys, and cannot attach behavior to broker-owned tables.
Nonempty v0, malformed v1, and unsupported future versions are not mutated.

## VOLUME-015: storage has no chain-control side channel

VolumeBroker stores complete Volumes and explicit retention policy. Canonical
tips, chain relationships, consensus state, and application metadata cannot
bypass the Volume contract through broker-owned storage.

## Verification map

| Area | Primary coverage |
| --- | --- |
| Content and membership integrity | `VolumeIntegrityTests`, `BrokerStorerTests` |
| Atomic memory publication and limits | `MemoryBrokerTests` |
| Pins and replay-safe release | `PinIndexTests`, `DiskBrokerTests` |
| Retained-root transitions | `MemoryBrokerTests`, `DiskBrokerTests` |
| Eviction and corruption quarantine | `EvictionEngineTests` |
| Schema identity and startup | `SchemaVersionTests` |
| Cashew boundary selection | Cashew `StoragePlanTests`, VolumeBroker `ContentStoreTests` |

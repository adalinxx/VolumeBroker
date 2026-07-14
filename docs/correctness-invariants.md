# Correctness invariants

## VOLUME-001 — a stored Volume includes its root

A declared root missing from the entry set is rejected before storage.

## VOLUME-002 — every entry is content-address correct

The broker recomputes each CID using its declared version, codec, and multihash algorithm. A mismatch is rejected before storage.

## VOLUME-003 — one malformed Volume aborts the batch

All Volumes are validated before a MemoryBroker mutation or SQLite transaction begins.

## VOLUME-004 — immutable CID bytes cannot conflict

An existing CID row must be byte-identical. Conflicting bytes are surfaced as corruption rather than hidden by `INSERT OR IGNORE`.

## VOLUME-005 — published Volume membership is immutable

Re-storing a Volume root must provide the same complete entry set. MemoryBroker and DiskBroker reject conflicting memberships before mutating storage.

## VOLUME-006 — legacy partial rows are unavailable

Presence and fetch checks reject a stored root when its root entry or any recorded CAS entry is missing.

## VOLUME-007 — incomplete traversal is not publishable

Cashew fully serializes a selected Volume boundary before invoking `VolumeStorer`. A missing or unserializable ordinary Header prevents that Volume from reaching the broker.

Established by: `VolumeIntegrityTests` and the companion cashew storage-plan tests.

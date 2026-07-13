# Correctness invariants

## VOLUME-001 — a stored Volume includes its root

A declared root missing from the entry set is rejected before storage.

## VOLUME-002 — every entry is content-address correct

The broker recomputes each CID using its declared version, codec, and multihash algorithm. A mismatch is rejected before storage.

## VOLUME-003 — one malformed Volume aborts the batch

All Volumes are validated before a MemoryBroker mutation or SQLite transaction begins.

## VOLUME-004 — immutable CID bytes cannot conflict

An existing CID row must be byte-identical. Conflicting bytes are surfaced as corruption rather than hidden by `INSERT OR IGNORE`.

## VOLUME-005 — incomplete traversal is not publishable

An open or aborted Volume scope cannot be collected or flushed as complete, exits must match stack order, and an exit without the declared root entry fails closed.

## VOLUME-006 — legacy partial rows are unavailable

Presence and fetch checks reject a stored root when its root entry or any recorded CAS entry is missing.

## VOLUME-007 — failed flushes are retryable

Completed in-memory Volumes are cleared only after the broker accepts the whole flush.

Established by: `VolumeIntegrityTests` and the companion cashew lifecycle tests.

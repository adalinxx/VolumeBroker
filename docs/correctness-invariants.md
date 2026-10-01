# Correctness invariants

"Published" means visible to presence, whole-Volume fetch, and per-CID fetch.
Retention never substitutes for publication.

| ID | Law |
| --- | --- |
| VOLUME-001 | The declared root is present in the Volume. |
| VOLUME-002 | Every `(CID, bytes)` pair uses the repository's canonical CID spelling and validates using its declared digest length. |
| VOLUME-003 | A batch owns a copied snapshot and publishes all valid Volumes or none. |
| VOLUME-004 | Valid existing CID content cannot acquire different bytes; invalid local bytes may be repaired only by submitted bytes that authenticate to that CID. |
| VOLUME-005 | An existing Volume root cannot acquire different membership. |
| VOLUME-006 | Declared count, membership, owned CAS rows, root membership, and quarantine state must agree before publication. |
| VOLUME-007 | Cashew completes one independently durable selected Volume per `VolumeStorer` callback; a caller with a preassembled all-or-none batch may use `storeVolumesLocal`. |
| VOLUME-008 | Loose rows, dangling membership, malformed metadata, and quarantined Volumes grant no visibility or ownership; requested bytes are returned only after CID validation. |
| VOLUME-009 | Retained-root sets accept only complete local roots, all-or-none. A root is live if retained, or if it is a member of a live, complete Volume and is itself a stored Volume root. One definition serves both reachability and sweep. |
| VOLUME-010 | Retained-set replacement and merge are naturally idempotent. |
| VOLUME-011 | `sweep` removes every non-live Volume and then every unowned CAS row in one transaction, serialized with stores and advances; it has no grace window. |
| VOLUME-012 | Shared CAS bytes remain until their last owning Volume is removed. |
| VOLUME-013 | Immutable `near` and `far` links are read tiers in one domain; writes and cross-domain synchronization are explicit. |
| VOLUME-014 | Empty v0 initializes atomically; malformed, nonempty, and unsupported schemas are not mutated. |
| VOLUME-015 | Chain state, canonicity, and application metadata cannot bypass the Volume contract. |
| VOLUME-016 | Presence and retained reachability use the structural serve gate; a point read validates only its requested `(CID, bytes)`, while a whole-Volume read validates the whole Volume. |
| VOLUME-017 | A proved read mismatch durably quarantines every manifest owning the mismatched CID without deleting bytes or intent. A quarantined retained root stays live but does not extend liveness to its members. |
| VOLUME-018 | SQLite WAL with `synchronous=FULL` is the durable-commit boundary: a returned store survives a crash, so a later advance never names lost content. |
| VOLUME-019 | A batched CID read applies the same complete-owner serve gate, CID validation, quarantine, and local-to-far precedence as scalar reads. |

Corruption is quarantined only after it is proved; a database error is not proof
of corruption. Quarantine preserves retained intent and the quarantined bytes;
valid republication clears it.

Primary coverage: `VolumeIntegrityTests`, `MemoryBrokerTests`,
`DiskBrokerTests`, `RetentionTests`, `SweepTests`,
`SchemaVersionTests`, and `CashewProtocolTests`.

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
| VOLUME-009 | Pins and retained-root sets accept published local roots and protect only those roots. |
| VOLUME-010 | Retained-set replacement and merge are naturally idempotent. Pin-count mutations are not replay-deduplicated; the caller owns durable transition replay. |
| VOLUME-011 | A bounded memory store rejects a batch before mutation if protected and submitted Volumes cannot fit. |
| VOLUME-012 | Shared CAS bytes remain until their last owning Volume is removed; eviction never decrements pins. |
| VOLUME-013 | `near` and `far` are read tiers in one domain; writes and cross-domain synchronization are explicit. |
| VOLUME-014 | Empty v0 initializes atomically; malformed, nonempty, and unsupported schemas are not mutated. |
| VOLUME-015 | Chain state, canonicity, and application metadata cannot bypass the Volume contract. |
| VOLUME-016 | Presence and pin reachability use the structural serve gate; a point read validates only its requested `(CID, bytes)`, while a whole-Volume read validates the whole Volume. |
| VOLUME-017 | A proved read mismatch durably quarantines every manifest owning the mismatched CID without deleting bytes or intent; explicit eviction performs and counts reclamation. |
| VOLUME-018 | SQLite WAL with `synchronous=FULL` is a deliberate durable-commit boundary. |

Corruption is quarantined only after it is proved; a database error is not proof
of corruption. Quarantine preserves pins and retained intent. Explicit eviction
may remove the manifest and its pins; retained-root sets remain authoritative,
so valid republication reactivates retained policy.

Primary coverage: `VolumeIntegrityTests`, `MemoryBrokerTests`,
`DiskBrokerTests`, `PinIndexTests`, `EvictionEngineTests`,
`SchemaVersionTests`, `BrokerStorerTests`, and `ContentStoreTests`.

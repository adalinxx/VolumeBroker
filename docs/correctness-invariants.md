# Correctness invariants

"Published" means visible to presence, whole-Volume fetch, and per-CID fetch.
Retention never substitutes for publication.

| ID | Law |
| --- | --- |
| VOLUME-001 | The declared root is present in the Volume. |
| VOLUME-002 | Every `(CID, bytes)` pair uses the repository's canonical CID spelling and validates using its declared digest length. |
| VOLUME-003 | A batch owns a copied snapshot and publishes all valid Volumes or none. |
| VOLUME-004 | An existing CID cannot acquire different bytes. |
| VOLUME-005 | An existing Volume root cannot acquire different membership. |
| VOLUME-006 | Declared count, membership, owned CAS rows, and root membership must agree before publication. |
| VOLUME-007 | Cashew completes a selected Volume boundary before calling `VolumeStorer`. |
| VOLUME-008 | Loose rows, dangling membership, malformed metadata, and corrupt Volumes grant no visibility or ownership. |
| VOLUME-009 | Pins and retained-root sets accept published local roots and protect only those roots. |
| VOLUME-010 | Replacing a retained-root scope with the same set and merging roots already present are naturally idempotent; the node owns operation ordering and replay policy. |
| VOLUME-011 | A bounded memory store rejects a batch before mutation if protected and submitted Volumes cannot fit. |
| VOLUME-012 | Shared CAS bytes remain until their last owning Volume is removed; eviction never decrements pins. |
| VOLUME-013 | `near` and `far` are read tiers in one domain; writes and cross-domain synchronization are explicit. |
| VOLUME-014 | Empty v0 initializes atomically; malformed, nonempty, and unsupported schemas are not mutated. |
| VOLUME-015 | Chain state, canonicity, and application metadata cannot bypass the Volume contract. |

Corruption is quarantined only after it is proved; a database error is not proof
of corruption. Retained-root queries report authoritative stored intent even
when content is missing or quarantined, so valid republication restores current
policy.

Primary coverage: `VolumeIntegrityTests`, `MemoryBrokerTests`,
`DiskBrokerTests`, `PinIndexTests`, `EvictionEngineTests`,
`SchemaVersionTests`, `BrokerStorerTests`, and `ContentStoreTests`.

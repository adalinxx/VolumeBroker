# Stack architecture source

The normative architectural laws for this work live in `adalinxx/Lattice/docs/foundational-architecture.md` on the coordinated redesign branch.

VolumeBroker remains semantics-blind: it enforces generic complete-Volume, CID-integrity, atomicity, retention, and eviction contracts without deciding consensus or application workflow completeness.

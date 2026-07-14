# Foundational architecture alignment

VolumeBroker stores and retains complete declared Volumes. It does not interpret application DAG semantics or decide which nested Volumes a workflow requires.

This change enforces the generic invariants the storage layer can own:

1. a Volume has a non-empty declared root;
2. the root object is included in the Volume;
3. every `(CID, bytes)` pair is content-address correct;
4. an existing CID cannot silently map to conflicting bytes;
5. a published Volume root cannot silently change its entry set;
6. cashew serializes a complete boundary before calling the broker;
7. MemoryBroker and DiskBroker validate a batch before mutating storage;
8. legacy rows with missing root or CAS data are not reported as available.

Nested Volumes remain independent availability units. Root presence does not imply availability of every nested Volume, and VolumeBroker does not attempt to certify application-level materialization.

The tests cover malformed roots, CID mismatch, conflicting membership, atomic pre-write rejection in both broker implementations, and legacy partial-row rejection.

# Foundational architecture alignment

VolumeBroker stores and retains complete declared Volumes. It does not interpret application DAG semantics or decide which nested Volumes a workflow requires.

This change enforces the generic invariants the storage layer can own:

1. a Volume has a non-empty declared root;
2. the root object is included in the Volume;
3. every `(CID, bytes)` pair is content-address correct;
4. an existing CID cannot silently map to conflicting bytes;
5. completed Volume scopes close in stack order;
6. an incomplete traversal cannot be collected or flushed as complete;
7. a batch is validated before one atomic SQLite transaction.

Nested Volumes remain independent availability units. Root presence does not imply availability of every nested Volume, and VolumeBroker does not attempt to certify application-level materialization.

The tests cover malformed roots, CID mismatch, pre-write rejection, abort behavior, and unbalanced scopes.

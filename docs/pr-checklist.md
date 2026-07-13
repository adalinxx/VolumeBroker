# Volume integrity review checklist

- [ ] Declared root is present.
- [ ] Every entry hashes to its CID.
- [ ] Conflicting bytes for an existing CID fail visibly.
- [ ] Entire batch validates before a transaction begins.
- [ ] Memory and disk brokers enforce the same integrity checks.
- [ ] Open scopes cannot collect or flush.
- [ ] A scope cannot exit before storing its root entry.
- [ ] Legacy partial rows are not reported as available.
- [ ] A failed broker flush leaves completed Volumes available for retry.
- [ ] Stack mismatches fail closed.
- [ ] No application DAG semantics enter VolumeBroker.

# Volume integrity review checklist

- [ ] Declared root is present.
- [ ] Every entry hashes to its CID.
- [ ] Conflicting bytes for an existing CID fail visibly.
- [ ] Entire batch validates before a transaction begins.
- [ ] Open scopes cannot collect or flush.
- [ ] Stack mismatches fail closed.
- [ ] No application DAG semantics enter VolumeBroker.

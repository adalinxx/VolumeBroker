# Volume integrity review checklist

- [ ] Declared root is present.
- [ ] Every entry hashes to its CID.
- [ ] Conflicting bytes for an existing CID fail visibly.
- [ ] Conflicting entry sets for an existing Volume root fail visibly.
- [ ] Entire batch validates before a transaction begins.
- [ ] Memory and disk brokers enforce the same integrity checks.
- [ ] Cashew serializes a complete boundary before calling the broker.
- [ ] Legacy partial rows are not reported as available.
- [ ] A parent Volume remains stored if a later selected child fails.
- [ ] No application DAG semantics enter VolumeBroker.

public enum BrokerError: Error, Sendable, Equatable {
    case openFailed(String)
    case sqlFailed(String)
    case notFound
    case invalidRetainedRootOperation(String)
    case missingRetainedRoot(String)
    case conflictingRetainedRootOperation(String)

    /// Existing CAS bytes for a CID differ from the bytes being stored. Under
    /// content addressing this is corruption (or an impossible hash collision),
    /// never a benign overwrite.
    case conflictingContent(String)

    /// A published Volume root cannot later name a different complete entry set.
    case conflictingVolume(String)
}

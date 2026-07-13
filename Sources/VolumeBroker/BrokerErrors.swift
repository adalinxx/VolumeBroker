public enum BrokerError: Error, Sendable, Equatable {
    case openFailed(String)
    case sqlFailed(String)
    case notFound
    case invalidRetainedRootOperation(String)
    case missingRetainedRoot(String)
    case conflictingRetainedRootOperation(String)

    /// Volume scopes must close in strict stack order. A mismatch means the
    /// storer cannot prove that the pending bytes form a complete Volume.
    case unbalancedVolumeScope(expected: String?, actual: String)

    /// A flush while scopes remain open would publish an incomplete traversal.
    case incompleteVolumeScopes([String])

    /// Existing CAS bytes for a CID differ from the bytes being stored. Under
    /// content addressing this is corruption (or an impossible hash collision),
    /// never a benign overwrite.
    case conflictingContent(String)
}

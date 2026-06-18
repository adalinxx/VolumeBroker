public enum BrokerError: Error, Sendable {
    case openFailed(String)
    case sqlFailed(String)
    case notFound
    case invalidRetainedRootOperation(String)
    case missingRetainedRoot(String)
    case conflictingRetainedRootOperation(String)
}

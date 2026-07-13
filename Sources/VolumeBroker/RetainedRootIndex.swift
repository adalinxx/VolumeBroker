import Foundation
#if canImport(SQLite3)
import SQLite3
#else
import VolumeBrokerSQLite
#endif

/// Named root sets that act as durable GC/serve roots outside the owner/count
/// pin index. Advances are idempotent by operation id and payload-bound so a
/// retry cannot silently install a different retained set.
struct RetainedRootIndex {
    let connection: SQLiteConnection

    func advanceRetainedRoots(scope: String, roots: [String], operationID: String) async throws {
        let canonicalRoots = try Self.canonicalRoots(roots)
        guard !scope.isEmpty else {
            throw BrokerError.invalidRetainedRootOperation("scope must not be empty")
        }
        guard !operationID.isEmpty else {
            throw BrokerError.invalidRetainedRootOperation("operationID must not be empty")
        }
        let encodedRoots = try Self.encodeRoots(canonicalRoots)

        try await connection.write {
            try connection.transaction {
                if let existing = try lookupOperation(operationID: operationID) {
                    guard existing.scope == scope && existing.canonicalRoots == encodedRoots else {
                        throw BrokerError.conflictingRetainedRootOperation(operationID)
                    }
                    return
                }

                for root in canonicalRoots {
                    try validateStoredVolume(root: root)
                }

                try connection.execBind("DELETE FROM retained_roots WHERE scope=?1") { stmt in
                    sqlite3_bind_text(stmt, 1, scope, -1, SQLITE_TRANSIENT_SHIM)
                }
                for root in canonicalRoots {
                    try connection.execBind("INSERT INTO retained_roots(scope, root) VALUES(?1, ?2)") { stmt in
                        sqlite3_bind_text(stmt, 1, scope, -1, SQLITE_TRANSIENT_SHIM)
                        sqlite3_bind_text(stmt, 2, root, -1, SQLITE_TRANSIENT_SHIM)
                    }
                }
                try connection.execBind("""
                    INSERT INTO retained_root_operations(operation_id, scope, canonical_roots)
                    VALUES(?1, ?2, ?3)
                    """) { stmt in
                    sqlite3_bind_text(stmt, 1, operationID, -1, SQLITE_TRANSIENT_SHIM)
                    sqlite3_bind_text(stmt, 2, scope, -1, SQLITE_TRANSIENT_SHIM)
                    sqlite3_bind_text(stmt, 3, encodedRoots, -1, SQLITE_TRANSIENT_SHIM)
                }
            }
        }
    }

    func mergeRetainedRoots(scope: String, roots: [String], operationID: String) async throws {
        let canonicalRoots = try Self.canonicalRoots(roots)
        guard !scope.isEmpty else {
            throw BrokerError.invalidRetainedRootOperation("scope must not be empty")
        }
        guard !operationID.isEmpty else {
            throw BrokerError.invalidRetainedRootOperation("operationID must not be empty")
        }
        let encodedRoots = "merge:" + (try Self.encodeRoots(canonicalRoots))

        try await connection.write {
            try connection.transaction {
                if let existing = try lookupOperation(operationID: operationID) {
                    guard existing.scope == scope && existing.canonicalRoots == encodedRoots else {
                        throw BrokerError.conflictingRetainedRootOperation(operationID)
                    }
                    return
                }

                for root in canonicalRoots {
                    try validateStoredVolume(root: root)
                    try connection.execBind("INSERT OR IGNORE INTO retained_roots(scope, root) VALUES(?1, ?2)") { stmt in
                        sqlite3_bind_text(stmt, 1, scope, -1, SQLITE_TRANSIENT_SHIM)
                        sqlite3_bind_text(stmt, 2, root, -1, SQLITE_TRANSIENT_SHIM)
                    }
                }
                try connection.execBind("""
                    INSERT INTO retained_root_operations(operation_id, scope, canonical_roots)
                    VALUES(?1, ?2, ?3)
                    """) { stmt in
                    sqlite3_bind_text(stmt, 1, operationID, -1, SQLITE_TRANSIENT_SHIM)
                    sqlite3_bind_text(stmt, 2, scope, -1, SQLITE_TRANSIENT_SHIM)
                    sqlite3_bind_text(stmt, 3, encodedRoots, -1, SQLITE_TRANSIENT_SHIM)
                }
            }
        }
    }

    func retainedRoots(scope: String) async -> [String] {
        guard !scope.isEmpty else { return [] }
        return await connection.read {
            var stmt: OpaquePointer?
            defer { sqlite3_finalize(stmt) }
            let sql = "SELECT root FROM retained_roots WHERE scope=?1 ORDER BY root"
            guard sqlite3_prepare_v2(connection.readDb, sql, -1, &stmt, nil) == SQLITE_OK else { return [] }
            sqlite3_bind_text(stmt, 1, scope, -1, SQLITE_TRANSIENT_SHIM)
            var result: [String] = []
            while sqlite3_step(stmt) == SQLITE_ROW {
                if let ptr = sqlite3_column_text(stmt, 0) {
                    result.append(String(cString: ptr))
                }
            }
            return result
        }
    }

    private static func canonicalRoots(_ roots: [String]) throws -> [String] {
        let unique = Array(Set(roots))
        if unique.contains(where: { $0.isEmpty }) {
            throw BrokerError.invalidRetainedRootOperation("roots must not contain empty strings")
        }
        return unique.sorted()
    }

    private static func encodeRoots(_ roots: [String]) throws -> String {
        let data = try JSONEncoder().encode(roots)
        return String(decoding: data, as: UTF8.self)
    }

    private func lookupOperation(operationID: String) throws -> (scope: String, canonicalRoots: String)? {
        var stmt: OpaquePointer?
        defer { sqlite3_finalize(stmt) }
        let sql = "SELECT scope, canonical_roots FROM retained_root_operations WHERE operation_id=?1"
        guard sqlite3_prepare_v2(connection.db, sql, -1, &stmt, nil) == SQLITE_OK else {
            throw BrokerError.sqlFailed(String(cString: sqlite3_errmsg(connection.db)))
        }
        sqlite3_bind_text(stmt, 1, operationID, -1, SQLITE_TRANSIENT_SHIM)
        guard sqlite3_step(stmt) == SQLITE_ROW else { return nil }
        let scope = sqlite3_column_text(stmt, 0).map { String(cString: $0) } ?? ""
        let roots = sqlite3_column_text(stmt, 1).map { String(cString: $0) } ?? ""
        return (scope, roots)
    }

    private func validateStoredVolume(root: String) throws {
        var stmt: OpaquePointer?
        defer { sqlite3_finalize(stmt) }
        let sql = """
            SELECT ?1
            WHERE NOT EXISTS (
                SELECT 1 FROM volume_metadata WHERE root = ?1
            ) OR NOT EXISTS (
                SELECT 1
                FROM volume_entries ve
                JOIN cas_data cd ON cd.cid = ve.cid
                WHERE ve.root = ?1 AND ve.cid = ?1
            ) OR EXISTS (
                SELECT 1 FROM volume_entries ve
                LEFT JOIN cas_data cd ON cd.cid = ve.cid
                WHERE ve.root = ?1 AND cd.cid IS NULL
            )
            LIMIT 1
            """
        guard sqlite3_prepare_v2(connection.db, sql, -1, &stmt, nil) == SQLITE_OK else {
            throw BrokerError.sqlFailed(String(cString: sqlite3_errmsg(connection.db)))
        }
        sqlite3_bind_text(stmt, 1, root, -1, SQLITE_TRANSIENT_SHIM)
        if sqlite3_step(stmt) == SQLITE_ROW {
            let missing = sqlite3_column_text(stmt, 0).map { String(cString: $0) } ?? root
            throw BrokerError.missingRetainedRoot(missing)
        }
    }
}

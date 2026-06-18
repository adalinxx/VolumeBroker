import Foundation
#if canImport(SQLite3)
import SQLite3
#else
import VolumeBrokerSQLite
#endif

/// Pin reference-count index.
///
/// Owns the `volume_pins` and `volume_unpin_operations` tables: ref-counted
/// pins per (root, owner), TTL expiry timestamps, idempotent batched unpins,
/// and owner/prefix-scoped pinned-root queries.
struct PinIndex {
    let connection: SQLiteConnection

    private func isoNow() -> String { SQLiteConnection.isoFormatter.string(from: Date.now) }

    func pin(root: String, owner: String, count: Int, ttl: Duration?) async throws {
        let iso: String? = ttl.map { SQLiteConnection.isoFormatter.string(from: Date.now.addingTimeInterval(Double($0.components.seconds))) }
        let sql = """
            INSERT INTO volume_pins(root, owner, count, expires_at) VALUES(?1, ?2, ?3, ?4)
            ON CONFLICT(root, owner) DO UPDATE SET
                count = volume_pins.count + excluded.count,
                expires_at = CASE
                    WHEN excluded.expires_at IS NULL OR volume_pins.expires_at IS NULL THEN NULL
                    WHEN excluded.expires_at > volume_pins.expires_at THEN excluded.expires_at
                    ELSE volume_pins.expires_at
                END
            """
        try await connection.write {
            try connection.execBind(sql) { stmt in
                sqlite3_bind_text(stmt, 1, root, -1, SQLITE_TRANSIENT_SHIM)
                sqlite3_bind_text(stmt, 2, owner, -1, SQLITE_TRANSIENT_SHIM)
                sqlite3_bind_int64(stmt, 3, Int64(count))
                if let iso {
                    sqlite3_bind_text(stmt, 4, iso, -1, SQLITE_TRANSIENT_SHIM)
                } else {
                    sqlite3_bind_null(stmt, 4)
                }
            }
        }
    }

    /// Pin multiple roots under the same owner in a single SQLite transaction.
    func pinBatch(roots: [String], owner: String) async throws {
        guard !roots.isEmpty else { return }
        let sql = """
            INSERT INTO volume_pins(root, owner, count, expires_at) VALUES(?1, ?2, 1, NULL)
            ON CONFLICT(root, owner) DO UPDATE SET
                count = volume_pins.count + 1,
                expires_at = CASE
                    WHEN volume_pins.expires_at IS NULL THEN NULL
                    ELSE volume_pins.expires_at
                END
            """
        try await connection.write {
            try connection.transaction {
                for root in roots {
                    try connection.execBind(sql) { stmt in
                        sqlite3_bind_text(stmt, 1, root, -1, SQLITE_TRANSIENT_SHIM)
                        sqlite3_bind_text(stmt, 2, owner, -1, SQLITE_TRANSIENT_SHIM)
                    }
                }
            }
        }
    }

    func unpin(root: String, owner: String, count: Int) async throws {
        try await connection.write {
            try connection.transaction {
                try connection.execBind("UPDATE volume_pins SET count = count - ?3 WHERE root=?1 AND owner=?2") { stmt in
                    sqlite3_bind_text(stmt, 1, root, -1, SQLITE_TRANSIENT_SHIM)
                    sqlite3_bind_text(stmt, 2, owner, -1, SQLITE_TRANSIENT_SHIM)
                    sqlite3_bind_int64(stmt, 3, Int64(count))
                }
                try connection.execBind("DELETE FROM volume_pins WHERE root=?1 AND owner=?2 AND count <= 0") { stmt in
                    sqlite3_bind_text(stmt, 1, root, -1, SQLITE_TRANSIENT_SHIM)
                    sqlite3_bind_text(stmt, 2, owner, -1, SQLITE_TRANSIENT_SHIM)
                }
            }
        }
    }

    /// Decrement pin counts for multiple (root, owner, count) tuples in one transaction.
    func unpinBatch(items: [(root: String, owner: String, count: Int)]) async throws {
        guard !items.isEmpty else { return }
        if items.count == 1 { try await unpin(root: items[0].root, owner: items[0].owner, count: items[0].count); return }
        try await connection.write {
            try connection.transaction {
                for item in items {
                    try connection.execBind("UPDATE volume_pins SET count = count - ?3 WHERE root=?1 AND owner=?2") { stmt in
                        sqlite3_bind_text(stmt, 1, item.root, -1, SQLITE_TRANSIENT_SHIM)
                        sqlite3_bind_text(stmt, 2, item.owner, -1, SQLITE_TRANSIENT_SHIM)
                        sqlite3_bind_int64(stmt, 3, Int64(item.count))
                    }
                }
                try connection.exec("DELETE FROM volume_pins WHERE count <= 0")
            }
        }
    }

    /// Apply a counted unpin batch at most once for `operationID`.
    ///
    /// The operation marker and pin decrements live in the same transaction, so
    /// callers can safely retry cleanup after a process crash: if the prior
    /// transaction committed, the retry is a no-op; if it did not, the retry
    /// applies the decrements exactly once.
    func unpinBatchOnce(operationID: String, items: [(root: String, owner: String, count: Int)]) async throws {
        guard !operationID.isEmpty, !items.isEmpty else { return }
        try await connection.write {
            try connection.transaction {
                try connection.execBind("INSERT OR IGNORE INTO volume_unpin_operations(operation_id) VALUES(?1)") { stmt in
                    sqlite3_bind_text(stmt, 1, operationID, -1, SQLITE_TRANSIENT_SHIM)
                }
                guard connection.changes() > 0 else {
                    return
                }
                for item in items {
                    try connection.execBind("UPDATE volume_pins SET count = count - ?3 WHERE root=?1 AND owner=?2") { stmt in
                        sqlite3_bind_text(stmt, 1, item.root, -1, SQLITE_TRANSIENT_SHIM)
                        sqlite3_bind_text(stmt, 2, item.owner, -1, SQLITE_TRANSIENT_SHIM)
                        sqlite3_bind_int64(stmt, 3, Int64(item.count))
                    }
                }
                try connection.exec("DELETE FROM volume_pins WHERE count <= 0")
            }
        }
    }

    func unpinAll(owner: String) async throws {
        try await connection.write {
            try connection.execBind("DELETE FROM volume_pins WHERE owner=?1") { stmt in
                sqlite3_bind_text(stmt, 1, owner, -1, SQLITE_TRANSIENT_SHIM)
            }
        }
    }

    /// Delete all pins for multiple owners in one transaction.
    func unpinAllBatch(owners: [String]) async throws {
        guard !owners.isEmpty else { return }
        if owners.count == 1 { try await unpinAll(owner: owners[0]); return }
        try await connection.write {
            try connection.transaction {
                for owner in owners {
                    try connection.execBind("DELETE FROM volume_pins WHERE owner=?1") { stmt in
                        sqlite3_bind_text(stmt, 1, owner, -1, SQLITE_TRANSIENT_SHIM)
                    }
                }
            }
        }
    }

    /// True iff `cid` is covered by a live pin or retained root — the cid is a
    /// root itself, or reachable UPWARD through `volume_entries` from a retained
    /// object closure. This mirrors the eviction engine's downward protected set.
    /// The upward walk follows "which volumes contain this cid", a short path
    /// for merkle closures; UNION dedups the root-contains-itself self-edge.
    func isPinReachable(cid: String) async -> Bool {
        await connection.read {
            let now = isoNow()
            var stmt: OpaquePointer?
            defer { sqlite3_finalize(stmt) }
            let sql = """
                WITH RECURSIVE up(c) AS (
                    SELECT ?1
                    UNION
                    SELECT ve.root FROM volume_entries ve INNER JOIN up ON ve.cid = up.c
                ),
                live_roots(root) AS (
                    SELECT root FROM volume_pins
                    WHERE count > 0 AND (expires_at IS NULL OR expires_at > ?2)
                    UNION
                    SELECT root FROM retained_roots
                )
                SELECT 1 FROM live_roots lr
                INNER JOIN up ON lr.root = up.c
                LIMIT 1
                """
            guard sqlite3_prepare_v2(connection.readDb, sql, -1, &stmt, nil) == SQLITE_OK else { return false }
            sqlite3_bind_text(stmt, 1, cid, -1, SQLITE_TRANSIENT_SHIM)
            sqlite3_bind_text(stmt, 2, now, -1, SQLITE_TRANSIENT_SHIM)
            return sqlite3_step(stmt) == SQLITE_ROW
        }
    }

    func owners(root: String) async -> Set<String> {
        await connection.read {
            let now = isoNow()
            var stmt: OpaquePointer?
            defer { sqlite3_finalize(stmt) }
            guard sqlite3_prepare_v2(connection.readDb, "SELECT owner FROM volume_pins WHERE root=?1 AND (expires_at IS NULL OR expires_at > ?2)", -1, &stmt, nil) == SQLITE_OK else { return [] }
            sqlite3_bind_text(stmt, 1, root, -1, SQLITE_TRANSIENT_SHIM)
            sqlite3_bind_text(stmt, 2, now, -1, SQLITE_TRANSIENT_SHIM)
            var result: Set<String> = []
            while sqlite3_step(stmt) == SQLITE_ROW {
                if let ptr = sqlite3_column_text(stmt, 0) {
                    result.insert(String(cString: ptr))
                }
            }
            return result
        }
    }

    func pinnedRoots() async -> [String] {
        await connection.read {
            let now = isoNow()
            var stmt: OpaquePointer?
            defer { sqlite3_finalize(stmt) }
            guard sqlite3_prepare_v2(connection.readDb, "SELECT DISTINCT root FROM volume_pins WHERE expires_at IS NULL OR expires_at > ?1", -1, &stmt, nil) == SQLITE_OK else { return [] }
            sqlite3_bind_text(stmt, 1, now, -1, SQLITE_TRANSIENT_SHIM)
            var result: [String] = []
            while sqlite3_step(stmt) == SQLITE_ROW {
                if let ptr = sqlite3_column_text(stmt, 0) {
                    result.append(String(cString: ptr))
                }
            }
            return result
        }
    }

    /// Return live pinned roots owned by any exact owner or owner prefix.
    func pinnedRoots(owners: [String], ownerPrefixes: [String]) async -> [String] {
        await connection.read {
            let exactOwners = Array(Set(owners.filter { !$0.isEmpty })).sorted()
            let prefixes = Array(Set(ownerPrefixes.filter { !$0.isEmpty })).sorted()
            guard !exactOwners.isEmpty || !prefixes.isEmpty else { return [] }

            let now = isoNow()
            var clauses: [String] = []
            var bindings: [String] = [now]
            for owner in exactOwners {
                bindings.append(owner)
                clauses.append("owner = ?\(bindings.count)")
            }
            for prefix in prefixes {
                bindings.append(prefix)
                let lower = bindings.count
                bindings.append(prefixUpperBound(prefix))
                let upper = bindings.count
                clauses.append("(owner >= ?\(lower) AND owner < ?\(upper))")
            }

            let sql = """
                SELECT DISTINCT root FROM volume_pins
                WHERE count > 0
                  AND (expires_at IS NULL OR expires_at > ?1)
                  AND (\(clauses.joined(separator: " OR ")))
                """

            var stmt: OpaquePointer?
            defer { sqlite3_finalize(stmt) }
            guard sqlite3_prepare_v2(connection.readDb, sql, -1, &stmt, nil) == SQLITE_OK else { return [] }
            for (offset, binding) in bindings.enumerated() {
                sqlite3_bind_text(stmt, Int32(offset + 1), binding, -1, SQLITE_TRANSIENT_SHIM)
            }

            var result: [String] = []
            while sqlite3_step(stmt) == SQLITE_ROW {
                if let ptr = sqlite3_column_text(stmt, 0) {
                    result.append(String(cString: ptr))
                }
            }
            return result
        }
    }

    /// Return live pin owners matching a prefix.
    func pinnedOwners(prefix: String) async -> [String] {
        guard !prefix.isEmpty else { return [] }
        return await connection.read {
            let now = isoNow()
            var stmt: OpaquePointer?
            defer { sqlite3_finalize(stmt) }
            let sql = """
                SELECT DISTINCT owner FROM volume_pins
                WHERE count > 0
                  AND (expires_at IS NULL OR expires_at > ?1)
                  AND owner >= ?2
                  AND owner < ?3
                """
            guard sqlite3_prepare_v2(connection.readDb, sql, -1, &stmt, nil) == SQLITE_OK else { return [] }
            sqlite3_bind_text(stmt, 1, now, -1, SQLITE_TRANSIENT_SHIM)
            sqlite3_bind_text(stmt, 2, prefix, -1, SQLITE_TRANSIENT_SHIM)
            sqlite3_bind_text(stmt, 3, prefixUpperBound(prefix), -1, SQLITE_TRANSIENT_SHIM)
            var result: [String] = []
            while sqlite3_step(stmt) == SQLITE_ROW {
                if let ptr = sqlite3_column_text(stmt, 0) {
                    result.append(String(cString: ptr))
                }
            }
            return result
        }
    }

    /// Delete idempotency markers whose operation id begins with `prefix` and
    /// whose last colon-delimited component is a parseable height below
    /// `belowHeight`.
    func deleteUnpinOperations(belowHeight: Int, prefix: String) async throws -> Int {
        guard belowHeight > 0, !prefix.isEmpty else { return 0 }
        let operationIDs = await unpinOperationIDs(prefix: prefix)
        let expiredIDs = operationIDs.filter { operationID in
            guard let last = operationID.split(separator: ":").last,
                  let height = Int(String(last)) else { return false }
            return height < belowHeight
        }
        guard !expiredIDs.isEmpty else { return 0 }
        try await connection.write {
            try connection.transaction {
                for operationID in expiredIDs {
                    try connection.execBind("DELETE FROM volume_unpin_operations WHERE operation_id=?1") { stmt in
                        sqlite3_bind_text(stmt, 1, operationID, -1, SQLITE_TRANSIENT_SHIM)
                    }
                }
            }
        }
        return expiredIDs.count
    }

    func unpinOperationCount(prefix: String? = nil) async -> Int {
        await connection.read {
            var stmt: OpaquePointer?
            defer { sqlite3_finalize(stmt) }
            let sql: String
            if prefix == nil {
                sql = "SELECT COUNT(*) FROM volume_unpin_operations"
            } else {
                sql = "SELECT COUNT(*) FROM volume_unpin_operations WHERE operation_id >= ?1 AND operation_id < ?2"
            }
            guard sqlite3_prepare_v2(connection.readDb, sql, -1, &stmt, nil) == SQLITE_OK else { return 0 }
            if let prefix {
                sqlite3_bind_text(stmt, 1, prefix, -1, SQLITE_TRANSIENT_SHIM)
                sqlite3_bind_text(stmt, 2, prefixUpperBound(prefix), -1, SQLITE_TRANSIENT_SHIM)
            }
            guard sqlite3_step(stmt) == SQLITE_ROW else { return 0 }
            return Int(sqlite3_column_int64(stmt, 0))
        }
    }

    func deleteUnpinOperations(prefix: String) async throws -> Int {
        guard !prefix.isEmpty else { return 0 }
        return try await connection.write {
            try connection.execBind("""
                DELETE FROM volume_unpin_operations
                WHERE operation_id >= ?1 AND operation_id < ?2
                """) { stmt in
                sqlite3_bind_text(stmt, 1, prefix, -1, SQLITE_TRANSIENT_SHIM)
                sqlite3_bind_text(stmt, 2, prefixUpperBound(prefix), -1, SQLITE_TRANSIENT_SHIM)
            }
            return connection.changes()
        }
    }

    private func unpinOperationIDs(prefix: String) async -> [String] {
        await connection.read {
            var stmt: OpaquePointer?
            defer { sqlite3_finalize(stmt) }
            let sql = """
                SELECT operation_id FROM volume_unpin_operations
                WHERE operation_id >= ?1 AND operation_id < ?2
                """
            guard sqlite3_prepare_v2(connection.readDb, sql, -1, &stmt, nil) == SQLITE_OK else { return [] }
            sqlite3_bind_text(stmt, 1, prefix, -1, SQLITE_TRANSIENT_SHIM)
            sqlite3_bind_text(stmt, 2, prefixUpperBound(prefix), -1, SQLITE_TRANSIENT_SHIM)
            var ids: [String] = []
            while sqlite3_step(stmt) == SQLITE_ROW {
                if let ptr = sqlite3_column_text(stmt, 0) {
                    ids.append(String(cString: ptr))
                }
            }
            return ids
        }
    }

    private func prefixUpperBound(_ prefix: String) -> String {
        var bytes = Array(prefix.utf8)
        guard !bytes.isEmpty else { return prefix }
        for index in bytes.indices.reversed() {
            guard bytes[index] < UInt8.max else { continue }
            bytes[index] += 1
            bytes.removeSubrange(bytes.index(after: index)..<bytes.endIndex)
            if let upper = String(bytes: bytes, encoding: .utf8) {
                return upper
            }
            break
        }
        return prefix + "\u{10ffff}"
    }
}

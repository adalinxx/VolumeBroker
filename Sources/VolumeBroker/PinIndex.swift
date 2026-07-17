import Foundation
#if canImport(SQLite3)
import SQLite3
#else
import VolumeBrokerSQLite
#endif

/// Pin reference-count index.
///
/// Owns the `volume_pins` table: ref-counted pins per (root, owner), TTL expiry
/// timestamps, batched updates, and owner/prefix-scoped pinned-root queries.
struct PinIndex {
    let connection: SQLiteConnection

    private func isoNow() -> String { SQLiteConnection.isoFormatter.string(from: Date.now) }

    func pin(root: String, owner: String, count: Int, ttl: Duration?) async throws {
        guard count > 0 else { throw BrokerError.invalidPinCount }
        let now = Date.now
        let nowISO = SQLiteConnection.isoFormatter.string(from: now)
        let iso = try expiration(ttl: ttl, from: now)
        let sql = """
            INSERT INTO volume_pins(root, owner, count, expires_at) VALUES(?1, ?2, ?3, ?4)
            ON CONFLICT(root, owner) DO UPDATE SET
                count = CASE
                    WHEN volume_pins.expires_at IS NOT NULL AND volume_pins.expires_at <= ?5
                        THEN excluded.count
                    ELSE volume_pins.count + excluded.count
                END,
                expires_at = CASE
                    WHEN volume_pins.expires_at IS NOT NULL AND volume_pins.expires_at <= ?5
                        THEN excluded.expires_at
                    WHEN excluded.expires_at IS NULL OR volume_pins.expires_at IS NULL THEN NULL
                    WHEN excluded.expires_at > volume_pins.expires_at THEN excluded.expires_at
                    ELSE volume_pins.expires_at
                END
        """
        try await connection.write {
            try connection.transaction {
                try validateStoredVolume(root: root)
                try connection.execBind(sql) { stmt in
                    sqlite3_bind_text(stmt, 1, root, -1, SQLITE_TRANSIENT_SHIM)
                    sqlite3_bind_text(stmt, 2, owner, -1, SQLITE_TRANSIENT_SHIM)
                    sqlite3_bind_int64(stmt, 3, Int64(count))
                    if let iso {
                        sqlite3_bind_text(stmt, 4, iso, -1, SQLITE_TRANSIENT_SHIM)
                    } else {
                        sqlite3_bind_null(stmt, 4)
                    }
                    sqlite3_bind_text(stmt, 5, nowISO, -1, SQLITE_TRANSIENT_SHIM)
                }
            }
        }
    }

    /// Pin multiple roots under the same owner in a single SQLite transaction.
    func pinBatch(roots: [String], owner: String) async throws {
        guard !roots.isEmpty else { return }
        let now = isoNow()
        let sql = """
            INSERT INTO volume_pins(root, owner, count, expires_at) VALUES(?1, ?2, 1, NULL)
            ON CONFLICT(root, owner) DO UPDATE SET
                count = CASE
                    WHEN volume_pins.expires_at IS NOT NULL AND volume_pins.expires_at <= ?3 THEN 1
                    ELSE volume_pins.count + 1
                END,
                expires_at = NULL
            """
        try await connection.write {
            try connection.transaction {
                for root in roots { try validateStoredVolume(root: root) }
                for root in roots {
                    try connection.execBind(sql) { stmt in
                        sqlite3_bind_text(stmt, 1, root, -1, SQLITE_TRANSIENT_SHIM)
                        sqlite3_bind_text(stmt, 2, owner, -1, SQLITE_TRANSIENT_SHIM)
                        sqlite3_bind_text(stmt, 3, now, -1, SQLITE_TRANSIENT_SHIM)
                    }
                }
            }
        }
    }

    func unpin(root: String, owner: String, count: Int) async throws {
        guard count > 0 else { throw BrokerError.invalidPinCount }
        try await connection.write {
            try connection.transaction {
                try connection.execBind("DELETE FROM volume_pins WHERE root=?1 AND owner=?2 AND count <= ?3") { stmt in
                    sqlite3_bind_text(stmt, 1, root, -1, SQLITE_TRANSIENT_SHIM)
                    sqlite3_bind_text(stmt, 2, owner, -1, SQLITE_TRANSIENT_SHIM)
                    sqlite3_bind_int64(stmt, 3, Int64(count))
                }
                try connection.execBind("UPDATE volume_pins SET count = count - ?3 WHERE root=?1 AND owner=?2") { stmt in
                    sqlite3_bind_text(stmt, 1, root, -1, SQLITE_TRANSIENT_SHIM)
                    sqlite3_bind_text(stmt, 2, owner, -1, SQLITE_TRANSIENT_SHIM)
                    sqlite3_bind_int64(stmt, 3, Int64(count))
                }
            }
        }
    }

    /// Decrement pin counts for multiple (root, owner, count) tuples in one transaction.
    func unpinBatch(items: [(root: String, owner: String, count: Int)]) async throws {
        guard !items.isEmpty else { return }
        guard items.allSatisfy({ $0.count > 0 }) else { throw BrokerError.invalidPinCount }
        if items.count == 1 { try await unpin(root: items[0].root, owner: items[0].owner, count: items[0].count); return }
        try await connection.write {
            try connection.transaction {
                for item in items {
                    try connection.execBind("DELETE FROM volume_pins WHERE root=?1 AND owner=?2 AND count <= ?3") { stmt in
                        sqlite3_bind_text(stmt, 1, item.root, -1, SQLITE_TRANSIENT_SHIM)
                        sqlite3_bind_text(stmt, 2, item.owner, -1, SQLITE_TRANSIENT_SHIM)
                        sqlite3_bind_int64(stmt, 3, Int64(item.count))
                    }
                    try connection.execBind("UPDATE volume_pins SET count = count - ?3 WHERE root=?1 AND owner=?2") { stmt in
                        sqlite3_bind_text(stmt, 1, item.root, -1, SQLITE_TRANSIENT_SHIM)
                        sqlite3_bind_text(stmt, 2, item.owner, -1, SQLITE_TRANSIENT_SHIM)
                        sqlite3_bind_int64(stmt, 3, Int64(item.count))
                    }
                }
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

    /// True iff `cid` is a live Volume root or a direct entry of one. Related
    /// Volume roots are independent and must be pinned or retained separately.
    func isPinReachable(cid: String) async -> Bool {
        let candidateRoots: [String] = await connection.read {
            let now = isoNow()
            var stmt: OpaquePointer?
            defer { sqlite3_finalize(stmt) }
            let sql = """
                WITH live_roots(root) AS (
                    SELECT root FROM volume_pins
                    WHERE count > 0 AND (expires_at IS NULL OR expires_at > ?2)
                    UNION
                    SELECT root FROM retained_roots
                )
                SELECT DISTINCT lr.root
                FROM live_roots lr
                LEFT JOIN volume_entries ve ON ve.root = lr.root
                WHERE lr.root = ?1 OR ve.cid = ?1
                """
            guard sqlite3_prepare_v2(connection.readDb, sql, -1, &stmt, nil) == SQLITE_OK else { return [] }
            sqlite3_bind_text(stmt, 1, cid, -1, SQLITE_TRANSIENT_SHIM)
            sqlite3_bind_text(stmt, 2, now, -1, SQLITE_TRANSIENT_SHIM)
            var roots: [String] = []
            while true {
                let result = sqlite3_step(stmt)
                if result == SQLITE_DONE { return roots }
                guard result == SQLITE_ROW,
                      let root = sqlite3_column_text(stmt, 0) else { return [] }
                roots.append(String(cString: root))
            }
        }

        let volumes = CASVolumeStore(connection: connection)
        for root in candidateRoots {
            if let volume = await volumes.fetchVolumeLocal(root: root),
               volume.entries[cid] != nil {
                return true
            }
        }
        return false
    }

    func owners(root: String) async -> Set<String> {
        await connection.read {
            let now = isoNow()
            var stmt: OpaquePointer?
            defer { sqlite3_finalize(stmt) }
            guard sqlite3_prepare_v2(connection.readDb, "SELECT owner FROM volume_pins WHERE root=?1 AND count > 0 AND (expires_at IS NULL OR expires_at > ?2)", -1, &stmt, nil) == SQLITE_OK else { return [] }
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
            guard sqlite3_prepare_v2(connection.readDb, "SELECT DISTINCT root FROM volume_pins WHERE count > 0 AND (expires_at IS NULL OR expires_at > ?1)", -1, &stmt, nil) == SQLITE_OK else { return [] }
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

    private func validateStoredVolume(root: String) throws {
        guard case .volume = try CASVolumeStore.loadValidatedVolume(
            root: root,
            db: connection.db
        ) else {
            throw BrokerError.notFound
        }
    }

    private func expiration(ttl: Duration?, from now: Date) throws -> String? {
        guard let ttl else { return nil }
        guard ttl >= .zero else { throw BrokerError.invalidPinTTL }
        let components = ttl.components
        let seconds = Double(components.seconds)
            + Double(components.attoseconds) / 1_000_000_000_000_000_000
        guard seconds.isFinite else { throw BrokerError.invalidPinTTL }
        let expiration = now.addingTimeInterval(seconds)
        guard expiration.timeIntervalSinceReferenceDate.isFinite else {
            throw BrokerError.invalidPinTTL
        }
        return SQLiteConnection.isoFormatter.string(from: expiration)
    }
}

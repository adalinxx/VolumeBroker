import Foundation
#if canImport(SQLite3)
import SQLite3
#else
import VolumeBrokerSQLite
#endif

/// Eviction engine.
///
/// Reclaims unpinned Volumes and then removes CAS rows with no surviving owner.
struct EvictionEngine {
    let connection: SQLiteConnection

    func evictUnpinned(graceSeconds: Int = 600) async throws -> Int {
        try await connection.write {
            let now = SQLiteConnection.isoFormatter.string(from: Date.now)
            let graceModifier = "-\(max(0, graceSeconds)) seconds"
            let evictedRoots = try connection.transaction {
                try connection.execBind("DELETE FROM volume_pins WHERE expires_at IS NOT NULL AND expires_at <= ?1") { stmt in
                    sqlite3_bind_text(stmt, 1, now, -1, SQLITE_TRANSIENT_SHIM)
                }
                let evictedRoots = try unpinnedRoots(
                    graceModifier: graceModifier,
                    now: now
                )

                for root in evictedRoots {
                    try connection.execBind("DELETE FROM volume_entries WHERE root = ?1") { stmt in
                        sqlite3_bind_text(stmt, 1, root, -1, SQLITE_TRANSIENT_SHIM)
                    }
                }
                for root in evictedRoots {
                    try connection.execBind("DELETE FROM volume_metadata WHERE root = ?1") { stmt in
                        sqlite3_bind_text(stmt, 1, root, -1, SQLITE_TRANSIENT_SHIM)
                    }
                }
                try connection.exec("""
                    DELETE FROM volume_entries
                    WHERE NOT EXISTS (
                        SELECT 1
                        FROM volume_metadata vm
                        WHERE vm.root = volume_entries.root
                          AND \(CASVolumeStore.completeVolumePredicate)
                    )
                    """)
                try connection.exec("""
                    DELETE FROM volume_metadata
                    WHERE NOT EXISTS (
                        SELECT 1 FROM volume_entries ve
                        WHERE ve.root = volume_metadata.root
                    )
                    """)
                try connection.exec("DELETE FROM volume_pins WHERE root NOT IN (SELECT root FROM volume_metadata)")
                try connection.exec("""
                    DELETE FROM cas_data
                    WHERE NOT EXISTS (
                        SELECT 1 FROM volume_entries ve WHERE ve.cid = cas_data.cid
                    )
                    """)
                return evictedRoots
            }
            return evictedRoots.count
        }
    }

    private func unpinnedRoots(graceModifier: String, now: String) throws -> [String] {
        var stmt: OpaquePointer?
        defer { sqlite3_finalize(stmt) }
        let sql = """
            WITH live_roots(root) AS (
                SELECT DISTINCT root FROM volume_pins
                WHERE count > 0 AND (expires_at IS NULL OR expires_at > ?2)
                UNION
                SELECT root FROM retained_roots
            )
            SELECT root FROM volume_metadata
            WHERE root NOT IN (SELECT root FROM live_roots)
              AND stored_at <= datetime('now', ?1)
            """
        guard sqlite3_prepare_v2(connection.db, sql, -1, &stmt, nil) == SQLITE_OK,
              let stmt else {
            throw BrokerError.sqlFailed(String(cString: sqlite3_errmsg(connection.db)))
        }
        sqlite3_bind_text(stmt, 1, graceModifier, -1, SQLITE_TRANSIENT_SHIM)
        sqlite3_bind_text(stmt, 2, now, -1, SQLITE_TRANSIENT_SHIM)
        var roots: [String] = []
        while true {
            let result = sqlite3_step(stmt)
            if result == SQLITE_DONE { return roots }
            guard result == SQLITE_ROW, let ptr = sqlite3_column_text(stmt, 0) else {
                throw BrokerError.sqlFailed(String(cString: sqlite3_errmsg(connection.db)))
            }
            roots.append(String(cString: ptr))
        }
    }

}

import Foundation
#if canImport(SQLite3)
import SQLite3
#else
import VolumeBrokerSQLite
#endif

/// Eviction engine.
///
/// Reclaims storage for unpinned volumes: it first prunes pins whose TTL has
/// expired, then deletes the CAS data, entries, and metadata for any root no
/// longer referenced by a live pin or retained-root scope. Roots referenced by
/// any remaining pin/retained root — and CAS blobs shared with them — are never
/// evicted.
struct EvictionEngine {
    let connection: SQLiteConnection
    let negativeCache: NegativeCache

    func evictUnpinned(graceSeconds: Int = 600) async throws -> Int {
        try await connection.write {
            let now = SQLiteConnection.isoFormatter.string(from: Date.now)
            let graceModifier = "-\(max(0, graceSeconds)) seconds"
            let evictedRoots = try connection.transaction {
                try connection.execBind("DELETE FROM volume_pins WHERE expires_at IS NOT NULL AND expires_at <= ?1") { stmt in
                    sqlite3_bind_text(stmt, 1, now, -1, SQLITE_TRANSIENT_SHIM)
                }
                let evictedRoots = unpinnedRoots(graceModifier: graceModifier, now: now)
                // A pin protects exactly one Volume root and that Volume's direct
                // entries. Related Volume roots are retained independently.
                try connection.execBind("""
                    WITH live_roots(root) AS (
                        SELECT DISTINCT root FROM volume_pins
                        WHERE count > 0 AND (expires_at IS NULL OR expires_at > ?2)
                        UNION
                        SELECT root FROM retained_roots
                    ),
                    protected_cids(cid) AS (
                        SELECT root FROM live_roots
                        UNION
                        SELECT ve.cid FROM volume_entries ve
                        INNER JOIN live_roots lr ON ve.root = lr.root
                    ),
                    evictable AS (
                        SELECT root FROM volume_metadata
                        WHERE root NOT IN (SELECT root FROM live_roots)
                          AND stored_at <= datetime('now', ?1)
                    )
                    DELETE FROM cas_data WHERE cid IN (
                        SELECT ve.cid FROM volume_entries ve
                        INNER JOIN evictable e ON ve.root = e.root
                        WHERE ve.cid NOT IN (SELECT cid FROM protected_cids)
                    )
                    """) { stmt in
                    sqlite3_bind_text(stmt, 1, graceModifier, -1, SQLITE_TRANSIENT_SHIM)
                    sqlite3_bind_text(stmt, 2, now, -1, SQLITE_TRANSIENT_SHIM)
                }
                try connection.execBind("""
                    WITH live_roots(root) AS (
                        SELECT DISTINCT root FROM volume_pins
                        WHERE count > 0 AND (expires_at IS NULL OR expires_at > ?2)
                        UNION
                        SELECT root FROM retained_roots
                    )
                    DELETE FROM volume_entries
                    WHERE root NOT IN (SELECT root FROM live_roots)
                      AND root IN (SELECT root FROM volume_metadata WHERE stored_at <= datetime('now', ?1))
                    """) { stmt in
                    sqlite3_bind_text(stmt, 1, graceModifier, -1, SQLITE_TRANSIENT_SHIM)
                    sqlite3_bind_text(stmt, 2, now, -1, SQLITE_TRANSIENT_SHIM)
                }
                try connection.execBind("""
                    WITH live_roots(root) AS (
                        SELECT DISTINCT root FROM volume_pins
                        WHERE count > 0 AND (expires_at IS NULL OR expires_at > ?2)
                        UNION
                        SELECT root FROM retained_roots
                    )
                    DELETE FROM volume_metadata
                    WHERE root NOT IN (SELECT root FROM live_roots) AND stored_at <= datetime('now', ?1)
                    """) { stmt in
                    sqlite3_bind_text(stmt, 1, graceModifier, -1, SQLITE_TRANSIENT_SHIM)
                    sqlite3_bind_text(stmt, 2, now, -1, SQLITE_TRANSIENT_SHIM)
                }
                return evictedRoots
            }
            for root in evictedRoots {
                negativeCache.recordEvicted(root)
            }
            return evictedRoots.count
        }
    }

    private func unpinnedRoots(graceModifier: String, now: String) -> [String] {
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
        guard sqlite3_prepare_v2(connection.db, sql, -1, &stmt, nil) == SQLITE_OK else { return [] }
        sqlite3_bind_text(stmt, 1, graceModifier, -1, SQLITE_TRANSIENT_SHIM)
        sqlite3_bind_text(stmt, 2, now, -1, SQLITE_TRANSIENT_SHIM)
        var roots: [String] = []
        while sqlite3_step(stmt) == SQLITE_ROW {
            if let ptr = sqlite3_column_text(stmt, 0) {
                roots.append(String(cString: ptr))
            }
        }
        return roots
    }
}

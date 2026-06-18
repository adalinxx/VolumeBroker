import Foundation
#if canImport(SQLite3)
import SQLite3
#else
import VolumeBrokerSQLite
#endif

/// Chain metadata key/value store.
///
/// Owns the `chain_meta` table. Reads use the read-only WAL connection so they
/// stay concurrent with block writes; writes commit atomically after block
/// volumes are durable, so a stored value always points at a CID in `cas_data`.
struct ChainMetaStore {
    let connection: SQLiteConnection

    func getChainMeta(key: String) async -> String? {
        await connection.read {
            var stmt: OpaquePointer?
            defer { sqlite3_finalize(stmt) }
            guard sqlite3_prepare_v2(connection.readDb, "SELECT value FROM chain_meta WHERE key=?1 LIMIT 1", -1, &stmt, nil) == SQLITE_OK else { return nil }
            sqlite3_bind_text(stmt, 1, key, -1, SQLITE_TRANSIENT_SHIM)
            guard sqlite3_step(stmt) == SQLITE_ROW else { return nil }
            guard let ptr = sqlite3_column_text(stmt, 0) else { return nil }
            return String(cString: ptr)
        }
    }

    func setChainMeta(key: String, value: String) async throws {
        try await connection.write {
            try connection.execBind("INSERT OR REPLACE INTO chain_meta(key, value) VALUES(?1, ?2)") { stmt in
                sqlite3_bind_text(stmt, 1, key, -1, SQLITE_TRANSIENT_SHIM)
                sqlite3_bind_text(stmt, 2, value, -1, SQLITE_TRANSIENT_SHIM)
            }
        }
    }
}

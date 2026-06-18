import Foundation
#if canImport(SQLite3)
import SQLite3
#else
import VolumeBrokerSQLite
#endif

/// Content-addressed volume store.
///
/// Owns the `cas_data`, `volume_entries`, and `volume_metadata` tables: storing
/// serialized volumes, fetching them back, and reporting presence. Eviction and
/// pin layers read these tables but only this layer writes them on the store path.
struct CASVolumeStore {
    let connection: SQLiteConnection
    let negativeCache: NegativeCache

    func hasVolume(root: String) async -> Bool {
        await connection.read {
            // Fast-path: bloom says this root was previously confirmed absent AND we
            // haven't stored it recently. This eliminates the SQLite round-trip for
            // ~99.9% of misses during initial sync, where most CIDs haven't been
            // downloaded yet.
            if negativeCache.mightBeAbsent(root) { return false }
            var stmt: OpaquePointer?
            defer { sqlite3_finalize(stmt) }
            guard sqlite3_prepare_v2(connection.readDb, "SELECT 1 FROM volume_metadata WHERE root=?1 LIMIT 1", -1, &stmt, nil) == SQLITE_OK else { return false }
            sqlite3_bind_text(stmt, 1, root, -1, SQLITE_TRANSIENT_SHIM)
            let found = sqlite3_step(stmt) == SQLITE_ROW
            if !found {
                negativeCache.recordAbsent(root)
            }
            return found
        }
    }

    /// Uses the dedicated read-only WAL connection — concurrent with writes.
    func fetchVolumeLocal(root: String) async -> SerializedVolume? {
        await connection.read {
            var stmt: OpaquePointer?
            defer { sqlite3_finalize(stmt) }
            let sql = """
                SELECT ve.cid, cd.data FROM volume_entries ve
                JOIN cas_data cd ON cd.cid = ve.cid
                WHERE ve.root = ?1
                """
            guard sqlite3_prepare_v2(connection.readDb, sql, -1, &stmt, nil) == SQLITE_OK else { return nil }
            sqlite3_bind_text(stmt, 1, root, -1, SQLITE_TRANSIENT_SHIM)
            var entries: [String: Data] = [:]
            while sqlite3_step(stmt) == SQLITE_ROW {
                guard let cidPtr = sqlite3_column_text(stmt, 0),
                      let blobPtr = sqlite3_column_blob(stmt, 1) else { continue }
                let cid = String(cString: cidPtr)
                let len = Int(sqlite3_column_bytes(stmt, 1))
                entries[cid] = Data(bytes: blobPtr, count: len)
            }
            guard !entries.isEmpty else { return nil }
            return SerializedVolume(root: root, entries: entries)
        }
    }

    /// Direct content-by-CID lookup against `cas_data` — resolves any stored
    /// node, not just volume roots.
    func fetchDataLocal(cid: String) async -> Data? {
        await connection.read {
            var stmt: OpaquePointer?
            defer { sqlite3_finalize(stmt) }
            let sql = "SELECT data FROM cas_data WHERE cid = ?1"
            guard sqlite3_prepare_v2(connection.readDb, sql, -1, &stmt, nil) == SQLITE_OK else { return nil }
            sqlite3_bind_text(stmt, 1, cid, -1, SQLITE_TRANSIENT_SHIM)
            guard sqlite3_step(stmt) == SQLITE_ROW, let blobPtr = sqlite3_column_blob(stmt, 0) else { return nil }
            let len = Int(sqlite3_column_bytes(stmt, 0))
            return Data(bytes: blobPtr, count: len)
        }
    }

    /// Store multiple serialized volumes in a single SQLite transaction.
    func storeVolumesLocal(_ volumes: [SerializedVolume]) async throws {
        guard !volumes.isEmpty else { return }
        for volume in volumes { negativeCache.recordStored(volume.root) }
        try await connection.write {
            try connection.transaction {
                for volume in volumes {
                    try upsertMetadata(root: volume.root)
                    for (cid, data) in volume.entries {
                        try upsertCASData(cid: cid, data: data)
                        try insertVolumeEntry(root: volume.root, cid: cid)
                    }
                }
            }
        }
    }

    func storeVolumeLocal(_ volume: SerializedVolume) async throws {
        negativeCache.recordStored(volume.root)
        try await connection.write {
            try connection.transaction {
                try upsertMetadata(root: volume.root)
                for (cid, data) in volume.entries {
                    try upsertCASData(cid: cid, data: data)
                    try insertVolumeEntry(root: volume.root, cid: cid)
                }
            }
        }
    }

    // MARK: - Row helpers (write connection; call inside a transaction)

    private func upsertCASData(cid: String, data: Data) throws {
        var stmt: OpaquePointer?
        defer { sqlite3_finalize(stmt) }
        let sql = "INSERT OR IGNORE INTO cas_data(cid, data) VALUES(?1, ?2)"
        guard sqlite3_prepare_v2(connection.db, sql, -1, &stmt, nil) == SQLITE_OK, let stmt else {
            throw BrokerError.sqlFailed("prepare")
        }
        sqlite3_bind_text(stmt, 1, cid, -1, SQLITE_TRANSIENT_SHIM)
        _ = data.withUnsafeBytes { buf in
            sqlite3_bind_blob(stmt, 2, buf.baseAddress, Int32(data.count), SQLITE_TRANSIENT_SHIM)
        }
        guard sqlite3_step(stmt) == SQLITE_DONE else {
            throw BrokerError.sqlFailed(String(cString: sqlite3_errmsg(connection.db)))
        }
    }

    private func insertVolumeEntry(root: String, cid: String) throws {
        try connection.execBind("INSERT OR IGNORE INTO volume_entries(root, cid) VALUES(?1, ?2)") { stmt in
            sqlite3_bind_text(stmt, 1, root, -1, SQLITE_TRANSIENT_SHIM)
            sqlite3_bind_text(stmt, 2, cid, -1, SQLITE_TRANSIENT_SHIM)
        }
    }

    private func upsertMetadata(root: String) throws {
        // INSERT OR IGNORE preserves the original stored_at for an existing
        // root; re-storing must not extend the store-then-pin grace window.
        try connection.execBind("INSERT OR IGNORE INTO volume_metadata(root) VALUES(?1)") { stmt in
            sqlite3_bind_text(stmt, 1, root, -1, SQLITE_TRANSIENT_SHIM)
        }
    }
}

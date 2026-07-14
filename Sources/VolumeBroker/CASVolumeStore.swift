import Foundation
#if canImport(SQLite3)
import SQLite3
#else
import VolumeBrokerSQLite
#endif

/// Content-addressed volume store.
///
/// Owns the `cas_data`, `volume_entries`, and `volume_metadata`
/// tables: storing serialized Volumes, fetching them back, and reporting presence.
/// Eviction and pin layers read these tables but only this layer writes them on the
/// store path.
struct CASVolumeStore {
    let connection: SQLiteConnection
    let negativeCache: NegativeCache

    func hasVolume(root: String) async -> Bool {
        await connection.read {
            if negativeCache.mightBeAbsent(root) { return false }
            var stmt: OpaquePointer?
            defer { sqlite3_finalize(stmt) }
            let sql = """
                SELECT 1
                FROM volume_metadata vm
                WHERE vm.root = ?1
                  AND EXISTS (
                      SELECT 1
                      FROM volume_entries ve
                      JOIN cas_data cd ON cd.cid = ve.cid
                      WHERE ve.root = vm.root AND ve.cid = vm.root
                  )
                  AND NOT EXISTS (
                      SELECT 1
                      FROM volume_entries ve
                      LEFT JOIN cas_data cd ON cd.cid = ve.cid
                      WHERE ve.root = vm.root AND cd.cid IS NULL
                  )
                LIMIT 1
                """
            guard sqlite3_prepare_v2(connection.readDb, sql, -1, &stmt, nil) == SQLITE_OK else { return false }
            sqlite3_bind_text(stmt, 1, root, -1, SQLITE_TRANSIENT_SHIM)
            let found = sqlite3_step(stmt) == SQLITE_ROW
            if !found { negativeCache.recordAbsent(root) }
            return found
        }
    }

    /// Uses the dedicated read-only WAL connection — concurrent with writes.
    /// Legacy/incomplete rows fail closed: a Volume is available only when its
    /// declared root entry is present in the returned complete entry set.
    func fetchVolumeLocal(root: String) async -> SerializedVolume? {
        await connection.read {
            var entryStmt: OpaquePointer?
            defer { sqlite3_finalize(entryStmt) }
            let entrySQL = """
                SELECT ve.cid, cd.data FROM volume_entries ve
                LEFT JOIN cas_data cd ON cd.cid = ve.cid
                WHERE ve.root = ?1
                  AND EXISTS (SELECT 1 FROM volume_metadata vm WHERE vm.root = ve.root)
                """
            guard sqlite3_prepare_v2(connection.readDb, entrySQL, -1, &entryStmt, nil) == SQLITE_OK else { return nil }
            sqlite3_bind_text(entryStmt, 1, root, -1, SQLITE_TRANSIENT_SHIM)

            var entries: [String: Data] = [:]
            while sqlite3_step(entryStmt) == SQLITE_ROW {
                guard let cidPtr = sqlite3_column_text(entryStmt, 0) else { continue }
                let cid = String(cString: cidPtr)
                guard sqlite3_column_type(entryStmt, 1) != SQLITE_NULL else { return nil }
                let len = Int(sqlite3_column_bytes(entryStmt, 1))
                if len == 0 {
                    entries[cid] = Data()
                } else if let blobPtr = sqlite3_column_blob(entryStmt, 1) {
                    entries[cid] = Data(bytes: blobPtr, count: len)
                }
            }
            guard entries[root] != nil else { return nil }

            return SerializedVolume(root: root, entries: entries)
        }
    }

    /// Direct content-by-CID lookup against `cas_data` — resolves any stored
    /// node, not just Volume roots.
    func fetchDataLocal(cid: String) async -> Data? {
        await connection.read {
            var stmt: OpaquePointer?
            defer { sqlite3_finalize(stmt) }
            let sql = "SELECT data FROM cas_data WHERE cid = ?1"
            guard sqlite3_prepare_v2(connection.readDb, sql, -1, &stmt, nil) == SQLITE_OK else { return nil }
            sqlite3_bind_text(stmt, 1, cid, -1, SQLITE_TRANSIENT_SHIM)
            guard sqlite3_step(stmt) == SQLITE_ROW else { return nil }
            let len = Int(sqlite3_column_bytes(stmt, 0))
            if len == 0 { return Data() }
            guard let blobPtr = sqlite3_column_blob(stmt, 0) else { return nil }
            return Data(bytes: blobPtr, count: len)
        }
    }

    /// Store multiple complete serialized Volumes in a single SQLite transaction.
    /// Validation occurs before any write, so one malformed Volume aborts the batch.
    func storeVolumesLocal(_ volumes: [SerializedVolume]) async throws {
        guard !volumes.isEmpty else { return }
        for volume in volumes { try volume.validate() }
        try await connection.write {
            try connection.transaction {
                for volume in volumes {
                    try store(volume)
                }
            }
        }
        for volume in volumes { negativeCache.recordStored(volume.root) }
    }

    func storeVolumeLocal(_ volume: SerializedVolume) async throws {
        try volume.validate()
        try await connection.write {
            try connection.transaction {
                try store(volume)
            }
        }
        negativeCache.recordStored(volume.root)
    }

    // MARK: - Row helpers (write connection; call inside a transaction)

    private func store(_ volume: SerializedVolume) throws {
        try validateExistingMembership(of: volume)
        try upsertMetadata(root: volume.root)
        for (cid, data) in volume.entries {
            try upsertCASData(cid: cid, data: data)
            try insertVolumeEntry(root: volume.root, cid: cid)
        }
    }

    private func validateExistingMembership(of volume: SerializedVolume) throws {
        var stmt: OpaquePointer?
        defer { sqlite3_finalize(stmt) }
        guard sqlite3_prepare_v2(connection.db, "SELECT cid FROM volume_entries WHERE root = ?1", -1, &stmt, nil) == SQLITE_OK,
              let stmt else {
            throw BrokerError.sqlFailed("prepare existing Volume lookup")
        }
        sqlite3_bind_text(stmt, 1, volume.root, -1, SQLITE_TRANSIENT_SHIM)

        var existing = Set<String>()
        while true {
            let result = sqlite3_step(stmt)
            if result == SQLITE_DONE { break }
            guard result == SQLITE_ROW else {
                throw BrokerError.sqlFailed(String(cString: sqlite3_errmsg(connection.db)))
            }
            guard let cid = sqlite3_column_text(stmt, 0) else {
                throw BrokerError.sqlFailed("read existing Volume membership")
            }
            existing.insert(String(cString: cid))
        }

        if !existing.isEmpty, existing != Set(volume.entries.keys) {
            throw BrokerError.conflictingVolume(volume.root)
        }
    }

    /// Insert immutable content, or verify that an existing row is byte-identical.
    /// `INSERT OR IGNORE` alone would silently hide database corruption.
    private func upsertCASData(cid: String, data: Data) throws {
        var lookup: OpaquePointer?
        defer { sqlite3_finalize(lookup) }
        guard sqlite3_prepare_v2(connection.db, "SELECT data FROM cas_data WHERE cid = ?1 LIMIT 1", -1, &lookup, nil) == SQLITE_OK,
              let lookup else {
            throw BrokerError.sqlFailed("prepare existing content lookup")
        }
        sqlite3_bind_text(lookup, 1, cid, -1, SQLITE_TRANSIENT_SHIM)
        if sqlite3_step(lookup) == SQLITE_ROW {
            let len = Int(sqlite3_column_bytes(lookup, 0))
            let existing: Data
            if len == 0 {
                existing = Data()
            } else if let ptr = sqlite3_column_blob(lookup, 0) {
                existing = Data(bytes: ptr, count: len)
            } else {
                throw BrokerError.sqlFailed("read existing content")
            }
            guard existing == data else { throw BrokerError.conflictingContent(cid) }
            return
        }

        var stmt: OpaquePointer?
        defer { sqlite3_finalize(stmt) }
        let sql = "INSERT INTO cas_data(cid, data) VALUES(?1, ?2)"
        guard sqlite3_prepare_v2(connection.db, sql, -1, &stmt, nil) == SQLITE_OK, let stmt else {
            throw BrokerError.sqlFailed("prepare content insert")
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

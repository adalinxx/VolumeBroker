import Foundation
#if canImport(SQLite3)
import SQLite3
#else
import VolumeBrokerSQLite
#endif

/// Content-addressed storage for complete published Volumes.
struct CASVolumeStore {
    let connection: SQLiteConnection

    /// Every durable read uses this predicate. A manifest is complete only when
    /// its declared count matches both membership and owned CAS rows and the
    /// root itself is one of those rows.
    static let completeVolumePredicate = """
        typeof(vm.entry_count) = 'integer'
        AND vm.entry_count > 0
        AND vm.entry_count = (
            SELECT COUNT(*) FROM volume_entries manifest_members
            WHERE manifest_members.root = vm.root
        )
        AND vm.entry_count = (
            SELECT COUNT(*)
            FROM volume_entries owned_members
            JOIN cas_data owned_data ON owned_data.cid = owned_members.cid
            WHERE owned_members.root = vm.root AND owned_data.data IS NOT NULL
        )
        AND EXISTS (
            SELECT 1
            FROM volume_entries root_member
            JOIN cas_data root_data ON root_data.cid = root_member.cid
            WHERE root_member.root = vm.root
              AND root_member.cid = vm.root
              AND root_data.data IS NOT NULL
        )
        """

    func hasVolume(root: String) async -> Bool {
        await fetchVolumeLocal(root: root) != nil
    }

    func fetchVolumeLocal(root: String) async -> SerializedVolume? {
        let loaded = await connection.read {
            try? Self.loadValidatedVolume(root: root, db: connection.readDb)
        }
        switch loaded {
        case .volume(let volume):
            return volume
        case .invalid:
            await quarantineInvalidVolume(root: root)
            return nil
        case .missing, nil:
            return nil
        }
    }

    enum ValidatedVolumeLoad: Sendable {
        case missing
        case invalid
        case volume(SerializedVolume)
    }

    static func loadValidatedVolume(root: String, db: OpaquePointer) throws -> ValidatedVolumeLoad {
        var stmt: OpaquePointer?
        defer { sqlite3_finalize(stmt) }
        let sql = """
            SELECT vm.entry_count, ve.cid, cd.data
            FROM volume_metadata vm
            LEFT JOIN volume_entries ve ON ve.root = vm.root
            LEFT JOIN cas_data cd ON cd.cid = ve.cid
            WHERE vm.root = ?1
            """
        guard sqlite3_prepare_v2(db, sql, -1, &stmt, nil) == SQLITE_OK,
              let stmt else {
            throw BrokerError.sqlFailed(String(cString: sqlite3_errmsg(db)))
        }
        sqlite3_bind_text(stmt, 1, root, -1, SQLITE_TRANSIENT_SHIM)

        var entries: [String: Data] = [:]
        var expectedCount: Int?
        while true {
            let result = sqlite3_step(stmt)
            if result == SQLITE_DONE { break }
            guard result == SQLITE_ROW else {
                throw BrokerError.sqlFailed(String(cString: sqlite3_errmsg(db)))
            }
            guard sqlite3_column_type(stmt, 0) == SQLITE_INTEGER else { return .invalid }
            expectedCount = Int(sqlite3_column_int64(stmt, 0))
            guard let cidPtr = sqlite3_column_text(stmt, 1),
                  sqlite3_column_type(stmt, 2) != SQLITE_NULL else { return .invalid }
            let cid = String(cString: cidPtr)
            let length = Int(sqlite3_column_bytes(stmt, 2))
            if length == 0 {
                entries[cid] = Data()
            } else if let bytes = sqlite3_column_blob(stmt, 2) {
                entries[cid] = Data(bytes: bytes, count: length)
            } else {
                return .invalid
            }
        }
        guard let expectedCount else { return .missing }
        guard expectedCount > 0, entries.count == expectedCount else { return .invalid }
        let volume = SerializedVolume(root: root, entries: entries)
        guard (try? volume.validate()) != nil else { return .invalid }
        return .volume(volume)
    }

    private func quarantineInvalidVolume(root: String) async {
        try? await connection.write {
            try connection.transaction {
                guard case .invalid = try Self.loadValidatedVolume(root: root, db: connection.db) else {
                    return
                }
                try connection.execBind("DELETE FROM volume_metadata WHERE root=?1") { stmt in
                    sqlite3_bind_text(stmt, 1, root, -1, SQLITE_TRANSIENT_SHIM)
                }
                try connection.exec("""
                    DELETE FROM cas_data
                    WHERE NOT EXISTS (
                        SELECT 1 FROM volume_entries ve WHERE ve.cid = cas_data.cid
                    )
                    """)
            }
        }
    }

    /// A CAS row is readable only through at least one complete owning Volume.
    func fetchDataLocal(cid: String) async -> Data? {
        let ownerRoots: [String] = await connection.read {
            var stmt: OpaquePointer?
            defer { sqlite3_finalize(stmt) }
            let sql = """
                SELECT vm.root
                FROM volume_entries owner
                JOIN volume_metadata vm ON vm.root = owner.root
                WHERE owner.cid = ?1
                  AND \(Self.completeVolumePredicate)
                """
            guard sqlite3_prepare_v2(connection.readDb, sql, -1, &stmt, nil) == SQLITE_OK,
                  let stmt else { return [] }
            sqlite3_bind_text(stmt, 1, cid, -1, SQLITE_TRANSIENT_SHIM)
            var roots: [String] = []
            while true {
                let result = sqlite3_step(stmt)
                if result == SQLITE_DONE { return roots }
                guard result == SQLITE_ROW,
                      let root = sqlite3_column_text(stmt, 0) else { return [] }
                roots.append(String(cString: root))
            }
        }

        for root in ownerRoots {
            if let volume = await fetchVolumeLocal(root: root),
               let data = volume.entries[cid] {
                return data
            }
        }
        return nil
    }

    func storeVolumeLocal(_ volume: SerializedVolume) async throws {
        try await storeVolumesLocal([volume])
    }

    func storeVolumesLocal(_ volumes: [SerializedVolume]) async throws {
        let batch = try Self.validateBatch(volumes)
        guard !batch.volumes.isEmpty else { return }

        try await connection.write {
            try connection.transaction {
                for volume in batch.volumes {
                    try validateExistingMembership(of: volume)
                }
                for (cid, data) in batch.contentByCID {
                    try validateExistingContent(cid: cid, data: data)
                }

                for volume in batch.volumes {
                    try insertMetadata(root: volume.root, entryCount: volume.entries.count)
                }
                for (cid, data) in batch.contentByCID {
                    try insertCASData(cid: cid, data: data)
                }
                for volume in batch.volumes {
                    for cid in volume.entries.keys {
                        try insertVolumeEntry(root: volume.root, cid: cid)
                    }
                }
                for volume in batch.volumes {
                    try verifyComplete(root: volume.root, entryCount: volume.entries.count)
                }
            }
        }
    }

    private struct ValidatedBatch: Sendable {
        let volumes: [SerializedVolume]
        let contentByCID: [String: Data]
    }

    private static func validateBatch(_ submitted: [SerializedVolume]) throws -> ValidatedBatch {
        var memberships: [String: Set<String>] = [:]
        var contentByCID: [String: Data] = [:]
        var volumes: [SerializedVolume] = []

        for submittedVolume in submitted {
            let volume = submittedVolume.ownedCopy()
            try volume.validate()
            let members = Set(volume.entries.keys)
            if let existing = memberships[volume.root] {
                guard existing == members else {
                    throw BrokerError.conflictingVolume(volume.root)
                }
            } else {
                memberships[volume.root] = members
                volumes.append(volume)
            }

            for (cid, data) in volume.entries {
                if let existing = contentByCID[cid], existing != data {
                    throw BrokerError.conflictingContent(cid)
                }
                contentByCID[cid] = data
            }
        }
        return ValidatedBatch(volumes: volumes, contentByCID: contentByCID)
    }

    // MARK: - Transaction helpers

    private func validateExistingMembership(of volume: SerializedVolume) throws {
        var metadataStmt: OpaquePointer?
        defer { sqlite3_finalize(metadataStmt) }
        guard sqlite3_prepare_v2(
            connection.db,
            "SELECT entry_count FROM volume_metadata WHERE root = ?1",
            -1,
            &metadataStmt,
            nil
        ) == SQLITE_OK, let metadataStmt else {
            throw BrokerError.sqlFailed(String(cString: sqlite3_errmsg(connection.db)))
        }
        sqlite3_bind_text(metadataStmt, 1, volume.root, -1, SQLITE_TRANSIENT_SHIM)

        let metadataResult = sqlite3_step(metadataStmt)
        let existingEntryCount: Int?
        if metadataResult == SQLITE_ROW {
            guard sqlite3_column_type(metadataStmt, 0) == SQLITE_INTEGER else {
                throw BrokerError.conflictingVolume(volume.root)
            }
            existingEntryCount = Int(sqlite3_column_int64(metadataStmt, 0))
        } else if metadataResult == SQLITE_DONE {
            existingEntryCount = nil
        } else {
            throw BrokerError.sqlFailed(String(cString: sqlite3_errmsg(connection.db)))
        }

        var membersStmt: OpaquePointer?
        defer { sqlite3_finalize(membersStmt) }
        guard sqlite3_prepare_v2(
            connection.db,
            "SELECT cid FROM volume_entries WHERE root = ?1",
            -1,
            &membersStmt,
            nil
        ) == SQLITE_OK, let membersStmt else {
            throw BrokerError.sqlFailed(String(cString: sqlite3_errmsg(connection.db)))
        }
        sqlite3_bind_text(membersStmt, 1, volume.root, -1, SQLITE_TRANSIENT_SHIM)

        var existingMembers = Set<String>()
        while true {
            let result = sqlite3_step(membersStmt)
            if result == SQLITE_DONE { break }
            guard result == SQLITE_ROW, let cid = sqlite3_column_text(membersStmt, 0) else {
                throw BrokerError.sqlFailed(String(cString: sqlite3_errmsg(connection.db)))
            }
            existingMembers.insert(String(cString: cid))
        }

        let expectedMembers = Set(volume.entries.keys)
        if let existingEntryCount {
            guard existingEntryCount == expectedMembers.count,
                  existingMembers == expectedMembers else {
                throw BrokerError.conflictingVolume(volume.root)
            }
        } else if !existingMembers.isEmpty {
            throw BrokerError.conflictingVolume(volume.root)
        }
    }

    private func validateExistingContent(cid: String, data: Data) throws {
        var stmt: OpaquePointer?
        defer { sqlite3_finalize(stmt) }
        guard sqlite3_prepare_v2(
            connection.db,
            "SELECT data FROM cas_data WHERE cid = ?1",
            -1,
            &stmt,
            nil
        ) == SQLITE_OK, let stmt else {
            throw BrokerError.sqlFailed(String(cString: sqlite3_errmsg(connection.db)))
        }
        sqlite3_bind_text(stmt, 1, cid, -1, SQLITE_TRANSIENT_SHIM)

        let result = sqlite3_step(stmt)
        if result == SQLITE_DONE { return }
        guard result == SQLITE_ROW else {
            throw BrokerError.sqlFailed(String(cString: sqlite3_errmsg(connection.db)))
        }
        guard sqlite3_column_type(stmt, 0) != SQLITE_NULL else {
            throw BrokerError.sqlFailed("read existing content")
        }

        let length = Int(sqlite3_column_bytes(stmt, 0))
        let existing: Data
        if length == 0 {
            existing = Data()
        } else if let bytes = sqlite3_column_blob(stmt, 0) {
            existing = Data(bytes: bytes, count: length)
        } else {
            throw BrokerError.sqlFailed("read existing content")
        }
        guard existing == data else { throw BrokerError.conflictingContent(cid) }
    }

    private func insertMetadata(root: String, entryCount: Int) throws {
        try connection.execBind(
            "INSERT OR IGNORE INTO volume_metadata(root, entry_count) VALUES(?1, ?2)"
        ) { stmt in
            sqlite3_bind_text(stmt, 1, root, -1, SQLITE_TRANSIENT_SHIM)
            sqlite3_bind_int64(stmt, 2, Int64(entryCount))
        }
    }

    private func insertCASData(cid: String, data: Data) throws {
        try connection.execBind("INSERT OR IGNORE INTO cas_data(cid, data) VALUES(?1, ?2)") { stmt in
            sqlite3_bind_text(stmt, 1, cid, -1, SQLITE_TRANSIENT_SHIM)
            if data.isEmpty {
                sqlite3_bind_zeroblob(stmt, 2, 0)
            } else {
                _ = data.withUnsafeBytes { bytes in
                    sqlite3_bind_blob(stmt, 2, bytes.baseAddress, Int32(data.count), SQLITE_TRANSIENT_SHIM)
                }
            }
        }
    }

    private func insertVolumeEntry(root: String, cid: String) throws {
        try connection.execBind(
            "INSERT OR IGNORE INTO volume_entries(root, cid) VALUES(?1, ?2)"
        ) { stmt in
            sqlite3_bind_text(stmt, 1, root, -1, SQLITE_TRANSIENT_SHIM)
            sqlite3_bind_text(stmt, 2, cid, -1, SQLITE_TRANSIENT_SHIM)
        }
    }

    private func verifyComplete(root: String, entryCount: Int) throws {
        var stmt: OpaquePointer?
        defer { sqlite3_finalize(stmt) }
        let sql = """
            SELECT 1
            FROM volume_metadata vm
            WHERE vm.root = ?1
              AND vm.entry_count = ?2
              AND \(Self.completeVolumePredicate)
            LIMIT 1
            """
        guard sqlite3_prepare_v2(connection.db, sql, -1, &stmt, nil) == SQLITE_OK,
              let stmt else {
            throw BrokerError.sqlFailed(String(cString: sqlite3_errmsg(connection.db)))
        }
        sqlite3_bind_text(stmt, 1, root, -1, SQLITE_TRANSIENT_SHIM)
        sqlite3_bind_int64(stmt, 2, Int64(entryCount))
        let result = sqlite3_step(stmt)
        if result == SQLITE_ROW { return }
        if result == SQLITE_DONE { throw BrokerError.conflictingVolume(root) }
        throw BrokerError.sqlFailed(String(cString: sqlite3_errmsg(connection.db)))
    }
}

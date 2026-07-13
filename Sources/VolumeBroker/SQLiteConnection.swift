import Foundation
#if canImport(SQLite3)
import SQLite3
#else
import VolumeBrokerSQLite
#endif

let SQLITE_TRANSIENT_SHIM = unsafeBitCast(-1, to: sqlite3_destructor_type.self)

/// Low-level SQLite connection layer.
///
/// Owns the write and read connections, PRAGMA/schema setup, the read/write
/// dispatch queues that serialise SQL access, and the raw `exec`/`execBind`
/// primitives the higher layers build on. Higher layers (CAS store, pin index,
/// eviction, metadata) compose this type and never open SQLite handles directly.
final class SQLiteConnection: @unchecked Sendable {
    private let readQueue = DispatchQueue(label: "VolumeBroker.DiskBroker.read", qos: .utility, attributes: .concurrent)
    private let writeQueue = DispatchQueue(label: "VolumeBroker.DiskBroker.write", qos: .utility)

    /// Write connection — SQLITE_OPEN_FULLMUTEX serialises all SQL calls.
    let db: OpaquePointer
    /// Separate read-only WAL connection — concurrent reads alongside writes.
    let readDb: OpaquePointer

    init(path: String) throws {
        var handle: OpaquePointer?
        // FULLMUTEX: serialize all SQLite calls on this connection so that
        // the actor's task-to-thread migrations under Swift concurrency cannot
        // race with deinit's sqlite3_close, which runs outside actor isolation.
        let flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_FULLMUTEX
        guard sqlite3_open_v2(path, &handle, flags, nil) == SQLITE_OK, let handle else {
            let msg = handle.flatMap { String(cString: sqlite3_errmsg($0)) } ?? "unknown"
            if let handle { sqlite3_close(handle) }
            throw BrokerError.openFailed(msg)
        }
        self.db = handle
        try Self.execRaw(db: handle, "PRAGMA journal_mode=WAL")
        // Retry on lock contention instead of failing immediately. Without this,
        // a read that races a write/checkpoint (e.g. the eviction sweep's
        // wal_checkpoint(TRUNCATE), which briefly excludes readers) gets
        // SQLITE_BUSY and the read returns nil — a transient "not found" for
        // content that is durably present and pinned. A bounded busy timeout makes
        // both connections wait out the contention window and read the real row.
        try Self.execRaw(db: handle, "PRAGMA busy_timeout=5000")
        try Self.execRaw(db: handle, "PRAGMA synchronous=NORMAL")
        try Self.execRaw(db: handle, "PRAGMA cache_size=-65536")    // 64MB page cache
        try Self.execRaw(db: handle, "PRAGMA mmap_size=268435456")  // 256MB memory-mapped I/O
        try Self.execRaw(db: handle, "PRAGMA temp_store=MEMORY")    // temp tables in RAM
        try Self.execRaw(db: handle, "PRAGMA auto_vacuum=INCREMENTAL") // reclaim evicted space
        try Self.createTables(db: handle)

        // Separate read-only connection so fetchVolumeLocal can run concurrently
        // with write transactions in WAL mode without blocking.
        var rhandle: OpaquePointer?
        let rflags = SQLITE_OPEN_READONLY | SQLITE_OPEN_FULLMUTEX
        if sqlite3_open_v2(path, &rhandle, rflags, nil) == SQLITE_OK, let rhandle {
            try? Self.execRaw(db: rhandle, "PRAGMA journal_mode=WAL")
            // See above: the read connection must also wait out checkpoint/write
            // contention rather than return SQLITE_BUSY (→ nil → spurious miss).
            try? Self.execRaw(db: rhandle, "PRAGMA busy_timeout=5000")
            try? Self.execRaw(db: rhandle, "PRAGMA cache_size=-16384")
            self.readDb = rhandle
        } else {
            // Fallback to the write connection if read-only open fails
            self.readDb = handle
        }
    }

    deinit {
        if readDb != db { sqlite3_close(readDb) }
        sqlite3_close(db)
    }

    // MARK: - Serialised access

    /// Run a read on the concurrent read queue against the read-only connection.
    func read<T: Sendable>(_ body: @escaping @Sendable () -> T) async -> T {
        await withCheckedContinuation { continuation in
            readQueue.async {
                continuation.resume(returning: body())
            }
        }
    }

    /// Run a write on the serial write queue against the write connection.
    func write<T: Sendable>(_ body: @escaping @Sendable () throws -> T) async throws -> T {
        try await withCheckedThrowingContinuation { continuation in
            writeQueue.async {
                do {
                    continuation.resume(returning: try body())
                } catch {
                    continuation.resume(throwing: error)
                }
            }
        }
    }

    func checkpoint() async {
        _ = try? await write { [self] in
            sqlite3_wal_checkpoint_v2(db, nil, SQLITE_CHECKPOINT_TRUNCATE, nil, nil)
        }
    }

    // MARK: - Primitives (write connection)

    /// Execute a statement-less SQL string on the write connection.
    func exec(_ sql: String) throws {
        guard sqlite3_exec(db, sql, nil, nil, nil) == SQLITE_OK else {
            throw BrokerError.sqlFailed(String(cString: sqlite3_errmsg(db)))
        }
    }

    /// Prepare, bind, and step a single write statement to completion.
    func execBind(_ sql: String, bind: (OpaquePointer) -> Void) throws {
        var stmt: OpaquePointer?
        defer { sqlite3_finalize(stmt) }
        guard sqlite3_prepare_v2(db, sql, -1, &stmt, nil) == SQLITE_OK, let stmt else {
            throw BrokerError.sqlFailed(String(cString: sqlite3_errmsg(db)))
        }
        bind(stmt)
        guard sqlite3_step(stmt) == SQLITE_DONE else {
            throw BrokerError.sqlFailed(String(cString: sqlite3_errmsg(db)))
        }
    }

    /// Run `body` inside a BEGIN IMMEDIATE / COMMIT transaction on the write
    /// connection, rolling back on any thrown error.
    func transaction<T>(_ body: () throws -> T) throws -> T {
        try exec("BEGIN IMMEDIATE")
        do {
            let result = try body()
            try exec("COMMIT")
            return result
        } catch {
            try? exec("ROLLBACK")
            throw error
        }
    }

    /// Number of rows changed by the most recent write statement.
    func changes() -> Int { Int(sqlite3_changes(db)) }

    // MARK: - Schema

    static func execRaw(db: OpaquePointer, _ sql: String) throws {
        guard sqlite3_exec(db, sql, nil, nil, nil) == SQLITE_OK else {
            throw BrokerError.sqlFailed(String(cString: sqlite3_errmsg(db)))
        }
    }

    private static func createTables(db: OpaquePointer) throws {
        try execRaw(db: db, """
            CREATE TABLE IF NOT EXISTS cas_data (
                cid TEXT PRIMARY KEY,
                data BLOB NOT NULL
            )
            """)
        try execRaw(db: db, """
            CREATE TABLE IF NOT EXISTS volume_entries (
                root TEXT NOT NULL,
                cid TEXT NOT NULL,
                PRIMARY KEY (root, cid)
            )
            """)
        try execRaw(db: db, """
            CREATE TABLE IF NOT EXISTS volume_edges (
                parent_root TEXT NOT NULL,
                child_root TEXT NOT NULL,
                PRIMARY KEY (parent_root, child_root)
            )
            """)
        try execRaw(db: db, """
            CREATE TABLE IF NOT EXISTS volume_pins (
                root TEXT NOT NULL,
                owner TEXT NOT NULL,
                count INTEGER NOT NULL DEFAULT 1,
                expires_at TEXT,
                PRIMARY KEY (root, owner)
            )
            """)
        try? execRaw(db: db, "ALTER TABLE volume_pins ADD COLUMN count INTEGER NOT NULL DEFAULT 1")
        try execRaw(db: db, """
            CREATE TABLE IF NOT EXISTS volume_unpin_operations (
                operation_id TEXT PRIMARY KEY
            )
            """)
        try execRaw(db: db, """
            CREATE TABLE IF NOT EXISTS volume_metadata (
                root TEXT PRIMARY KEY,
                stored_at TEXT NOT NULL DEFAULT (datetime('now'))
            )
            """)
        try execRaw(db: db, """
            CREATE TABLE IF NOT EXISTS retained_roots (
                scope TEXT NOT NULL,
                root TEXT NOT NULL,
                PRIMARY KEY (scope, root)
            )
            """)
        try execRaw(db: db, """
            CREATE TABLE IF NOT EXISTS retained_root_operations (
                operation_id TEXT PRIMARY KEY,
                scope TEXT NOT NULL,
                canonical_roots TEXT NOT NULL
            )
            """)
        try execRaw(db: db, "CREATE INDEX IF NOT EXISTS idx_ve_cid ON volume_entries(cid)")
        try execRaw(db: db, "CREATE INDEX IF NOT EXISTS idx_ve_root ON volume_entries(root)")
        try execRaw(db: db, "CREATE INDEX IF NOT EXISTS idx_volume_edges_child ON volume_edges(child_root)")
        try execRaw(db: db, "CREATE INDEX IF NOT EXISTS idx_volume_edges_parent ON volume_edges(parent_root)")
        try execRaw(db: db, "CREATE INDEX IF NOT EXISTS idx_retained_roots_root ON retained_roots(root)")
        // P-603: index owner for unpinAll/unpinAllBatch DELETE WHERE owner=? (was full-table scan)
        try execRaw(db: db, "CREATE INDEX IF NOT EXISTS idx_vp_owner ON volume_pins(owner)")
        // Owner-scoped root lookups power chain-local reannounce without scanning all pins.
        try execRaw(db: db, "CREATE INDEX IF NOT EXISTS idx_vp_owner_expires_root ON volume_pins(owner, expires_at, root)")
        // P-606: index expires_at for pinnedRoots() partial-index scan
        try execRaw(db: db, "CREATE INDEX IF NOT EXISTS idx_vp_expires ON volume_pins(expires_at)")
        try execRaw(db: db, """
            CREATE TABLE IF NOT EXISTS chain_meta (
                key   TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
            """)
    }
}

extension SQLiteConnection {
    /// Shared ISO8601 formatter for `expires_at` timestamps.
    static nonisolated(unsafe) let isoFormatter: ISO8601DateFormatter = {
        let f = ISO8601DateFormatter()
        f.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
        return f
    }()
}

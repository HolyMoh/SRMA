"""
Full-Text Acquisition System — Database Layer

SQLite database with WAL mode, serialized write queue, and atomic
state machine transitions. All tables, CRUD operations, and reporting
queries for the acquisition pipeline.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import traceback
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Coroutine, Dict, List, Optional, Set, Tuple

import aiosqlite

from full_text_acquisition.models import (
    CURRENT_CONFIG_VERSION,
    DEFAULT_CONFIG,
    VALID_TRANSITIONS,
    AuditLogEntry,
    ApiCacheEntry,
    ColumnMapping,
    ConfidenceLevel,
    ContentDriftStatus,
    ConfigSnapshot,
    DeduplicationMatch,
    DeduplicationReport,
    FailureCode,
    HealthMetrics,
    IdentityStatus,
    IntegrationExport,
    Paper,
    PaperRun,
    PaperState,
    PrismaReport,
    PublisherCooldown,
    PublisherEnum,
    RunRecord,
    RunStatus,
    SupplementFile,
    TierEnum,
    ValidationStatus,
    VersionType,
    calculate_integrity_score,
    normalize_doi,
    validate_state_transition,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# SQL Schema Definitions
# ---------------------------------------------------------------------------

SQL_CREATE_PAPERS = """
CREATE TABLE IF NOT EXISTS papers (
    canonical_id        TEXT PRIMARY KEY,
    doi                 TEXT,
    pmid                TEXT,
    openalex_id         TEXT,
    title_hash          TEXT,
    title               TEXT NOT NULL DEFAULT '',
    authors             TEXT NOT NULL DEFAULT '',
    first_author_lastname TEXT NOT NULL DEFAULT '',
    year                INTEGER,
    journal             TEXT,
    state               TEXT NOT NULL DEFAULT 'INGESTED',
    previous_state      TEXT,
    publisher           TEXT NOT NULL DEFAULT 'OTHER',
    publisher_signal_source TEXT,
    retrieval_tier      TEXT,
    retrieval_method    TEXT,
    retrieval_url       TEXT,
    attempt_count       INTEGER NOT NULL DEFAULT 0,
    last_failure_code   TEXT,
    pdf_path            TEXT,
    pdf_filename        TEXT,
    sha256_checksum     TEXT,
    pdf_size_bytes      INTEGER,
    pdf_page_count      INTEGER,
    validation_status   TEXT,
    identity_status     TEXT,
    version_type        TEXT NOT NULL DEFAULT 'UNKNOWN',
    is_primary          INTEGER NOT NULL DEFAULT 1,
    integrity_score     INTEGER,
    confidence_level    TEXT,
    ocr_applied         INTEGER NOT NULL DEFAULT 0,
    ocr_text_extracted  INTEGER NOT NULL DEFAULT 0,
    content_drift_status TEXT,
    content_drift_version INTEGER NOT NULL DEFAULT 1,
    supplement_count    INTEGER NOT NULL DEFAULT 0,
    user_override       TEXT,
    override_reason     TEXT,
    override_timestamp  TEXT,
    worker_id           TEXT,
    claimed_at          TEXT,
    run_id              TEXT,
    run_id_of_first_success TEXT,
    created_at          TEXT NOT NULL,
    updated_at          TEXT,
    raw_doi             TEXT,
    raw_title           TEXT,
    raw_authors         TEXT,
    enrichment_source   TEXT,
    enriched_fields     TEXT
);
"""

SQL_CREATE_PAPERS_INDEXES = """
CREATE INDEX IF NOT EXISTS idx_papers_state ON papers(state);
CREATE INDEX IF NOT EXISTS idx_papers_doi ON papers(doi);
CREATE INDEX IF NOT EXISTS idx_papers_pmid ON papers(pmid);
CREATE INDEX IF NOT EXISTS idx_papers_run_id ON papers(run_id);
CREATE INDEX IF NOT EXISTS idx_papers_publisher ON papers(publisher);
CREATE INDEX IF NOT EXISTS idx_papers_validation_status ON papers(validation_status);
CREATE INDEX IF NOT EXISTS idx_papers_state_claimed ON papers(state, claimed_at);
"""

SQL_CREATE_RUNS = """
CREATE TABLE IF NOT EXISTS runs (
    run_id              TEXT PRIMARY KEY,
    started_at          TEXT NOT NULL,
    completed_at        TEXT,
    status              TEXT NOT NULL DEFAULT 'RUNNING',
    total_submitted     INTEGER NOT NULL DEFAULT 0,
    config_snapshot     TEXT
);
"""

SQL_CREATE_PAPER_RUNS = """
CREATE TABLE IF NOT EXISTS paper_runs (
    canonical_id                TEXT NOT NULL,
    run_id                      TEXT NOT NULL,
    submitted_in_this_run       INTEGER NOT NULL DEFAULT 1,
    retrieval_attempted_in_this_run INTEGER NOT NULL DEFAULT 0,
    outcome_in_this_run         TEXT,
    PRIMARY KEY (canonical_id, run_id),
    FOREIGN KEY (canonical_id) REFERENCES papers(canonical_id),
    FOREIGN KEY (run_id) REFERENCES runs(run_id)
);
"""

SQL_CREATE_PAPER_RUNS_INDEXES = """
CREATE INDEX IF NOT EXISTS idx_paper_runs_run_id ON paper_runs(run_id);
CREATE INDEX IF NOT EXISTS idx_paper_runs_outcome ON paper_runs(outcome_in_this_run);
"""

SQL_CREATE_PUBLISHER_COOLDOWNS = """
CREATE TABLE IF NOT EXISTS publisher_cooldowns (
    publisher           TEXT PRIMARY KEY,
    failure_count       INTEGER NOT NULL DEFAULT 0,
    success_count       INTEGER NOT NULL DEFAULT 0,
    failure_rate        REAL NOT NULL DEFAULT 0.0,
    cooldown_until      TEXT,
    extended_count      INTEGER NOT NULL DEFAULT 0,
    last_updated        TEXT NOT NULL
);
"""

SQL_CREATE_API_CACHE = """
CREATE TABLE IF NOT EXISTS api_cache (
    doi                 TEXT NOT NULL,
    api_name            TEXT NOT NULL,
    response_json       TEXT NOT NULL,
    cached_at           TEXT NOT NULL,
    expires_at          TEXT,
    PRIMARY KEY (doi, api_name)
);
"""

SQL_CREATE_API_CACHE_INDEXES = """
CREATE INDEX IF NOT EXISTS idx_api_cache_expires ON api_cache(expires_at);
"""

SQL_CREATE_AUDIT_LOG = """
CREATE TABLE IF NOT EXISTS audit_log (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp           TEXT NOT NULL,
    canonical_id        TEXT,
    run_id              TEXT,
    tier                TEXT,
    method              TEXT,
    url_attempted       TEXT,
    http_status         INTEGER,
    content_type_received TEXT,
    outcome             TEXT,
    failure_code        TEXT,
    execution_time_ms   REAL,
    retry_count         INTEGER NOT NULL DEFAULT 0,
    cache_hit           INTEGER NOT NULL DEFAULT 0,
    details             TEXT,
    exception_type      TEXT,
    exception_message   TEXT,
    exception_traceback TEXT
);
"""

SQL_CREATE_AUDIT_LOG_INDEXES = """
CREATE INDEX IF NOT EXISTS idx_audit_canonical ON audit_log(canonical_id);
CREATE INDEX IF NOT EXISTS idx_audit_run_id ON audit_log(run_id);
CREATE INDEX IF NOT EXISTS idx_audit_timestamp ON audit_log(timestamp);
CREATE INDEX IF NOT EXISTS idx_audit_failure_code ON audit_log(failure_code);
"""

SQL_CREATE_SUPPLEMENTS = """
CREATE TABLE IF NOT EXISTS supplements (
    canonical_id        TEXT NOT NULL,
    supplement_index    INTEGER NOT NULL,
    filename            TEXT NOT NULL,
    file_path           TEXT NOT NULL,
    file_extension      TEXT NOT NULL,
    file_size_bytes     INTEGER,
    sha256_checksum     TEXT,
    validation_status   TEXT,
    source_url          TEXT,
    downloaded_at       TEXT NOT NULL,
    PRIMARY KEY (canonical_id, supplement_index),
    FOREIGN KEY (canonical_id) REFERENCES papers(canonical_id)
);
"""

ALL_SCHEMA_SQL: List[str] = [
    SQL_CREATE_PAPERS,
    SQL_CREATE_PAPERS_INDEXES,
    SQL_CREATE_RUNS,
    SQL_CREATE_PAPER_RUNS,
    SQL_CREATE_PAPER_RUNS_INDEXES,
    SQL_CREATE_PUBLISHER_COOLDOWNS,
    SQL_CREATE_API_CACHE,
    SQL_CREATE_API_CACHE_INDEXES,
    SQL_CREATE_AUDIT_LOG,
    SQL_CREATE_AUDIT_LOG_INDEXES,
    SQL_CREATE_SUPPLEMENTS,
]


# ---------------------------------------------------------------------------
# Write queue sentinel
# ---------------------------------------------------------------------------
_STOP_SENTINEL = object()


class Database:
    """Async SQLite database with WAL mode and serialized write queue.

    Design principles:
        - WAL mode + foreign_keys ON set on every connection open
        - All writes go through a single asyncio.Queue worker coroutine
        - Reads may occur freely from any coroutine
        - State transitions are atomic SQL transactions
        - Task claiming uses database-level atomicity (no in-memory locks)
    """

    def __init__(self, db_path: str) -> None:
        self._db_path: str = db_path
        self._write_conn: Optional[aiosqlite.Connection] = None
        self._write_queue: asyncio.Queue[
            Tuple[
                Callable[..., Coroutine[Any, Any, Any]],
                Tuple[Any, ...],
                asyncio.Future[Any],
            ]
            | object
        ] = asyncio.Queue()
        self._write_worker_task: Optional[asyncio.Task[None]] = None
        self._initialized: bool = False

    async def initialize(self) -> None:
        """Open connections, set pragmas, create schema, start write worker.

        Must be called once before any database operations.
        """
        if self._initialized:
            return

        # Ensure parent directory exists
        db_dir = os.path.dirname(self._db_path)
        if db_dir:
            os.makedirs(db_dir, exist_ok=True)

        # Open dedicated write connection
        self._write_conn = await aiosqlite.connect(self._db_path)
        await self._set_pragmas(self._write_conn)

        # Create all tables
        await self._create_schema()

        # Start write worker
        self._write_worker_task = asyncio.create_task(
            self._write_worker(), name="db-write-worker"
        )

        self._initialized = True
        logger.info("Database initialized: %s", self._db_path)

    async def _set_pragmas(self, conn: aiosqlite.Connection) -> None:
        """Set WAL mode and foreign keys on a connection."""
        await conn.execute("PRAGMA journal_mode=WAL")
        await conn.execute("PRAGMA foreign_keys=ON")

    async def _create_schema(self) -> None:
        """Create all tables and indexes if they don't exist."""
        if self._write_conn is None:
            raise RuntimeError("Database not initialized")
        for sql_block in ALL_SCHEMA_SQL:
            # Each block may contain multiple statements separated by ;
            for statement in sql_block.strip().split(";"):
                statement = statement.strip()
                if statement:
                    await self._write_conn.execute(statement)
        await self._write_conn.commit()
        logger.info("Database schema verified")

    async def _write_worker(self) -> None:
        """Consume write operations from the queue sequentially.

        Each item is a tuple of (coroutine_factory, args, future).
        The coroutine receives the write connection and args, and its
        return value is set on the future.
        """
        while True:
            item = await self._write_queue.get()
            if item is _STOP_SENTINEL:
                self._write_queue.task_done()
                break

            coro_factory, args, future = item
            try:
                result = await coro_factory(self._write_conn, *args)
                if not future.cancelled():
                    future.set_result(result)
            except Exception as exc:
                logger.error(
                    "Write queue error: %s: %s\n%s",
                    type(exc).__name__,
                    exc,
                    traceback.format_exc(),
                )
                if not future.cancelled():
                    future.set_exception(exc)
            finally:
                self._write_queue.task_done()

    async def _enqueue_write(
        self,
        coro_factory: Callable[..., Coroutine[Any, Any, Any]],
        *args: Any,
    ) -> Any:
        """Enqueue a write operation and wait for its result.

        The coro_factory receives (connection, *args) and is executed
        by the write worker. Returns whatever the coro_factory returns.
        """
        if not self._initialized:
            raise RuntimeError("Database not initialized — call initialize() first")
        loop = asyncio.get_running_loop()
        future: asyncio.Future[Any] = loop.create_future()
        await self._write_queue.put((coro_factory, args, future))
        return await future

    async def _read_connection(self) -> aiosqlite.Connection:
        """Open a fresh read connection with pragmas set.

        Callers must close the connection when done.
        """
        conn = await aiosqlite.connect(self._db_path)
        await self._set_pragmas(conn)
        conn.row_factory = aiosqlite.Row
        return conn

    async def read_one(
        self, sql: str, params: Tuple[Any, ...] = ()
    ) -> Optional[aiosqlite.Row]:
        """Execute a read query and return the first row or None."""
        conn = await self._read_connection()
        try:
            cursor = await conn.execute(sql, params)
            row = await cursor.fetchone()
            return row
        finally:
            await conn.close()

    async def read_all(
        self, sql: str, params: Tuple[Any, ...] = ()
    ) -> List[aiosqlite.Row]:
        """Execute a read query and return all rows."""
        conn = await self._read_connection()
        try:
            cursor = await conn.execute(sql, params)
            rows = await cursor.fetchall()
            return rows
        finally:
            await conn.close()

    async def read_scalar(
        self, sql: str, params: Tuple[Any, ...] = ()
    ) -> Any:
        """Execute a read query and return the first column of the first row."""
        conn = await self._read_connection()
        try:
            cursor = await conn.execute(sql, params)
            row = await cursor.fetchone()
            return row[0] if row else None
        finally:
            await conn.close()

    async def flush_write_queue(self) -> None:
        """Wait until all pending write operations have completed."""
        await self._write_queue.join()

    async def close(self) -> None:
        """Shut down the write worker and close all connections."""
        if self._write_worker_task and not self._write_worker_task.done():
            # Send stop sentinel
            await self._write_queue.put(_STOP_SENTINEL)
            try:
                await asyncio.wait_for(self._write_worker_task, timeout=10.0)
            except asyncio.TimeoutError:
                logger.warning("Write worker did not stop in time, cancelling")
                self._write_worker_task.cancel()
                try:
                    await self._write_worker_task
                except asyncio.CancelledError:
                    pass

        if self._write_conn:
            await self._write_conn.close()
            self._write_conn = None

        self._initialized = False
        logger.info("Database closed")

    # -----------------------------------------------------------------------
    # Config migration
    # -----------------------------------------------------------------------

    async def run_config_migration(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Check config_version and run additive migrations if needed.

        Rules:
            - Missing config_version → assume version 0, migrate to current.
            - Version < current → run migration functions in sequence.
            - Version > current → raise ValueError (warn user and exit).
            - Migrations are additive only: add missing keys with defaults.
            - Never delete existing keys.

        Returns the (possibly updated) config dict.
        """
        version = config.get("config_version", 0)

        if version > CURRENT_CONFIG_VERSION:
            raise ValueError(
                f"Config version {version} is newer than supported "
                f"version {CURRENT_CONFIG_VERSION}. Please update the application."
            )

        if version == CURRENT_CONFIG_VERSION:
            return config

        old_version = version
        updated = dict(config)

        # Migration 0 → 1: add all default keys that are missing
        if version < 1:
            for key, default_value in DEFAULT_CONFIG.items():
                if key not in updated:
                    updated[key] = default_value
            updated["config_version"] = 1
            version = 1

        # Future migrations would follow the same pattern:
        # if version < 2:
        #     updated.setdefault("new_key", "default_value")
        #     updated["config_version"] = 2
        #     version = 2

        await self.log_audit(AuditLogEntry(
            outcome="CONFIG_MIGRATION",
            details=json.dumps({
                "old_version": old_version,
                "new_version": version,
                "keys_added": [
                    k for k in updated if k not in config
                ],
            }),
        ))

        logger.info(
            "Config migrated from version %d to %d",
            old_version, version,
        )
        return updated

    # -----------------------------------------------------------------------
    # Startup: interrupted state reset
    # -----------------------------------------------------------------------

    async def reset_interrupted_states(self) -> List[Dict[str, Any]]:
        """Reset papers stuck in transient states from prior sessions.

        RETRIEVING → READY_FOR_RETRIEVAL
        VALIDATING → RETRIEVED

        Returns list of reset records for logging.
        """

        async def _do_reset(
            conn: aiosqlite.Connection,
        ) -> List[Dict[str, Any]]:
            resets: List[Dict[str, Any]] = []
            now = datetime.now(timezone.utc).isoformat()

            # Find papers in RETRIEVING state
            cursor = await conn.execute(
                "SELECT canonical_id, state, run_id FROM papers "
                "WHERE state = ?",
                (PaperState.RETRIEVING.value,),
            )
            retrieving_rows = await cursor.fetchall()

            for row in retrieving_rows:
                cid = row[0]
                prior_state = row[1]
                origin_run = row[2]
                await conn.execute(
                    "UPDATE papers SET state = ?, previous_state = ?, "
                    "worker_id = NULL, claimed_at = NULL, updated_at = ? "
                    "WHERE canonical_id = ?",
                    (
                        PaperState.READY_FOR_RETRIEVAL.value,
                        prior_state,
                        now,
                        cid,
                    ),
                )
                resets.append({
                    "canonical_id": cid,
                    "prior_state": prior_state,
                    "new_state": PaperState.READY_FOR_RETRIEVAL.value,
                    "reset_timestamp": now,
                    "run_id_of_origin": origin_run,
                })

            # Find papers in VALIDATING state
            cursor = await conn.execute(
                "SELECT canonical_id, state, run_id FROM papers "
                "WHERE state = ?",
                (PaperState.VALIDATING.value,),
            )
            validating_rows = await cursor.fetchall()

            for row in validating_rows:
                cid = row[0]
                prior_state = row[1]
                origin_run = row[2]
                await conn.execute(
                    "UPDATE papers SET state = ?, previous_state = ?, "
                    "worker_id = NULL, claimed_at = NULL, updated_at = ? "
                    "WHERE canonical_id = ?",
                    (
                        PaperState.RETRIEVED.value,
                        prior_state,
                        now,
                        cid,
                    ),
                )
                resets.append({
                    "canonical_id": cid,
                    "prior_state": prior_state,
                    "new_state": PaperState.RETRIEVED.value,
                    "reset_timestamp": now,
                    "run_id_of_origin": origin_run,
                })

            await conn.commit()
            return resets

        resets = await self._enqueue_write(_do_reset)

        # Log each reset
        for reset_info in resets:
            await self.log_audit(AuditLogEntry(
                canonical_id=reset_info["canonical_id"],
                run_id=reset_info.get("run_id_of_origin"),
                outcome="INTERRUPTED_RESET",
                failure_code=FailureCode.INTERRUPTED_RESET.value,
                details=json.dumps(reset_info),
            ))

        if resets:
            logger.info(
                "Reset %d interrupted papers (RETRIEVING: %d, VALIDATING: %d)",
                len(resets),
                sum(1 for r in resets if r["prior_state"] == PaperState.RETRIEVING.value),
                sum(1 for r in resets if r["prior_state"] == PaperState.VALIDATING.value),
            )

        return resets

    # -----------------------------------------------------------------------
    # Startup: filesystem reconciliation
    # -----------------------------------------------------------------------

    async def reconcile_filesystem(self) -> List[Dict[str, str]]:
        """Verify pdf_path exists on disk for papers that should have files.

        Checks papers in RETRIEVED, VALIDATING, VALIDATED, or COMPLETE states.
        If file is missing:
            - Log FILE_MISSING
            - Reset state to READY_FOR_RETRIEVAL
            - Clear pdf_path, sha256_checksum, pdf_size_bytes, pdf_page_count

        Returns list of papers with missing files.
        """
        states_to_check = (
            PaperState.RETRIEVED.value,
            PaperState.VALIDATING.value,
            PaperState.VALIDATED.value,
            PaperState.COMPLETE.value,
        )
        placeholders = ",".join("?" for _ in states_to_check)

        rows = await self.read_all(
            f"SELECT canonical_id, pdf_path, state FROM papers "
            f"WHERE state IN ({placeholders}) AND pdf_path IS NOT NULL",
            states_to_check,
        )

        missing: List[Dict[str, str]] = []

        for row in rows:
            cid = row[0]
            pdf_path = row[1]
            current_state = row[2]

            if not os.path.isfile(pdf_path):
                missing.append({
                    "canonical_id": cid,
                    "expected_path": pdf_path,
                    "prior_state": current_state,
                })

        # Process missing files through write queue
        if missing:
            async def _reset_missing(
                conn: aiosqlite.Connection,
                missing_list: List[Dict[str, str]],
            ) -> None:
                now = datetime.now(timezone.utc).isoformat()
                for info in missing_list:
                    await conn.execute(
                        "UPDATE papers SET state = ?, previous_state = state, "
                        "pdf_path = NULL, sha256_checksum = NULL, "
                        "pdf_size_bytes = NULL, pdf_page_count = NULL, "
                        "updated_at = ? "
                        "WHERE canonical_id = ?",
                        (
                            PaperState.READY_FOR_RETRIEVAL.value,
                            now,
                            info["canonical_id"],
                        ),
                    )
                await conn.commit()

            await self._enqueue_write(_reset_missing, missing)

            for info in missing:
                await self.log_audit(AuditLogEntry(
                    canonical_id=info["canonical_id"],
                    outcome="FILE_MISSING",
                    failure_code=FailureCode.FILE_MISSING.value,
                    details=json.dumps({
                        "expected_path": info["expected_path"],
                        "prior_state": info["prior_state"],
                    }),
                ))

            logger.warning(
                "Filesystem reconciliation: %d papers had missing PDF files, "
                "reset to READY_FOR_RETRIEVAL",
                len(missing),
            )

        return missing

    # -----------------------------------------------------------------------
    # Shutdown: reset in-progress states
    # -----------------------------------------------------------------------

    async def reset_shutdown_states(self) -> int:
        """Reset transient states during graceful shutdown.

        RETRIEVING → READY_FOR_RETRIEVAL
        VALIDATING → RETRIEVED

        Returns count of papers reset.
        """

        async def _do_shutdown_reset(conn: aiosqlite.Connection) -> int:
            now = datetime.now(timezone.utc).isoformat()
            count = 0

            cursor = await conn.execute(
                "UPDATE papers SET state = ?, previous_state = state, "
                "worker_id = NULL, claimed_at = NULL, updated_at = ? "
                "WHERE state = ?",
                (
                    PaperState.READY_FOR_RETRIEVAL.value,
                    now,
                    PaperState.RETRIEVING.value,
                ),
            )
            count += cursor.rowcount

            cursor = await conn.execute(
                "UPDATE papers SET state = ?, previous_state = state, "
                "worker_id = NULL, claimed_at = NULL, updated_at = ? "
                "WHERE state = ?",
                (
                    PaperState.RETRIEVED.value,
                    now,
                    PaperState.VALIDATING.value,
                ),
            )
            count += cursor.rowcount

            await conn.commit()
            return count

        count = await self._enqueue_write(_do_shutdown_reset)
        if count > 0:
            logger.info("Shutdown reset: %d papers returned to safe states", count)
        return count

    # -----------------------------------------------------------------------
    # State machine: atomic transitions
    # -----------------------------------------------------------------------

    async def transition_state(
        self,
        canonical_id: str,
        new_state: str,
        failure_code: Optional[str] = None,
        run_id: Optional[str] = None,
    ) -> bool:
        """Atomically transition a paper to a new state.

        Validates against VALID_TRANSITIONS. On invalid transition,
        logs STATE_MACHINE_ERROR and returns False without modifying state.

        RETRIEVING → FAILED requires an explicit failure_code argument.

        Returns True on success, False on invalid transition or missing paper.
        """

        async def _do_transition(
            conn: aiosqlite.Connection,
            cid: str,
            target: str,
            f_code: Optional[str],
            r_id: Optional[str],
        ) -> Tuple[bool, Optional[str]]:
            # Read current state
            cursor = await conn.execute(
                "SELECT state FROM papers WHERE canonical_id = ?",
                (cid,),
            )
            row = await cursor.fetchone()
            if row is None:
                return False, "PAPER_NOT_FOUND"

            current_state = row[0]

            # Validate transition
            if not validate_state_transition(current_state, target):
                return False, current_state

            # Enforce failure_code on RETRIEVING → FAILED
            if (
                current_state == PaperState.RETRIEVING.value
                and target == PaperState.FAILED.value
                and not f_code
            ):
                return False, "MISSING_FAILURE_CODE"

            now = datetime.now(timezone.utc).isoformat()
            update_fields = [
                "state = ?",
                "previous_state = ?",
                "updated_at = ?",
            ]
            params: List[Any] = [target, current_state, now]

            if f_code:
                update_fields.append("last_failure_code = ?")
                params.append(f_code)

            # Clear worker tracking when leaving transient states
            if target not in (PaperState.RETRIEVING.value, PaperState.VALIDATING.value):
                update_fields.append("worker_id = NULL")
                update_fields.append("claimed_at = NULL")

            params.append(cid)
            sql = f"UPDATE papers SET {', '.join(update_fields)} WHERE canonical_id = ?"
            await conn.execute(sql, tuple(params))
            await conn.commit()
            return True, None

        success, error_info = await self._enqueue_write(
            _do_transition, canonical_id, new_state, failure_code, run_id
        )

        if not success:
            if error_info == "PAPER_NOT_FOUND":
                logger.error(
                    "State transition failed: paper %s not found", canonical_id
                )
            elif error_info == "MISSING_FAILURE_CODE":
                logger.error(
                    "State transition RETRIEVING→FAILED requires failure_code "
                    "for paper %s",
                    canonical_id,
                )
            else:
                logger.error(
                    "Invalid state transition for %s: %s → %s",
                    canonical_id,
                    error_info,
                    new_state,
                )
                await self.log_audit(AuditLogEntry(
                    canonical_id=canonical_id,
                    run_id=run_id,
                    outcome="STATE_MACHINE_ERROR",
                    failure_code=FailureCode.STATE_MACHINE_ERROR.value,
                    details=json.dumps({
                        "current_state": error_info,
                        "attempted_state": new_state,
                    }),
                ))

        return success

    # -----------------------------------------------------------------------
    # Task claiming: retrieval (database-level atomicity)
    # -----------------------------------------------------------------------

    async def claim_retrieval_task(
        self,
        canonical_id: str,
        worker_id: str,
    ) -> bool:
        """Atomically claim a paper for retrieval.

        Executes:
            UPDATE papers
            SET state='RETRIEVING', claimed_at=NOW, worker_id=?
            WHERE canonical_id=? AND state='READY_FOR_RETRIEVAL'

        rows_affected=1 → claimed, proceed.
        rows_affected=0 → already claimed or wrong state, skip.

        Returns True if claimed successfully.
        """

        async def _do_claim(
            conn: aiosqlite.Connection,
            cid: str,
            wid: str,
        ) -> bool:
            now = datetime.now(timezone.utc).isoformat()
            cursor = await conn.execute(
                "UPDATE papers SET state = ?, claimed_at = ?, "
                "worker_id = ?, previous_state = state, updated_at = ? "
                "WHERE canonical_id = ? AND state = ?",
                (
                    PaperState.RETRIEVING.value,
                    now,
                    wid,
                    now,
                    cid,
                    PaperState.READY_FOR_RETRIEVAL.value,
                ),
            )
            await conn.commit()
            return cursor.rowcount == 1

        claimed = await self._enqueue_write(_do_claim, canonical_id, worker_id)
        if claimed:
            logger.debug(
                "Worker %s claimed retrieval task for %s", worker_id, canonical_id
            )
        return claimed

    # -----------------------------------------------------------------------
    # Task claiming: validation (database-level atomicity)
    # -----------------------------------------------------------------------

    async def claim_validation_task(
        self,
        canonical_id: str,
        worker_id: str,
    ) -> bool:
        """Atomically claim a paper for validation.

        Executes:
            UPDATE papers
            SET state='VALIDATING', claimed_at=NOW, worker_id=?
            WHERE canonical_id=? AND state='RETRIEVED'

        rows_affected=1 → claimed, proceed.
        rows_affected=0 → already claimed or wrong state, skip.

        Returns True if claimed successfully.
        """

        async def _do_claim(
            conn: aiosqlite.Connection,
            cid: str,
            wid: str,
        ) -> bool:
            now = datetime.now(timezone.utc).isoformat()
            cursor = await conn.execute(
                "UPDATE papers SET state = ?, claimed_at = ?, "
                "worker_id = ?, previous_state = state, updated_at = ? "
                "WHERE canonical_id = ? AND state = ?",
                (
                    PaperState.VALIDATING.value,
                    now,
                    wid,
                    now,
                    cid,
                    PaperState.RETRIEVED.value,
                ),
            )
            await conn.commit()
            return cursor.rowcount == 1

        claimed = await self._enqueue_write(_do_claim, canonical_id, worker_id)
        if claimed:
            logger.debug(
                "Worker %s claimed validation task for %s", worker_id, canonical_id
            )
        return claimed

    # -----------------------------------------------------------------------
    # Fetch next claimable tasks (for worker polling)
    # -----------------------------------------------------------------------

    async def get_next_retrieval_candidates(self, limit: int = 10) -> List[str]:
        """Return canonical_ids of papers in READY_FOR_RETRIEVAL state.

        Ordered by created_at (oldest first) for FIFO processing.
        """
        rows = await self.read_all(
            "SELECT canonical_id FROM papers "
            "WHERE state = ? ORDER BY created_at ASC LIMIT ?",
            (PaperState.READY_FOR_RETRIEVAL.value, limit),
        )
        return [row[0] for row in rows]

    async def get_next_validation_candidates(self, limit: int = 10) -> List[str]:
        """Return canonical_ids of papers in RETRIEVED state.

        Ordered by created_at (oldest first) for FIFO processing.
        """
        rows = await self.read_all(
            "SELECT canonical_id FROM papers "
            "WHERE state = ? ORDER BY created_at ASC LIMIT ?",
            (PaperState.RETRIEVED.value, limit),
        )
        return [row[0] for row in rows]

    # -----------------------------------------------------------------------
    # Paper CRUD
    # -----------------------------------------------------------------------

    def _row_to_paper(self, row: aiosqlite.Row) -> Paper:
        """Convert a database row to a Paper dataclass."""
        keys = row.keys()
        data = {k: row[k] for k in keys}
        # SQLite stores booleans as integers
        for bool_field in ("is_primary", "ocr_applied", "ocr_text_extracted"):
            if bool_field in data and data[bool_field] is not None:
                data[bool_field] = bool(data[bool_field])
        return Paper(**data)

    async def insert_paper(self, paper: Paper) -> bool:
        """Insert a new paper into the papers table.

        Returns True on success, False if canonical_id already exists.
        """

        async def _do_insert(
            conn: aiosqlite.Connection, p: Paper
        ) -> bool:
            d = p.to_dict()
            # Convert booleans to integers for SQLite
            for bool_field in ("is_primary", "ocr_applied", "ocr_text_extracted"):
                if bool_field in d:
                    d[bool_field] = int(d[bool_field])

            columns = ", ".join(d.keys())
            placeholders = ", ".join("?" for _ in d)
            sql = f"INSERT OR IGNORE INTO papers ({columns}) VALUES ({placeholders})"
            cursor = await conn.execute(sql, tuple(d.values()))
            await conn.commit()
            return cursor.rowcount == 1

        return await self._enqueue_write(_do_insert, paper)

    async def insert_papers_bulk(self, papers: List[Paper]) -> int:
        """Insert multiple papers in a single transaction.

        Uses INSERT OR IGNORE so existing canonical_ids are skipped.
        Returns count of newly inserted papers.
        """

        async def _do_bulk_insert(
            conn: aiosqlite.Connection, paper_list: List[Paper]
        ) -> int:
            if not paper_list:
                return 0

            inserted = 0
            for p in paper_list:
                d = p.to_dict()
                for bool_field in ("is_primary", "ocr_applied", "ocr_text_extracted"):
                    if bool_field in d:
                        d[bool_field] = int(d[bool_field])

                columns = ", ".join(d.keys())
                placeholders = ", ".join("?" for _ in d)
                sql = (
                    f"INSERT OR IGNORE INTO papers ({columns}) "
                    f"VALUES ({placeholders})"
                )
                cursor = await conn.execute(sql, tuple(d.values()))
                inserted += cursor.rowcount

            await conn.commit()
            return inserted

        return await self._enqueue_write(_do_bulk_insert, papers)

    async def get_paper(self, canonical_id: str) -> Optional[Paper]:
        """Fetch a single paper by canonical_id."""
        row = await self.read_one(
            "SELECT * FROM papers WHERE canonical_id = ?",
            (canonical_id,),
        )
        if row is None:
            return None
        return self._row_to_paper(row)

    async def get_papers_by_state(
        self, state: str, limit: int = 100, offset: int = 0
    ) -> List[Paper]:
        """Fetch papers in a given state with pagination."""
        rows = await self.read_all(
            "SELECT * FROM papers WHERE state = ? "
            "ORDER BY created_at ASC LIMIT ? OFFSET ?",
            (state, limit, offset),
        )
        return [self._row_to_paper(row) for row in rows]

    async def get_papers_by_states(
        self, states: List[str], limit: int = 500
    ) -> List[Paper]:
        """Fetch papers in any of the given states."""
        if not states:
            return []
        placeholders = ",".join("?" for _ in states)
        rows = await self.read_all(
            f"SELECT * FROM papers WHERE state IN ({placeholders}) "
            f"ORDER BY created_at ASC LIMIT ?",
            tuple(states) + (limit,),
        )
        return [self._row_to_paper(row) for row in rows]

    async def get_papers_by_run(
        self, run_id: str, limit: int = 500, offset: int = 0
    ) -> List[Paper]:
        """Fetch papers associated with a specific run via paper_runs."""
        rows = await self.read_all(
            "SELECT p.* FROM papers p "
            "INNER JOIN paper_runs pr ON p.canonical_id = pr.canonical_id "
            "WHERE pr.run_id = ? ORDER BY p.created_at ASC LIMIT ? OFFSET ?",
            (run_id, limit, offset),
        )
        return [self._row_to_paper(row) for row in rows]

    async def get_all_papers(
        self, limit: int = 1000, offset: int = 0
    ) -> List[Paper]:
        """Fetch all papers with pagination."""
        rows = await self.read_all(
            "SELECT * FROM papers ORDER BY created_at ASC LIMIT ? OFFSET ?",
            (limit, offset),
        )
        return [self._row_to_paper(row) for row in rows]

    async def update_paper_fields(
        self, canonical_id: str, **fields: Any
    ) -> bool:
        """Update arbitrary fields on a paper record.

        Does not enforce state transitions — use transition_state() for state
        changes. This method is for updating metadata, file paths, scores, etc.
        """
        if not fields:
            return False

        # Prevent direct state changes through this method
        if "state" in fields:
            raise ValueError(
                "Cannot update state via update_paper_fields(). "
                "Use transition_state() for state changes."
            )

        async def _do_update(
            conn: aiosqlite.Connection,
            cid: str,
            update_fields: Dict[str, Any],
        ) -> bool:
            update_fields["updated_at"] = datetime.now(timezone.utc).isoformat()

            # Convert booleans
            for bool_field in ("is_primary", "ocr_applied", "ocr_text_extracted"):
                if bool_field in update_fields and isinstance(
                    update_fields[bool_field], bool
                ):
                    update_fields[bool_field] = int(update_fields[bool_field])

            set_clause = ", ".join(f"{k} = ?" for k in update_fields)
            values = list(update_fields.values()) + [cid]
            cursor = await conn.execute(
                f"UPDATE papers SET {set_clause} WHERE canonical_id = ?",
                tuple(values),
            )
            await conn.commit()
            return cursor.rowcount == 1

        return await self._enqueue_write(_do_update, canonical_id, dict(fields))

    async def count_papers_by_state(self) -> Dict[str, int]:
        """Return count of papers grouped by state."""
        rows = await self.read_all(
            "SELECT state, COUNT(*) as cnt FROM papers GROUP BY state"
        )
        return {row[0]: row[1] for row in rows}

    async def paper_exists(self, canonical_id: str) -> bool:
        """Check whether a paper with this canonical_id exists."""
        count = await self.read_scalar(
            "SELECT COUNT(*) FROM papers WHERE canonical_id = ?",
            (canonical_id,),
        )
        return count > 0

    async def get_paper_state(self, canonical_id: str) -> Optional[str]:
        """Get the current state of a paper."""
        row = await self.read_one(
            "SELECT state FROM papers WHERE canonical_id = ?",
            (canonical_id,),
        )
        return row[0] if row else None

    async def check_duplicate_doi(self, doi: str) -> Optional[str]:
        """Check if a normalized DOI already exists. Returns canonical_id or None."""
        if not doi:
            return None
        row = await self.read_one(
            "SELECT canonical_id FROM papers WHERE doi = ?",
            (doi,),
        )
        return row[0] if row else None

    async def get_failed_papers(
        self, run_id: Optional[str] = None
    ) -> List[Paper]:
        """Fetch all papers in FAILED state, optionally filtered by run."""
        if run_id:
            rows = await self.read_all(
                "SELECT p.* FROM papers p "
                "INNER JOIN paper_runs pr ON p.canonical_id = pr.canonical_id "
                "WHERE p.state = ? AND pr.run_id = ?",
                (PaperState.FAILED.value, run_id),
            )
        else:
            rows = await self.read_all(
                "SELECT * FROM papers WHERE state = ?",
                (PaperState.FAILED.value,),
            )
        return [self._row_to_paper(row) for row in rows]

    async def get_manual_required_papers(
        self, run_id: Optional[str] = None
    ) -> List[Paper]:
        """Fetch all papers in MANUAL_REQUIRED state, optionally by run."""
        if run_id:
            rows = await self.read_all(
                "SELECT p.* FROM papers p "
                "INNER JOIN paper_runs pr ON p.canonical_id = pr.canonical_id "
                "WHERE p.state = ? AND pr.run_id = ?",
                (PaperState.MANUAL_REQUIRED.value, run_id),
            )
        else:
            rows = await self.read_all(
                "SELECT * FROM papers WHERE state = ?",
                (PaperState.MANUAL_REQUIRED.value,),
            )
        return [self._row_to_paper(row) for row in rows]

    async def get_flagged_papers(
        self, run_id: Optional[str] = None
    ) -> List[Paper]:
        """Fetch papers with VERSION_MISMATCH or CONTENT_UNVERIFIED identity."""
        flagged_statuses = (
            IdentityStatus.VERSION_MISMATCH.value,
            IdentityStatus.CONTENT_UNVERIFIED.value,
        )
        if run_id:
            rows = await self.read_all(
                "SELECT p.* FROM papers p "
                "INNER JOIN paper_runs pr ON p.canonical_id = pr.canonical_id "
                "WHERE p.identity_status IN (?, ?) AND pr.run_id = ?",
                flagged_statuses + (run_id,),
            )
        else:
            rows = await self.read_all(
                "SELECT * FROM papers WHERE identity_status IN (?, ?)",
                flagged_statuses,
            )
        return [self._row_to_paper(row) for row in rows]

    async def apply_user_override(
        self,
        canonical_id: str,
        action: str,
        reason: str,
    ) -> bool:
        """Apply a user validation override on a flagged paper.

        action='confirm_correct' → store override, keep COMPLETE state.
        action='re_retrieve' → reset to READY_FOR_RETRIEVAL.
        """

        async def _do_override(
            conn: aiosqlite.Connection,
            cid: str,
            act: str,
            rsn: str,
        ) -> bool:
            now = datetime.now(timezone.utc).isoformat()

            if act == "confirm_correct":
                cursor = await conn.execute(
                    "UPDATE papers SET user_override = 'CONFIRMED_CORRECT', "
                    "override_reason = ?, override_timestamp = ?, "
                    "updated_at = ? WHERE canonical_id = ?",
                    (rsn, now, now, cid),
                )
            elif act == "re_retrieve":
                cursor = await conn.execute(
                    "UPDATE papers SET state = ?, previous_state = state, "
                    "user_override = 'RE_RETRIEVE', override_reason = ?, "
                    "override_timestamp = ?, pdf_path = NULL, "
                    "sha256_checksum = NULL, pdf_size_bytes = NULL, "
                    "pdf_page_count = NULL, validation_status = NULL, "
                    "identity_status = NULL, integrity_score = NULL, "
                    "confidence_level = NULL, updated_at = ? "
                    "WHERE canonical_id = ?",
                    (
                        PaperState.READY_FOR_RETRIEVAL.value,
                        rsn,
                        now,
                        now,
                        cid,
                    ),
                )
            else:
                return False

            await conn.commit()
            return cursor.rowcount == 1

        success = await self._enqueue_write(
            _do_override, canonical_id, action, reason
        )
        if success:
            await self.log_audit(AuditLogEntry(
                canonical_id=canonical_id,
                outcome=f"USER_OVERRIDE_{action.upper()}",
                details=json.dumps({"reason": reason}),
            ))
        return success

    async def reset_paper_for_retry(self, canonical_id: str) -> bool:
        """Reset a FAILED or MANUAL_REQUIRED paper to READY_FOR_RETRIEVAL."""

        async def _do_reset(
            conn: aiosqlite.Connection, cid: str
        ) -> bool:
            now = datetime.now(timezone.utc).isoformat()
            cursor = await conn.execute(
                "UPDATE papers SET state = ?, previous_state = state, "
                "worker_id = NULL, claimed_at = NULL, "
                "last_failure_code = NULL, updated_at = ? "
                "WHERE canonical_id = ? AND state IN (?, ?)",
                (
                    PaperState.READY_FOR_RETRIEVAL.value,
                    now,
                    cid,
                    PaperState.FAILED.value,
                    PaperState.MANUAL_REQUIRED.value,
                ),
            )
            await conn.commit()
            return cursor.rowcount == 1

        return await self._enqueue_write(_do_reset, canonical_id)

    async def reset_papers_for_retry_bulk(
        self, canonical_ids: Optional[List[str]] = None, retry_type: str = "failed"
    ) -> int:
        """Reset multiple papers for retry.

        retry_type:
            'failed' → all FAILED papers
            'mismatch' → VERSION_MISMATCH or CONTENT_UNVERIFIED
            'manual' → all MANUAL_REQUIRED papers
            'all_eligible' → all of the above

        If canonical_ids is provided, only those specific papers are reset.
        Returns count of papers reset.
        """

        async def _do_bulk_reset(
            conn: aiosqlite.Connection,
            cids: Optional[List[str]],
            rtype: str,
        ) -> int:
            now = datetime.now(timezone.utc).isoformat()

            if cids:
                placeholders = ",".join("?" for _ in cids)
                eligible_states = (
                    PaperState.FAILED.value,
                    PaperState.MANUAL_REQUIRED.value,
                )
                cursor = await conn.execute(
                    f"UPDATE papers SET state = ?, previous_state = state, "
                    f"worker_id = NULL, claimed_at = NULL, "
                    f"last_failure_code = NULL, updated_at = ? "
                    f"WHERE canonical_id IN ({placeholders}) "
                    f"AND state IN (?, ?)",
                    (PaperState.READY_FOR_RETRIEVAL.value, now)
                    + tuple(cids)
                    + eligible_states,
                )
            elif rtype == "failed":
                cursor = await conn.execute(
                    "UPDATE papers SET state = ?, previous_state = state, "
                    "worker_id = NULL, claimed_at = NULL, "
                    "last_failure_code = NULL, updated_at = ? "
                    "WHERE state = ?",
                    (PaperState.READY_FOR_RETRIEVAL.value, now, PaperState.FAILED.value),
                )
            elif rtype == "manual":
                cursor = await conn.execute(
                    "UPDATE papers SET state = ?, previous_state = state, "
                    "worker_id = NULL, claimed_at = NULL, "
                    "last_failure_code = NULL, updated_at = ? "
                    "WHERE state = ?",
                    (
                        PaperState.READY_FOR_RETRIEVAL.value,
                        now,
                        PaperState.MANUAL_REQUIRED.value,
                    ),
                )
            elif rtype == "mismatch":
                cursor = await conn.execute(
                    "UPDATE papers SET state = ?, previous_state = state, "
                    "worker_id = NULL, claimed_at = NULL, "
                    "last_failure_code = NULL, validation_status = NULL, "
                    "identity_status = NULL, pdf_path = NULL, "
                    "sha256_checksum = NULL, updated_at = ? "
                    "WHERE identity_status IN (?, ?)",
                    (
                        PaperState.READY_FOR_RETRIEVAL.value,
                        now,
                        IdentityStatus.VERSION_MISMATCH.value,
                        IdentityStatus.CONTENT_UNVERIFIED.value,
                    ),
                )
            elif rtype == "all_eligible":
                cursor = await conn.execute(
                    "UPDATE papers SET state = ?, previous_state = state, "
                    "worker_id = NULL, claimed_at = NULL, "
                    "last_failure_code = NULL, updated_at = ? "
                    "WHERE state IN (?, ?)",
                    (
                        PaperState.READY_FOR_RETRIEVAL.value,
                        now,
                        PaperState.FAILED.value,
                        PaperState.MANUAL_REQUIRED.value,
                    ),
                )
            else:
                return 0

            await conn.commit()
            return cursor.rowcount

        return await self._enqueue_write(_do_bulk_reset, canonical_ids, retry_type)

    # -----------------------------------------------------------------------
    # Run management
    # -----------------------------------------------------------------------

    async def create_run(self, run_record: RunRecord) -> bool:
        """Insert a new run record."""

        async def _do_insert(
            conn: aiosqlite.Connection, rec: RunRecord
        ) -> bool:
            d = rec.to_dict()
            columns = ", ".join(d.keys())
            placeholders = ", ".join("?" for _ in d)
            cursor = await conn.execute(
                f"INSERT OR IGNORE INTO runs ({columns}) VALUES ({placeholders})",
                tuple(d.values()),
            )
            await conn.commit()
            return cursor.rowcount == 1

        return await self._enqueue_write(_do_insert, run_record)

    async def complete_run(
        self,
        run_id: str,
        status: str = RunStatus.COMPLETED.value,
    ) -> bool:
        """Mark a run as completed with a timestamp."""

        async def _do_complete(
            conn: aiosqlite.Connection,
            rid: str,
            st: str,
        ) -> bool:
            now = datetime.now(timezone.utc).isoformat()
            cursor = await conn.execute(
                "UPDATE runs SET completed_at = ?, status = ? WHERE run_id = ?",
                (now, st, rid),
            )
            await conn.commit()
            return cursor.rowcount == 1

        return await self._enqueue_write(_do_complete, run_id, status)

    async def get_run(self, run_id: str) -> Optional[RunRecord]:
        """Fetch a single run record by run_id."""
        row = await self.read_one(
            "SELECT * FROM runs WHERE run_id = ?", (run_id,)
        )
        if row is None:
            return None
        return RunRecord(
            run_id=row["run_id"],
            started_at=row["started_at"],
            completed_at=row["completed_at"],
            status=row["status"],
            total_submitted=row["total_submitted"],
            config_snapshot=row["config_snapshot"],
        )

    async def list_runs(self, limit: int = 50) -> List[RunRecord]:
        """List all runs ordered by most recent first."""
        rows = await self.read_all(
            "SELECT * FROM runs ORDER BY started_at DESC LIMIT ?",
            (limit,),
        )
        return [
            RunRecord(
                run_id=row["run_id"],
                started_at=row["started_at"],
                completed_at=row["completed_at"],
                status=row["status"],
                total_submitted=row["total_submitted"],
                config_snapshot=row["config_snapshot"],
            )
            for row in rows
        ]

    async def update_run_submitted_count(
        self, run_id: str, total_submitted: int
    ) -> bool:
        """Update the total_submitted count on a run record."""

        async def _do_update(
            conn: aiosqlite.Connection, rid: str, count: int
        ) -> bool:
            cursor = await conn.execute(
                "UPDATE runs SET total_submitted = ? WHERE run_id = ?",
                (count, rid),
            )
            await conn.commit()
            return cursor.rowcount == 1

        return await self._enqueue_write(_do_update, run_id, total_submitted)

    # -----------------------------------------------------------------------
    # Paper-run linkage
    # -----------------------------------------------------------------------

    async def create_paper_run(self, paper_run: PaperRun) -> bool:
        """Insert a paper-run linkage record."""

        async def _do_insert(
            conn: aiosqlite.Connection, pr: PaperRun
        ) -> bool:
            d = pr.to_dict()
            # Convert booleans for SQLite
            for bool_field in (
                "submitted_in_this_run",
                "retrieval_attempted_in_this_run",
            ):
                if bool_field in d:
                    d[bool_field] = int(d[bool_field])

            columns = ", ".join(d.keys())
            placeholders = ", ".join("?" for _ in d)
            cursor = await conn.execute(
                f"INSERT OR IGNORE INTO paper_runs ({columns}) "
                f"VALUES ({placeholders})",
                tuple(d.values()),
            )
            await conn.commit()
            return cursor.rowcount == 1

        return await self._enqueue_write(_do_insert, paper_run)

    async def create_paper_runs_bulk(self, paper_runs: List[PaperRun]) -> int:
        """Insert multiple paper-run records in a single transaction."""

        async def _do_bulk(
            conn: aiosqlite.Connection, prs: List[PaperRun]
        ) -> int:
            if not prs:
                return 0
            inserted = 0
            for pr in prs:
                d = pr.to_dict()
                for bool_field in (
                    "submitted_in_this_run",
                    "retrieval_attempted_in_this_run",
                ):
                    if bool_field in d:
                        d[bool_field] = int(d[bool_field])

                columns = ", ".join(d.keys())
                placeholders = ", ".join("?" for _ in d)
                cursor = await conn.execute(
                    f"INSERT OR IGNORE INTO paper_runs ({columns}) "
                    f"VALUES ({placeholders})",
                    tuple(d.values()),
                )
                inserted += cursor.rowcount
            await conn.commit()
            return inserted

        return await self._enqueue_write(_do_bulk, paper_runs)

    async def update_paper_run_outcome(
        self,
        canonical_id: str,
        run_id: str,
        outcome: str,
        retrieval_attempted: bool = False,
    ) -> bool:
        """Update the outcome and retrieval_attempted flag for a paper-run."""

        async def _do_update(
            conn: aiosqlite.Connection,
            cid: str,
            rid: str,
            out: str,
            attempted: bool,
        ) -> bool:
            cursor = await conn.execute(
                "UPDATE paper_runs SET outcome_in_this_run = ?, "
                "retrieval_attempted_in_this_run = ? "
                "WHERE canonical_id = ? AND run_id = ?",
                (out, int(attempted), cid, rid),
            )
            await conn.commit()
            return cursor.rowcount == 1

        return await self._enqueue_write(
            _do_update, canonical_id, run_id, outcome, retrieval_attempted
        )

    async def get_paper_runs_for_run(
        self, run_id: str
    ) -> List[PaperRun]:
        """Fetch all paper-run linkage records for a given run."""
        rows = await self.read_all(
            "SELECT * FROM paper_runs WHERE run_id = ?", (run_id,)
        )
        return [
            PaperRun(
                canonical_id=row["canonical_id"],
                run_id=row["run_id"],
                submitted_in_this_run=bool(row["submitted_in_this_run"]),
                retrieval_attempted_in_this_run=bool(
                    row["retrieval_attempted_in_this_run"]
                ),
                outcome_in_this_run=row["outcome_in_this_run"],
            )
            for row in rows
        ]

    async def get_paper_run_history(
        self, canonical_id: str
    ) -> List[PaperRun]:
        """Fetch all run linkage records for a given paper across all runs."""
        rows = await self.read_all(
            "SELECT * FROM paper_runs WHERE canonical_id = ? "
            "ORDER BY run_id ASC",
            (canonical_id,),
        )
        return [
            PaperRun(
                canonical_id=row["canonical_id"],
                run_id=row["run_id"],
                submitted_in_this_run=bool(row["submitted_in_this_run"]),
                retrieval_attempted_in_this_run=bool(
                    row["retrieval_attempted_in_this_run"]
                ),
                outcome_in_this_run=row["outcome_in_this_run"],
            )
            for row in rows
        ]

    async def check_already_retrieved(
        self, canonical_id: str
    ) -> Optional[str]:
        """Check if a paper was already successfully retrieved in a prior run.

        Returns the originating run_id if COMPLETE, else None.
        """
        row = await self.read_one(
            "SELECT run_id_of_first_success FROM papers "
            "WHERE canonical_id = ? AND state = ?",
            (canonical_id, PaperState.COMPLETE.value),
        )
        if row and row[0]:
            return row[0]
        return None

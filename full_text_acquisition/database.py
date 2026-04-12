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

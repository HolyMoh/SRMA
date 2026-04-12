"""
Full-Text Acquisition System — Workers

Retrieval and validation worker pools with backpressure control,
disk space safety, and graceful shutdown support. All task claiming
uses database-level atomicity (no in-memory locks).
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
import traceback
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from full_text_acquisition.models import (
    DEFAULT_BACKPRESSURE_THRESHOLD,
    DEFAULT_MIN_DISK_SPACE_BYTES,
    DEFAULT_RETRIEVAL_CONCURRENCY,
    DEFAULT_VALIDATION_CONCURRENCY,
    MAX_RETRIEVAL_CONCURRENCY,
    MAX_VALIDATION_CONCURRENCY,
    OCR_BACKPRESSURE_WEIGHT,
    STANDARD_BACKPRESSURE_WEIGHT,
    AuditLogEntry,
    FailureCode,
    PaperState,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

POLL_INTERVAL_S: float = 2.0
BACKPRESSURE_CHECK_INTERVAL_S: float = 5.0
DISK_CHECK_INTERVAL_PAPERS: int = 50
DISK_CHECK_INTERVAL_S: float = 60.0

# Stale claim reaper: a paper whose state is RETRIEVING or VALIDATING for
# more than STALE_CLAIM_TIMEOUT_MINUTES without any progress is assumed
# to be abandoned by a crashed or disconnected worker.
STALE_CLAIM_TIMEOUT_MINUTES: int = 30
STALE_CLAIM_CHECK_INTERVAL_S: float = 600.0  # 10 minutes


def generate_worker_id(prefix: str = "w") -> str:
    """Generate a unique worker ID for task claiming."""
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


def check_disk_space(path: str = ".") -> int:
    """Return available disk space in bytes for the given path.

    Returns 0 on error or unsupported platforms.
    """
    try:
        stat = os.statvfs(path)
        return stat.f_bavail * stat.f_frsize
    except (OSError, AttributeError):
        # Windows fallback
        try:
            import shutil
            total, used, free = shutil.disk_usage(path)
            return free
        except Exception:
            return 0


def compute_weighted_backpressure(
    retrieved_count: int,
    ocr_pending_count: int,
) -> int:
    """Compute the weighted backpressure depth.

    Standard PDF = 1 slot. IMAGE_ONLY_SCAN (OCR) = 5 slots.
    """
    non_ocr = max(0, retrieved_count - ocr_pending_count)
    return (
        non_ocr * STANDARD_BACKPRESSURE_WEIGHT
        + ocr_pending_count * OCR_BACKPRESSURE_WEIGHT
    )


# ===========================================================================
# Stale Claim Reaper
# ===========================================================================


async def run_stale_claim_reaper_once(
    db: Any,
    timeout_minutes: int = STALE_CLAIM_TIMEOUT_MINUTES,
) -> int:
    """Reset papers whose claim has exceeded the staleness timeout.

    Finds all papers where:
        state = 'RETRIEVING' AND claimed_at < now - timeout_minutes
        state = 'VALIDATING' AND claimed_at < now - timeout_minutes

    Resets:
        RETRIEVING → READY_FOR_RETRIEVAL
        VALIDATING → RETRIEVED

    Each reset is logged to the audit log with outcome='STALE_CLAIM_RESET'
    and details containing canonical_id, worker_id, prior_state, claimed_at,
    and reset_timestamp.

    Returns the count of papers reset.
    """
    if db is None:
        return 0

    now_dt = datetime.now(timezone.utc)
    cutoff_dt = now_dt - timedelta(minutes=timeout_minutes)
    cutoff_iso = cutoff_dt.isoformat()
    now_iso = now_dt.isoformat()

    # Read stale papers (read-only, no lock on write queue)
    try:
        stale_rows = await db.read_all(
            "SELECT canonical_id, state, worker_id, claimed_at, run_id "
            "FROM papers "
            "WHERE state IN (?, ?) "
            "AND claimed_at IS NOT NULL AND claimed_at < ?",
            (
                PaperState.RETRIEVING.value,
                PaperState.VALIDATING.value,
                cutoff_iso,
            ),
        )
    except Exception as exc:
        logger.error(
            "Stale claim reaper: failed to read stale papers: %s", exc
        )
        return 0

    if not stale_rows:
        return 0

    # Reset each paper's state via the write queue (serialized atomic updates)
    reset_records: List[Dict[str, Any]] = []
    for row in stale_rows:
        cid = row[0]
        prior_state = row[1]
        worker_id = row[2]
        claimed_at = row[3]
        run_id = row[4]

        if prior_state == PaperState.RETRIEVING.value:
            new_state = PaperState.READY_FOR_RETRIEVAL.value
        elif prior_state == PaperState.VALIDATING.value:
            new_state = PaperState.RETRIEVED.value
        else:
            continue

        reset_records.append({
            "canonical_id": cid,
            "prior_state": prior_state,
            "new_state": new_state,
            "worker_id": worker_id,
            "claimed_at": claimed_at,
            "reset_timestamp": now_iso,
            "run_id": run_id,
        })

    if not reset_records:
        return 0

    async def _do_reset(conn: Any, records: List[Dict[str, Any]]) -> int:
        count = 0
        for rec in records:
            cursor = await conn.execute(
                "UPDATE papers SET state = ?, previous_state = ?, "
                "worker_id = NULL, claimed_at = NULL, updated_at = ? "
                "WHERE canonical_id = ? AND state = ? "
                "AND claimed_at IS NOT NULL AND claimed_at < ?",
                (
                    rec["new_state"],
                    rec["prior_state"],
                    rec["reset_timestamp"],
                    rec["canonical_id"],
                    rec["prior_state"],
                    cutoff_iso,
                ),
            )
            # rowcount==0 means the paper transitioned out of the stuck
            # state between our read and the update (a worker may have
            # progressed it) — that's fine, we just skip the log.
            if cursor.rowcount == 1:
                count += 1
                rec["reset_applied"] = True
            else:
                rec["reset_applied"] = False
        await conn.commit()
        return count

    try:
        applied_count = await db._enqueue_write(_do_reset, reset_records)
    except Exception as exc:
        logger.error(
            "Stale claim reaper: write queue failed: %s\n%s",
            exc, traceback.format_exc(),
        )
        return 0

    # Audit log entries (one per successful reset)
    for rec in reset_records:
        if not rec.get("reset_applied"):
            continue
        try:
            await db.log_audit(AuditLogEntry(
                canonical_id=rec["canonical_id"],
                run_id=rec["run_id"],
                outcome="STALE_CLAIM_RESET",
                failure_code=FailureCode.INTERRUPTED_RESET.value,
                details=json.dumps({
                    "canonical_id": rec["canonical_id"],
                    "worker_id": rec["worker_id"],
                    "prior_state": rec["prior_state"],
                    "new_state": rec["new_state"],
                    "claimed_at": rec["claimed_at"],
                    "reset_timestamp": rec["reset_timestamp"],
                    "timeout_minutes": timeout_minutes,
                }),
            ))
        except Exception as exc:
            logger.debug(
                "Stale claim reaper: audit log failed for %s: %s",
                rec["canonical_id"], exc,
            )

    if applied_count > 0:
        logger.warning(
            "Stale claim reaper: reset %d papers "
            "(timeout=%dmin, cutoff=%s)",
            applied_count, timeout_minutes, cutoff_iso,
        )
    return applied_count


async def stale_claim_reaper_loop(
    db: Any,
    shutdown_event: asyncio.Event,
    interval_s: float = STALE_CLAIM_CHECK_INTERVAL_S,
    timeout_minutes: int = STALE_CLAIM_TIMEOUT_MINUTES,
) -> None:
    """Periodic stale claim reaper.

    Runs every interval_s seconds until shutdown is signaled. Each iteration
    calls run_stale_claim_reaper_once. Exceptions are logged and do not
    stop the loop.
    """
    logger.info(
        "Stale claim reaper loop started (interval=%.0fs, timeout=%dmin)",
        interval_s, timeout_minutes,
    )

    while not shutdown_event.is_set():
        try:
            await run_stale_claim_reaper_once(db, timeout_minutes)
        except Exception as exc:
            logger.error(
                "Stale claim reaper: unhandled exception: %s\n%s",
                exc, traceback.format_exc(),
            )

        # Wait interval or until shutdown
        try:
            await asyncio.wait_for(
                shutdown_event.wait(), timeout=interval_s
            )
            break  # shutdown signaled
        except asyncio.TimeoutError:
            pass  # normal: keep looping

    logger.info("Stale claim reaper loop stopped")


# ===========================================================================
# Retrieval Worker
# ===========================================================================


async def retrieval_worker(
    worker_id: str,
    db: Any,
    engine: Any,
    run_id: str,
    semaphore: asyncio.Semaphore,
    shutdown_event: asyncio.Event,
    retrieval_allowed: asyncio.Event,
    papers_processed_counter: Dict[str, int],
    output_dir: str = ".",
    min_disk_space: int = DEFAULT_MIN_DISK_SPACE_BYTES,
) -> None:
    """Single retrieval worker coroutine.

    Polls for READY_FOR_RETRIEVAL papers, claims them via atomic SQL,
    and runs the multi-tier retrieval orchestrator.

    Args:
        worker_id: Unique identifier for this worker.
        db: Database instance.
        engine: RetrievalEngine instance.
        run_id: Current run ID.
        semaphore: Concurrency limiter shared across retrieval workers.
        shutdown_event: Set when shutdown is requested.
        retrieval_allowed: Cleared when backpressure pauses retrieval.
        papers_processed_counter: Shared dict for tracking {"retrieved": N, "failed": N}.
        output_dir: Directory to check disk space against.
        min_disk_space: Minimum bytes of free space required.
    """
    logger.info("Retrieval worker %s started", worker_id)
    local_count = 0

    while not shutdown_event.is_set():
        # Wait for retrieval to be allowed (backpressure control)
        try:
            # Check with timeout so we can also check shutdown
            await asyncio.wait_for(
                retrieval_allowed.wait(),
                timeout=POLL_INTERVAL_S,
            )
        except asyncio.TimeoutError:
            continue

        if shutdown_event.is_set():
            break

        # Disk space check every N papers
        if local_count > 0 and local_count % DISK_CHECK_INTERVAL_PAPERS == 0:
            free_space = check_disk_space(output_dir)
            if free_space > 0 and free_space < min_disk_space:
                logger.warning(
                    "Worker %s: Low disk space (%d bytes), pausing",
                    worker_id, free_space,
                )
                retrieval_allowed.clear()
                continue

        # Acquire semaphore slot
        async with semaphore:
            if shutdown_event.is_set():
                break

            # Get next candidate
            candidates = await db.get_next_retrieval_candidates(limit=1)
            if not candidates:
                # Nothing to do — wait before polling again
                try:
                    await asyncio.wait_for(
                        shutdown_event.wait(),
                        timeout=POLL_INTERVAL_S,
                    )
                except asyncio.TimeoutError:
                    pass
                continue

            canonical_id = candidates[0]

            # Claim the task (atomic SQL)
            claimed = await db.claim_retrieval_task(canonical_id, worker_id)
            if not claimed:
                # Another worker got it — try next
                continue

            # Execute retrieval
            start_ms = time.monotonic() * 1000
            try:
                paper = await db.get_paper(canonical_id)
                if paper is None:
                    logger.error(
                        "Worker %s: Paper %s disappeared after claiming",
                        worker_id, canonical_id,
                    )
                    continue

                success, failure_code = await engine.orchestrate_retrieval(
                    paper, run_id
                )

                elapsed_ms = (time.monotonic() * 1000) - start_ms

                if success:
                    # Transition to RETRIEVED
                    await db.transition_state(
                        canonical_id,
                        PaperState.RETRIEVED.value,
                        run_id=run_id,
                    )
                    await db.update_paper_run_outcome(
                        canonical_id, run_id,
                        outcome="RETRIEVED",
                        retrieval_attempted=True,
                    )
                    papers_processed_counter["retrieved"] = (
                        papers_processed_counter.get("retrieved", 0) + 1
                    )
                    logger.info(
                        "Worker %s: Retrieved %s (%.0fms)",
                        worker_id, canonical_id, elapsed_ms,
                    )
                elif failure_code == FailureCode.MANUAL_REQUIRED.value:
                    # Transition to MANUAL_REQUIRED
                    await db.transition_state(
                        canonical_id,
                        PaperState.MANUAL_REQUIRED.value,
                        failure_code=failure_code,
                        run_id=run_id,
                    )
                    await db.update_paper_run_outcome(
                        canonical_id, run_id,
                        outcome="MANUAL_REQUIRED",
                        retrieval_attempted=True,
                    )
                    papers_processed_counter["manual"] = (
                        papers_processed_counter.get("manual", 0) + 1
                    )
                else:
                    # Transition to FAILED
                    await db.transition_state(
                        canonical_id,
                        PaperState.FAILED.value,
                        failure_code=failure_code or FailureCode.NO_OA_SOURCE.value,
                        run_id=run_id,
                    )
                    await db.update_paper_run_outcome(
                        canonical_id, run_id,
                        outcome="FAILED",
                        retrieval_attempted=True,
                    )
                    papers_processed_counter["failed"] = (
                        papers_processed_counter.get("failed", 0) + 1
                    )

                local_count += 1

            except Exception as exc:
                elapsed_ms = (time.monotonic() * 1000) - start_ms
                logger.error(
                    "Worker %s: Unhandled exception for %s (%.0fms): %s\n%s",
                    worker_id, canonical_id, elapsed_ms,
                    exc, traceback.format_exc(),
                )
                # Best-effort transition to FAILED
                try:
                    await db.transition_state(
                        canonical_id,
                        PaperState.FAILED.value,
                        failure_code=FailureCode.STATE_MACHINE_ERROR.value,
                        run_id=run_id,
                    )
                    await db.update_paper_run_outcome(
                        canonical_id, run_id,
                        outcome="FAILED",
                        retrieval_attempted=True,
                    )
                except Exception:
                    pass

    logger.info(
        "Retrieval worker %s stopped (processed %d papers)",
        worker_id, local_count,
    )


# ===========================================================================
# Validation Worker
# ===========================================================================


async def validation_worker(
    worker_id: str,
    db: Any,
    engine: Any,
    run_id: str,
    semaphore: asyncio.Semaphore,
    shutdown_event: asyncio.Event,
    papers_processed_counter: Dict[str, int],
) -> None:
    """Single validation worker coroutine.

    Polls for RETRIEVED papers, claims them via atomic SQL,
    and runs the validation pipeline (Steps 3, 3.5, 3.6, 4).

    OCR papers consume 5 weighted backpressure slots.

    Args:
        worker_id: Unique identifier for this worker.
        db: Database instance.
        engine: RetrievalEngine instance.
        run_id: Current run ID.
        semaphore: Concurrency limiter shared across validation workers.
        shutdown_event: Set when shutdown is requested.
        papers_processed_counter: Shared dict {"validated": N, "val_failed": N}.
    """
    logger.info("Validation worker %s started", worker_id)
    local_count = 0

    while not shutdown_event.is_set():
        # Acquire semaphore slot
        async with semaphore:
            if shutdown_event.is_set():
                break

            # Get next candidate
            candidates = await db.get_next_validation_candidates(limit=1)
            if not candidates:
                # Release semaphore and wait
                pass
            else:
                canonical_id = candidates[0]

                # Claim the task (atomic SQL)
                claimed = await db.claim_validation_task(canonical_id, worker_id)
                if not claimed:
                    continue

                # Execute validation pipeline
                start_ms = time.monotonic() * 1000
                try:
                    paper = await db.get_paper(canonical_id)
                    if paper is None:
                        logger.error(
                            "Validation worker %s: Paper %s disappeared",
                            worker_id, canonical_id,
                        )
                        continue

                    validation_status, identity_status, score = (
                        await engine.run_validation_pipeline(paper, run_id)
                    )

                    elapsed_ms = (time.monotonic() * 1000) - start_ms

                    # Transition based on validation result
                    # INVALID → FAILED (terminal)
                    if validation_status == "INVALID":
                        await db.transition_state(
                            canonical_id,
                            PaperState.FAILED.value,
                            failure_code=FailureCode.PDF_INVALID.value,
                            run_id=run_id,
                        )
                        await db.update_paper_run_outcome(
                            canonical_id, run_id,
                            outcome="FAILED",
                            retrieval_attempted=True,
                        )
                        papers_processed_counter["val_failed"] = (
                            papers_processed_counter.get("val_failed", 0) + 1
                        )
                        logger.info(
                            "Validation worker %s: %s INVALID (%.0fms)",
                            worker_id, canonical_id, elapsed_ms,
                        )
                    else:
                        # VALIDATED → COMPLETE (or remain for user review if flagged)
                        await db.transition_state(
                            canonical_id,
                            PaperState.VALIDATED.value,
                            run_id=run_id,
                        )

                        # Auto-complete unless flagged
                        if identity_status in (
                            "VERSION_MISMATCH",
                            "CONTENT_UNVERIFIED",
                        ):
                            # Leave at VALIDATED — user must override
                            await db.update_paper_run_outcome(
                                canonical_id, run_id,
                                outcome="FLAGGED",
                                retrieval_attempted=True,
                            )
                            logger.info(
                                "Validation worker %s: %s FLAGGED (%s, score=%d, %.0fms)",
                                worker_id, canonical_id,
                                identity_status, score, elapsed_ms,
                            )
                        else:
                            # Complete
                            await db.transition_state(
                                canonical_id,
                                PaperState.COMPLETE.value,
                                run_id=run_id,
                            )

                            # Set run_id_of_first_success if not already set
                            refreshed = await db.get_paper(canonical_id)
                            if refreshed and not refreshed.run_id_of_first_success:
                                await db.update_paper_fields(
                                    canonical_id,
                                    run_id_of_first_success=run_id,
                                )

                            await db.update_paper_run_outcome(
                                canonical_id, run_id,
                                outcome="COMPLETE",
                                retrieval_attempted=True,
                            )
                            logger.info(
                                "Validation worker %s: %s COMPLETE "
                                "(status=%s, score=%d, %.0fms)",
                                worker_id, canonical_id,
                                validation_status, score, elapsed_ms,
                            )

                        papers_processed_counter["validated"] = (
                            papers_processed_counter.get("validated", 0) + 1
                        )

                    local_count += 1

                except Exception as exc:
                    elapsed_ms = (time.monotonic() * 1000) - start_ms
                    logger.error(
                        "Validation worker %s: Unhandled exception for %s "
                        "(%.0fms): %s\n%s",
                        worker_id, canonical_id, elapsed_ms,
                        exc, traceback.format_exc(),
                    )
                    # Best-effort: revert to RETRIEVED so it can be retried
                    try:
                        await db.transition_state(
                            canonical_id,
                            PaperState.FAILED.value,
                            failure_code=FailureCode.STATE_MACHINE_ERROR.value,
                            run_id=run_id,
                        )
                    except Exception:
                        pass

        # Poll interval (outside semaphore)
        if not shutdown_event.is_set():
            try:
                await asyncio.wait_for(
                    shutdown_event.wait(),
                    timeout=POLL_INTERVAL_S,
                )
            except asyncio.TimeoutError:
                pass

    logger.info(
        "Validation worker %s stopped (processed %d papers)",
        worker_id, local_count,
    )


# ===========================================================================
# Worker Pool
# ===========================================================================


class WorkerPool:
    """Manages retrieval and validation worker pools.

    Features:
        - Configurable concurrency (bounded by semaphores)
        - Backpressure monitoring (pauses retrieval when validation lags)
        - Disk space monitoring (pauses all when <threshold)
        - Graceful start/stop/pause/resume
        - Status reporting for health dashboard
    """

    def __init__(
        self,
        db: Any,
        engine: Any,
        config: Dict[str, Any],
        output_dir: str = ".",
    ) -> None:
        self._db = db
        self._engine = engine
        self._config = config
        self._output_dir = output_dir

        # Current run
        self._run_id: Optional[str] = None

        # Shutdown coordination
        self._shutdown_event = asyncio.Event()

        # Backpressure: when cleared, retrieval workers pause
        self._retrieval_allowed = asyncio.Event()
        self._retrieval_allowed.set()  # Start allowed

        # Disk space: when cleared, ALL workers see low-disk
        self._disk_space_ok = asyncio.Event()
        self._disk_space_ok.set()

        # Semaphores
        retrieval_conc = min(
            config.get("retrieval_concurrency", DEFAULT_RETRIEVAL_CONCURRENCY),
            MAX_RETRIEVAL_CONCURRENCY,
        )
        validation_conc = min(
            config.get("validation_concurrency", DEFAULT_VALIDATION_CONCURRENCY),
            MAX_VALIDATION_CONCURRENCY,
        )
        self._retrieval_semaphore = asyncio.Semaphore(retrieval_conc)
        self._validation_semaphore = asyncio.Semaphore(validation_conc)

        # Concurrency counts for reporting
        self._retrieval_concurrency = retrieval_conc
        self._validation_concurrency = validation_conc

        # Worker tasks
        self._retrieval_tasks: List[asyncio.Task[None]] = []
        self._validation_tasks: List[asyncio.Task[None]] = []
        self._monitor_tasks: List[asyncio.Task[None]] = []

        # Shared counters
        self._retrieval_counters: Dict[str, int] = {
            "retrieved": 0,
            "failed": 0,
            "manual": 0,
        }
        self._validation_counters: Dict[str, int] = {
            "validated": 0,
            "val_failed": 0,
        }

        # State
        self._running = False
        self._paused = False

    async def start(self, run_id: str) -> None:
        """Start all worker pools and monitor tasks.

        Args:
            run_id: The current run ID for task attribution.
        """
        if self._running:
            logger.warning("WorkerPool already running")
            return

        self._run_id = run_id
        self._shutdown_event.clear()
        self._retrieval_allowed.set()
        self._disk_space_ok.set()
        self._running = True
        self._paused = False

        # Reset counters
        for k in self._retrieval_counters:
            self._retrieval_counters[k] = 0
        for k in self._validation_counters:
            self._validation_counters[k] = 0

        min_disk = self._config.get(
            "min_disk_space_bytes", DEFAULT_MIN_DISK_SPACE_BYTES
        )

        # One-shot stale claim reaper BEFORE workers start.
        # Clears any papers left stuck in RETRIEVING/VALIDATING by a crashed
        # or killed prior run, so fresh workers don't find the queue empty
        # while stale papers hoard their claims.
        try:
            initial_reset = await run_stale_claim_reaper_once(self._db)
            if initial_reset > 0:
                logger.warning(
                    "WorkerPool startup: stale claim reaper reset %d papers",
                    initial_reset,
                )
        except Exception as exc:
            logger.error(
                "WorkerPool startup: stale claim reaper failed: %s", exc
            )

        # Start retrieval workers
        for i in range(self._retrieval_concurrency):
            wid = generate_worker_id(f"ret-{i}")
            task = asyncio.create_task(
                retrieval_worker(
                    worker_id=wid,
                    db=self._db,
                    engine=self._engine,
                    run_id=run_id,
                    semaphore=self._retrieval_semaphore,
                    shutdown_event=self._shutdown_event,
                    retrieval_allowed=self._retrieval_allowed,
                    papers_processed_counter=self._retrieval_counters,
                    output_dir=self._output_dir,
                    min_disk_space=min_disk,
                ),
                name=f"retrieval-worker-{i}",
            )
            self._retrieval_tasks.append(task)

        # Start validation workers
        for i in range(self._validation_concurrency):
            wid = generate_worker_id(f"val-{i}")
            task = asyncio.create_task(
                validation_worker(
                    worker_id=wid,
                    db=self._db,
                    engine=self._engine,
                    run_id=run_id,
                    semaphore=self._validation_semaphore,
                    shutdown_event=self._shutdown_event,
                    papers_processed_counter=self._validation_counters,
                ),
                name=f"validation-worker-{i}",
            )
            self._validation_tasks.append(task)

        # Start backpressure monitor
        bp_task = asyncio.create_task(
            self._backpressure_monitor(),
            name="backpressure-monitor",
        )
        self._monitor_tasks.append(bp_task)

        # Start disk space monitor
        disk_task = asyncio.create_task(
            self._disk_space_monitor(),
            name="disk-space-monitor",
        )
        self._monitor_tasks.append(disk_task)

        # Start stale claim reaper — runs every 10 minutes while active.
        # Any paper that remains in RETRIEVING or VALIDATING for more than
        # 30 minutes is assumed to have been abandoned by a crashed worker
        # and is reset so another worker can pick it up.
        reaper_task = asyncio.create_task(
            stale_claim_reaper_loop(
                db=self._db,
                shutdown_event=self._shutdown_event,
            ),
            name="stale-claim-reaper",
        )
        self._monitor_tasks.append(reaper_task)

        logger.info(
            "WorkerPool started: %d retrieval, %d validation workers (run=%s)",
            self._retrieval_concurrency,
            self._validation_concurrency,
            run_id,
        )

    async def stop(self, timeout_s: float = 30.0) -> int:
        """Graceful shutdown of all workers.

        1. Signal shutdown
        2. Wait up to timeout_s for workers to drain
        3. Cancel remaining tasks
        4. Return count of interrupted papers

        Returns count of papers that were interrupted.
        """
        if not self._running:
            return 0

        logger.info("WorkerPool stopping (timeout=%.0fs)...", timeout_s)

        # Signal shutdown
        self._shutdown_event.set()
        # Ensure retrieval is unblocked so workers can see the event
        self._retrieval_allowed.set()
        self._disk_space_ok.set()

        all_tasks = self._retrieval_tasks + self._validation_tasks + self._monitor_tasks

        # Wait for graceful drain
        if all_tasks:
            done, pending = await asyncio.wait(
                all_tasks,
                timeout=timeout_s,
            )

            # Force-cancel remaining
            for task in pending:
                task.cancel()
                try:
                    await task
                except (asyncio.CancelledError, Exception):
                    pass

            cancelled_count = len(pending)
            if cancelled_count > 0:
                logger.warning(
                    "Force-cancelled %d worker tasks after timeout",
                    cancelled_count,
                )

        # Reset state for interrupted papers
        interrupted = await self._db.reset_shutdown_states()

        # Clean up
        self._retrieval_tasks.clear()
        self._validation_tasks.clear()
        self._monitor_tasks.clear()
        self._running = False
        self._paused = False

        logger.info(
            "WorkerPool stopped: %d papers interrupted",
            interrupted,
        )
        return interrupted

    def pause(self) -> None:
        """Pause retrieval workers (validation continues)."""
        if not self._paused:
            self._retrieval_allowed.clear()
            self._paused = True
            logger.info("WorkerPool: retrieval paused")

    def resume(self) -> None:
        """Resume retrieval workers."""
        if self._paused:
            self._retrieval_allowed.set()
            self._disk_space_ok.set()
            self._paused = False
            logger.info("WorkerPool: retrieval resumed")

    @property
    def is_running(self) -> bool:
        """Whether the worker pool is currently active."""
        return self._running

    @property
    def is_paused(self) -> bool:
        """Whether retrieval is currently paused."""
        return self._paused

    # -----------------------------------------------------------------------
    # Backpressure monitor
    # -----------------------------------------------------------------------

    async def _backpressure_monitor(self) -> None:
        """Monitor weighted backpressure depth and pause/resume retrieval.

        Runs continuously until shutdown.
        """
        threshold = self._config.get(
            "backpressure_threshold", DEFAULT_BACKPRESSURE_THRESHOLD
        )

        while not self._shutdown_event.is_set():
            try:
                # Count papers in RETRIEVED state
                state_counts = await self._db.count_papers_by_state()
                retrieved_count = state_counts.get(
                    PaperState.RETRIEVED.value, 0
                )

                # Count OCR-pending among RETRIEVED
                ocr_count = await self._db.read_scalar(
                    "SELECT COUNT(*) FROM papers "
                    "WHERE state = ? AND ocr_applied = 1",
                    (PaperState.RETRIEVED.value,),
                ) or 0

                weighted = compute_weighted_backpressure(
                    retrieved_count, ocr_count
                )

                if weighted > threshold and not self._paused:
                    self._retrieval_allowed.clear()
                    logger.info(
                        "Backpressure: weighted depth %d > threshold %d, "
                        "pausing retrieval",
                        weighted, threshold,
                    )
                elif weighted <= threshold and not self._paused:
                    if not self._retrieval_allowed.is_set():
                        self._retrieval_allowed.set()
                        logger.info(
                            "Backpressure: weighted depth %d <= threshold %d, "
                            "resuming retrieval",
                            weighted, threshold,
                        )

            except Exception as exc:
                logger.error("Backpressure monitor error: %s", exc)

            # Wait before next check
            try:
                await asyncio.wait_for(
                    self._shutdown_event.wait(),
                    timeout=BACKPRESSURE_CHECK_INTERVAL_S,
                )
            except asyncio.TimeoutError:
                pass

    # -----------------------------------------------------------------------
    # Disk space monitor
    # -----------------------------------------------------------------------

    async def _disk_space_monitor(self) -> None:
        """Monitor available disk space and pause all workers if low.

        Runs continuously until shutdown.
        """
        min_space = self._config.get(
            "min_disk_space_bytes", DEFAULT_MIN_DISK_SPACE_BYTES
        )

        while not self._shutdown_event.is_set():
            try:
                free = check_disk_space(self._output_dir)

                if free > 0 and free < min_space:
                    if self._disk_space_ok.is_set():
                        self._disk_space_ok.clear()
                        self._retrieval_allowed.clear()
                        logger.warning(
                            "Disk space low: %d bytes free (minimum %d). "
                            "All workers paused. Free space and click Resume.",
                            free, min_space,
                        )
                elif not self._disk_space_ok.is_set():
                    # Space recovered
                    self._disk_space_ok.set()
                    if not self._paused:
                        self._retrieval_allowed.set()
                    logger.info(
                        "Disk space recovered: %d bytes free", free
                    )

            except Exception as exc:
                logger.error("Disk space monitor error: %s", exc)

            try:
                await asyncio.wait_for(
                    self._shutdown_event.wait(),
                    timeout=DISK_CHECK_INTERVAL_S,
                )
            except asyncio.TimeoutError:
                pass

    # -----------------------------------------------------------------------
    # Status reporting
    # -----------------------------------------------------------------------

    def get_status(self) -> Dict[str, Any]:
        """Return current worker pool status for the health dashboard."""
        retrieval_active = sum(
            1 for t in self._retrieval_tasks if not t.done()
        )
        validation_active = sum(
            1 for t in self._validation_tasks if not t.done()
        )

        return {
            "running": self._running,
            "paused": self._paused,
            "retrieval_workers_configured": self._retrieval_concurrency,
            "retrieval_workers_active": retrieval_active,
            "retrieval_paused": not self._retrieval_allowed.is_set(),
            "validation_workers_configured": self._validation_concurrency,
            "validation_workers_active": validation_active,
            "disk_space_ok": self._disk_space_ok.is_set(),
            "counters": {
                **self._retrieval_counters,
                **self._validation_counters,
            },
            "run_id": self._run_id,
        }
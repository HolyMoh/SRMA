"""
Full-Text Acquisition System — FastAPI Application

Main entry point. Handles startup sequence, graceful shutdown,
API endpoints, SSE health dashboard, and serves the single-page UI.

Launch: python -m full_text_acquisition.app
"""

from __future__ import annotations

import asyncio
import csv
import io
import json
import logging
import os
import platform
import shutil
import signal
import subprocess
import sys
import time
import traceback
import uuid
import webbrowser
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, AsyncIterator, Dict, List, Optional

import uvicorn
from fastapi import FastAPI, File, Form, HTTPException, Query, Request, UploadFile
from fastapi.responses import (
    FileResponse,
    HTMLResponse,
    JSONResponse,
    StreamingResponse,
)
from fastapi.staticfiles import StaticFiles

from full_text_acquisition.browser_manager import (
    BrowserManager,
    install_chromium,
    is_chromium_installed,
    is_playwright_installed,
)
from full_text_acquisition.database import Database
from full_text_acquisition.models import (
    COLUMN_ALIASES,
    CURRENT_CONFIG_VERSION,
    DEFAULT_BACKPRESSURE_THRESHOLD,
    DEFAULT_CACHE_TTL_DAYS,
    DEFAULT_CONFIG,
    AuditLogEntry,
    ColumnMapping,
    ConfigSnapshot,
    DeduplicationMatch,
    DeduplicationReport,
    EnrichmentResult,
    ExportRequest,
    FailureCode,
    HealthMetrics,
    OverrideRequest,
    Paper,
    PaperResponse,
    PaperRun,
    PaperState,
    PrismaReport,
    RetryRequest,
    RunRecord,
    RunStatus,
    SettingsResponse,
    SettingsUpdate,
    UploadResponse,
    auto_detect_columns,
    extract_first_author_lastname,
    generate_canonical_id,
    normalize_doi,
    normalize_title,
)
from full_text_acquisition.retrieval_engine import RetrievalEngine
from full_text_acquisition.workers import WorkerPool

logger = logging.getLogger("full_text_acquisition")

# ---------------------------------------------------------------------------
# Global application state (set during lifespan)
# ---------------------------------------------------------------------------
app_state: Dict[str, Any] = {}


# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------

def setup_logging(log_dir: str = ".") -> None:
    """Configure console + rotating file logging."""
    os.makedirs(log_dir, exist_ok=True)

    root_logger = logging.getLogger("full_text_acquisition")
    root_logger.setLevel(logging.DEBUG)

    # Console handler — INFO level
    console = logging.StreamHandler(sys.stdout)
    console.setLevel(logging.INFO)
    console.setFormatter(logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    ))
    root_logger.addHandler(console)

    # File handler — DEBUG level
    log_path = os.path.join(log_dir, "acquisition.log")
    try:
        from logging.handlers import RotatingFileHandler
        file_handler = RotatingFileHandler(
            log_path, maxBytes=10_000_000, backupCount=3,
        )
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(logging.Formatter(
            "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        ))
        root_logger.addHandler(file_handler)
    except Exception as exc:
        root_logger.warning("Could not set up file logging: %s", exc)


# ---------------------------------------------------------------------------
# Config management
# ---------------------------------------------------------------------------

CONFIG_PATH = "config.json"


def load_config() -> Dict[str, Any]:
    """Load config from config.json, or create with defaults if missing."""
    if os.path.isfile(CONFIG_PATH):
        try:
            with open(CONFIG_PATH, "r") as f:
                config = json.load(f)
            logger.info("Config loaded from %s (version %s)",
                        CONFIG_PATH, config.get("config_version", "?"))
            return config
        except (json.JSONDecodeError, OSError) as exc:
            logger.error("Failed to read config.json: %s — using defaults", exc)

    # Create default config
    config = dict(DEFAULT_CONFIG)
    save_config(config)
    logger.info("Created default config.json (version %d)", CURRENT_CONFIG_VERSION)
    return config


def save_config(config: Dict[str, Any]) -> None:
    """Persist config to config.json atomically."""
    tmp_path = CONFIG_PATH + ".tmp"
    try:
        with open(tmp_path, "w") as f:
            json.dump(config, f, indent=2)
        os.replace(tmp_path, CONFIG_PATH)
    except Exception as exc:
        logger.error("Failed to save config: %s", exc)
        try:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)
        except OSError:
            pass


# ---------------------------------------------------------------------------
# First-launch setup wizard
# ---------------------------------------------------------------------------

class WizardResult:
    """Collects first-launch wizard check results."""

    def __init__(self) -> None:
        self.python_ok: bool = False
        self.python_version: str = ""
        self.dependencies_ok: bool = False
        self.missing_deps: List[str] = []
        self.playwright_ok: bool = False
        self.chromium_ok: bool = False
        self.chromium_installed_now: bool = False
        self.tesseract_available: bool = False
        self.ghostscript_available: bool = False
        self.ocr_available: bool = False
        self.ocr_mode: str = "disabled"  # "full" | "tesseract_only" | "disabled"
        self.config_ok: bool = False
        self.config_migrated: bool = False
        self.errors: List[str] = []

    def to_dict(self) -> Dict[str, Any]:
        return {
            "python_ok": self.python_ok,
            "python_version": self.python_version,
            "dependencies_ok": self.dependencies_ok,
            "missing_deps": self.missing_deps,
            "playwright_ok": self.playwright_ok,
            "chromium_ok": self.chromium_ok,
            "chromium_installed_now": self.chromium_installed_now,
            "tesseract_available": self.tesseract_available,
            "ghostscript_available": self.ghostscript_available,
            "ocr_available": self.ocr_available,
            "ocr_mode": self.ocr_mode,
            "config_ok": self.config_ok,
            "config_migrated": self.config_migrated,
            "errors": self.errors,
        }


async def run_wizard(config: Dict[str, Any], db: Database) -> WizardResult:
    """Execute the first-launch setup wizard.

    Checks:
        1. Python version >= 3.9
        2. All pip dependencies importable
        3. Playwright Chromium installed (auto-install if missing)
        4. tesseract on PATH
        5. ghostscript (gs) on PATH
        6. config.json version current
    """
    result = WizardResult()

    # 1. Python version
    version_info = sys.version_info
    result.python_version = f"{version_info.major}.{version_info.minor}.{version_info.micro}"
    result.python_ok = version_info >= (3, 9)
    if not result.python_ok:
        result.errors.append(
            f"Python >= 3.9 required, found {result.python_version}"
        )

    # 2. Dependencies
    required_modules = [
        ("fastapi", "fastapi"),
        ("uvicorn", "uvicorn"),
        ("httpx", "httpx"),
        ("aiosqlite", "aiosqlite"),
        ("pydantic", "pydantic"),
    ]
    optional_modules = [
        ("openpyxl", "openpyxl"),
        ("fitz", "PyMuPDF"),
        ("pypdf", "pypdf"),
    ]
    result.dependencies_ok = True
    for module_name, pip_name in required_modules:
        try:
            __import__(module_name)
        except ImportError:
            result.missing_deps.append(pip_name)
            result.dependencies_ok = False

    for module_name, pip_name in optional_modules:
        try:
            __import__(module_name)
        except ImportError:
            logger.info("Optional dependency %s not installed", pip_name)

    if not result.dependencies_ok:
        result.errors.append(
            f"Missing required packages: {', '.join(result.missing_deps)}"
        )

    # 3. Playwright + Chromium
    result.playwright_ok = is_playwright_installed()
    if result.playwright_ok:
        result.chromium_ok = is_chromium_installed()
        if not result.chromium_ok:
            logger.info("Chromium not installed — attempting auto-install...")
            installed = await install_chromium()
            if installed:
                result.chromium_ok = True
                result.chromium_installed_now = True
                logger.info("Chromium auto-installed successfully")
            else:
                result.errors.append(
                    "Playwright Chromium not installed. "
                    "Run: playwright install chromium"
                )
    else:
        result.errors.append(
            "Playwright not installed. Run: pip install playwright"
        )

    # 4. tesseract
    result.tesseract_available = _check_binary("tesseract")
    if not result.tesseract_available:
        logger.info("tesseract not found on PATH — OCR will be limited")

    # 5. ghostscript
    result.ghostscript_available = _check_binary("gs")
    if not result.ghostscript_available:
        # Try Windows name
        result.ghostscript_available = _check_binary("gswin64c")
    if not result.ghostscript_available:
        logger.info("ghostscript not found on PATH — ocrmypdf disabled")

    # Determine OCR mode
    if result.tesseract_available and result.ghostscript_available:
        result.ocr_available = True
        result.ocr_mode = "full"
    elif result.tesseract_available:
        result.ocr_available = True
        result.ocr_mode = "tesseract_only"
    else:
        result.ocr_available = False
        result.ocr_mode = "disabled"

    # 6. Config version
    try:
        current_version = config.get("config_version", 0)
        if current_version < CURRENT_CONFIG_VERSION:
            config = await db.run_config_migration(config)
            save_config(config)
            result.config_migrated = True
        result.config_ok = True
    except ValueError as exc:
        result.errors.append(str(exc))
        result.config_ok = False

    logger.info(
        "Wizard complete: python=%s deps=%s playwright=%s chromium=%s "
        "tesseract=%s gs=%s ocr=%s config=%s",
        result.python_ok, result.dependencies_ok,
        result.playwright_ok, result.chromium_ok,
        result.tesseract_available, result.ghostscript_available,
        result.ocr_mode, result.config_ok,
    )

    return result


def _check_binary(name: str) -> bool:
    """Check if a binary is available on the system PATH."""
    return shutil.which(name) is not None


# ---------------------------------------------------------------------------
# FastAPI lifespan (startup + shutdown)
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(application: FastAPI) -> AsyncIterator[None]:
    """Full startup sequence and graceful shutdown.

    Startup (10 steps):
        1. Set WAL + foreign_keys pragmas (in db.initialize)
        2. Run config version check and migration
        3. Run first-launch wizard
        4. Run startup filesystem reconciliation
        5. Reset interrupted states
        6. Start SQLite write queue worker (in db.initialize)
        7. Initialize RetrievalEngine
        8. Initialize WorkerPool
        9. Store references in app_state
        10. Auto-open browser

    Shutdown (8 steps):
        1. Stop accepting new papers
        2. Signal shutdown
        3. Drain workers (30s timeout)
        4. Force-stop remaining
        5. Reset RETRIEVING/VALIDATING states
        6. Flush write queue
        7. Close browser manager
        8. Close database and log
    """
    # ---- STARTUP ----
    setup_logging()
    logger.info("=" * 60)
    logger.info("Full-Text Acquisition System starting...")
    logger.info("=" * 60)

    # Load config
    config = load_config()

    # Initialize database (steps 1, 6: WAL, FK, schema, write queue)
    db_path = config.get("database_path", "./acquisition.db")
    db = Database(db_path)
    await db.initialize()

    # Step 2: Config migration
    try:
        config = await db.run_config_migration(config)
        save_config(config)
    except ValueError as exc:
        logger.critical("Config version error: %s — exiting", exc)
        await db.close()
        sys.exit(1)

    # Step 3: First-launch wizard
    wizard = await run_wizard(config, db)
    app_state["wizard_result"] = wizard

    if wizard.errors:
        for err in wizard.errors:
            logger.warning("Wizard issue: %s", err)

    # Step 4: Reset interrupted states
    resets = await db.reset_interrupted_states()

    # Step 5: Startup filesystem reconciliation
    missing = await db.reconcile_filesystem()

    # Initialize browser manager
    browser_mgr = BrowserManager.get_instance()

    # Step 7: Initialize RetrievalEngine
    output_dir = config.get("output_directory", "./downloads")
    supplement_dir = config.get("supplement_directory", "./downloads/Supplements")
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(supplement_dir, exist_ok=True)

    engine = RetrievalEngine(
        db=db,
        browser_manager=browser_mgr,
        config=config,
        output_directory=output_dir,
        supplement_directory=supplement_dir,
    )
    engine.tesseract_available = wizard.tesseract_available
    engine.ghostscript_available = wizard.ghostscript_available

    # Step 8: Initialize WorkerPool
    worker_pool = WorkerPool(
        db=db,
        engine=engine,
        config=config,
        output_dir=output_dir,
    )

    # Step 9: Store in app_state
    app_state["config"] = config
    app_state["db"] = db
    app_state["browser_manager"] = browser_mgr
    app_state["engine"] = engine
    app_state["worker_pool"] = worker_pool
    app_state["shutdown_event"] = asyncio.Event()
    app_state["enrichment_status"] = {
        "in_progress": False,
        "run_id": None,
        "total": 0,
        "completed": 0,
        "enriched_count": 0,
        "failed_count": 0,
        "started_at": None,
        "completed_at": None,
    }
    app_state["enrichment_task"] = None

    # SSO session state — SERIALIZABLE SUBSET ONLY.
    # Never includes credentials, cookies, page content, or screenshots.
    # Only tracks progress and UI state.
    app_state["sso_session"] = {
        "active": False,
        "phase": "idle",               # idle | waiting_login | downloading | paused_reauth | ended
        "proxy_url": "",               # The EZproxy prefix in use
        "login_url": "",               # Where we navigated the browser on Start
        "started_at": None,
        "login_detected_at": None,
        "ended_at": None,
        "queue_total": 0,
        "queue_processed": 0,
        "queue_succeeded": 0,
        "queue_manual": 0,
        "current_paper_id": None,
        "current_paper_title": None,
        "last_message": "",
        "session_expired": False,
    }
    app_state["sso_task"] = None
    app_state["sso_cancel_event"] = None

    logger.info(
        "Startup complete: %d interrupted resets, %d missing files reconciled",
        len(resets), len(missing),
    )

    # Step 10: Auto-open browser (non-blocking)
    try:
        webbrowser.open("http://localhost:8000")
    except Exception:
        logger.info("Could not auto-open browser — navigate to http://localhost:8000")

    yield

    # ---- SHUTDOWN ----
    logger.info("Shutdown sequence initiated...")

    # Step 1-2: Signal shutdown
    shutdown_event: asyncio.Event = app_state.get("shutdown_event", asyncio.Event())
    shutdown_event.set()

    # Step 3-4: Drain workers
    wp: Optional[WorkerPool] = app_state.get("worker_pool")
    interrupted_count = 0
    if wp and wp.is_running:
        interrupted_count = await wp.stop(timeout_s=30.0)

    # Step 5: Reset states (handled by wp.stop → db.reset_shutdown_states)

    # Step 6: Flush write queue
    db_ref: Optional[Database] = app_state.get("db")
    if db_ref:
        await db_ref.flush_write_queue()

    # Step 7: Close browser manager
    bm: Optional[BrowserManager] = app_state.get("browser_manager")
    if bm:
        await bm.close_all()

    # Step 8: Close RetrievalEngine HTTP client
    eng: Optional[RetrievalEngine] = app_state.get("engine")
    if eng:
        await eng.close()

    # Close database
    if db_ref:
        await db_ref.close()

    logger.info(
        "Shutdown complete: %d papers interrupted",
        interrupted_count,
    )


# ---------------------------------------------------------------------------
# Create FastAPI app
# ---------------------------------------------------------------------------

app = FastAPI(
    title="Full-Text Acquisition System",
    description="Deterministic, auditable evidence acquisition for SRMAs",
    version="1.0.0",
    lifespan=lifespan,
)


# ---------------------------------------------------------------------------
# Signal handlers
# ---------------------------------------------------------------------------

def _install_signal_handlers(loop: asyncio.AbstractEventLoop) -> None:
    """Install SIGTERM and SIGINT handlers for graceful shutdown."""
    shutdown_event = app_state.get("shutdown_event")
    if not shutdown_event:
        return

    def _signal_handler(sig: int) -> None:
        sig_name = signal.Signals(sig).name
        logger.info("Received %s — initiating graceful shutdown", sig_name)
        shutdown_event.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, _signal_handler, sig)
        except (NotImplementedError, RuntimeError):
            # Windows doesn't support add_signal_handler
            pass


# ---------------------------------------------------------------------------
# Helper to get app state components
# ---------------------------------------------------------------------------

def _get_db() -> Database:
    db = app_state.get("db")
    if db is None:
        raise HTTPException(status_code=503, detail="Database not initialized")
    return db


def _get_engine() -> RetrievalEngine:
    engine = app_state.get("engine")
    if engine is None:
        raise HTTPException(status_code=503, detail="Engine not initialized")
    return engine


def _get_worker_pool() -> WorkerPool:
    wp = app_state.get("worker_pool")
    if wp is None:
        raise HTTPException(status_code=503, detail="Worker pool not initialized")
    return wp


def _get_config() -> Dict[str, Any]:
    return app_state.get("config", DEFAULT_CONFIG)


# ===========================================================================
# INGESTION ENDPOINTS
# ===========================================================================


@app.post("/api/upload", response_model=UploadResponse)
async def upload_file(
    file: UploadFile = File(...),
    output_directory_override: Optional[str] = Form(default=None),
) -> UploadResponse:
    """Upload a CSV or Excel file for ingestion.

    Performs:
        1. Parse file and detect columns
        2. DOI normalization
        3. Deduplication (exact DOI + fuzzy title)
        4. Metadata enrichment (OpenAlex → CrossRef)
        5. Canonical ID assignment
        6. ALREADY_RETRIEVED detection
        7. Create run record and paper_runs linkage

    output_directory_override: optional per-batch override. If valid,
    is recorded in the run's config_snapshot JSON and used (instead
    of the global default) when this run is Started. Must pass the
    same validation as /api/fs/check-path.
    """
    db = _get_db()
    config = _get_config()

    filename = file.filename or "upload"
    content = await file.read()

    # Parse file
    try:
        rows, headers = _parse_upload_file(content, filename)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    if not rows:
        raise HTTPException(status_code=400, detail="File contains no data rows")

    # Auto-detect columns
    detected = auto_detect_columns(headers)

    # Resolve the output directory for THIS run.
    # Order: per-batch override > global config setting > project default.
    effective_output_dir = config.get("output_directory", "./downloads")
    override_validation: Optional[Dict[str, Any]] = None
    if output_directory_override and output_directory_override.strip():
        override_validation = _validate_output_path(output_directory_override)
        if not override_validation["ok"]:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid output_directory_override: "
                       f"{override_validation.get('error') or 'unknown error'}",
            )
        effective_output_dir = override_validation["abs_path"]

    # Create run with snapshot reflecting the effective output_dir
    run_id = f"run-{uuid.uuid4().hex[:12]}"
    snapshot_config = dict(config)
    snapshot_config["output_directory"] = effective_output_dir
    config_snap = ConfigSnapshot.from_dict(snapshot_config)
    run_record = RunRecord(
        run_id=run_id,
        total_submitted=len(rows),
        config_snapshot=config_snap.to_json(),
    )
    await db.create_run(run_record)

    # Process rows
    papers: List[Paper] = []
    dedup_matches: List[DeduplicationMatch] = []
    seen_dois: Dict[str, str] = {}  # normalized_doi → first title
    already_retrieved_count = 0

    for row in rows:
        # Extract fields using detected columns
        raw_doi = _get_field(row, headers, detected.doi_column)
        raw_title = _get_field(row, headers, detected.title_column)
        raw_authors = _get_field(row, headers, detected.authors_column)
        raw_year = _get_field(row, headers, detected.year_column)
        raw_journal = _get_field(row, headers, detected.journal_column)

        # Normalize
        doi = normalize_doi(raw_doi)
        title = normalize_title(raw_title) if raw_title else ""
        authors = raw_authors.strip() if raw_authors else ""
        first_author = extract_first_author_lastname(authors)

        year: Optional[int] = None
        if raw_year:
            try:
                year = int(str(raw_year).strip()[:4])
            except (ValueError, TypeError):
                pass

        # Within-batch deduplication (exact DOI)
        if doi and doi in seen_dois:
            dedup_matches.append(DeduplicationMatch(
                paper_a_title=seen_dois[doi],
                paper_a_doi=doi,
                paper_b_title=raw_title or "",
                paper_b_doi=doi,
                match_type="exact_doi",
                similarity_score=1.0,
            ))
            continue
        if doi:
            seen_dois[doi] = raw_title or title

        # Generate canonical ID
        canonical_id = generate_canonical_id(
            doi=doi, title=raw_title, first_author_lastname=first_author, year=year,
        )

        # Cross-run ALREADY_RETRIEVED check
        prior_run = await db.check_already_retrieved(canonical_id)
        if prior_run:
            already_retrieved_count += 1
            # Register in paper_runs but don't re-retrieve
            paper_run = PaperRun(
                canonical_id=canonical_id,
                run_id=run_id,
                submitted_in_this_run=True,
                retrieval_attempted_in_this_run=False,
                outcome_in_this_run=FailureCode.ALREADY_RETRIEVED.value,
            )
            await db.create_paper_run(paper_run)
            await db.log_audit(AuditLogEntry(
                canonical_id=canonical_id,
                run_id=run_id,
                outcome="ALREADY_RETRIEVED",
                failure_code=FailureCode.ALREADY_RETRIEVED.value,
                details=json.dumps({"prior_run_id": prior_run}),
            ))
            continue

        # Check if paper exists from a prior failed run
        existing_state = await db.get_paper_state(canonical_id)
        if existing_state == PaperState.FAILED.value:
            # Reset for retry in this run
            await db.reset_paper_for_retry(canonical_id)
            paper_run = PaperRun(
                canonical_id=canonical_id,
                run_id=run_id,
                submitted_in_this_run=True,
            )
            await db.create_paper_run(paper_run)
            continue

        if existing_state is not None:
            # Paper exists in some other state — just link to this run
            paper_run = PaperRun(
                canonical_id=canonical_id,
                run_id=run_id,
                submitted_in_this_run=True,
            )
            await db.create_paper_run(paper_run)
            continue

        # Create new paper
        paper = Paper(
            canonical_id=canonical_id,
            doi=doi,
            title=raw_title or title,
            authors=authors,
            first_author_lastname=first_author,
            year=year,
            journal=raw_journal.strip() if raw_journal else None,
            state=PaperState.INGESTED.value,
            raw_doi=raw_doi,
            raw_title=raw_title,
            raw_authors=raw_authors,
            run_id=run_id,
        )
        papers.append(paper)

    # Bulk insert new papers
    inserted = await db.insert_papers_bulk(papers)

    # Transition INGESTED → NORMALIZED for all new papers (synchronous, fast)
    for paper in papers:
        await db.transition_state(paper.canonical_id, PaperState.NORMALIZED.value)

    # Create paper_runs for new papers (synchronous, fast)
    paper_runs = [
        PaperRun(canonical_id=p.canonical_id, run_id=run_id, submitted_in_this_run=True)
        for p in papers
    ]
    await db.create_paper_runs_bulk(paper_runs)

    # Update run count
    total_ready = inserted
    await db.update_run_submitted_count(run_id, len(rows))

    # Launch enrichment + downstream state transitions in the background.
    # The response below returns immediately; the UI polls /api/enrichment/status
    # (or consumes /api/enrichment/stream) to observe progress. Each paper
    # transitions NORMALIZED → ENRICHED → IDENTITY_ASSIGNED → READY_FOR_RETRIEVAL
    # as its enrichment finishes.
    if papers:
        app_state["enrichment_status"] = {
            "in_progress": True,
            "run_id": run_id,
            "total": len(papers),
            "completed": 0,
            "enriched_count": 0,
            "failed_count": 0,
            "started_at": datetime.now(timezone.utc).isoformat(),
            "completed_at": None,
        }
        canonical_ids = [p.canonical_id for p in papers]
        task = asyncio.create_task(
            _enrich_papers_background(canonical_ids, run_id),
            name=f"enrichment-{run_id}",
        )
        app_state["enrichment_task"] = task
    # No papers to enrich → status stays with in_progress=False

    enrichment: Optional[EnrichmentResult] = None  # Populated async; kept None in response

    # Build preview (first 10 rows)
    preview = []
    for row in rows[:10]:
        preview.append({h: row.get(h, "") for h in headers})

    dedup_report = DeduplicationReport(
        total_submitted=len(rows),
        unique_papers=inserted + already_retrieved_count,
        duplicates_removed=len(dedup_matches),
        matches=dedup_matches,
    )

    return UploadResponse(
        run_id=run_id,
        total_submitted=len(rows),
        total_after_dedup=inserted + already_retrieved_count,
        preview_rows=preview,
        detected_columns=detected,
        deduplication_report=dedup_report,
        enrichment_result=enrichment,
        already_retrieved_count=already_retrieved_count,
        ready_for_retrieval_count=inserted,
        output_directory=os.path.abspath(effective_output_dir),
    )


def _parse_upload_file(
    content: bytes, filename: str
) -> tuple[List[Dict[str, str]], List[str]]:
    """Parse CSV or Excel file content into rows and headers."""
    lower = filename.lower()

    if lower.endswith(".csv") or lower.endswith(".tsv"):
        text = content.decode("utf-8-sig", errors="replace")
        delimiter = "\t" if lower.endswith(".tsv") else ","
        reader = csv.DictReader(io.StringIO(text), delimiter=delimiter)
        headers = reader.fieldnames or []
        rows = [dict(row) for row in reader]
        return rows, list(headers)

    if lower.endswith((".xlsx", ".xls")):
        try:
            import openpyxl

            wb = openpyxl.load_workbook(io.BytesIO(content), read_only=True)
            ws = wb.active
            if ws is None:
                raise ValueError("Excel file has no active sheet")

            all_rows = list(ws.iter_rows(values_only=True))
            if not all_rows:
                return [], []

            headers = [str(h) if h else f"col_{i}" for i, h in enumerate(all_rows[0])]
            rows = []
            for data_row in all_rows[1:]:
                row_dict = {}
                for i, val in enumerate(data_row):
                    if i < len(headers):
                        row_dict[headers[i]] = str(val) if val is not None else ""
                rows.append(row_dict)

            wb.close()
            return rows, headers

        except ImportError:
            raise ValueError(
                "openpyxl is required for Excel files. "
                "Install with: pip install openpyxl"
            )

    raise ValueError(f"Unsupported file format: {filename}. Use CSV, TSV, or Excel.")


def _get_field(
    row: Dict[str, str],
    headers: List[str],
    column_name: Optional[str],
) -> Optional[str]:
    """Extract a field value from a row using the mapped column name."""
    if not column_name:
        return None
    value = row.get(column_name, "")
    return value.strip() if value else None


async def _enrich_papers_background(
    canonical_ids: List[str],
    run_id: str,
) -> None:
    """Background task: enrich papers + advance state to READY_FOR_RETRIEVAL.

    For each paper:
        1. OpenAlex API (primary)
        2. CrossRef API (fallback)
        3. Transition NORMALIZED → ENRICHED → IDENTITY_ASSIGNED → READY_FOR_RETRIEVAL
        4. Update live progress in app_state["enrichment_status"]

    Papers without a DOI skip API calls but still transition through the pipeline.
    All failures are non-fatal — a paper that can't be enriched still advances.
    """
    db = app_state.get("db")
    engine = app_state.get("engine")
    config = app_state.get("config", DEFAULT_CONFIG)
    status = app_state.get("enrichment_status")

    if db is None or engine is None or status is None:
        logger.error("Enrichment background: app_state not initialized")
        return

    logger.info(
        "Enrichment background: starting for run=%s, %d papers",
        run_id, len(canonical_ids),
    )

    try:
        for cid in canonical_ids:
            # Refresh paper from DB in case worker tasks touched it
            paper = await db.get_paper(cid)
            if paper is None:
                status["completed"] += 1
                continue

            enriched = False

            # Skip API calls for papers without a DOI
            if paper.doi:
                # Try OpenAlex first
                try:
                    oa_url = f"https://api.openalex.org/works/doi:{paper.doi}"
                    oa_data, _ = await engine._api_request(
                        url=oa_url,
                        doi=paper.doi,
                        api_name="openalex",
                        canonical_id=paper.canonical_id,
                        run_id=run_id,
                        tier="ENRICHMENT",
                    )
                    if oa_data:
                        filled = await _apply_enrichment_openalex(paper, oa_data, db)
                        if filled:
                            enriched = True
                except Exception as exc:
                    logger.debug(
                        "OpenAlex enrichment failed for %s: %s", paper.doi, exc
                    )

                # Fallback to CrossRef
                if not enriched:
                    try:
                        cr_url = f"https://api.crossref.org/works/{paper.doi}"
                        cr_data, _ = await engine._api_request(
                            url=cr_url,
                            doi=paper.doi,
                            api_name="crossref",
                            canonical_id=paper.canonical_id,
                            run_id=run_id,
                            tier="ENRICHMENT",
                        )
                        if cr_data:
                            msg = cr_data.get("message", cr_data)
                            filled = await _apply_enrichment_crossref(paper, msg, db)
                            if filled:
                                enriched = True
                    except Exception as exc:
                        logger.debug(
                            "CrossRef enrichment failed for %s: %s", paper.doi, exc
                        )

            # Always advance through the state machine even if enrichment failed
            try:
                await db.transition_state(cid, PaperState.ENRICHED.value)
                await db.transition_state(cid, PaperState.IDENTITY_ASSIGNED.value)
                await db.transition_state(cid, PaperState.READY_FOR_RETRIEVAL.value)
            except Exception as exc:
                logger.error(
                    "State transition failed for %s during enrichment: %s",
                    cid, exc,
                )

            # Update progress counters
            if enriched:
                status["enriched_count"] += 1
            else:
                status["failed_count"] += 1
            status["completed"] += 1

    except asyncio.CancelledError:
        logger.info("Enrichment background: cancelled")
        raise
    except Exception as exc:
        logger.error(
            "Enrichment background: unhandled exception: %s\n%s",
            exc, traceback.format_exc(),
        )
    finally:
        status["in_progress"] = False
        status["completed_at"] = datetime.now(timezone.utc).isoformat()

        try:
            await db.log_audit(AuditLogEntry(
                run_id=run_id,
                outcome="ENRICHMENT_COMPLETE",
                details=json.dumps({
                    "total": status["total"],
                    "completed": status["completed"],
                    "enriched": status["enriched_count"],
                    "failed": status["failed_count"],
                }),
            ))
        except Exception:
            pass

        logger.info(
            "Enrichment background: complete for run=%s (%d/%d enriched, %d without metadata)",
            run_id, status["enriched_count"], status["total"],
            status["failed_count"],
        )


async def _enrich_papers(
    papers: List[Paper],
    db: Database,
    config: Dict[str, Any],
) -> EnrichmentResult:
    """Enrich papers with metadata from OpenAlex and CrossRef.

    OpenAlex is primary; CrossRef is fallback.
    Results are cached in api_cache.
    """
    result = EnrichmentResult(
        total_papers=len(papers),
        enriched_count=0,
        enrichment_sources={},
        fields_filled={},
        failed_count=0,
    )

    engine = _get_engine()

    for paper in papers:
        if not paper.doi:
            result.failed_count += 1
            continue

        enriched = False
        source = ""

        # Try OpenAlex first
        try:
            oa_url = f"https://api.openalex.org/works/doi:{paper.doi}"
            oa_data, cache_hit = await engine._api_request(
                url=oa_url,
                doi=paper.doi,
                api_name="openalex",
                canonical_id=paper.canonical_id,
                tier="ENRICHMENT",
            )

            if oa_data:
                filled = await _apply_enrichment_openalex(paper, oa_data, db)
                if filled:
                    enriched = True
                    source = "openalex"
                    for field_name in filled:
                        result.fields_filled[field_name] = (
                            result.fields_filled.get(field_name, 0) + 1
                        )
        except Exception as exc:
            logger.debug("OpenAlex enrichment failed for %s: %s", paper.doi, exc)

        # Fallback to CrossRef
        if not enriched:
            try:
                cr_url = f"https://api.crossref.org/works/{paper.doi}"
                cr_data, cache_hit = await engine._api_request(
                    url=cr_url,
                    doi=paper.doi,
                    api_name="crossref",
                    canonical_id=paper.canonical_id,
                    tier="ENRICHMENT",
                )

                if cr_data:
                    msg = cr_data.get("message", cr_data)
                    filled = await _apply_enrichment_crossref(paper, msg, db)
                    if filled:
                        enriched = True
                        source = "crossref"
                        for field_name in filled:
                            result.fields_filled[field_name] = (
                                result.fields_filled.get(field_name, 0) + 1
                            )
            except Exception as exc:
                logger.debug("CrossRef enrichment failed for %s: %s", paper.doi, exc)

        if enriched:
            result.enriched_count += 1
            result.enrichment_sources[source] = (
                result.enrichment_sources.get(source, 0) + 1
            )
        else:
            result.failed_count += 1

    return result


async def _apply_enrichment_openalex(
    paper: Paper, data: Dict[str, Any], db: Database
) -> List[str]:
    """Apply OpenAlex enrichment data to a paper. Returns list of fields filled."""
    filled: List[str] = []
    updates: Dict[str, Any] = {}

    # PMID
    ext_ids = data.get("ids", {})
    if not paper.pmid and ext_ids.get("pmid"):
        pmid_url = ext_ids["pmid"]
        pmid = pmid_url.replace("https://pubmed.ncbi.nlm.nih.gov/", "").strip("/")
        if pmid:
            updates["pmid"] = pmid
            filled.append("pmid")

    # OpenAlex ID
    if not paper.openalex_id and data.get("id"):
        updates["openalex_id"] = data["id"]
        filled.append("openalex_id")

    # Title
    if not paper.title and data.get("title"):
        updates["title"] = data["title"]
        filled.append("title")

    # Authors
    if not paper.authors and data.get("authorships"):
        authors_list = []
        for authorship in data["authorships"]:
            author = authorship.get("author", {})
            name = author.get("display_name")
            if name:
                authors_list.append(name)
        if authors_list:
            updates["authors"] = "; ".join(authors_list)
            updates["first_author_lastname"] = extract_first_author_lastname(
                updates["authors"]
            )
            filled.append("authors")

    # Year
    if not paper.year and data.get("publication_year"):
        updates["year"] = data["publication_year"]
        filled.append("year")

    # Journal
    if not paper.journal:
        location = data.get("primary_location", {})
        source = location.get("source", {}) if location else {}
        journal_name = source.get("display_name") if source else None
        if journal_name:
            updates["journal"] = journal_name
            filled.append("journal")

    if updates:
        updates["enrichment_source"] = "openalex"
        updates["enriched_fields"] = json.dumps(filled)
        await db.update_paper_fields(paper.canonical_id, **updates)

    return filled


async def _apply_enrichment_crossref(
    paper: Paper, data: Dict[str, Any], db: Database
) -> List[str]:
    """Apply CrossRef enrichment data to a paper. Returns list of fields filled."""
    filled: List[str] = []
    updates: Dict[str, Any] = {}

    # Title
    titles = data.get("title", [])
    if not paper.title and titles:
        updates["title"] = titles[0]
        filled.append("title")

    # Authors
    if not paper.authors:
        authors_raw = data.get("author", [])
        if authors_raw:
            author_names = []
            for a in authors_raw:
                given = a.get("given", "")
                family = a.get("family", "")
                name = f"{family}, {given}".strip(", ")
                if name:
                    author_names.append(name)
            if author_names:
                updates["authors"] = "; ".join(author_names)
                updates["first_author_lastname"] = extract_first_author_lastname(
                    updates["authors"]
                )
                filled.append("authors")

    # Year
    if not paper.year:
        published = data.get("published-print", data.get("published-online", {}))
        date_parts = published.get("date-parts", [[]])
        if date_parts and date_parts[0]:
            try:
                updates["year"] = int(date_parts[0][0])
                filled.append("year")
            except (ValueError, TypeError, IndexError):
                pass

    # Journal
    if not paper.journal:
        container = data.get("container-title", [])
        if container:
            updates["journal"] = container[0]
            filled.append("journal")

    # Publisher (for publisher resolution Signal 2)
    publisher_name = data.get("publisher", "")
    if publisher_name:
        from full_text_acquisition.retrieval_engine import PublisherResolver
        resolved = PublisherResolver.resolve_from_crossref(publisher_name)
        if resolved:
            updates["publisher"] = resolved
            updates["publisher_signal_source"] = "crossref"

    if updates:
        updates["enrichment_source"] = "crossref"
        updates["enriched_fields"] = json.dumps(filled)
        await db.update_paper_fields(paper.canonical_id, **updates)

    return filled


# ===========================================================================
# RETRIEVAL CONTROL ENDPOINTS
# ===========================================================================


@app.post("/api/run/start")
async def start_run(
    run_id: Optional[str] = Query(default=None, description="Run ID to start. If None, starts the most recent run."),
) -> JSONResponse:
    """Start retrieval workers for a run.

    If run_id is not specified, starts the most recent run.
    """
    db = _get_db()
    wp = _get_worker_pool()

    if wp.is_running:
        raise HTTPException(
            status_code=409, detail="A run is already in progress. Pause or cancel first."
        )

    # Determine run_id
    if not run_id:
        runs = await db.list_runs(limit=1)
        if not runs:
            raise HTTPException(
                status_code=404, detail="No runs found. Upload a file first."
            )
        run_id = runs[0].run_id

    # Verify run exists
    run_record = await db.get_run(run_id)
    if run_record is None:
        raise HTTPException(status_code=404, detail=f"Run {run_id} not found")

    # Check there are papers to retrieve
    state_counts = await db.count_papers_by_state()
    ready_count = state_counts.get(PaperState.READY_FOR_RETRIEVAL.value, 0)
    retrieved_count = state_counts.get(PaperState.RETRIEVED.value, 0)

    if ready_count == 0 and retrieved_count == 0:
        raise HTTPException(
            status_code=400,
            detail="No papers are ready for retrieval or validation.",
        )

    # Apply per-run output_directory override (if any).
    # The override is stored in the run's config_snapshot at upload time.
    # Limitation: only one active run at a time, so retargeting the
    # engine's _output_dir on Start is safe. Concurrent runs are not
    # supported and would race; the WorkerPool already enforces single-run
    # semantics via wp.is_running.
    engine = _get_engine()
    try:
        snap = json.loads(run_record.config_snapshot or "{}")
        run_output_dir = snap.get("output_directory") or ""
        if run_output_dir:
            run_output_abs = os.path.abspath(os.path.expanduser(run_output_dir))
            os.makedirs(run_output_abs, exist_ok=True)
            engine._output_dir = run_output_abs
            engine._supplement_dir = os.path.join(run_output_abs, "Supplements")
            os.makedirs(engine._supplement_dir, exist_ok=True)
            logger.info(
                "Run %s: output directory set to %s (per-run override)",
                run_id, run_output_abs,
            )
    except Exception as exc:
        logger.warning("Failed to apply per-run output dir for %s: %s", run_id, exc)

    # Start workers
    await wp.start(run_id)

    logger.info("Run %s started (%d ready, %d awaiting validation)", run_id, ready_count, retrieved_count)

    return JSONResponse({
        "status": "started",
        "run_id": run_id,
        "ready_for_retrieval": ready_count,
        "awaiting_validation": retrieved_count,
    })


@app.post("/api/run/pause")
async def pause_run() -> JSONResponse:
    """Pause retrieval workers. Validation continues."""
    wp = _get_worker_pool()

    if not wp.is_running:
        raise HTTPException(status_code=409, detail="No run is in progress")

    if wp.is_paused:
        raise HTTPException(status_code=409, detail="Run is already paused")

    wp.pause()

    return JSONResponse({"status": "paused"})


@app.post("/api/run/resume")
async def resume_run() -> JSONResponse:
    """Resume paused retrieval workers."""
    wp = _get_worker_pool()

    if not wp.is_running:
        raise HTTPException(status_code=409, detail="No run is in progress")

    if not wp.is_paused:
        raise HTTPException(status_code=409, detail="Run is not paused")

    wp.resume()

    return JSONResponse({"status": "resumed"})


@app.post("/api/run/cancel")
async def cancel_run() -> JSONResponse:
    """Cancel the current run and stop all workers."""
    db = _get_db()
    wp = _get_worker_pool()

    if not wp.is_running:
        raise HTTPException(status_code=409, detail="No run is in progress")

    interrupted = await wp.stop(timeout_s=30.0)

    # Mark run as cancelled
    status = wp.get_status()
    current_run_id = status.get("run_id")
    if current_run_id:
        await db.complete_run(current_run_id, status=RunStatus.CANCELLED.value)

    return JSONResponse({
        "status": "cancelled",
        "papers_interrupted": interrupted,
    })


@app.post("/api/retry")
async def retry_papers(request: RetryRequest) -> JSONResponse:
    """Retry failed or mismatched papers.

    retry_type: 'failed' | 'mismatch' | 'manual' | 'all_eligible'
    canonical_ids: Optional specific papers to retry.
    """
    db = _get_db()

    count = await db.reset_papers_for_retry_bulk(
        canonical_ids=request.canonical_ids,
        retry_type=request.retry_type,
    )

    await db.log_audit(AuditLogEntry(
        outcome="RETRY_REQUESTED",
        details=json.dumps({
            "retry_type": request.retry_type,
            "canonical_ids": request.canonical_ids,
            "papers_reset": count,
        }),
    ))

    return JSONResponse({
        "status": "retry_queued",
        "papers_reset": count,
        "retry_type": request.retry_type,
    })


@app.post("/api/reset-and-retry-all")
async def reset_and_retry_all() -> JSONResponse:
    """Reset every non-COMPLETE / non-in-flight paper to READY_FOR_RETRIEVAL.

    Rescues papers stuck in:
      - NORMALIZED, ENRICHED, IDENTITY_ASSIGNED (interrupted ingestion)
      - FAILED, MANUAL_REQUIRED (prior tier exhaustion)
      - VALIDATED (only if identity is flagged — still needs review)

    Leaves alone:
      - COMPLETE, DUPLICATE (terminal success/dedup)
      - RETRIEVING, VALIDATING (active workers — don't disrupt)
      - READY_FOR_RETRIEVAL, RETRIEVED (already queued or in validation queue)
    """
    db = _get_db()

    async def _do_full_reset(conn: Any) -> int:
        now = datetime.now(timezone.utc).isoformat()
        rescue_states = (
            PaperState.NORMALIZED.value,
            PaperState.ENRICHED.value,
            PaperState.IDENTITY_ASSIGNED.value,
            PaperState.FAILED.value,
            PaperState.MANUAL_REQUIRED.value,
        )
        placeholders = ",".join("?" for _ in rescue_states)
        cursor = await conn.execute(
            f"UPDATE papers SET state = ?, previous_state = state, "
            f"worker_id = NULL, claimed_at = NULL, "
            f"last_failure_code = NULL, updated_at = ? "
            f"WHERE state IN ({placeholders})",
            (PaperState.READY_FOR_RETRIEVAL.value, now) + rescue_states,
        )
        await conn.commit()
        return cursor.rowcount

    count = await db._enqueue_write(_do_full_reset)

    await db.log_audit(AuditLogEntry(
        outcome="RESET_AND_RETRY_ALL",
        details=json.dumps({"papers_reset": count}),
    ))

    return JSONResponse({
        "status": "reset_complete",
        "papers_reset": count,
    })


@app.get("/api/runs")
async def list_runs(
    limit: int = Query(default=50, ge=1, le=200),
) -> JSONResponse:
    """List all runs ordered by most recent first."""
    db = _get_db()
    runs = await db.list_runs(limit=limit)

    return JSONResponse({
        "runs": [
            {
                "run_id": r.run_id,
                "started_at": r.started_at,
                "completed_at": r.completed_at,
                "status": r.status,
                "total_submitted": r.total_submitted,
            }
            for r in runs
        ]
    })


@app.get("/api/run/{run_id}")
async def get_run_detail(run_id: str) -> JSONResponse:
    """Get detailed information about a specific run."""
    db = _get_db()

    run_record = await db.get_run(run_id)
    if run_record is None:
        raise HTTPException(status_code=404, detail=f"Run {run_id} not found")

    paper_runs = await db.get_paper_runs_for_run(run_id)

    # Count outcomes
    outcomes: Dict[str, int] = {}
    for pr in paper_runs:
        outcome = pr.outcome_in_this_run or "PENDING"
        outcomes[outcome] = outcomes.get(outcome, 0) + 1

    return JSONResponse({
        "run_id": run_record.run_id,
        "started_at": run_record.started_at,
        "completed_at": run_record.completed_at,
        "status": run_record.status,
        "total_submitted": run_record.total_submitted,
        "outcome_counts": outcomes,
        "paper_count": len(paper_runs),
    })


# ===========================================================================
# SSO ENDPOINTS
# ===========================================================================


@app.post("/api/sso/start")
async def sso_start() -> JSONResponse:
    """Initiate SSO login by opening the institutional login page.

    Opens a headed browser and navigates to the configured SSO URL.
    The user completes authentication manually.
    ABSOLUTE RULE: Never access anything the user types.
    """
    config = _get_config()
    bm: BrowserManager = app_state.get("browser_manager")
    if bm is None:
        raise HTTPException(status_code=503, detail="Browser manager not initialized")

    # Determine SSO URL
    sso_url = (
        config.get("sso_proxy_url")
        or config.get("openathens_url")
        or config.get("institutional_resolver_url")
    )

    if not sso_url:
        raise HTTPException(
            status_code=400,
            detail="No SSO URL configured. Set sso_proxy_url, openathens_url, "
                   "or institutional_resolver_url in Settings.",
        )

    try:
        page = await bm.initiate_sso_login(sso_url)
        await _get_db().log_audit(AuditLogEntry(
            outcome="SSO_LOGIN_INITIATED",
            details=json.dumps({"sso_url": sso_url}),
        ))
        return JSONResponse({
            "status": "sso_login_page_opened",
            "sso_url": sso_url,
            "message": "Complete authentication in the browser window, then click Continue.",
        })
    except Exception as exc:
        logger.error("SSO start failed: %s", exc)
        raise HTTPException(status_code=500, detail=f"Failed to open SSO page: {exc}")


@app.post("/api/sso/continue")
async def sso_continue() -> JSONResponse:
    """User signals that SSO login is complete.

    Verifies the session appears active before allowing Tier 3 retrieval.
    """
    bm: BrowserManager = app_state.get("browser_manager")
    if bm is None:
        raise HTTPException(status_code=503, detail="Browser manager not initialized")

    session_ok = await bm.check_sso_session_valid()

    if session_ok:
        await _get_db().log_audit(AuditLogEntry(
            outcome="SSO_LOGIN_CONFIRMED",
        ))
        return JSONResponse({
            "status": "sso_active",
            "message": "SSO session confirmed. Tier 3 and 3.5 retrieval enabled.",
        })
    else:
        return JSONResponse(
            status_code=200,
            content={
                "status": "sso_uncertain",
                "message": "Session could not be verified. You may need to re-authenticate. "
                           "Tier 3 retrieval will attempt but may fail.",
            },
        )


@app.post("/api/sso/reauth")
async def sso_reauth() -> JSONResponse:
    """Re-authenticate SSO session.

    Destroys the current persistent context and opens a fresh login page.
    """
    config = _get_config()
    bm: BrowserManager = app_state.get("browser_manager")
    if bm is None:
        raise HTTPException(status_code=503, detail="Browser manager not initialized")

    sso_url = (
        config.get("sso_proxy_url")
        or config.get("openathens_url")
        or config.get("institutional_resolver_url")
    )

    if not sso_url:
        raise HTTPException(
            status_code=400,
            detail="No SSO URL configured.",
        )

    try:
        await bm.handle_session_expired(sso_url)
        await _get_db().log_audit(AuditLogEntry(
            outcome="SSO_REAUTH_INITIATED",
            failure_code=FailureCode.SESSION_EXPIRED.value,
        ))
        return JSONResponse({
            "status": "reauth_started",
            "sso_url": sso_url,
            "message": "Session destroyed. Complete re-authentication in the browser.",
        })
    except Exception as exc:
        logger.error("SSO re-auth failed: %s", exc)
        raise HTTPException(status_code=500, detail=f"Re-authentication failed: {exc}")


@app.get("/api/sso/status")
async def sso_status() -> JSONResponse:
    """Check current SSO session status."""
    bm: BrowserManager = app_state.get("browser_manager")
    if bm is None:
        return JSONResponse({
            "has_context": False,
            "session_valid": False,
        })

    has_ctx = bm.has_persistent_context
    session_valid = await bm.check_sso_session_valid() if has_ctx else False

    return JSONResponse({
        "has_context": has_ctx,
        "session_valid": session_valid,
    })


@app.get("/api/sso/queue-count")
async def sso_queue_count() -> JSONResponse:
    """Return the count of papers likely to require Tier 3 SSO retrieval.

    Papers with a DOI that are currently FAILED, READY_FOR_RETRIEVAL,
    or MANUAL_REQUIRED are candidates. Once the user logs in once, the
    persistent browser context is reused for ALL these papers without
    requiring login again for each one.
    """
    db = _get_db()
    config = _get_config()

    # Count papers that would flow to Tier 3 on next attempt
    ready = await db.read_scalar(
        "SELECT COUNT(*) FROM papers "
        "WHERE state IN (?, ?, ?) AND doi IS NOT NULL AND doi != ''",
        (
            PaperState.READY_FOR_RETRIEVAL.value,
            PaperState.FAILED.value,
            PaperState.MANUAL_REQUIRED.value,
        ),
    ) or 0

    # Papers that actually failed at a pre-Tier-3 stage (most relevant)
    failed_non_sso = await db.read_scalar(
        "SELECT COUNT(DISTINCT canonical_id) FROM audit_log "
        "WHERE failure_code IN (?, ?, ?)",
        (
            FailureCode.PAYWALL_DETECTED.value,
            FailureCode.ACCESS_DENIED.value,
            FailureCode.PUBLISHER_SOFT_BLOCK.value,
        ),
    ) or 0

    sso_configured = bool(
        config.get("sso_proxy_url") or config.get("openathens_url")
        or config.get("institutional_resolver_url")
    )

    return JSONResponse({
        "pending_retrieval": ready,
        "previously_failed_paywall": failed_non_sso,
        "sso_configured": sso_configured,
        "session_reuse_note": (
            "After you log in once, the browser session is reused "
            "for all subsequent papers until you click Re-authenticate "
            "or close the app."
        ),
    })


# ===========================================================================
# SSO OVERHAUL — User-Controlled, System-Assisted
# ===========================================================================
#
# Philosophy: the USER stays in full control of credentials. The SYSTEM
# does all the mechanical work (URL construction, navigation, PDF
# extraction, progress tracking). We never read, log, or store anything
# the user types, and we never store cookies outside the Playwright
# context (which is destroyed on session end).

# Default EZproxy prefix. Overridden by config.sso_proxy_url if set.
# UofT's EZproxy is the reference implementation.
DEFAULT_EZPROXY_PREFIX = "https://myaccess.library.utoronto.ca/login?url="
DEFAULT_EZPROXY_LOGIN_URL = "https://myaccess.library.utoronto.ca/login"
# Well-known open-access DOI used to probe login state. PLOS ONE's first
# article — publicly available, fast to load, and will resolve through
# any functioning institutional proxy.
SSO_LOGIN_PROBE_DOI = "10.1371/journal.pone.0000308"
DEFAULT_SSO_INTER_PAPER_DELAY_S = 3.0
SSO_LOGIN_POLL_INTERVAL_S = 3.0
SSO_REAUTH_POLL_INTERVAL_S = 5.0


def _ezproxy_prefix() -> str:
    """Return the configured EZproxy prefix, or the UofT default.

    Callers append the target article URL to this prefix.
    """
    raw = (_get_config().get("sso_proxy_url") or "").strip()
    if not raw:
        return DEFAULT_EZPROXY_PREFIX
    # Normalize to "...login?url=" form: users may paste either
    # `https://proxy.uni.edu` or `https://proxy.uni.edu/login?url=`.
    if raw.endswith("?url=") or raw.endswith("url="):
        return raw
    raw_stripped = raw.rstrip("/")
    if raw_stripped.endswith("/login"):
        return f"{raw_stripped}?url="
    return f"{raw_stripped}/login?url="


def _ezproxy_login_url() -> str:
    """Bare login URL (no ?url= suffix) — where Start SSO Session lands."""
    prefix = _ezproxy_prefix()
    if "?" in prefix:
        return prefix.split("?", 1)[0]
    return prefix


def _build_ezproxy_url(article_url: str) -> str:
    """Wrap an article URL with the EZproxy prefix."""
    return f"{_ezproxy_prefix()}{article_url}"


def _sso_test_url() -> str:
    """EZproxy-wrapped URL of the login probe DOI."""
    return _build_ezproxy_url(f"https://doi.org/{SSO_LOGIN_PROBE_DOI}")


# ---------------------------------------------------------------------------
# STEP 1 — Queue visibility
# ---------------------------------------------------------------------------


@app.get("/api/sso/queue")
async def sso_queue(
    run_id: Optional[str] = Query(default=None),
) -> JSONResponse:
    """List papers in the SSO queue (MANUAL_REQUIRED).

    For each paper returns:
        canonical_id, doi, title, first_author, year, journal,
        publisher, last_failure_code, expected_ezproxy_url, doi_url

    The expected_ezproxy_url is what the system will navigate the
    browser to when it processes the paper.
    """
    db = _get_db()
    papers = await db.get_manual_required_papers(run_id=run_id)

    prefix = _ezproxy_prefix()
    items: List[Dict[str, Any]] = []
    for p in papers:
        doi_url = f"https://doi.org/{p.doi}" if p.doi else None
        ezproxy_url = f"{prefix}{doi_url}" if doi_url else None
        items.append({
            "canonical_id": p.canonical_id,
            "doi": p.doi,
            "doi_url": doi_url,
            "title": p.title,
            "first_author": p.first_author_lastname,
            "year": p.year,
            "journal": p.journal,
            "publisher": p.publisher,
            "last_failure_code": p.last_failure_code,
            "expected_ezproxy_url": ezproxy_url,
            "attempt_count": p.attempt_count,
        })

    return JSONResponse({
        "total": len(items),
        "proxy_prefix": prefix,
        "login_url": _ezproxy_login_url(),
        "papers": items,
    })


# ---------------------------------------------------------------------------
# STEP 2 — Session initialization
# ---------------------------------------------------------------------------


@app.post("/api/sso/session/start")
async def sso_session_start() -> JSONResponse:
    """STEP 2 — Open the authenticated browser and begin the SSO flow.

    Opens a headed Playwright Chromium window with stealth injected.
    Navigates to the EZproxy bare login URL (no ?url= suffix).
    Starts a background task that:
      - Polls the browser (every 3s) for login completion
      - Once detected, processes the SSO queue paper-by-paper
      - Handles session expiry mid-queue (pause + wait for re-login)
      - Emits live progress into app_state['sso_session'] for SSE

    SECURITY:
      - We navigate the browser; the USER types credentials.
      - We never read input fields, cookies, or body text except for
        structural detection (URL + known-paywall selectors).
    """
    bm: BrowserManager = app_state.get("browser_manager")
    db: Database = _get_db()
    engine: RetrievalEngine = _get_engine()
    sso: Dict[str, Any] = app_state["sso_session"]

    if bm is None or db is None or engine is None:
        raise HTTPException(status_code=503, detail="System not initialized")

    if sso.get("active"):
        raise HTTPException(
            status_code=409,
            detail="An SSO session is already active. End it before starting a new one.",
        )

    login_url = _ezproxy_login_url()
    proxy_prefix = _ezproxy_prefix()

    try:
        await bm.initiate_sso_login(login_url)
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to open SSO login page: {exc}",
        )

    # Reset session state
    cancel_event = asyncio.Event()
    now_iso = datetime.now(timezone.utc).isoformat()
    sso.update({
        "active": True,
        "phase": "waiting_login",
        "proxy_url": proxy_prefix,
        "login_url": login_url,
        "started_at": now_iso,
        "login_detected_at": None,
        "ended_at": None,
        "queue_total": 0,
        "queue_processed": 0,
        "queue_succeeded": 0,
        "queue_manual": 0,
        "current_paper_id": None,
        "current_paper_title": None,
        "last_message": "Browser opened. Log in with your institution credentials. We never see what you type.",
        "session_expired": False,
    })
    app_state["sso_cancel_event"] = cancel_event

    # Launch background worker (queue processing after login detection)
    task = asyncio.create_task(
        _sso_queue_processor(cancel_event),
        name="sso-queue-processor",
    )
    app_state["sso_task"] = task

    await db.log_audit(AuditLogEntry(
        outcome="SSO_SESSION_STARTED",
        details=json.dumps({
            "login_url": login_url,
            "proxy_prefix": proxy_prefix,
        }),
    ))

    return JSONResponse({
        "status": "session_started",
        "login_url": login_url,
        "proxy_prefix": proxy_prefix,
        "message": sso["last_message"],
    })


@app.get("/api/sso/session/status")
async def sso_session_status_detailed() -> JSONResponse:
    """One-shot snapshot of the SSO session status.

    The same data is pushed continuously via the unified SSE stream
    (/api/stream → payload.sso). This endpoint exists for page-load
    bootstrap and ad-hoc reads.
    """
    sso = dict(app_state.get("sso_session") or {})
    # Never expose internal task handles
    sso.pop("_internal", None)
    return JSONResponse(sso)


@app.post("/api/sso/session/end")
async def sso_session_end() -> JSONResponse:
    """STEP 4b — Close the SSO browser and save progress.

    Destroys the Playwright persistent context. Papers not reached
    before session end remain MANUAL_REQUIRED for the next session.
    """
    bm: BrowserManager = app_state.get("browser_manager")
    sso = app_state["sso_session"]
    cancel_event: Optional[asyncio.Event] = app_state.get("sso_cancel_event")
    task: Optional[asyncio.Task] = app_state.get("sso_task")

    if not sso.get("active"):
        raise HTTPException(status_code=409, detail="No active SSO session")

    # Signal the worker to stop
    if cancel_event:
        cancel_event.set()
    if task and not task.done():
        try:
            await asyncio.wait_for(task, timeout=10.0)
        except asyncio.TimeoutError:
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass

    # Destroy browser context (security: no persistence outside session)
    if bm is not None:
        try:
            await bm.destroy_persistent_context(reason="user_ended_session")
        except Exception as exc:
            logger.debug("Error destroying persistent context: %s", exc)

    now_iso = datetime.now(timezone.utc).isoformat()
    sso.update({
        "active": False,
        "phase": "ended",
        "ended_at": now_iso,
        "last_message": "Session ended. Browser closed.",
        "current_paper_id": None,
        "current_paper_title": None,
    })
    app_state["sso_task"] = None
    app_state["sso_cancel_event"] = None

    try:
        await _get_db().log_audit(AuditLogEntry(
            outcome="SSO_SESSION_ENDED",
            details=json.dumps({
                "queue_processed": sso.get("queue_processed", 0),
                "queue_succeeded": sso.get("queue_succeeded", 0),
                "queue_manual": sso.get("queue_manual", 0),
            }),
        ))
    except Exception:
        pass

    return JSONResponse({
        "status": "session_ended",
        "queue_processed": sso.get("queue_processed", 0),
        "queue_succeeded": sso.get("queue_succeeded", 0),
        "queue_manual": sso.get("queue_manual", 0),
    })


# ---------------------------------------------------------------------------
# STEP 5 — Manual fallback helpers
# ---------------------------------------------------------------------------


@app.post("/api/sso/paper/{canonical_id}/mark-retrieved")
async def sso_mark_retrieved(
    canonical_id: str,
    note: str = Query(default=""),
) -> JSONResponse:
    """User signals they manually obtained a paper's PDF.

    Records an audit entry and sets user_override='MANUALLY_RETRIEVED'
    on the paper. The paper stays in MANUAL_REQUIRED — it's still
    not in the system's PDF store, but reviewers can see it's resolved.
    """
    db = _get_db()
    paper = await db.get_paper(canonical_id)
    if paper is None:
        raise HTTPException(status_code=404, detail="Paper not found")

    now_iso = datetime.now(timezone.utc).isoformat()
    await db.update_paper_fields(
        canonical_id,
        user_override="MANUALLY_RETRIEVED",
        override_reason=note or "User marked as manually retrieved outside the system",
        override_timestamp=now_iso,
    )
    await db.log_audit(AuditLogEntry(
        canonical_id=canonical_id,
        outcome="MANUALLY_RETRIEVED",
        details=json.dumps({"note": note or ""}),
    ))

    return JSONResponse({
        "status": "marked",
        "canonical_id": canonical_id,
    })


@app.get("/api/sso/manual-fallback")
async def sso_manual_fallback() -> JSONResponse:
    """List of papers still needing manual retrieval after SSO session ends."""
    db = _get_db()
    papers = await db.get_manual_required_papers()

    prefix = _ezproxy_prefix()
    items = []
    for p in papers:
        if p.user_override == "MANUALLY_RETRIEVED":
            continue  # already resolved by user
        doi_url = f"https://doi.org/{p.doi}" if p.doi else None
        items.append({
            "canonical_id": p.canonical_id,
            "title": p.title,
            "first_author": p.first_author_lastname,
            "year": p.year,
            "doi": p.doi,
            "doi_url": doi_url,
            "ezproxy_url": f"{prefix}{doi_url}" if doi_url else None,
            "publisher": p.publisher,
            "last_failure_code": p.last_failure_code,
        })
    return JSONResponse({"total": len(items), "papers": items})


@app.get("/api/sso/manual-fallback/csv")
async def sso_manual_fallback_csv() -> Any:
    """CSV export of the manual-fallback list for external retrieval tracking."""
    db = _get_db()
    papers = await db.get_manual_required_papers()

    prefix = _ezproxy_prefix()
    export_dir = os.path.join(
        _get_config().get("output_directory", "./downloads"), "exports"
    )
    os.makedirs(export_dir, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    filename = f"sso_manual_fallback_{timestamp}.csv"
    filepath = os.path.join(export_dir, filename)

    with open(filepath, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow([
            "canonical_id", "title", "first_author", "year", "doi",
            "doi_url", "ezproxy_url", "publisher", "last_failure_code",
            "user_override",
        ])
        for p in papers:
            doi_url = f"https://doi.org/{p.doi}" if p.doi else ""
            w.writerow([
                p.canonical_id, p.title, p.first_author_lastname,
                p.year or "", p.doi or "", doi_url,
                f"{prefix}{doi_url}" if doi_url else "",
                p.publisher, p.last_failure_code or "",
                p.user_override or "",
            ])
    return FileResponse(filepath, media_type="text/csv", filename=filename)


# ---------------------------------------------------------------------------
# STEP 3 — Background queue processor
# ---------------------------------------------------------------------------


async def _sso_queue_processor(cancel_event: asyncio.Event) -> None:
    """Background task: detect login, then process SSO queue.

    Phases:
        waiting_login  → probe every 3s until browser is authenticated
        downloading    → iterate MANUAL_REQUIRED papers
        paused_reauth  → session expired mid-queue; poll for re-login
        ended          → terminal

    Per-paper loop:
        1. Check session validity (if expired: enter paused_reauth)
        2. Reset paper to READY_FOR_RETRIEVAL so engine.tier3 can run
        3. Run engine.tier3_institutional_sso — this constructs the
           EZproxy URL, navigates, applies publisher-specific PDF
           extraction, downloads and atomically stores the PDF
        4. On success: advance through validation pipeline → COMPLETE
        5. On block/paywall: revert to MANUAL_REQUIRED with audit
        6. Sleep configured inter-paper delay before next
    """
    bm: BrowserManager = app_state.get("browser_manager")
    db: Database = app_state.get("db")
    engine: RetrievalEngine = app_state.get("engine")
    sso: Dict[str, Any] = app_state["sso_session"]
    config = _get_config()
    inter_paper_delay = float(
        config.get("sso_inter_paper_delay_s", DEFAULT_SSO_INTER_PAPER_DELAY_S)
    )

    # --- Phase: waiting_login ---
    sso["phase"] = "waiting_login"
    sso["last_message"] = "Waiting for you to log in..."
    login_detected = False
    while not cancel_event.is_set() and not login_detected:
        try:
            result = await bm.probe_sso_login(_sso_test_url())
            if result.get("authenticated"):
                login_detected = True
                sso["login_detected_at"] = datetime.now(timezone.utc).isoformat()
                sso["last_message"] = "\u2713 Session active — starting downloads"
                break
        except Exception as exc:
            logger.debug("SSO login probe error: %s", exc)

        try:
            await asyncio.wait_for(
                cancel_event.wait(), timeout=SSO_LOGIN_POLL_INTERVAL_S
            )
            break  # cancelled
        except asyncio.TimeoutError:
            pass

    if cancel_event.is_set():
        return

    # --- Phase: downloading ---
    sso["phase"] = "downloading"

    # Load queue
    try:
        papers = await db.get_manual_required_papers()
    except Exception as exc:
        sso["phase"] = "ended"
        sso["last_message"] = f"Could not load queue: {exc}"
        return
    sso["queue_total"] = len(papers)

    run_id = f"sso-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}"

    for idx, paper in enumerate(papers, start=1):
        if cancel_event.is_set():
            break

        # --- session validity recheck ---
        try:
            probe = await bm.probe_sso_login(_sso_test_url())
            if not probe.get("authenticated"):
                sso["session_expired"] = True
                sso["phase"] = "paused_reauth"
                sso["last_message"] = "Session expired. Please log in again in the browser."

                # Wait for re-login
                while not cancel_event.is_set():
                    try:
                        probe2 = await bm.probe_sso_login(_sso_test_url())
                        if probe2.get("authenticated"):
                            sso["session_expired"] = False
                            sso["phase"] = "downloading"
                            sso["last_message"] = "\u2713 Session restored — resuming downloads"
                            break
                    except Exception:
                        pass
                    try:
                        await asyncio.wait_for(
                            cancel_event.wait(),
                            timeout=SSO_REAUTH_POLL_INTERVAL_S,
                        )
                        break
                    except asyncio.TimeoutError:
                        pass

                if cancel_event.is_set():
                    break
        except Exception as exc:
            logger.debug("SSO session probe failed: %s", exc)

        sso["current_paper_id"] = paper.canonical_id
        sso["current_paper_title"] = paper.title
        sso["queue_processed"] = idx
        sso["last_message"] = f"Paper {idx}/{len(papers)} — downloading..."

        # Reset paper state so tier3 can run — MANUAL_REQUIRED → READY_FOR_RETRIEVAL
        try:
            await db.reset_paper_for_retry(paper.canonical_id)
        except Exception as exc:
            logger.debug("Reset paper %s failed: %s", paper.canonical_id, exc)
            continue

        # Claim for retrieval so state machine is happy
        worker_id = "sso-worker"
        claimed = await db.claim_retrieval_task(paper.canonical_id, worker_id)
        if not claimed:
            # Another worker might have it — skip
            continue

        # Refresh paper (state is now RETRIEVING)
        fresh = await db.get_paper(paper.canonical_id)
        if fresh is None:
            continue

        # Run Tier 3 (SSO) only — reuses EZproxy URL construction +
        # publisher-specific PDF extraction + atomic write
        try:
            success, failure_code = await engine.tier3_institutional_sso(
                fresh, run_id
            )
        except Exception as exc:
            logger.error("SSO retrieval error for %s: %s", paper.canonical_id, exc)
            success, failure_code = False, "SSO_FAILED"

        if success:
            # Advance state RETRIEVING → RETRIEVED so validation kicks in
            await db.transition_state(
                paper.canonical_id, PaperState.RETRIEVED.value, run_id=run_id,
            )
            # Run validation pipeline inline so the paper reaches COMPLETE
            try:
                claimed_v = await db.claim_validation_task(
                    paper.canonical_id, worker_id
                )
                if claimed_v:
                    refreshed = await db.get_paper(paper.canonical_id)
                    if refreshed:
                        vstatus, istatus, score = await engine.run_validation_pipeline(
                            refreshed, run_id
                        )
                        await db.transition_state(
                            paper.canonical_id, PaperState.VALIDATED.value,
                            run_id=run_id,
                        )
                        # Auto-complete unless flagged
                        if istatus not in (
                            "VERSION_MISMATCH", "CONTENT_UNVERIFIED",
                            "TITLE_VERIFIED_WEAK",
                        ):
                            await db.transition_state(
                                paper.canonical_id, PaperState.COMPLETE.value,
                                run_id=run_id,
                            )
            except Exception as exc:
                logger.debug("SSO validation error for %s: %s", paper.canonical_id, exc)
            sso["queue_succeeded"] += 1
        else:
            # Revert to MANUAL_REQUIRED so user can act
            try:
                await db.transition_state(
                    paper.canonical_id, PaperState.MANUAL_REQUIRED.value,
                    failure_code=failure_code or "SSO_FAILED",
                    run_id=run_id,
                )
            except Exception:
                pass
            sso["queue_manual"] += 1

        # Inter-paper delay
        if cancel_event.is_set():
            break
        try:
            await asyncio.wait_for(
                cancel_event.wait(), timeout=inter_paper_delay
            )
            break
        except asyncio.TimeoutError:
            pass

    sso["current_paper_id"] = None
    sso["current_paper_title"] = None
    if not cancel_event.is_set():
        sso["phase"] = "ended"
        sso["last_message"] = (
            f"Queue complete: {sso['queue_succeeded']} succeeded, "
            f"{sso['queue_manual']} still manual"
        )


# ===========================================================================
# CAPTCHA ENDPOINTS
# ===========================================================================


@app.post("/api/captcha/resolved")
async def captcha_resolved() -> JSONResponse:
    """User signals that a CAPTCHA has been solved.

    Logs the resolution and allows Scholar-Assisted Retrieval to resume.
    """
    await _get_db().log_audit(AuditLogEntry(
        outcome="CAPTCHA_RESOLVED",
        details=json.dumps({"resolved_by": "user"}),
    ))

    return JSONResponse({
        "status": "captcha_resolved",
        "message": "CAPTCHA marked as resolved. Retrieval will resume.",
    })


# ===========================================================================
# RESULTS & PAPER ENDPOINTS
# ===========================================================================


@app.get("/api/papers")
async def get_papers(
    run_id: Optional[str] = Query(default=None),
    state_filter: Optional[str] = Query(default=None, description="all|downloaded|partial|mismatch|failed|manual"),
    sort_by: str = Query(default="created_at"),
    sort_order: str = Query(default="ASC"),
    limit: int = Query(default=100, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
) -> JSONResponse:
    """Fetch papers for the results table with filters and pagination."""
    db = _get_db()

    papers, total_count = await db.get_results_table(
        run_id=run_id,
        state_filter=state_filter,
        sort_by=sort_by,
        sort_order=sort_order,
        limit=limit,
        offset=offset,
    )

    return JSONResponse({
        "papers": [PaperResponse.from_paper(p).model_dump() for p in papers],
        "total_count": total_count,
        "limit": limit,
        "offset": offset,
    })


@app.get("/api/pdf/{canonical_id}")
async def serve_paper_pdf(canonical_id: str) -> FileResponse:
    """Stream a paper's PDF inline so the browser renders it in a tab.

    SECURITY: validates that the resolved file path is inside the
    configured output_directory (path-traversal guard). If the database
    ever held a path like /etc/passwd (e.g. via a future bug), this
    endpoint refuses to serve it.

    Returns 404 if the paper is unknown or has no PDF on disk.
    Returns 403 if pdf_path resolves outside output_directory.
    """
    db = _get_db()
    config = _get_config()

    paper = await db.get_paper(canonical_id)
    if paper is None:
        raise HTTPException(status_code=404, detail="Paper not found")
    if not paper.pdf_path:
        raise HTTPException(status_code=404, detail="No PDF for this paper")

    # Path-traversal guard
    output_dir = os.path.abspath(
        config.get("output_directory", "./downloads")
    )
    pdf_abs = os.path.abspath(paper.pdf_path)

    try:
        common = os.path.commonpath([output_dir, pdf_abs])
    except ValueError:
        # Different drives on Windows etc.
        common = ""
    if common != output_dir:
        logger.warning(
            "Refusing to serve PDF outside output_directory: "
            "canonical_id=%s pdf_path=%s output_dir=%s",
            canonical_id, paper.pdf_path, output_dir,
        )
        raise HTTPException(
            status_code=403,
            detail="PDF path is outside the configured output directory",
        )

    if not os.path.isfile(pdf_abs):
        raise HTTPException(status_code=404, detail="PDF file missing on disk")

    # FileResponse streams the file; inline disposition makes the browser
    # render it in the tab rather than offering download
    filename = paper.pdf_filename or os.path.basename(pdf_abs)
    return FileResponse(
        pdf_abs,
        media_type="application/pdf",
        filename=filename,
        headers={"Content-Disposition": f'inline; filename="{filename}"'},
    )


@app.get("/api/paper/{canonical_id}")
async def get_paper_detail(canonical_id: str) -> JSONResponse:
    """Get full detail for a single paper including audit log."""
    db = _get_db()

    paper = await db.get_paper(canonical_id)
    if paper is None:
        raise HTTPException(status_code=404, detail="Paper not found")

    audit_entries = await db.get_audit_log_for_paper(canonical_id, limit=100)
    supplements = await db.get_supplements_for_paper(canonical_id)
    run_history = await db.get_paper_run_history(canonical_id)

    return JSONResponse({
        "paper": PaperResponse.from_paper(paper).model_dump(),
        "audit_log": [e.to_dict() for e in audit_entries],
        "supplements": [s.to_dict() for s in supplements],
        "run_history": [
            {
                "run_id": pr.run_id,
                "submitted": pr.submitted_in_this_run,
                "attempted": pr.retrieval_attempted_in_this_run,
                "outcome": pr.outcome_in_this_run,
            }
            for pr in run_history
        ],
    })


@app.post("/api/override")
async def apply_override(request: OverrideRequest) -> JSONResponse:
    """Apply a user validation override on a flagged paper.

    action='confirm_correct': Accept with free-text reason.
    action='re_retrieve': Reset to READY_FOR_RETRIEVAL.
    """
    db = _get_db()

    if request.action == "confirm_correct" and not request.reason.strip():
        raise HTTPException(
            status_code=400,
            detail="A reason is required when confirming a flagged paper.",
        )

    success = await db.apply_user_override(
        canonical_id=request.canonical_id,
        action=request.action,
        reason=request.reason,
    )

    if not success:
        raise HTTPException(status_code=404, detail="Paper not found or override failed")

    return JSONResponse({
        "status": "override_applied",
        "canonical_id": request.canonical_id,
        "action": request.action,
    })


# ===========================================================================
# EXPORT ENDPOINTS
# ===========================================================================


@app.get("/api/export/excel")
async def export_excel(
    run_id: Optional[str] = Query(default=None),
) -> FileResponse:
    """Export results as Excel (.xlsx) with two sheets.

    Sheet 1: Full results table.
    Sheet 2: Summary statistics.
    """
    db = _get_db()

    try:
        import openpyxl
    except ImportError:
        raise HTTPException(
            status_code=500,
            detail="openpyxl required for Excel export. Install: pip install openpyxl",
        )

    # Fetch data
    if run_id:
        papers = await db.get_papers_by_run(run_id, limit=10000)
    else:
        papers = await db.get_all_papers(limit=10000)

    summary = await db.get_summary_stats(run_id=run_id)

    # Build workbook
    wb = openpyxl.Workbook()

    # Sheet 1: Full results
    ws1 = wb.active
    ws1.title = "Results"
    headers = [
        "Canonical ID", "DOI", "PMID", "Title", "Authors", "Year", "Journal",
        "State", "Validation Status", "Identity Status", "Version Type",
        "Integrity Score", "Confidence", "Retrieval Tier", "Attempts",
        "Failure Code", "PDF Filename", "Size (bytes)", "Pages",
        "OCR Applied", "Supplements", "Override", "Override Reason",
        "Run ID", "Drift Version",
    ]
    ws1.append(headers)

    for p in papers:
        ws1.append([
            p.canonical_id, p.doi, p.pmid, p.title, p.authors, p.year,
            p.journal, p.state, p.validation_status, p.identity_status,
            p.version_type, p.integrity_score, p.confidence_level,
            p.retrieval_tier, p.attempt_count, p.last_failure_code,
            p.pdf_filename, p.pdf_size_bytes, p.pdf_page_count,
            "Yes" if p.ocr_applied else "No", p.supplement_count,
            p.user_override, p.override_reason, p.run_id,
            p.content_drift_version,
        ])

    # Sheet 2: Summary
    ws2 = wb.create_sheet("Summary")
    summary_rows = [
        ("Total Papers", summary.get("total_papers", 0)),
        ("Completed", summary.get("completed", 0)),
        ("Failed", summary.get("failed", 0)),
        ("Manual Required", summary.get("manual_required", 0)),
        ("Success Rate", f"{summary.get('success_rate', 0):.1%}"),
        ("OCR Count", summary.get("ocr_count", 0)),
        ("Content Drift Count", summary.get("content_drift_count", 0)),
        ("Overrides Confirmed", summary.get("overrides_confirmed", 0)),
        ("Overrides Re-retrieved", summary.get("overrides_reretried", 0)),
        ("Cooldown Events", summary.get("cooldown_events", 0)),
        ("Avg Retrieval Time (ms)", summary.get("avg_retrieval_time_ms", 0)),
        ("Avg Attempts", summary.get("avg_attempts", 0)),
        ("Already Retrieved", summary.get("already_retrieved_count", 0)),
    ]
    ws2.append(["Metric", "Value"])
    for metric, value in summary_rows:
        ws2.append([metric, value])

    # Score distribution
    ws2.append([])
    ws2.append(["Score Distribution", "Count"])
    for level, count in summary.get("score_distribution", {}).items():
        ws2.append([level, count])

    # Save to temp file
    export_dir = os.path.join(_get_config().get("output_directory", "./downloads"), "exports")
    os.makedirs(export_dir, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    filename = f"acquisition_report_{timestamp}.xlsx"
    filepath = os.path.join(export_dir, filename)
    wb.save(filepath)
    wb.close()

    return FileResponse(
        filepath,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename=filename,
    )


@app.get("/api/export/prisma")
async def export_prisma(
    run_id: str = Query(..., description="Run ID for PRISMA report"),
) -> FileResponse:
    """Export PRISMA 2020 compliance report as CSV for a specific run."""
    db = _get_db()

    report = await db.generate_prisma_report(run_id)

    # Write CSV
    export_dir = os.path.join(_get_config().get("output_directory", "./downloads"), "exports")
    os.makedirs(export_dir, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    filename = f"prisma_{run_id}_{timestamp}.csv"
    filepath = os.path.join(export_dir, filename)

    with open(filepath, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["Category", "Count"])
        writer.writerow(["Run ID", report.run_id])
        writer.writerow(["Total Sought", report.total_sought])
        writer.writerow(["Verified (Retrieved + Validated)", report.verified])
        writer.writerow(["Flagged (Needs Review)", report.flagged])
        writer.writerow(["Not Retrieved - No OA Source", report.not_retrieved_no_oa])
        writer.writerow(["Not Retrieved - Paywall", report.not_retrieved_paywall])
        writer.writerow(["Not Retrieved - Access Denied", report.not_retrieved_access_denied])
        writer.writerow(["Not Retrieved - Not Found", report.not_retrieved_not_found])
        writer.writerow(["Not Retrieved - Timeout", report.not_retrieved_timeout])
        writer.writerow(["Not Retrieved - Other", report.not_retrieved_other])
        writer.writerow(["Manual Required", report.manual_required])
        writer.writerow(["Already Retrieved (Prior Run)", report.already_retrieved])
        writer.writerow(["User Overrides - Confirmed", report.user_overrides_confirmed])
        writer.writerow(["User Overrides - Re-retrieved", report.user_overrides_reretried])

    return FileResponse(
        filepath,
        media_type="text/csv",
        filename=filename,
    )


@app.get("/api/export/audit")
async def export_audit(
    run_id: Optional[str] = Query(default=None),
) -> FileResponse:
    """Export complete audit log as JSONL (per run_id or all)."""
    db = _get_db()

    entries = await db.export_audit_log_jsonl(run_id=run_id)

    export_dir = os.path.join(_get_config().get("output_directory", "./downloads"), "exports")
    os.makedirs(export_dir, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    run_suffix = f"_{run_id}" if run_id else "_all"
    filename = f"audit_log{run_suffix}_{timestamp}.jsonl"
    filepath = os.path.join(export_dir, filename)

    with open(filepath, "w") as f:
        for entry in entries:
            f.write(json.dumps(entry, default=str) + "\n")

    return FileResponse(
        filepath,
        media_type="application/jsonl",
        filename=filename,
    )


@app.get("/api/export/integration")
async def export_integration(
    run_id: Optional[str] = Query(default=None),
) -> FileResponse:
    """Export integration JSON for downstream tools (ASReview, etc.)."""
    db = _get_db()

    exports = await db.build_integration_export_bulk(run_id=run_id, limit=10000)

    export_dir = os.path.join(_get_config().get("output_directory", "./downloads"), "exports")
    os.makedirs(export_dir, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    run_suffix = f"_{run_id}" if run_id else "_all"
    filename = f"integration{run_suffix}_{timestamp}.json"
    filepath = os.path.join(export_dir, filename)

    with open(filepath, "w") as f:
        json.dump(
            [e.model_dump() for e in exports],
            f, indent=2, default=str,
        )

    return FileResponse(
        filepath,
        media_type="application/json",
        filename=filename,
    )


@app.get("/api/export/bundle")
async def export_bundle(
    run_id: str = Query(..., description="Run ID to export (required)"),
) -> FileResponse:
    """Reproducible submission bundle for journal submission.

    Produces a single ZIP file containing everything a reviewer needs
    to verify the acquisition run:

        /config_snapshot.json      Exact config used at run start
        /audit_log.jsonl           Full audit trail (JSONL)
        /prisma_report.csv         PRISMA 2020 compliance report
        /integration_export.json   Structured per-paper export
        /sha256_manifest.txt       SHA-256 of every PDF (sha256sum format)
        /pdfs/                     All successfully retrieved PDFs
        /supplements/              All supplement files
        /README.txt                Explanation of each artifact

    SHA-256 manifest follows the `sha256sum` format:
        {64-hex-hash}  {filename}
    Each line can be verified with:  sha256sum -c sha256_manifest.txt
    """
    import hashlib
    import zipfile

    db = _get_db()

    # Verify run exists
    run_record = await db.get_run(run_id)
    if run_record is None:
        raise HTTPException(status_code=404, detail=f"Run {run_id} not found")

    export_dir = os.path.join(
        _get_config().get("output_directory", "./downloads"), "exports"
    )
    os.makedirs(export_dir, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    bundle_filename = f"submission_bundle_{run_id}_{timestamp}.zip"
    bundle_path = os.path.join(export_dir, bundle_filename)

    # Gather data
    papers = await db.get_papers_by_run(run_id, limit=10000)
    prisma = await db.generate_prisma_report(run_id)
    audit_entries = await db.export_audit_log_jsonl(run_id=run_id)
    integration = await db.build_integration_export_bulk(
        run_id=run_id, limit=10000
    )

    config_snapshot_json = run_record.config_snapshot or "{}"

    # Build PRISMA CSV in memory
    prisma_rows = [
        ("Category", "Count"),
        ("Run ID", prisma.run_id),
        ("Total Sought", prisma.total_sought),
        ("Verified (Retrieved + Validated)", prisma.verified),
        ("Flagged (Needs Review)", prisma.flagged),
        ("Not Retrieved - No OA Source", prisma.not_retrieved_no_oa),
        ("Not Retrieved - Paywall", prisma.not_retrieved_paywall),
        ("Not Retrieved - Access Denied", prisma.not_retrieved_access_denied),
        ("Not Retrieved - Not Found", prisma.not_retrieved_not_found),
        ("Not Retrieved - Timeout", prisma.not_retrieved_timeout),
        ("Not Retrieved - Other", prisma.not_retrieved_other),
        ("Manual Required", prisma.manual_required),
        ("Already Retrieved (Prior Run)", prisma.already_retrieved),
        ("User Overrides - Confirmed", prisma.user_overrides_confirmed),
        ("User Overrides - Re-retrieved", prisma.user_overrides_reretried),
    ]
    prisma_buf = io.StringIO()
    csv.writer(prisma_buf).writerows(prisma_rows)
    prisma_csv = prisma_buf.getvalue()

    # Audit log JSONL (one JSON object per line)
    audit_jsonl = "\n".join(
        json.dumps(e, default=str) for e in audit_entries
    )
    if audit_jsonl:
        audit_jsonl += "\n"

    # Integration JSON
    integration_json = json.dumps(
        [e.model_dump() for e in integration],
        indent=2, default=str,
    )

    # ---- Build the zip ----
    manifest_entries: List[str] = []  # lines for sha256_manifest.txt
    included_pdfs = 0
    included_supplements = 0
    skipped_missing = 0

    def sha256_of_file(path: str) -> str:
        h = hashlib.sha256()
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(65536), b""):
                h.update(chunk)
        return h.hexdigest()

    with zipfile.ZipFile(
        bundle_path, "w", compression=zipfile.ZIP_DEFLATED
    ) as zf:
        # Primary artifacts
        zf.writestr("config_snapshot.json", config_snapshot_json)
        zf.writestr("audit_log.jsonl", audit_jsonl)
        zf.writestr("prisma_report.csv", prisma_csv)
        zf.writestr("integration_export.json", integration_json)

        # PDFs
        for paper in papers:
            if not paper.pdf_path or not os.path.isfile(paper.pdf_path):
                if paper.pdf_path:
                    skipped_missing += 1
                continue
            # Use the stored filename, or derive from path
            arc_name = paper.pdf_filename or os.path.basename(paper.pdf_path)
            arc_path = f"pdfs/{arc_name}"
            zf.write(paper.pdf_path, arc_path)
            sha = paper.sha256_checksum or sha256_of_file(paper.pdf_path)
            manifest_entries.append(f"{sha}  {arc_path}")
            included_pdfs += 1

            # Supplements
            supplements = await db.get_supplements_for_paper(paper.canonical_id)
            for sup in supplements:
                if not sup.file_path or not os.path.isfile(sup.file_path):
                    continue
                # Namespace supplements by canonical_id so names don't collide
                safe_cid = paper.canonical_id.replace(":", "_").replace("/", "_")
                sup_arc = f"supplements/{safe_cid}/{sup.filename}"
                zf.write(sup.file_path, sup_arc)
                sup_sha = sup.sha256_checksum or sha256_of_file(sup.file_path)
                manifest_entries.append(f"{sup_sha}  {sup_arc}")
                included_supplements += 1

        # Manifest — also cover the JSON/CSV artifacts so a reviewer can
        # verify the whole bundle end-to-end.
        def sha_str(s: str) -> str:
            return hashlib.sha256(s.encode("utf-8")).hexdigest()
        manifest_entries.insert(0, f"{sha_str(config_snapshot_json)}  config_snapshot.json")
        manifest_entries.insert(1, f"{sha_str(audit_jsonl)}  audit_log.jsonl")
        manifest_entries.insert(2, f"{sha_str(prisma_csv)}  prisma_report.csv")
        manifest_entries.insert(3, f"{sha_str(integration_json)}  integration_export.json")
        manifest_text = "\n".join(manifest_entries) + "\n"
        zf.writestr("sha256_manifest.txt", manifest_text)

        # README for reviewers
        readme = _build_bundle_readme(
            run_record=run_record,
            prisma=prisma,
            included_pdfs=included_pdfs,
            included_supplements=included_supplements,
            skipped_missing=skipped_missing,
            total_audit_entries=len(audit_entries),
            total_papers=len(papers),
        )
        zf.writestr("README.txt", readme)

    # Audit log entry for the export itself
    await db.log_audit(AuditLogEntry(
        run_id=run_id,
        outcome="BUNDLE_EXPORTED",
        details=json.dumps({
            "bundle": bundle_filename,
            "pdfs_included": included_pdfs,
            "supplements_included": included_supplements,
            "pdfs_missing_on_disk": skipped_missing,
        }),
    ))

    return FileResponse(
        bundle_path,
        media_type="application/zip",
        filename=bundle_filename,
    )


def _build_bundle_readme(
    run_record: RunRecord,
    prisma: PrismaReport,
    included_pdfs: int,
    included_supplements: int,
    skipped_missing: int,
    total_audit_entries: int,
    total_papers: int,
) -> str:
    """Human-readable README explaining the bundle contents."""
    now = datetime.now(timezone.utc).isoformat()
    lines = [
        "FULL-TEXT ACQUISITION — SUBMISSION BUNDLE",
        "=" * 52,
        "",
        f"Run ID:              {run_record.run_id}",
        f"Run started:         {run_record.started_at}",
        f"Run completed:       {run_record.completed_at or '(still open)'}",
        f"Run status:          {run_record.status}",
        f"Bundle generated:    {now}",
        "",
        f"Papers submitted:    {run_record.total_submitted}",
        f"Papers in bundle:    {total_papers}",
        f"PDFs included:       {included_pdfs}",
        f"Supplements included:{included_supplements}",
        f"PDFs missing on disk (state/disk drift): {skipped_missing}",
        f"Audit log entries:   {total_audit_entries}",
        "",
        "-" * 52,
        "PRISMA 2020 SUMMARY",
        "-" * 52,
        f"Total sought:        {prisma.total_sought}",
        f"Verified:            {prisma.verified}",
        f"Flagged:             {prisma.flagged}",
        f"Manual required:     {prisma.manual_required}",
        f"Already retrieved:   {prisma.already_retrieved}",
        f"Overrides (confirmed):   {prisma.user_overrides_confirmed}",
        f"Overrides (re-retried):  {prisma.user_overrides_reretried}",
        "",
        "Not retrieved, by category:",
        f"  No OA source:      {prisma.not_retrieved_no_oa}",
        f"  Paywall:           {prisma.not_retrieved_paywall}",
        f"  Access denied:     {prisma.not_retrieved_access_denied}",
        f"  Not found:         {prisma.not_retrieved_not_found}",
        f"  Timeout:           {prisma.not_retrieved_timeout}",
        f"  Other:             {prisma.not_retrieved_other}",
        "",
        "-" * 52,
        "FILES IN THIS BUNDLE",
        "-" * 52,
        "",
        "config_snapshot.json",
        "  The exact configuration used at the start of this run. Contains",
        "  all timeouts, concurrency settings, rate limits, cooldown",
        "  parameters, and institutional URLs. Does NOT contain credentials",
        "  (the system never stores them).",
        "",
        "audit_log.jsonl",
        "  Complete event trail for this run. One JSON object per line.",
        "  Each entry records: timestamp, paper canonical_id, tier,",
        "  method, URL attempted, HTTP status, content type, outcome,",
        "  failure_code, execution time, and retry count. This is the",
        "  authoritative record of what the system did.",
        "",
        "prisma_report.csv",
        "  PRISMA 2020 compliance report. Drop this into your systematic",
        "  review flow diagram. All 'not retrieved' reasons are",
        "  subcategorized by cause (no OA / paywall / access denied /",
        "  not found / timeout / other).",
        "",
        "integration_export.json",
        "  Structured per-paper export ready for downstream tools",
        "  (ASReview, extraction pipelines, risk-of-bias assessment).",
        "  Each paper carries canonical_id, DOIs, PMID, OpenAlex ID,",
        "  PDF path inside this bundle, SHA-256, page count, version",
        "  type (published / accepted / preprint), integrity score,",
        "  identity status, user override (if any), and the full",
        "  retrieval log for that paper.",
        "",
        "sha256_manifest.txt",
        "  SHA-256 hash of every file in this bundle, in the standard",
        "  `sha256sum` format:",
        "      {64-hex-hash}  {path}",
        "  Verify end-to-end integrity with:",
        "      sha256sum -c sha256_manifest.txt",
        "  (Run this from the directory where you extracted the bundle.)",
        "",
        "pdfs/",
        "  All successfully retrieved and validated PDFs for this run.",
        "  Filename convention: FirstAuthorLastName_Year_First4Words.pdf",
        "  Content-drifted versions use a _v{N} suffix.",
        "",
        "supplements/",
        "  Supplementary materials, organized by paper canonical_id.",
        "  PDFs in this tree were validated the same way as primary",
        "  PDFs. Non-PDF supplements (DOCX, XLSX, CSV, ZIP) were",
        "  accepted based on size check only; their validation_status",
        "  is SUPPLEMENT_NON_PDF in the integration export.",
        "",
        "-" * 52,
        "REPRODUCIBILITY",
        "-" * 52,
        "",
        "The exact config and seeded randomness (seed = hash(run_id))",
        "used for this run are preserved in config_snapshot.json.",
        "Re-running with the same inputs and the same config should",
        "produce the same retrieval attempts — though third-party API",
        "responses may naturally have evolved since then (new open-access",
        "releases, changed publisher URLs, expired cache entries).",
        "",
        "VERSION_MISMATCH and CONTENT_UNVERIFIED papers were never",
        "auto-completed — any paper in those states required a",
        "recorded user override, visible in the audit log and the",
        "integration export.",
        "",
        "The system never accessed paywalls without a user-mediated",
        "institutional session, never captured or injected credentials,",
        "and followed publisher rate limits and robots.txt.",
        "",
    ]
    return "\n".join(lines)


# ===========================================================================
# FILESYSTEM VALIDATION
# ===========================================================================

# Belt-and-suspenders blocklist. The REAL test is the probe-write below;
# this just labels system-y paths so the UI can warn even if the path
# happens to be writable as root.
SYSTEM_PATH_PREFIXES_POSIX: List[str] = [
    "/etc", "/usr", "/bin", "/sbin", "/sys", "/proc", "/dev",
    "/var", "/opt", "/boot", "/root",
    "/Library", "/System", "/Applications", "/private",
]

SYSTEM_PATH_PREFIXES_WINDOWS: List[str] = [
    r"C:\Windows", r"C:\Program Files", r"C:\Program Files (x86)",
    r"C:\ProgramData", r"C:\Users\Default",
]


def _is_system_path(real_path: str) -> bool:
    """Return True if real_path is inside a known OS-managed directory.

    Uses path-prefix matching after realpath resolution. Case-insensitive
    on Windows-style paths.
    """
    if not real_path:
        return False

    # POSIX
    norm = os.path.normpath(real_path)
    for prefix in SYSTEM_PATH_PREFIXES_POSIX:
        if norm == prefix or norm.startswith(prefix + os.sep):
            return True

    # Windows
    norm_lower = norm.lower()
    for prefix in SYSTEM_PATH_PREFIXES_WINDOWS:
        p_lower = prefix.lower()
        if norm_lower == p_lower or norm_lower.startswith(p_lower + os.sep) \
                or norm_lower.startswith(p_lower + "/"):
            return True
    return False


def _human_bytes(b: int) -> str:
    """Pretty-print byte counts."""
    if not b or b <= 0:
        return "0 B"
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if b < 1024.0:
            return f"{b:.1f} {unit}"
        b /= 1024.0
    return f"{b:.1f} PB"


def _validate_output_path(raw_path: str) -> Dict[str, Any]:
    """Validate a candidate output_directory.

    Steps:
      1. Expand ~ and resolve to absolute path
      2. Resolve symlinks via os.path.realpath
      3. Check against system-path blocklist (warn, but do not auto-fail —
         labelled in the response so UI can decide)
      4. If path doesn't exist, check that the closest existing ancestor
         is writable (so we can mkdir)
      5. If path exists, attempt a probe-write of a tiny file and delete
         it immediately
      6. Compute free disk space at the resolved location
      7. Return structured result

    Returns a dict with at minimum:
        ok           bool   — overall verdict (writable AND not system)
        abs_path     str    — input expanded + made absolute
        real_path    str    — symlinks resolved
        exists       bool
        writable     bool   — actually verified by probe-write
        system_path  bool   — labelled by blocklist
        free_bytes   int
        free_human   str
        error        str | None — human-readable explanation if !ok
    """
    result: Dict[str, Any] = {
        "ok": False, "abs_path": "", "real_path": "",
        "exists": False, "writable": False, "system_path": False,
        "free_bytes": 0, "free_human": "0 B", "error": None,
    }
    if not raw_path or not raw_path.strip():
        result["error"] = "Path is empty"
        return result

    try:
        expanded = os.path.expanduser(raw_path.strip())
        abs_path = os.path.abspath(expanded)
        real_path = os.path.realpath(abs_path)
    except (OSError, ValueError) as exc:
        result["error"] = f"Cannot resolve path: {exc}"
        return result

    result["abs_path"] = abs_path
    result["real_path"] = real_path
    result["system_path"] = _is_system_path(real_path)

    if result["system_path"]:
        result["error"] = "Path is inside a system-managed directory"
        return result

    # Find the deepest existing ancestor — that's where mkdir-ability matters
    exists = os.path.exists(real_path)
    result["exists"] = exists
    probe_dir = real_path if exists else os.path.dirname(real_path)
    while probe_dir and not os.path.exists(probe_dir):
        parent = os.path.dirname(probe_dir)
        if parent == probe_dir:
            break
        probe_dir = parent

    if not probe_dir or not os.path.exists(probe_dir):
        result["error"] = "No writable ancestor directory exists"
        return result

    # Probe-write — the only authoritative writability test
    probe_name = f".permcheck_{uuid.uuid4().hex[:12]}"
    probe_path = os.path.join(probe_dir, probe_name)
    try:
        with open(probe_path, "w") as fh:
            fh.write("ok")
        result["writable"] = True
    except (OSError, PermissionError) as exc:
        result["error"] = f"Probe write failed: {exc}"
        result["writable"] = False
    finally:
        try:
            if os.path.exists(probe_path):
                os.unlink(probe_path)
        except OSError:
            pass

    # Free disk space at the deepest existing ancestor
    try:
        usage = shutil.disk_usage(probe_dir)
        result["free_bytes"] = usage.free
        result["free_human"] = _human_bytes(usage.free)
    except (OSError, AttributeError):
        pass

    result["ok"] = result["writable"] and not result["system_path"]
    return result


@app.post("/api/fs/check-path")
async def check_output_path(
    path: str = Query(..., description="Candidate output directory path"),
) -> JSONResponse:
    """Validate an output_directory candidate.

    Used by the live validator next to the path inputs in Settings and
    the per-batch override on Upload. Cheap (write+delete a single
    byte) but synchronous — clients should debounce calls.
    """
    return JSONResponse(_validate_output_path(path))


# ===========================================================================
# SETTINGS ENDPOINTS
# ===========================================================================


@app.get("/api/settings")
async def get_settings() -> JSONResponse:
    """Get current system settings and runtime status."""
    config = _get_config()
    wizard: Optional[WizardResult] = app_state.get("wizard_result")

    resp = SettingsResponse(
        config=config,
        ocr_available=wizard.ocr_available if wizard else False,
        tesseract_available=wizard.tesseract_available if wizard else False,
        ghostscript_available=wizard.ghostscript_available if wizard else False,
        playwright_installed=wizard.playwright_ok if wizard else False,
        python_version=wizard.python_version if wizard else platform.python_version(),
        platform=platform.system(),
    )

    return JSONResponse(resp.model_dump())


@app.put("/api/settings")
async def update_settings(update: SettingsUpdate) -> JSONResponse:
    """Update system configuration.

    Only provided (non-None) fields are updated.
    Changes are persisted to config.json immediately.
    """
    config = _get_config()

    updated_fields: List[str] = []
    update_dict = update.model_dump(exclude_none=True)

    for key, value in update_dict.items():
        if key in config and config[key] != value:
            config[key] = value
            updated_fields.append(key)
        elif key not in config:
            config[key] = value
            updated_fields.append(key)

    if updated_fields:
        save_config(config)
        app_state["config"] = config

        await _get_db().log_audit(AuditLogEntry(
            outcome="SETTINGS_UPDATED",
            details=json.dumps({
                "updated_fields": updated_fields,
                "new_values": {k: config[k] for k in updated_fields},
            }),
        ))

        logger.info("Settings updated: %s", updated_fields)

    return JSONResponse({
        "status": "updated",
        "fields_changed": updated_fields,
        "config": config,
    })


@app.post("/api/cache/clear")
async def clear_cache() -> JSONResponse:
    """Clear all API response cache entries."""
    db = _get_db()
    deleted = await db.clear_all_cache()
    await db.log_audit(AuditLogEntry(
        outcome="CACHE_CLEARED",
        details=json.dumps({"entries_deleted": deleted}),
    ))
    return JSONResponse({"status": "cleared", "entries_deleted": deleted})


@app.get("/api/cache/stats")
async def get_cache_stats() -> JSONResponse:
    """Get API cache statistics."""
    db = _get_db()
    stats = await db.get_cache_stats()
    return JSONResponse(stats)


@app.post("/api/wizard")
async def run_wizard_endpoint() -> JSONResponse:
    """Re-run the first-launch setup wizard manually."""
    config = _get_config()
    db = _get_db()

    wizard = await run_wizard(config, db)
    app_state["wizard_result"] = wizard

    # Update engine OCR flags
    engine = app_state.get("engine")
    if engine:
        engine.tesseract_available = wizard.tesseract_available
        engine.ghostscript_available = wizard.ghostscript_available

    return JSONResponse(wizard.to_dict())


@app.post("/api/cooldown/force")
async def force_cooldown(
    publisher: str = Query(...),
    minutes: int = Query(default=30, ge=1, le=120),
) -> JSONResponse:
    """Force a publisher into cooldown (manual override from UI)."""
    db = _get_db()
    success = await db.force_publisher_cooldown(publisher, minutes)
    if not success:
        raise HTTPException(status_code=500, detail="Failed to set cooldown")
    return JSONResponse({
        "status": "cooldown_set",
        "publisher": publisher,
        "minutes": minutes,
    })


@app.post("/api/cooldown/reset")
async def reset_cooldown(
    publisher: str = Query(...),
) -> JSONResponse:
    """Clear cooldown for a specific publisher."""
    db = _get_db()
    success = await db.reset_publisher_cooldown(publisher)
    return JSONResponse({
        "status": "cooldown_reset" if success else "not_found",
        "publisher": publisher,
    })


# ===========================================================================
# HEALTH DASHBOARD — SSE + SNAPSHOT
# ===========================================================================


@app.get("/api/enrichment/status")
async def enrichment_status() -> JSONResponse:
    """Return current enrichment background-task status (single snapshot)."""
    status = app_state.get("enrichment_status") or {
        "in_progress": False,
        "run_id": None,
        "total": 0,
        "completed": 0,
        "enriched_count": 0,
        "failed_count": 0,
        "started_at": None,
        "completed_at": None,
    }
    return JSONResponse(status)


@app.get("/api/enrichment/stream")
async def enrichment_stream(request: Request) -> StreamingResponse:
    """SSE stream of enrichment progress.

    Emits a status JSON every ~1s while enrichment is in progress.
    Sends one final event when in_progress becomes False, then closes.
    Closes immediately if the client disconnects or shutdown is signaled.
    """

    async def event_generator() -> AsyncIterator[str]:
        shutdown_event: asyncio.Event = app_state.get(
            "shutdown_event", asyncio.Event()
        )

        # Always emit the current status first so late subscribers see it
        sent_final = False
        while not shutdown_event.is_set():
            if await request.is_disconnected():
                break

            status = app_state.get("enrichment_status") or {
                "in_progress": False,
                "total": 0,
                "completed": 0,
                "enriched_count": 0,
                "failed_count": 0,
            }
            yield f"data: {json.dumps(status, default=str)}\n\n"

            # Stop streaming once enrichment is complete (after one final event)
            if not status.get("in_progress"):
                if sent_final:
                    break
                sent_final = True  # loop once more to give clients the final state

            try:
                await asyncio.wait_for(shutdown_event.wait(), timeout=1.0)
                break  # shutdown signaled
            except asyncio.TimeoutError:
                pass

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


async def _build_stream_snapshot() -> Dict[str, Any]:
    """Produce a single-event snapshot of the unified SSE payload.

    Shared by /api/stream (streams it repeatedly) and /api/stream/snapshot
    (returns it once). Having a single builder means the snapshot shape
    is guaranteed to match the stream shape — no risk of drift.
    """
    config = _get_config()
    backpressure_threshold = config.get(
        "backpressure_threshold", DEFAULT_BACKPRESSURE_THRESHOLD
    )

    db = _get_db()
    metrics = await db.get_health_metrics(
        backpressure_threshold=backpressure_threshold,
    )
    payload: Dict[str, Any] = metrics.model_dump()

    state_counts = await db.count_papers_by_state()
    payload["state_counts"] = state_counts
    payload["total_papers"] = sum(state_counts.values())
    payload["ready_for_retrieval"] = state_counts.get(PaperState.READY_FOR_RETRIEVAL.value, 0)
    payload["retrieving"] = state_counts.get(PaperState.RETRIEVING.value, 0)
    payload["retrieved"] = state_counts.get(PaperState.RETRIEVED.value, 0)
    payload["validating"] = state_counts.get(PaperState.VALIDATING.value, 0)
    payload["complete"] = state_counts.get(PaperState.COMPLETE.value, 0)
    payload["failed"] = state_counts.get(PaperState.FAILED.value, 0)
    payload["manual_required"] = state_counts.get(PaperState.MANUAL_REQUIRED.value, 0)

    wp = app_state.get("worker_pool")
    if wp is not None:
        wp_status = wp.get_status()
        payload["worker_pool"] = wp_status
        payload["retrieval_workers_paused"] = wp_status.get("retrieval_paused", False)
        payload["validation_workers_paused"] = wp_status.get("validation_paused", False)
    else:
        payload["worker_pool"] = {
            "running": False, "paused": False,
            "retrieval_workers_active": 0, "validation_workers_active": 0,
            "counters": {}, "run_id": None,
        }

    enrichment = app_state.get("enrichment_status") or {}
    payload["enrichment"] = {
        "in_progress": bool(enrichment.get("in_progress")),
        "total": enrichment.get("total", 0),
        "completed": enrichment.get("completed", 0),
        "enriched_count": enrichment.get("enriched_count", 0),
        "failed_count": enrichment.get("failed_count", 0),
    }

    # SSO session (serializable subset only — no credentials/cookies/DOM)
    sso = app_state.get("sso_session") or {}
    session_duration_s = 0.0
    if sso.get("login_detected_at"):
        try:
            started = datetime.fromisoformat(sso["login_detected_at"])
            if started.tzinfo is None:
                started = started.replace(tzinfo=timezone.utc)
            session_duration_s = (datetime.now(timezone.utc) - started).total_seconds()
        except (ValueError, TypeError):
            pass
    payload["sso"] = {
        "active": bool(sso.get("active")),
        "phase": sso.get("phase", "idle"),
        "proxy_url": sso.get("proxy_url", ""),
        "started_at": sso.get("started_at"),
        "login_detected_at": sso.get("login_detected_at"),
        "session_duration_s": round(session_duration_s, 1),
        "queue_total": sso.get("queue_total", 0),
        "queue_processed": sso.get("queue_processed", 0),
        "queue_succeeded": sso.get("queue_succeeded", 0),
        "queue_manual": sso.get("queue_manual", 0),
        "current_paper_id": sso.get("current_paper_id"),
        "current_paper_title": sso.get("current_paper_title"),
        "last_message": sso.get("last_message", ""),
        "session_expired": bool(sso.get("session_expired")),
    }
    return payload


@app.get("/api/stream/snapshot")
async def stream_snapshot() -> JSONResponse:
    """Single-event snapshot of the unified stream payload.

    Useful for one-off UI fetches (e.g. the Upload-panel State Breakdown
    Refresh button) without opening a full SSE connection. NOT polled
    periodically — that would re-introduce the duplication this fix
    eliminated.
    """
    return JSONResponse(await _build_stream_snapshot())


@app.get("/api/stream")
@app.get("/api/health/stream")  # Back-compat alias
async def unified_stream(request: Request) -> StreamingResponse:
    """Unified Server-Sent Events stream for ALL live run data.

    Single source of truth for:
      - Health metrics (success rate, queue depths, cache rate, etc.)
      - Worker pool state (running/paused + counters for retrieved/
        failed/manual/validated/val_failed)
      - Full paper state counts (every PaperState → count)
      - Backpressure state (weighted depth vs threshold, retrieval_paused)
      - Disk space state
      - Active publisher cooldowns

    Emits one event every 2 seconds. Automatically closes on client
    disconnect or shutdown. No separate polling endpoint is needed —
    the frontend drives all UI updates from this single stream.
    """
    async def event_generator() -> AsyncIterator[str]:
        shutdown_event: asyncio.Event = app_state.get(
            "shutdown_event", asyncio.Event()
        )

        while not shutdown_event.is_set():
            if await request.is_disconnected():
                break

            try:
                payload = await _build_stream_snapshot()
                yield f"data: {json.dumps(payload, default=str)}\n\n"
            except Exception as exc:
                logger.debug("Unified stream error: %s", exc)
                yield f"data: {json.dumps({'error': str(exc)})}\n\n"

            try:
                await asyncio.wait_for(
                    shutdown_event.wait(), timeout=2.0
                )
                break  # Shutdown signaled
            except asyncio.TimeoutError:
                pass  # Normal: no shutdown, keep streaming

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@app.get("/api/health")
async def health_snapshot() -> JSONResponse:
    """One-shot health metrics snapshot."""
    config = _get_config()
    db = _get_db()
    metrics = await db.get_health_metrics(
        backpressure_threshold=config.get(
            "backpressure_threshold", DEFAULT_BACKPRESSURE_THRESHOLD
        ),
    )
    return JSONResponse(metrics.model_dump())


# ===========================================================================
# STATIC FILE SERVING & UI
# ===========================================================================


@app.get("/", response_class=HTMLResponse)
async def serve_index() -> HTMLResponse:
    """Serve the single-page UI."""
    template_path = os.path.join(
        os.path.dirname(__file__), "templates", "index.html"
    )
    if not os.path.isfile(template_path):
        return HTMLResponse(
            content="<h1>Full-Text Acquisition System</h1>"
            "<p>templates/index.html not found. Place it in the templates directory.</p>",
            status_code=200,
        )
    with open(template_path, "r") as f:
        html_content = f.read()
    return HTMLResponse(content=html_content)


@app.get("/api/status")
async def system_status() -> JSONResponse:
    """Overall system status for the UI to check on load."""
    wp = app_state.get("worker_pool")
    wizard: Optional[WizardResult] = app_state.get("wizard_result")
    config = _get_config()
    engine = app_state.get("engine")

    # Resolve the currently-active output directory:
    # - If a run is in progress with an override, the engine's
    #   _output_dir reflects it.
    # - Otherwise, fall back to the configured default.
    if engine is not None and getattr(engine, "_output_dir", None):
        active_output = os.path.abspath(engine._output_dir)
    else:
        active_output = os.path.abspath(
            config.get("output_directory", "./downloads")
        )

    free_bytes = 0
    try:
        # Probe the deepest existing ancestor for free-space figure
        probe = active_output
        while probe and not os.path.exists(probe):
            parent = os.path.dirname(probe)
            if parent == probe: break
            probe = parent
        if probe and os.path.exists(probe):
            free_bytes = shutil.disk_usage(probe).free
    except (OSError, AttributeError):
        pass

    return JSONResponse({
        "initialized": app_state.get("db") is not None,
        "workers_running": wp.is_running if wp else False,
        "workers_paused": wp.is_paused if wp else False,
        "wizard_ok": all([
            wizard.python_ok if wizard else False,
            wizard.dependencies_ok if wizard else False,
            wizard.config_ok if wizard else False,
        ]),
        "wizard_errors": wizard.errors if wizard else [],
        "ocr_mode": wizard.ocr_mode if wizard else "disabled",
        "platform": platform.system(),
        "output_directory": active_output,
        "output_directory_free_bytes": free_bytes,
    })


# ===========================================================================
# ENTRY POINT
# ===========================================================================


def main() -> None:
    """Launch the application."""
    uvicorn.run(
        "full_text_acquisition.app:app",
        host="0.0.0.0",
        port=8000,
        log_level="info",
        reload=False,
    )


if __name__ == "__main__":
    main()

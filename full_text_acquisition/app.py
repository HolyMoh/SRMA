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
from fastapi import FastAPI, File, HTTPException, Query, Request, UploadFile
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
async def upload_file(file: UploadFile = File(...)) -> UploadResponse:
    """Upload a CSV or Excel file for ingestion.

    Performs:
        1. Parse file and detect columns
        2. DOI normalization
        3. Deduplication (exact DOI + fuzzy title)
        4. Metadata enrichment (OpenAlex → CrossRef)
        5. Canonical ID assignment
        6. ALREADY_RETRIEVED detection
        7. Create run record and paper_runs linkage
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

    # Create run
    run_id = f"run-{uuid.uuid4().hex[:12]}"
    config_snap = ConfigSnapshot.from_dict(config)
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

    # Transition INGESTED → NORMALIZED for all new papers
    for paper in papers:
        await db.transition_state(paper.canonical_id, PaperState.NORMALIZED.value)

    # Metadata enrichment
    enrichment = await _enrich_papers(papers, db, config)

    # Transition NORMALIZED → ENRICHED → IDENTITY_ASSIGNED → READY_FOR_RETRIEVAL
    for paper in papers:
        await db.transition_state(paper.canonical_id, PaperState.ENRICHED.value)
        await db.transition_state(paper.canonical_id, PaperState.IDENTITY_ASSIGNED.value)
        await db.transition_state(paper.canonical_id, PaperState.READY_FOR_RETRIEVAL.value)

    # Create paper_runs for new papers
    paper_runs = [
        PaperRun(canonical_id=p.canonical_id, run_id=run_id, submitted_in_this_run=True)
        for p in papers
    ]
    await db.create_paper_runs_bulk(paper_runs)

    # Update run count
    total_ready = inserted
    await db.update_run_submitted_count(run_id, len(rows))

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

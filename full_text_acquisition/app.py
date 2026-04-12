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


@app.get("/api/health/stream")
async def health_stream(request: Request) -> StreamingResponse:
    """Server-Sent Events stream for live health metrics.

    Sends a health metrics JSON event every 3 seconds.
    Connection closes when the client disconnects or shutdown is signaled.
    """
    config = _get_config()
    backpressure_threshold = config.get(
        "backpressure_threshold", DEFAULT_BACKPRESSURE_THRESHOLD
    )

    async def event_generator() -> AsyncIterator[str]:
        shutdown_event: asyncio.Event = app_state.get(
            "shutdown_event", asyncio.Event()
        )

        while not shutdown_event.is_set():
            # Check if client disconnected
            if await request.is_disconnected():
                break

            try:
                db = _get_db()
                metrics = await db.get_health_metrics(
                    backpressure_threshold=backpressure_threshold,
                )

                # Add worker pool status
                wp = app_state.get("worker_pool")
                if wp:
                    wp_status = wp.get_status()
                    metrics.retrieval_workers_paused = wp_status.get("retrieval_paused", False)
                    metrics.validation_workers_paused = wp_status.get("validation_paused", False)

                data = json.dumps(metrics.model_dump(), default=str)
                yield f"data: {data}\n\n"

            except Exception as exc:
                logger.debug("Health stream error: %s", exc)
                yield f"data: {{\"error\": \"{exc}\"}}\n\n"

            try:
                await asyncio.wait_for(
                    shutdown_event.wait(), timeout=3.0
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

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

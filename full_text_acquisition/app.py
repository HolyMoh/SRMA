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
import urllib.parse
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
    PlainTextResponse,
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
    ProjectCreate,
    ProjectResponse,
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

# ---------------------------------------------------------------------------
# Per-project isolation — filesystem layout
# ---------------------------------------------------------------------------
PROJECTS_ROOT = "./projects"
PROJECTS_MANIFEST_PATH = os.path.join(PROJECTS_ROOT, "projects.json")
PROJECTS_MIGRATION_MARKER = os.path.join(PROJECTS_ROOT, ".migrated")
DEFAULT_PROJECT_SLUG = "default"   # the one migration target with a stable name
DEFAULT_PROJECT_DISPLAY = "Default"


def _slugify_project_name(name: str, existing_slugs: Set[str]) -> str:
    """Sanitize a user-supplied project name into a filesystem-safe slug.

    Rules:
      - Lowercase
      - Strip diacritics via NFKD decomposition
      - Replace anything not in [a-z0-9] with '-'
      - Collapse multiple hyphens, strip leading/trailing
      - Truncate to 50 chars
      - Append an 8-char UUID suffix to GUARANTEE uniqueness and to
        side-step Windows reserved names (CON, PRN, NUL, etc.)

    Returns a slug that matches ^[a-z0-9][a-z0-9-]*-[a-f0-9]{8}$ so
    path-traversal is structurally impossible.
    """
    import re as _re
    import unicodedata as _unicodedata

    normalized = _unicodedata.normalize("NFKD", name or "")
    ascii_only = "".join(c for c in normalized if not _unicodedata.combining(c))
    lower = ascii_only.lower()
    # Replace any run of non-alphanumeric with single hyphen
    base = _re.sub(r'[^a-z0-9]+', '-', lower).strip('-')
    base = base[:50].strip('-') or "project"
    # Append uniqueness suffix
    suffix = uuid.uuid4().hex[:8]
    slug = f"{base}-{suffix}"
    # Guard against the (astronomically unlikely) collision
    while slug in existing_slugs:
        slug = f"{base}-{uuid.uuid4().hex[:8]}"
    return slug


def _project_dir(slug: str) -> str:
    return os.path.join(PROJECTS_ROOT, slug)


def _project_db_path(slug: str) -> str:
    return os.path.join(_project_dir(slug), "acquisition.db")


def _project_config_path(slug: str) -> str:
    return os.path.join(_project_dir(slug), "config.json")


def _project_output_dir(slug: str) -> str:
    return os.path.join(_project_dir(slug), "downloads")


def _project_supplement_dir(slug: str) -> str:
    return os.path.join(_project_output_dir(slug), "Supplements")


def _load_projects_manifest() -> Dict[str, Any]:
    """Load ./projects/projects.json, returning an empty manifest if absent."""
    if not os.path.isfile(PROJECTS_MANIFEST_PATH):
        return {"version": 1, "current_project_slug": None, "projects": []}
    try:
        with open(PROJECTS_MANIFEST_PATH, "r") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError) as exc:
        logger.error("Failed to read projects manifest: %s", exc)
        return {"version": 1, "current_project_slug": None, "projects": []}


def _save_projects_manifest(manifest: Dict[str, Any]) -> None:
    """Atomically persist the manifest."""
    os.makedirs(PROJECTS_ROOT, exist_ok=True)
    tmp = PROJECTS_MANIFEST_PATH + ".tmp"
    with open(tmp, "w") as f:
        json.dump(manifest, f, indent=2)
    os.replace(tmp, PROJECTS_MANIFEST_PATH)


def _find_project(manifest: Dict[str, Any], slug: str) -> Optional[Dict[str, Any]]:
    for p in manifest.get("projects", []):
        if p.get("project_slug") == slug:
            return p
    return None


def _load_project_config_overlay(slug: str) -> Dict[str, Any]:
    """Read the per-project config.json overlay. Empty dict if absent."""
    path = _project_config_path(slug)
    if not os.path.isfile(path):
        return {}
    try:
        with open(path, "r") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return {}


def _save_project_config_overlay(slug: str, overlay: Dict[str, Any]) -> None:
    """Persist the per-project config overlay."""
    os.makedirs(_project_dir(slug), exist_ok=True)
    path = _project_config_path(slug)
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        json.dump(overlay, f, indent=2)
    os.replace(tmp, path)


# Fields the user is allowed to override per-project. Other global
# fields (config_version, database_path) are never project-scoped.
_PROJECT_OVERRIDABLE_KEYS: Set[str] = {
    "output_directory", "supplement_directory",
    "sso_proxy_url", "openathens_url", "institutional_resolver_url",
    "unpaywall_email",
    "manual_drop_folder", "manual_mode_enabled",
    "zotero_api_key", "zotero_user_id", "zotero_collection_key",
    "zotero_collection_name", "zotero_poller_enabled",
    "cooldown_minutes", "cooldown_failure_threshold", "cooldown_window_size",
    "retrieval_concurrency", "validation_concurrency",
    "backpressure_threshold",
    "api_timeout_s", "pdf_download_timeout_s",
    "page_load_timeout_s", "selector_timeout_s",
    "pdf_validation_timeout_s", "ocr_per_page_timeout_s",
    "max_retries", "max_total_attempts",
    "min_disk_space_bytes",
    "cache_ttl_days",
    "scholar_min_delay_s", "scholar_max_queries_per_paper",
}


def _merge_project_config(
    slug: str,
    global_config: Dict[str, Any],
) -> Dict[str, Any]:
    """Compose the effective runtime config for a project.

    Precedence: DEFAULT_CONFIG (implicit via global) ← global config.json
    ← project config.json overlay. Plus project-specific path defaults
    for output/supplement dirs if not overridden.
    """
    effective = dict(global_config)
    effective["database_path"] = _project_db_path(slug)

    # Sensible per-project defaults
    if not effective.get("output_directory"):
        effective["output_directory"] = _project_output_dir(slug)
    if not effective.get("supplement_directory"):
        effective["supplement_directory"] = _project_supplement_dir(slug)

    # Apply overlay (only whitelisted keys)
    overlay = _load_project_config_overlay(slug)
    for k, v in overlay.items():
        if k in _PROJECT_OVERRIDABLE_KEYS:
            effective[k] = v

    # If overlay omitted output_directory but the user had previously
    # set a global one, we should still honor the project default.
    # Use project-path if the current value is the global default.
    if "output_directory" not in overlay:
        effective["output_directory"] = _project_output_dir(slug)
    if "supplement_directory" not in overlay:
        effective["supplement_directory"] = _project_supplement_dir(slug)

    return effective


def _ensure_default_project(
    manifest: Dict[str, Any],
    global_config: Dict[str, Any],
) -> str:
    """Ensure the Default project entry exists + its directories exist.

    Returns the Default project's slug (always 'default').
    """
    default = _find_project(manifest, DEFAULT_PROJECT_SLUG)
    if default is None:
        default = {
            "project_id": str(uuid.uuid4()),
            "project_name": DEFAULT_PROJECT_DISPLAY,
            "project_slug": DEFAULT_PROJECT_SLUG,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        manifest.setdefault("projects", []).append(default)

    # Ensure directories exist
    os.makedirs(_project_dir(DEFAULT_PROJECT_SLUG), exist_ok=True)
    os.makedirs(_project_output_dir(DEFAULT_PROJECT_SLUG), exist_ok=True)
    os.makedirs(_project_supplement_dir(DEFAULT_PROJECT_SLUG), exist_ok=True)

    # Ensure overlay file exists (empty means "inherit everything")
    if not os.path.isfile(_project_config_path(DEFAULT_PROJECT_SLUG)):
        _save_project_config_overlay(DEFAULT_PROJECT_SLUG, {})

    # Seed current_project_slug if absent
    if not manifest.get("current_project_slug"):
        manifest["current_project_slug"] = DEFAULT_PROJECT_SLUG

    _save_projects_manifest(manifest)
    return DEFAULT_PROJECT_SLUG


def _migrate_legacy_to_default(global_config: Dict[str, Any]) -> Dict[str, Any]:
    """Idempotent copy of legacy ./acquisition.db + ./downloads/ into
    ./projects/default/. Never overwrites existing default data.

    Returns a dict summarizing what happened.
    """
    import shutil as _shutil
    result: Dict[str, Any] = {
        "migrated": False, "skipped": True, "reason": "",
        "db_copied": False, "downloads_copied": False,
    }

    os.makedirs(PROJECTS_ROOT, exist_ok=True)
    if os.path.isfile(PROJECTS_MIGRATION_MARKER):
        result["reason"] = "Migration marker already present"
        return result

    legacy_db = global_config.get("database_path", "./acquisition.db")
    legacy_downloads = global_config.get("output_directory", "./downloads")
    default_db = _project_db_path(DEFAULT_PROJECT_SLUG)
    default_downloads = _project_output_dir(DEFAULT_PROJECT_SLUG)

    # If the default project directory already has content, refuse to touch it.
    if os.path.isdir(_project_dir(DEFAULT_PROJECT_SLUG)) and (
        os.path.isfile(default_db)
        or (os.path.isdir(default_downloads) and os.listdir(default_downloads))
    ):
        # Mark migration as done (to avoid re-checking) but note the skip.
        with open(PROJECTS_MIGRATION_MARKER, "w") as f:
            f.write(f"skipped-existing-default at "
                    f"{datetime.now(timezone.utc).isoformat()}\n")
        result["reason"] = (
            "Default project directory already has data — not touched"
        )
        logger.warning("Skipping legacy migration: %s", result["reason"])
        return result

    os.makedirs(_project_dir(DEFAULT_PROJECT_SLUG), exist_ok=True)

    # COPY (not move) the legacy DB
    try:
        if os.path.isfile(legacy_db):
            _shutil.copy2(legacy_db, default_db)
            # Also copy WAL/SHM sidecars if present
            for sidecar in ("-wal", "-shm"):
                src = legacy_db + sidecar
                if os.path.isfile(src):
                    _shutil.copy2(src, default_db + sidecar)
            result["db_copied"] = True
            logger.info("Migration copy: %s -> %s", legacy_db, default_db)
    except OSError as exc:
        logger.error("Failed to copy legacy DB: %s", exc)

    # COPY the legacy downloads tree
    try:
        if os.path.isdir(legacy_downloads):
            _shutil.copytree(legacy_downloads, default_downloads, dirs_exist_ok=True)
            result["downloads_copied"] = True
            logger.info(
                "Migration copy: %s -> %s", legacy_downloads, default_downloads
            )
        else:
            os.makedirs(default_downloads, exist_ok=True)
    except OSError as exc:
        logger.error("Failed to copy legacy downloads: %s", exc)

    # Write marker so this never runs again
    with open(PROJECTS_MIGRATION_MARKER, "w") as f:
        f.write(
            f"migrated at {datetime.now(timezone.utc).isoformat()}\n"
            f"legacy_db={legacy_db}\n"
            f"legacy_downloads={legacy_downloads}\n"
            f"db_copied={result['db_copied']}\n"
            f"downloads_copied={result['downloads_copied']}\n"
            "Original files LEFT IN PLACE as backup. Delete manually "
            "once you're confident Default project is working.\n"
        )
    result["migrated"] = True
    result["skipped"] = False
    result["reason"] = "Legacy data copied to Default project"
    return result




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
# Project switching: activity check + rebind
# ---------------------------------------------------------------------------

def _check_busy_reasons() -> List[str]:
    """Return a list of human reasons why project-switching is not safe.

    Used by activate_project to refuse the switch rather than corrupt
    data by swapping the DB + output dir out from under live workers.
    """
    reasons: List[str] = []
    wp = app_state.get("worker_pool")
    if wp is not None:
        try:
            if wp.is_running:
                reasons.append("a retrieval run is active")
        except Exception:
            pass

    mm = app_state.get("manual_mode") or {}
    if mm.get("watcher_running") or mm.get("enabled"):
        reasons.append("Manual Mode watcher is running")

    sso = app_state.get("sso_session") or {}
    if sso.get("active"):
        reasons.append("an SSO session is active")

    enr = app_state.get("enrichment_status") or {}
    if enr.get("in_progress"):
        reasons.append("enrichment is still in progress")

    # Zotero poller holds the previous project's API credentials +
    # collection anchor; switching projects while it runs would cross
    # library boundaries. Require the user to stop it explicitly.
    zt = app_state.get("zotero") or {}
    if zt.get("poller_running"):
        reasons.append("the Zotero poller is running")

    return reasons


async def _rebind_to_project(slug: str) -> Dict[str, Any]:
    """Tear down current DB + engine + worker pool, rebind to a project.

    CRITICAL: callers MUST have verified _check_busy_reasons() is
    empty first. This function does NOT protect against live tasks.

    Returns the new project's manifest record.
    """
    manifest = _load_projects_manifest()
    project = _find_project(manifest, slug)
    if project is None:
        raise HTTPException(status_code=404, detail=f"Project '{slug}' not found")

    # 0. Defensive teardown of background tasks bound to the previous
    # project. _check_busy_reasons() should already have refused if any
    # of these are live, but guard against state-drift bugs here too.
    for cancel_key, task_key in (
        ("manual_cancel_event", "manual_task"),
        ("zotero_cancel_event", "zotero_task"),
    ):
        ev: Optional[asyncio.Event] = app_state.get(cancel_key)
        tk: Optional[asyncio.Task] = app_state.get(task_key)
        if ev is not None:
            ev.set()
        if tk is not None and not tk.done():
            try:
                await asyncio.wait_for(tk, timeout=5.0)
            except asyncio.TimeoutError:
                tk.cancel()
                try: await tk
                except (asyncio.CancelledError, Exception): pass
            except Exception as exc:
                logger.debug("Task %s raised during rebind teardown: %s", task_key, exc)

    # 1. Close current DB (flush write queue first)
    current_db = app_state.get("db")
    if current_db is not None:
        try:
            await current_db.flush_write_queue()
            await current_db.close()
        except Exception as exc:
            logger.warning("Error closing current DB during rebind: %s", exc)

    # 2. Close current engine's HTTP client
    current_engine = app_state.get("engine")
    if current_engine is not None:
        try:
            await current_engine.close()
        except Exception as exc:
            logger.debug("Error closing engine during rebind: %s", exc)

    # 3. Compose effective config for the new project
    global_config = load_config()  # reads ./config.json
    effective_config = _merge_project_config(slug, global_config)

    # 4. Open new DB + run schema migration + interrupted/filesystem reset
    new_db_path = _project_db_path(slug)
    os.makedirs(os.path.dirname(new_db_path), exist_ok=True)
    new_db = Database(new_db_path)
    await new_db.initialize()
    effective_config = await new_db.run_config_migration(effective_config)
    # Persist migrated overlay if keys changed
    overlay = _load_project_config_overlay(slug)
    _save_project_config_overlay(slug, overlay)  # no-op touch keeps file present
    await new_db.reset_interrupted_states()
    await new_db.reconcile_filesystem()

    # 5. Create new engine bound to this project's paths
    output_dir = effective_config.get("output_directory") or _project_output_dir(slug)
    supp_dir = effective_config.get("supplement_directory") or _project_supplement_dir(slug)
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(supp_dir, exist_ok=True)

    wizard = app_state.get("wizard_result")
    browser_mgr = app_state.get("browser_manager")
    new_engine = RetrievalEngine(
        db=new_db, browser_manager=browser_mgr,
        config=effective_config,
        output_directory=output_dir,
        supplement_directory=supp_dir,
    )
    if wizard is not None:
        new_engine.tesseract_available = wizard.tesseract_available
        new_engine.ghostscript_available = wizard.ghostscript_available

    # 6. Create new worker pool bound to new engine + DB
    new_pool = WorkerPool(
        db=new_db, engine=new_engine,
        config=effective_config, output_dir=output_dir,
    )

    # 7. Atomic swap of app_state references
    app_state["db"] = new_db
    app_state["engine"] = new_engine
    app_state["worker_pool"] = new_pool
    app_state["config"] = effective_config
    app_state["current_project"] = dict(project)

    # 8. Reset per-project live state (enrichment progress, manual mode).
    # These are project-local; a new project starts fresh.
    app_state["enrichment_status"] = {
        "in_progress": False, "run_id": None, "total": 0, "completed": 0,
        "enriched_count": 0, "failed_count": 0,
        "started_at": None, "completed_at": None,
    }
    app_state["enrichment_task"] = None
    app_state["manual_mode"] = {
        "enabled": bool(effective_config.get("manual_mode_enabled")),
        "drop_folder": effective_config.get("manual_drop_folder", ""),
        "watcher_running": False, "last_scan_at": None,
        "last_file_seen": None, "last_file_seen_at": None,
        "last_message": "",
        "matched_and_validated": 0, "validation_failed": 0,
        "unmatched_count": 0, "unmatched_files": [],
        "pending_disambiguation": None, "permanently_unavailable": 0,
    }
    app_state["manual_task"] = None
    app_state["manual_cancel_event"] = None

    # Zotero integration (Part 2 paywalled redesign). Same shape of
    # serializable state slice we use for Manual Mode + SSO.
    app_state["zotero"] = {
        "enabled": False,
        "collection_key": "",
        "collection_name": "",
        "poller_running": False,
        "last_poll_at": None,
        "last_poll_version": 0,     # incremental polling anchor
        "last_poll_items": 0,       # items seen in last poll
        "last_poll_error": None,
        "last_message": "",
        "ingested_count": 0,        # PDFs pulled + validated this session
        "validation_failed_count": 0,
        "pushed_last_run": 0,       # last push result
    }
    app_state["zotero_task"] = None
    app_state["zotero_cancel_event"] = None
    app_state["sso_session"] = {
        "active": False, "phase": "idle", "proxy_url": "",
        "login_url": "", "started_at": None, "login_detected_at": None,
        "ended_at": None, "queue_total": 0, "queue_processed": 0,
        "queue_succeeded": 0, "queue_manual": 0,
        "current_paper_id": None, "current_paper_title": None,
        "last_message": "", "session_expired": False,
    }
    app_state["sso_task"] = None
    app_state["sso_cancel_event"] = None

    # 9. Persist the switch in the manifest
    manifest["current_project_slug"] = slug
    _save_projects_manifest(manifest)

    logger.info(
        "Project rebound: %s (slug=%s, db=%s)",
        project["project_name"], slug, new_db_path,
    )
    return project


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

    # Load global config (defaults; projects can override)
    config = load_config()

    # --- Projects setup (runs BEFORE DB init so we know which DB to open) ---
    os.makedirs(PROJECTS_ROOT, exist_ok=True)
    # Idempotent migration of legacy ./acquisition.db + ./downloads/ into
    # ./projects/default/. Copy-not-move; leaves originals in place.
    migration_result = _migrate_legacy_to_default(config)
    if migration_result.get("migrated"):
        logger.info("Legacy data migrated to Default project: %s",
                    migration_result.get("reason"))
    manifest = _load_projects_manifest()
    _ensure_default_project(manifest, config)
    # Re-read manifest (ensure_default persisted it)
    manifest = _load_projects_manifest()
    current_slug = manifest.get("current_project_slug") or DEFAULT_PROJECT_SLUG
    if _find_project(manifest, current_slug) is None:
        # Manifest references a project that no longer exists — fall back
        current_slug = DEFAULT_PROJECT_SLUG
        manifest["current_project_slug"] = current_slug
        _save_projects_manifest(manifest)
    current_project = _find_project(manifest, current_slug)
    app_state["current_project"] = dict(current_project) if current_project else None
    app_state["migration_notice"] = migration_result

    # Compose effective config for the current project (global ← overlay)
    config = _merge_project_config(current_slug, config)

    # Initialize database for the current project
    db_path = config["database_path"]  # set by _merge_project_config
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    db = Database(db_path)
    await db.initialize()

    # Step 2: Config migration (schema additive)
    try:
        config = await db.run_config_migration(config)
        save_config(config)  # persist global
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

    # Step 7: Initialize RetrievalEngine for current project
    output_dir = config.get("output_directory") or _project_output_dir(current_slug)
    supplement_dir = config.get("supplement_directory") or _project_supplement_dir(current_slug)
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

    # Manual Orchestration Mode — system watches a drop folder for
    # user-downloaded PDFs and matches them to the MANUAL_REQUIRED queue.
    # Never opens a browser, never touches credentials.
    app_state["manual_mode"] = {
        "enabled": False,
        "drop_folder": "",
        "watcher_running": False,
        "last_scan_at": None,
        "last_file_seen": None,
        "last_file_seen_at": None,
        "last_message": "",
        "matched_and_validated": 0,
        "validation_failed": 0,
        "unmatched_count": 0,
        "unmatched_files": [],
        "pending_disambiguation": None,
        "permanently_unavailable": 0,
    }
    app_state["manual_task"] = None
    app_state["manual_cancel_event"] = None

    # Zotero integration (Part 2 paywalled redesign).
    app_state["zotero"] = {
        "enabled": False,
        "collection_key": config.get("zotero_collection_key", ""),
        "collection_name": config.get("zotero_collection_name", ""),
        "poller_running": False,
        "last_poll_at": None,
        "last_poll_version": 0,
        "last_poll_items": 0,
        "last_poll_error": None,
        "last_message": "",
        "ingested_count": 0,
        "validation_failed_count": 0,
        "pushed_last_run": 0,
    }
    app_state["zotero_task"] = None
    app_state["zotero_cancel_event"] = None
    # Auto-start the poller if the user previously enabled it
    if (config.get("zotero_poller_enabled")
            and config.get("zotero_api_key")
            and config.get("zotero_user_id")
            and config.get("zotero_collection_key")):
        try:
            asyncio.get_event_loop().create_task(_start_zotero_poller_internal())
        except Exception as exc:
            logger.warning("Could not auto-start Zotero poller: %s", exc)

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

    # Stop the Manual Mode watcher if running
    mm_cancel: Optional[asyncio.Event] = app_state.get("manual_cancel_event")
    mm_task: Optional[asyncio.Task] = app_state.get("manual_task")
    if mm_cancel:
        mm_cancel.set()
    if mm_task and not mm_task.done():
        try:
            await asyncio.wait_for(mm_task, timeout=10.0)
        except asyncio.TimeoutError:
            mm_task.cancel()
            try: await mm_task
            except (asyncio.CancelledError, Exception): pass

    # Stop the Zotero poller if running (Part 2 paywalled redesign).
    # Same teardown contract as Manual Mode: signal cancel, await, then
    # hard-cancel if it ignores us. The poller is a long sleep loop so
    # it's expected to respond to the cancel event within one iteration.
    zt_cancel: Optional[asyncio.Event] = app_state.get("zotero_cancel_event")
    zt_task: Optional[asyncio.Task] = app_state.get("zotero_task")
    if zt_cancel:
        zt_cancel.set()
    if zt_task and not zt_task.done():
        try:
            await asyncio.wait_for(zt_task, timeout=10.0)
        except asyncio.TimeoutError:
            zt_task.cancel()
            try: await zt_task
            except (asyncio.CancelledError, Exception): pass

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
# CORS — REQUIRED for the Queue Pack HTML (file:// origin) and for the
# optional browser bookmarklet that talks to localhost from publisher
# pages. Three origins need to reach this server:
#
#   1. null                        — HTML opened via file:// in Chrome/FF
#   2. http://localhost, 127.0.0.1 — dev + the UI itself
#   3. https://*                   — publisher pages running a bookmarklet
#
# SECURITY: credentials=False unconditionally. We NEVER accept cross-origin
# cookies. A bookmarklet on elsevier.com may have the user's Elsevier
# session, but when it POSTs bytes to us, those cookies are not forwarded.
# Our own localhost UI doesn't rely on cross-origin cookies either —
# same-origin by definition.
# ---------------------------------------------------------------------------
from fastapi.middleware.cors import CORSMiddleware

app.add_middleware(
    CORSMiddleware,
    # We accept:
    #  - "null" for file:// (Chrome sends Origin: null for HTML opened
    #    from disk; this is the Queue Pack's primary origin)
    #  - explicit localhost variants for the main UI + future dev tools
    # Publisher sites go through the regex below because CORSMiddleware's
    # allow_origins is literal-only.
    allow_origins=[
        "null",
        "http://localhost",
        "http://localhost:8000",
        "http://127.0.0.1",
        "http://127.0.0.1:8000",
    ],
    # Any https://* publisher page (bookmarklet use-case) matches this.
    # We still never accept cookies cross-origin.
    allow_origin_regex=r"^https://.+",
    allow_credentials=False,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["*"],
    expose_headers=[
        "Content-Disposition",
        # Bookmarklet debug headers — let the overlay tell users what the
        # publisher actually sent when a non-PDF body is rejected.
        "X-Received-Bytes",
        "X-Received-Content-Type",
        "X-Received-Preview",
    ],
    max_age=3600,
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

    # Auto-backup before workers touch data. Runs only when we've just
    # passed the busy-check (wp.is_running was false at the top), so
    # the DB is quiescent. 5-file rotation keeps disk use bounded.
    current_slug = _current_project_slug_or_404()
    try:
        await _create_backup(current_slug, kind=BACKUP_KIND_AUTO)
        _rotate_auto_backups(current_slug, keep=_AUTO_BACKUP_KEEP)
    except Exception as exc:
        # Do NOT abort the run if backup fails — just log and continue.
        # The user explicitly clicked Start; a flaky disk on the backup
        # folder shouldn't block their work.
        logger.warning("Auto-backup failed (run will proceed): %s", exc)

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


# ---------------------------------------------------------------------------
# Privacy Audit endpoints
# ---------------------------------------------------------------------------


@app.get("/api/sso/audit")
async def sso_audit_snapshot() -> JSONResponse:
    """Snapshot of SSO browser activity counters and navigation log.

    Counters are computed from REAL operations instrumented in
    BrowserManager — never hardcoded. If form_fields_read or
    screenshots_taken ever become non-zero, the UI will show the
    truth (we have not added any code path that does either).
    """
    bm: Optional[BrowserManager] = app_state.get("browser_manager")
    if bm is None:
        return JSONResponse({
            "pages_navigated": 0,
            "form_fields_read": 0,
            "screenshots_taken": 0,
            "files_downloaded": 0,
            "nav_log": [],
            "session_audit_dir": None,
            "session_started_at": None,
            "has_persistent_context": False,
        })
    return JSONResponse(bm.audit_snapshot())


@app.get("/api/sso/audit/cookies")
async def sso_audit_cookies() -> JSONResponse:
    """List cookies in the SSO context — names + domains only.

    SECURITY: cookie values are NEVER returned. Only metadata that
    lets the user identify what is set (domain, name, path, expires,
    httpOnly/secure/sameSite flags).
    """
    bm: Optional[BrowserManager] = app_state.get("browser_manager")
    if bm is None:
        return JSONResponse({"cookies": [], "count": 0})
    cookies = await bm.list_persistent_cookies()
    return JSONResponse({"cookies": cookies, "count": len(cookies)})


@app.get("/api/sso/audit/folder-listing")
async def sso_audit_folder_listing() -> JSONResponse:
    """List contents of the per-session audit folder.

    Powers the 'Verify now' button. If the folder contains exactly
    one file (SESSION_INFO.txt), the user has visual proof that no
    other session data is being persisted by the system.
    """
    bm: Optional[BrowserManager] = app_state.get("browser_manager")
    if bm is None:
        return JSONResponse({
            "session_dir": None, "exists": False,
            "contents": [], "is_empty_except_marker": True,
        })

    session_dir = bm.audit_snapshot().get("session_audit_dir")
    if not session_dir:
        return JSONResponse({
            "session_dir": None, "exists": False,
            "contents": [], "is_empty_except_marker": True,
        })

    if not os.path.isdir(session_dir):
        return JSONResponse({
            "session_dir": session_dir, "exists": False,
            "contents": [], "is_empty_except_marker": True,
        })

    contents: List[Dict[str, Any]] = []
    try:
        for name in sorted(os.listdir(session_dir)):
            full = os.path.join(session_dir, name)
            try:
                st = os.stat(full)
                contents.append({
                    "name": name,
                    "size_bytes": st.st_size,
                    "is_dir": os.path.isdir(full),
                    "modified": datetime.fromtimestamp(
                        st.st_mtime, tz=timezone.utc
                    ).isoformat(),
                })
            except OSError as exc:
                contents.append({
                    "name": name, "size_bytes": 0,
                    "is_dir": False, "modified": "",
                    "error": str(exc),
                })
    except OSError as exc:
        return JSONResponse({
            "session_dir": session_dir, "exists": True,
            "contents": [], "error": str(exc),
            "is_empty_except_marker": False,
        })

    only_marker = (
        len(contents) == 1
        and contents[0]["name"] == "SESSION_INFO.txt"
    )
    return JSONResponse({
        "session_dir": session_dir,
        "exists": True,
        "contents": contents,
        "is_empty_except_marker": only_marker,
    })


# Trust-boundary functions: the ENTIRE surface where the system
# touches the SSO browser. Scanned at request time so reported line
# numbers always match the running code, never drift on refactor.
SSO_TRUST_BOUNDARY_FUNCTIONS: List[Dict[str, str]] = [
    {"function": "initiate_sso_login",      "module": "browser_manager"},
    {"function": "probe_sso_login",         "module": "browser_manager"},
    {"function": "get_sso_page",            "module": "browser_manager"},
    {"function": "tier3_institutional_sso", "module": "retrieval_engine"},
]


@app.get("/api/sso/audit/source-functions")
async def sso_audit_source_functions() -> JSONResponse:
    """Locate the 4 functions that interact with the SSO browser.

    Reads the source files of the running modules and returns the
    current line number of each function definition. Line numbers
    are recomputed on every request so they are always accurate
    for the version actually running.
    """
    import full_text_acquisition.browser_manager as _bm_mod
    import full_text_acquisition.retrieval_engine as _re_mod

    module_paths = {
        "browser_manager":   _bm_mod.__file__,
        "retrieval_engine":  _re_mod.__file__,
    }

    results: List[Dict[str, Any]] = []
    for entry in SSO_TRUST_BOUNDARY_FUNCTIONS:
        fn = entry["function"]
        mod = entry["module"]
        path = module_paths.get(mod)
        out: Dict[str, Any] = {
            "function": fn,
            "module": mod,
            "file": os.path.basename(path) if path else None,
            "line": None,
            "exists": False,
        }
        if not path or not os.path.isfile(path):
            out["error"] = "Source file not readable"
            results.append(out)
            continue
        try:
            with open(path, "r") as fh:
                lines = fh.readlines()
            for i, line in enumerate(lines):
                stripped = line.lstrip()
                if (stripped.startswith(f"async def {fn}(")
                        or stripped.startswith(f"def {fn}(")):
                    out["line"] = i + 1
                    out["exists"] = True
                    break
        except OSError as exc:
            out["error"] = str(exc)
        results.append(out)

    return JSONResponse({"functions": results})


@app.get("/api/sso/audit/nav-log.csv")
async def sso_audit_nav_log_csv() -> FileResponse:
    """CSV export of the SSO navigation log (timestamp, url, reason)."""
    bm: Optional[BrowserManager] = app_state.get("browser_manager")
    nav: List[Dict[str, Any]] = []
    if bm is not None:
        nav = bm.audit_snapshot().get("nav_log", [])

    export_dir = os.path.join(
        _get_config().get("output_directory", "./downloads"), "exports"
    )
    os.makedirs(export_dir, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    fname = f"sso_nav_log_{timestamp}.csv"
    fpath = os.path.join(export_dir, fname)

    with open(fpath, "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["timestamp", "url", "reason"])
        for entry in nav:
            w.writerow([
                entry.get("timestamp", ""),
                entry.get("url", ""),
                entry.get("reason", ""),
            ])

    return FileResponse(fpath, media_type="text/csv", filename=fname)


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
# MANUAL ORCHESTRATION MODE — watch-folder, match, validate, install
# ===========================================================================
#
# Philosophy: in Manual Mode, the system NEVER opens a browser or
# touches credentials. The user downloads PDFs themselves through
# their own browser and drops them into a configured folder. The
# system watches that folder, matches dropped files to papers in
# the MANUAL_REQUIRED queue, runs the same validation pipeline as
# automatic retrieval, and marks matches as COMPLETE.

MANUAL_WATCHER_POLL_INTERVAL_S = 5.0
MANUAL_FILE_STABLE_POLLS = 2        # file size unchanged across 2 polls = ~10s
MANUAL_FUZZY_THRESHOLD = 85.0       # 0-100 scale (rapidfuzz token_set_ratio)
MANUAL_FUZZY_GAP_REQUIRED = 15.0    # best vs second-best gap for auto-match


def _publisher_suggested_action(paper: Paper) -> str:
    """Human hint for where the user should get this paper."""
    pub = (paper.publisher or "").upper()
    last = paper.last_failure_code or ""
    hints = {
        "ELSEVIER":        "Available via ScienceDirect — use the EZproxy link.",
        "SPRINGER":        "Available via SpringerLink — use the EZproxy link.",
        "WILEY":           "Available via Wiley Online Library — use the EZproxy link.",
        "NATURE":          "Available via Nature.com — use the EZproxy link.",
        "BMJ":             "Available via BMJ Journals — use the EZproxy link.",
        "LANCET":          "Available via TheLancet.com — use the EZproxy link.",
        "TAYLOR_FRANCIS":  "Available via Taylor & Francis Online — use the EZproxy link.",
        "SAGE":            "Available via SAGE Journals — use the EZproxy link.",
    }
    base = hints.get(pub, "Use the DOI link or EZproxy-wrapped link above.")
    if last == FailureCode.PAYWALL_DETECTED.value:
        return base + " (Paywall detected earlier — institutional access required.)"
    if last == FailureCode.NO_OA_SOURCE.value:
        return base + " (No open-access copy available.)"
    return base


async def _read_pdf_text_first_pages(pdf_path: str, max_pages: int = 3) -> str:
    """Best-effort first-N-pages text extraction for matching.

    Returns empty string on failure. Used only for matching —
    never logged or persisted.
    """
    try:
        try:
            import fitz
            doc = fitz.open(pdf_path)
            text = ""
            for i in range(min(max_pages, len(doc))):
                text += doc[i].get_text() + "\n"
            doc.close()
            return text
        except ImportError:
            from pypdf import PdfReader
            reader = PdfReader(pdf_path)
            text = ""
            for i in range(min(max_pages, len(reader.pages))):
                text += (reader.pages[i].extract_text() or "") + "\n"
            return text
    except Exception as exc:
        logger.debug("Text extraction failed for %s: %s", pdf_path, exc)
        return ""


async def _match_dropped_file(pdf_path: str) -> Dict[str, Any]:
    """Match a dropped PDF to a paper in the MANUAL_REQUIRED queue.

    Strategy:
        1. DOI in text → exact match
        2. Fuzzy title via rapidfuzz.token_set_ratio, BUT only
           auto-match when the top score >= 85 AND the gap to the
           second-best score is >= 15 points (Concern 1 guard).
        3. Narrow-gap or weak scores → return candidates for user
           disambiguation.

    Returns:
        {
            "matched": Paper | None,
            "confidence": float (0-100),
            "match_type": "doi" | "fuzzy" | "ambiguous" | "none",
            "candidates": [{"canonical_id", "title", "first_author",
                           "year", "score"}],
            "reason": str  # human explanation
        }
    """
    db = _get_db()
    papers = await db.get_manual_required_papers()
    if not papers:
        return {"matched": None, "confidence": 0.0, "match_type": "none",
                "candidates": [], "reason": "No papers in MANUAL_REQUIRED queue"}

    text = await _read_pdf_text_first_pages(pdf_path, max_pages=3)
    if not text:
        return {"matched": None, "confidence": 0.0, "match_type": "none",
                "candidates": [], "reason": "Could not extract text from PDF"}
    text_lower = text.lower()

    # 1. DOI match
    for p in papers:
        if p.doi and p.doi.lower() in text_lower:
            return {"matched": p, "confidence": 100.0, "match_type": "doi",
                    "candidates": [], "reason": "DOI matched in PDF text"}

    # 2. Fuzzy title match
    try:
        from rapidfuzz import fuzz
    except ImportError:
        return {"matched": None, "confidence": 0.0, "match_type": "none",
                "candidates": [], "reason": "rapidfuzz not available"}

    scores: List[tuple] = []
    for p in papers:
        title_norm = normalize_title(p.title or "")
        if len(title_norm) < 10:
            continue
        s = float(fuzz.token_set_ratio(title_norm, text_lower))
        scores.append((p, s))
    scores.sort(key=lambda x: x[1], reverse=True)

    if not scores or scores[0][1] < MANUAL_FUZZY_THRESHOLD:
        return {"matched": None, "confidence": scores[0][1] if scores else 0.0,
                "match_type": "none", "candidates": [],
                "reason": f"No fuzzy match ≥ {MANUAL_FUZZY_THRESHOLD}"}

    top_paper, top_score = scores[0]
    second_score = scores[1][1] if len(scores) > 1 else 0.0
    gap = top_score - second_score

    if gap < MANUAL_FUZZY_GAP_REQUIRED:
        # Ambiguous — surface candidates for user to pick
        candidates = []
        for p, s in scores[:5]:
            if s < MANUAL_FUZZY_THRESHOLD:
                break
            candidates.append({
                "canonical_id": p.canonical_id,
                "title": p.title,
                "first_author": p.first_author_lastname,
                "year": p.year,
                "doi": p.doi,
                "score": round(s, 1),
            })
        return {"matched": None, "confidence": top_score,
                "match_type": "ambiguous", "candidates": candidates,
                "reason": (f"Top candidate scored {top_score:.1f}, "
                           f"second {second_score:.1f} "
                           f"(gap {gap:.1f} < {MANUAL_FUZZY_GAP_REQUIRED}) — "
                           "please pick manually")}

    # Clear winner
    return {"matched": top_paper, "confidence": top_score,
            "match_type": "fuzzy", "candidates": [],
            "reason": f"Fuzzy title match: {top_score:.1f} (gap {gap:.1f})"}


async def _install_and_validate_dropped(
    paper: Paper,
    dropped_path: str,
    match_type: str,
) -> Dict[str, Any]:
    """Install a matched dropped file as the paper's PDF and validate.

    Steps:
        1. Atomic copy into output_directory with canonical filename
        2. Update paper.pdf_path / sha256 / size
        3. Walk the state machine MANUAL_REQUIRED → READY → RETRIEVING
           → RETRIEVED → VALIDATING via the normal claim methods
        4. Run engine.run_validation_pipeline (same 7 checks as
           automatic retrieval — including FIX 3's page-location-
           aware fuzzy check)
        5. On VALID + un-flagged identity: transition VALIDATED →
           COMPLETE, delete the dropped file
        6. On INVALID or flagged identity: leave at VALIDATED (user
           can override in Results) or FAILED; do NOT delete dropped
           file so user can inspect

    Returns a result dict for the watcher's status update.
    """
    import hashlib as _hashlib
    import shutil as _shutil

    db = _get_db()
    engine = _get_engine()
    config = _get_config()

    # 1. Atomic copy
    output_dir = os.path.abspath(
        getattr(engine, "_output_dir", None) or
        config.get("output_directory", "./downloads")
    )
    os.makedirs(output_dir, exist_ok=True)

    from full_text_acquisition.models import generate_filename as _gen_fn
    filename = _gen_fn(paper.first_author_lastname, paper.year, paper.title or "manual")
    final_path = os.path.join(output_dir, filename)

    # Handle collisions — append _v{N}
    if os.path.isfile(final_path):
        base, ext = os.path.splitext(final_path)
        v = 2
        while os.path.isfile(f"{base}_v{v}{ext}"):
            v += 1
        final_path = f"{base}_v{v}{ext}"
        filename = os.path.basename(final_path)

    tmp_path = final_path + ".tmp"
    try:
        _shutil.copy2(dropped_path, tmp_path)
        os.replace(tmp_path, final_path)
    except OSError as exc:
        if os.path.exists(tmp_path):
            try: os.unlink(tmp_path)
            except OSError: pass
        return {"outcome": "install_failed", "error": str(exc),
                "paper": paper.canonical_id, "filename": None}

    # Compute hash + size
    with open(final_path, "rb") as fh:
        content = fh.read()
    sha = _hashlib.sha256(content).hexdigest()

    await db.update_paper_fields(
        paper.canonical_id,
        pdf_path=final_path,
        pdf_filename=filename,
        sha256_checksum=sha,
        pdf_size_bytes=len(content),
        retrieval_tier="MANUAL",
        retrieval_method=f"user_dropped_file_{match_type}",
        retrieval_url=f"file://{os.path.abspath(dropped_path)}",
    )

    # 2. Walk state machine
    run_id = f"manual-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}"
    worker_id = "manual-watcher"

    # MANUAL_REQUIRED → READY_FOR_RETRIEVAL
    await db.reset_paper_for_retry(paper.canonical_id)
    # READY → RETRIEVING
    claimed = await db.claim_retrieval_task(paper.canonical_id, worker_id)
    if not claimed:
        return {"outcome": "claim_failed", "paper": paper.canonical_id,
                "filename": filename}
    # RETRIEVING → RETRIEVED
    await db.transition_state(paper.canonical_id, PaperState.RETRIEVED.value,
                              run_id=run_id)
    # RETRIEVED → VALIDATING
    await db.claim_validation_task(paper.canonical_id, worker_id)

    # 3. Run validation pipeline
    refreshed = await db.get_paper(paper.canonical_id)
    if refreshed is None:
        return {"outcome": "paper_vanished", "paper": paper.canonical_id,
                "filename": filename}
    try:
        vstatus, istatus, score = await engine.run_validation_pipeline(
            refreshed, run_id
        )
    except Exception as exc:
        logger.error("Manual validation error: %s", exc)
        vstatus, istatus, score = "INVALID", "CONTENT_UNVERIFIED", 0

    # 4. Transition to terminal state
    flagged = istatus in ("VERSION_MISMATCH", "CONTENT_UNVERIFIED",
                          "TITLE_VERIFIED_WEAK")
    if vstatus == "INVALID":
        # VALIDATING → FAILED (terminal for this check, user can retry)
        await db.transition_state(
            paper.canonical_id, PaperState.FAILED.value,
            failure_code=FailureCode.MANUAL_VALIDATION_FAILED.value,
            run_id=run_id,
        )
        return {"outcome": "validation_failed",
                "paper": paper.canonical_id, "filename": filename,
                "validation_status": vstatus, "identity_status": istatus}

    # VALIDATING → VALIDATED
    await db.transition_state(paper.canonical_id, PaperState.VALIDATED.value,
                              run_id=run_id)

    if flagged:
        # Leave at VALIDATED — user will resolve via Confirm/Re-retrieve
        return {"outcome": "flagged",
                "paper": paper.canonical_id, "filename": filename,
                "validation_status": vstatus, "identity_status": istatus,
                "score": score}

    # COMPLETE
    await db.transition_state(paper.canonical_id, PaperState.COMPLETE.value,
                              run_id=run_id)

    # Delete the dropped source file — we have it now under the canonical name
    try:
        if os.path.abspath(dropped_path) != os.path.abspath(final_path):
            os.unlink(dropped_path)
    except OSError as exc:
        logger.debug("Could not remove dropped file %s: %s", dropped_path, exc)

    return {"outcome": "complete",
            "paper": paper.canonical_id, "filename": filename,
            "validation_status": vstatus, "identity_status": istatus,
            "score": score}


async def _manual_mode_watcher(cancel_event: asyncio.Event) -> None:
    """Background task: poll drop folder every 5s with stable-size
    check, match to queue, install, validate.

    File is only processed when its size has been unchanged across
    MANUAL_FILE_STABLE_POLLS consecutive polls (~10s). This avoids
    race conditions where the OS is still writing a large PDF.
    """
    status = app_state["manual_mode"]
    observed: Dict[str, Dict[str, Any]] = {}  # path -> {size, mtime, stable_count}
    processed_recently: Set[str] = set()      # paths we've already attempted
    status["watcher_running"] = True

    try:
        while not cancel_event.is_set():
            folder = (_get_config().get("manual_drop_folder") or "").strip()
            status["drop_folder"] = folder
            status["last_scan_at"] = datetime.now(timezone.utc).isoformat()

            if folder and os.path.isdir(folder):
                try:
                    entries = os.listdir(folder)
                except OSError:
                    entries = []

                current_paths = set()
                for name in entries:
                    if name.startswith(".") or not name.lower().endswith(".pdf"):
                        continue
                    full = os.path.join(folder, name)
                    if not os.path.isfile(full):
                        continue
                    current_paths.add(full)

                    if full in processed_recently:
                        continue

                    try:
                        st = os.stat(full)
                        key = (st.st_size, int(st.st_mtime))
                    except OSError:
                        continue

                    prev = observed.get(full)
                    if prev and prev["key"] == key:
                        prev["stable_count"] += 1
                        if prev["stable_count"] >= MANUAL_FILE_STABLE_POLLS:
                            # File is stable — process it
                            try:
                                await _process_dropped_file(full)
                            except Exception as exc:
                                logger.error(
                                    "Watcher error on %s: %s\n%s",
                                    full, exc, traceback.format_exc(),
                                )
                            processed_recently.add(full)
                    else:
                        observed[full] = {"key": key, "stable_count": 0}

                # Forget files that disappeared from the folder
                stale = set(observed.keys()) - current_paths
                for path in stale:
                    observed.pop(path, None)
                    processed_recently.discard(path)

            try:
                await asyncio.wait_for(
                    cancel_event.wait(),
                    timeout=MANUAL_WATCHER_POLL_INTERVAL_S,
                )
                break
            except asyncio.TimeoutError:
                pass
    finally:
        status["watcher_running"] = False
        logger.info("Manual mode watcher stopped")


async def _process_dropped_file(pdf_path: str) -> None:
    """Match + (install+validate) a single dropped PDF."""
    status = app_state["manual_mode"]
    status["last_file_seen"] = os.path.basename(pdf_path)
    status["last_file_seen_at"] = datetime.now(timezone.utc).isoformat()
    db = _get_db()

    match = await _match_dropped_file(pdf_path)

    if match["match_type"] == "ambiguous":
        status["pending_disambiguation"] = {
            "file_path": pdf_path,
            "file_name": os.path.basename(pdf_path),
            "candidates": match["candidates"],
        }
        status["last_message"] = (
            f"⚠ Ambiguous: {os.path.basename(pdf_path)} matches "
            f"{len(match['candidates'])} candidates — pick one in the UI"
        )
        try:
            await db.log_audit(AuditLogEntry(
                outcome="MANUAL_DROP_AMBIGUOUS",
                details=json.dumps({
                    "file": os.path.basename(pdf_path),
                    "candidates": match["candidates"],
                }),
            ))
        except Exception: pass
        return

    if match["matched"] is None:
        status["unmatched_count"] += 1
        entry = {
            "file_name": os.path.basename(pdf_path),
            "file_path": pdf_path,
            "detected_at": datetime.now(timezone.utc).isoformat(),
            "reason": match.get("reason", "No match"),
        }
        status["unmatched_files"].append(entry)
        # Keep list bounded
        if len(status["unmatched_files"]) > 200:
            status["unmatched_files"] = status["unmatched_files"][-200:]
        status["last_message"] = (
            f"⚠ No match: {os.path.basename(pdf_path)} — "
            "please rename to include the DOI"
        )
        try:
            await db.log_audit(AuditLogEntry(
                outcome="MANUAL_DROP_UNMATCHED",
                details=json.dumps(entry),
            ))
        except Exception: pass
        return

    # Matched — install + validate
    paper = match["matched"]
    result = await _install_and_validate_dropped(
        paper, pdf_path, match["match_type"]
    )
    outcome = result.get("outcome")

    try:
        await db.log_audit(AuditLogEntry(
            canonical_id=paper.canonical_id,
            outcome=f"MANUAL_DROP_{outcome.upper()}",
            details=json.dumps({
                "file": os.path.basename(pdf_path),
                "match_type": match["match_type"],
                "confidence": match["confidence"],
                **{k: v for k, v in result.items() if k not in ("paper",)},
            }),
        ))
    except Exception: pass

    if outcome == "complete":
        status["matched_and_validated"] += 1
        status["last_message"] = (
            f"✓ {result.get('filename') or os.path.basename(pdf_path)} "
            f"→ {paper.title[:50]} (VALID)"
        )
    elif outcome == "flagged":
        status["matched_and_validated"] += 1
        status["last_message"] = (
            f"⚠ {os.path.basename(pdf_path)} → {paper.title[:50]} "
            f"(flagged {result.get('identity_status')} — review in Results)"
        )
    else:
        status["validation_failed"] += 1
        reason = result.get("validation_status") or outcome
        status["last_message"] = (
            f"✗ {os.path.basename(pdf_path)} → {paper.title[:50]} "
            f"failed: {reason}"
        )


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@app.post("/api/manual/start")
async def manual_mode_start() -> JSONResponse:
    """Start the drop-folder watcher.

    Requires a configured manual_drop_folder. Idempotent — calling
    twice is a no-op.
    """
    config = _get_config()
    folder = (config.get("manual_drop_folder") or "").strip()
    if not folder:
        raise HTTPException(
            status_code=400,
            detail="manual_drop_folder is not set. Configure it in Settings.",
        )

    validation = _validate_output_path(folder)
    if not validation["ok"]:
        raise HTTPException(
            status_code=400,
            detail=f"Drop folder invalid: {validation.get('error') or 'unknown'}",
        )

    status = app_state["manual_mode"]
    if status.get("watcher_running"):
        return JSONResponse({"status": "already_running",
                             "drop_folder": status["drop_folder"]})

    # Reset counters for a fresh session
    status.update({
        "enabled": True,
        "drop_folder": validation["abs_path"],
        "last_message": "Watcher started",
        "matched_and_validated": 0,
        "validation_failed": 0,
        "unmatched_count": 0,
        "unmatched_files": [],
        "pending_disambiguation": None,
    })

    cancel = asyncio.Event()
    task = asyncio.create_task(
        _manual_mode_watcher(cancel),
        name="manual-mode-watcher",
    )
    app_state["manual_cancel_event"] = cancel
    app_state["manual_task"] = task

    # Persist enabled state
    config["manual_mode_enabled"] = True
    save_config(config)

    await _get_db().log_audit(AuditLogEntry(
        outcome="MANUAL_MODE_STARTED",
        details=json.dumps({"drop_folder": validation["abs_path"]}),
    ))

    return JSONResponse({"status": "started",
                         "drop_folder": validation["abs_path"]})


@app.post("/api/manual/stop")
async def manual_mode_stop() -> JSONResponse:
    """Stop the drop-folder watcher."""
    status = app_state["manual_mode"]
    cancel: Optional[asyncio.Event] = app_state.get("manual_cancel_event")
    task: Optional[asyncio.Task] = app_state.get("manual_task")

    if cancel:
        cancel.set()
    if task and not task.done():
        try:
            await asyncio.wait_for(task, timeout=10.0)
        except asyncio.TimeoutError:
            task.cancel()
            try: await task
            except (asyncio.CancelledError, Exception): pass

    status["enabled"] = False
    status["watcher_running"] = False
    status["last_message"] = "Watcher stopped"
    app_state["manual_task"] = None
    app_state["manual_cancel_event"] = None

    config = _get_config()
    config["manual_mode_enabled"] = False
    save_config(config)

    await _get_db().log_audit(AuditLogEntry(
        outcome="MANUAL_MODE_STOPPED",
        details=json.dumps({
            "matched": status.get("matched_and_validated", 0),
            "failed": status.get("validation_failed", 0),
            "unmatched": status.get("unmatched_count", 0),
        }),
    ))

    return JSONResponse({"status": "stopped"})


@app.get("/api/manual/status")
async def manual_mode_status() -> JSONResponse:
    """Full snapshot of Manual Mode state (heavier than the SSE slice)."""
    status = dict(app_state.get("manual_mode") or {})
    return JSONResponse(status)


@app.get("/api/manual/queue")
async def manual_mode_queue() -> JSONResponse:
    """Enriched queue listing with publisher-aware suggested action."""
    db = _get_db()
    papers = await db.get_manual_required_papers()
    prefix = _ezproxy_prefix()
    items: List[Dict[str, Any]] = []
    permanently = 0
    for p in papers:
        doi_url = f"https://doi.org/{p.doi}" if p.doi else None
        ezproxy_url = f"{prefix}{doi_url}" if doi_url else None
        is_unavail = p.user_override == "PERMANENTLY_UNAVAILABLE"
        if is_unavail:
            permanently += 1
        items.append({
            "canonical_id": p.canonical_id,
            "title": p.title,
            "first_author": p.first_author_lastname,
            "year": p.year,
            "journal": p.journal,
            "doi": p.doi,
            "doi_url": doi_url,
            "ezproxy_url": ezproxy_url,
            "publisher": p.publisher,
            "last_failure_code": p.last_failure_code,
            "suggested_action": _publisher_suggested_action(p),
            "user_override": p.user_override,
            "permanently_unavailable": is_unavail,
        })
    # Update gauge
    app_state["manual_mode"]["permanently_unavailable"] = permanently
    return JSONResponse({
        "total": len(items),
        "permanently_unavailable": permanently,
        "proxy_prefix": prefix,
        "papers": items,
    })


@app.get("/api/manual/queue.csv")
async def manual_mode_queue_csv() -> FileResponse:
    """CSV of the manual queue with EZproxy URLs for offline work."""
    db = _get_db()
    papers = await db.get_manual_required_papers()
    prefix = _ezproxy_prefix()

    export_dir = os.path.join(
        _get_config().get("output_directory", "./downloads"), "exports"
    )
    os.makedirs(export_dir, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    fname = f"manual_queue_{ts}.csv"
    fpath = os.path.join(export_dir, fname)

    with open(fpath, "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow([
            "canonical_id", "title", "first_author", "year", "journal",
            "publisher", "doi", "doi_url", "ezproxy_url",
            "last_failure_code", "suggested_action", "user_override",
        ])
        for p in papers:
            doi_url = f"https://doi.org/{p.doi}" if p.doi else ""
            w.writerow([
                p.canonical_id, p.title, p.first_author_lastname,
                p.year or "", p.journal or "", p.publisher,
                p.doi or "", doi_url,
                f"{prefix}{doi_url}" if doi_url else "",
                p.last_failure_code or "",
                _publisher_suggested_action(p),
                p.user_override or "",
            ])

    return FileResponse(fpath, media_type="text/csv", filename=fname)


@app.post("/api/manual/paper/{canonical_id}/mark-unobtainable")
async def manual_mark_unobtainable(
    canonical_id: str,
    note: str = Query(default=""),
) -> JSONResponse:
    """Tag a paper as PERMANENTLY_UNAVAILABLE.

    Keeps the paper in MANUAL_REQUIRED (so reviewers can still see
    it in the queue and in PRISMA reports) but marks it so it's
    visually distinguished and excluded from retry loops.
    """
    db = _get_db()
    paper = await db.get_paper(canonical_id)
    if paper is None:
        raise HTTPException(status_code=404, detail="Paper not found")

    now_iso = datetime.now(timezone.utc).isoformat()
    await db.update_paper_fields(
        canonical_id,
        user_override="PERMANENTLY_UNAVAILABLE",
        override_reason=note or "Marked as permanently unavailable",
        override_timestamp=now_iso,
        last_failure_code=FailureCode.PERMANENTLY_UNAVAILABLE.value,
    )
    await db.log_audit(AuditLogEntry(
        canonical_id=canonical_id,
        outcome="PERMANENTLY_UNAVAILABLE",
        failure_code=FailureCode.PERMANENTLY_UNAVAILABLE.value,
        details=json.dumps({"note": note or ""}),
    ))
    return JSONResponse({"status": "marked", "canonical_id": canonical_id})


@app.post("/api/manual/assign")
async def manual_mode_assign(
    file_path: str = Query(...),
    canonical_id: str = Query(...),
) -> JSONResponse:
    """User resolves an ambiguous match by picking a specific paper.

    Takes a dropped file that the auto-matcher couldn't confidently
    assign (narrow gap in fuzzy scores) and assigns it to the
    specified paper, then runs install + validation.
    """
    status = app_state["manual_mode"]
    if not os.path.isfile(file_path):
        raise HTTPException(status_code=404, detail="File no longer exists")

    db = _get_db()
    paper = await db.get_paper(canonical_id)
    if paper is None:
        raise HTTPException(status_code=404, detail="Paper not found")
    if paper.state != PaperState.MANUAL_REQUIRED.value:
        raise HTTPException(
            status_code=400,
            detail=f"Paper is in state {paper.state}, not MANUAL_REQUIRED",
        )

    result = await _install_and_validate_dropped(
        paper, file_path, match_type="user_assigned"
    )
    # Clear pending disambiguation if this was the candidate
    pd = status.get("pending_disambiguation") or {}
    if pd.get("file_path") == file_path:
        status["pending_disambiguation"] = None

    outcome = result.get("outcome")
    if outcome == "complete":
        status["matched_and_validated"] += 1
        status["last_message"] = f"✓ User-assigned: {os.path.basename(file_path)} → {paper.title[:40]}"
    elif outcome == "flagged":
        status["matched_and_validated"] += 1
        status["last_message"] = f"⚠ User-assigned (flagged): {paper.title[:40]}"
    else:
        status["validation_failed"] += 1
        status["last_message"] = f"✗ User-assigned failed: {outcome}"

    return JSONResponse({"status": "processed", "outcome": outcome, **result})


@app.post("/api/manual/discard-unmatched")
async def manual_mode_discard_unmatched(
    file_path: str = Query(...),
) -> JSONResponse:
    """Remove an unmatched file from the drop folder + the unmatched list."""
    status = app_state["manual_mode"]
    try:
        if os.path.isfile(file_path):
            os.unlink(file_path)
    except OSError as exc:
        raise HTTPException(status_code=500, detail=str(exc))
    status["unmatched_files"] = [
        f for f in status.get("unmatched_files", [])
        if f.get("file_path") != file_path
    ]
    return JSONResponse({"status": "discarded"})


# ===========================================================================
# ZOTERO INTEGRATION — Part 2 of paywalled-retrieval redesign
# ===========================================================================
#
# Philosophy: Zotero Connector is the best browser-side PDF capture
# tool that exists. Researchers using Zotero already have it set up.
# We piggyback: push the MANUAL_REQUIRED queue as a Zotero collection,
# then poll for child PDF attachments and ingest them through the
# same validation pipeline as Manual Mode (FIX 4).
#
# No new validation path. The Zotero poller writes a temp file and
# calls _install_and_validate_dropped() directly. The system never
# sees the user's Zotero credentials or library outside this module.

ZOTERO_POLL_INTERVAL_S = 60.0


def _zotero_client_from_config() -> Optional["ZoteroClient"]:
    """Construct a ZoteroClient from current effective config.

    Returns None if API key or user ID is missing.
    """
    from full_text_acquisition.zotero_client import ZoteroClient
    config = _get_config()
    api_key = (config.get("zotero_api_key") or "").strip()
    user_id = str(config.get("zotero_user_id") or "").strip()
    if not api_key or not user_id:
        return None
    return ZoteroClient(api_key=api_key, user_id=user_id)


@app.post("/api/zotero/test")
async def zotero_test_credentials() -> JSONResponse:
    """Verify the configured API key + user ID by calling /keys/{key}."""
    client = _zotero_client_from_config()
    if client is None:
        raise HTTPException(
            status_code=400,
            detail="Zotero API key and user ID must be configured first.",
        )
    try:
        result = await client.verify()
    finally:
        await client.close()
    if not result.get("ok"):
        raise HTTPException(
            status_code=400,
            detail=f"Zotero verification failed: {result.get('error')}",
        )
    data = result.get("data") or {}
    # Strip the API key out of the response — Zotero echoes it
    data.pop("key", None)
    return JSONResponse({"status": "ok", "info": data})


@app.post("/api/zotero/push")
async def zotero_push_queue() -> JSONResponse:
    """Push current MANUAL_REQUIRED queue to a Zotero collection.

    Idempotent: re-pushing items that already exist in the collection
    leaves them alone (Zotero dedups by item key; we always create
    new items so dedup is at the user's discretion).

    Creates the collection if it doesn't exist. The collection name
    is "SRMA Queue: {project_name}" by default; configurable via
    Settings.zotero_collection_name.
    """
    from full_text_acquisition.zotero_client import (
        build_item_from_paper, ZoteroError,
    )

    client = _zotero_client_from_config()
    if client is None:
        raise HTTPException(
            status_code=400,
            detail="Zotero API key and user ID must be configured first.",
        )
    config = _get_config()
    db = _get_db()
    cp = app_state.get("current_project") or {}
    state = app_state["zotero"]

    # Collection name: per config or auto-derived
    collection_name = (config.get("zotero_collection_name") or "").strip()
    if not collection_name:
        collection_name = f"SRMA Queue: {cp.get('project_name') or 'Default'}"

    try:
        # Find or create the collection
        col = await client.get_or_create_collection(collection_name)
        col_data = col.get("data") or col
        col_key = col_data.get("key") or col.get("key", "")
        if not col_key:
            raise HTTPException(
                status_code=500,
                detail=f"Zotero returned no collection key: {col}",
            )

        # Persist the discovered key + name into the project config
        # so the poller can find the collection later
        config["zotero_collection_key"] = col_key
        config["zotero_collection_name"] = collection_name
        save_config(config)

        # Build items from MANUAL_REQUIRED papers (limit DOI-bearing)
        papers = await db.get_manual_required_papers()
        items = []
        for p in papers:
            if not p.doi:
                continue   # Zotero items need DOI for matching
            items.append(build_item_from_paper(
                doi=p.doi, title=p.title or "",
                authors=p.authors or "",
                year=p.year, journal=p.journal,
            ))

        if not items:
            return JSONResponse({
                "status": "no_items",
                "collection_key": col_key,
                "collection_name": collection_name,
                "papers_in_queue": len(papers),
                "papers_with_doi": 0,
                "message": "No DOI-bearing papers in MANUAL_REQUIRED queue.",
            })

        # Push (batches of 50 inside the client)
        result = await client.add_items_to_collection(col_key, items)

        # Update state for SSE
        state["collection_key"] = col_key
        state["collection_name"] = collection_name
        state["pushed_last_run"] = result.get("created", 0)
        state["last_message"] = (
            f"Pushed {result.get('created', 0)} items to '{collection_name}'"
        )

        try:
            await db.log_audit(AuditLogEntry(
                outcome="ZOTERO_QUEUE_PUSHED",
                details=json.dumps({
                    "collection_key": col_key,
                    "collection_name": collection_name,
                    "items_pushed": result.get("created", 0),
                    "items_unchanged": result.get("unchanged", 0),
                    "items_failed": len(result.get("failed", [])),
                }),
            ))
        except Exception:
            pass

        return JSONResponse({
            "status": "pushed",
            "collection_key": col_key,
            "collection_name": collection_name,
            "papers_in_queue": len(papers),
            "papers_with_doi": len(items),
            **result,
        })
    except ZoteroError as exc:
        raise HTTPException(status_code=502, detail=str(exc))
    finally:
        await client.close()


@app.post("/api/zotero/poller/start")
async def zotero_poller_start() -> JSONResponse:
    """Start the Zotero poller.

    Requires API key, user ID, and a collection key (run /api/zotero/push
    first if the collection doesn't exist yet). Idempotent.
    """
    config = _get_config()
    if not (config.get("zotero_api_key") and config.get("zotero_user_id")):
        raise HTTPException(
            status_code=400,
            detail="Zotero API key + user ID required.",
        )
    if not config.get("zotero_collection_key"):
        raise HTTPException(
            status_code=400,
            detail=("No collection key configured. Run 'Push to Zotero' "
                    "first to create the collection."),
        )

    state = app_state["zotero"]
    if state.get("poller_running"):
        return JSONResponse({
            "status": "already_running",
            "collection_key": state.get("collection_key"),
        })

    await _start_zotero_poller_internal()
    config["zotero_poller_enabled"] = True
    save_config(config)

    return JSONResponse({
        "status": "started",
        "collection_key": state.get("collection_key"),
        "interval_s": ZOTERO_POLL_INTERVAL_S,
    })


@app.post("/api/zotero/poller/stop")
async def zotero_poller_stop() -> JSONResponse:
    """Stop the Zotero poller. Idempotent."""
    state = app_state["zotero"]
    cancel: Optional[asyncio.Event] = app_state.get("zotero_cancel_event")
    task: Optional[asyncio.Task] = app_state.get("zotero_task")
    if cancel:
        cancel.set()
    if task and not task.done():
        try:
            await asyncio.wait_for(task, timeout=10.0)
        except asyncio.TimeoutError:
            task.cancel()
            try: await task
            except (asyncio.CancelledError, Exception): pass
    app_state["zotero_task"] = None
    app_state["zotero_cancel_event"] = None
    state["enabled"] = False
    state["poller_running"] = False
    state["last_message"] = "Poller stopped"

    config = _get_config()
    config["zotero_poller_enabled"] = False
    save_config(config)
    return JSONResponse({"status": "stopped"})


@app.get("/api/zotero/status")
async def zotero_status() -> JSONResponse:
    """Full snapshot of Zotero integration state."""
    return JSONResponse(dict(app_state.get("zotero") or {}))


async def _start_zotero_poller_internal() -> None:
    """Spin up the poller task. Caller is responsible for the busy-check."""
    state = app_state["zotero"]
    cancel = asyncio.Event()
    task = asyncio.create_task(
        _zotero_poller(cancel),
        name="zotero-poller",
    )
    app_state["zotero_cancel_event"] = cancel
    app_state["zotero_task"] = task
    state["enabled"] = True
    state["poller_running"] = True
    state["ingested_count"] = 0
    state["validation_failed_count"] = 0
    state["last_message"] = "Poller starting..."
    state["last_poll_error"] = None


async def _zotero_poller(cancel_event: asyncio.Event) -> None:
    """Background loop: pull Zotero collection, ingest PDFs as papers."""
    from full_text_acquisition.zotero_client import (
        ZoteroError, extract_doi_from_zotero_item,
    )
    state = app_state["zotero"]
    state["poller_running"] = True
    seen_attachments: Set[str] = set()  # zotero attachment keys we've ingested

    try:
        while not cancel_event.is_set():
            await _zotero_poll_once(seen_attachments, state)
            try:
                await asyncio.wait_for(
                    cancel_event.wait(),
                    timeout=ZOTERO_POLL_INTERVAL_S,
                )
                break
            except asyncio.TimeoutError:
                pass
    finally:
        state["poller_running"] = False
        logger.info("Zotero poller stopped")


async def _zotero_poll_once(
    seen_attachments: Set[str],
    state: Dict[str, Any],
) -> None:
    """Single poll iteration. Updates state in place."""
    from full_text_acquisition.zotero_client import (
        ZoteroError, extract_doi_from_zotero_item,
    )
    config = _get_config()
    db = _get_db()
    collection_key = state.get("collection_key") or config.get("zotero_collection_key")
    if not collection_key:
        state["last_poll_error"] = "No collection key configured"
        state["last_message"] = "No collection — stop and re-push the queue"
        return

    client = _zotero_client_from_config()
    if client is None:
        state["last_poll_error"] = "Zotero credentials missing"
        return

    try:
        # List items in the collection (incremental via since-version)
        since_v = state.get("last_poll_version", 0) or 0
        items, new_version = await client.list_collection_items(
            collection_key, since_version=since_v,
        )
        state["last_poll_at"] = datetime.now(timezone.utc).isoformat()
        state["last_poll_items"] = len(items)
        state["last_poll_version"] = new_version
        state["last_poll_error"] = None

        if not items:
            state["last_message"] = "No new items since last poll"
            return

        # For each item: check for child PDF attachments and ingest
        ingested_this_round = 0
        for item in items:
            item_key = item.get("key") or item.get("data", {}).get("key", "")
            if not item_key:
                continue

            doi = extract_doi_from_zotero_item(item)
            if not doi:
                continue

            # Find the matching paper in our queue
            existing_cid = await db.check_duplicate_doi(doi)
            if not existing_cid:
                continue
            paper = await db.get_paper(existing_cid)
            if paper is None or paper.state != PaperState.MANUAL_REQUIRED.value:
                continue   # already handled (or wrong state)

            # Pull PDF attachment(s)
            try:
                children = await client.list_child_attachments(item_key)
            except ZoteroError as exc:
                logger.debug("List children failed for %s: %s", item_key, exc)
                continue

            pdf_attachments = []
            for c in children:
                cdata = c.get("data") or c
                if cdata.get("contentType") == "application/pdf":
                    ckey = c.get("key") or cdata.get("key", "")
                    if ckey and ckey not in seen_attachments:
                        pdf_attachments.append((ckey, cdata.get("filename") or "document.pdf"))

            if not pdf_attachments:
                continue

            # Take the first PDF attachment we haven't seen
            att_key, att_filename = pdf_attachments[0]
            try:
                pdf_bytes = await client.download_attachment(att_key)
            except ZoteroError as exc:
                logger.debug("Download failed for %s: %s", att_key, exc)
                continue

            if not pdf_bytes:
                continue

            # Write to a temp file, hand off to FIX 4's validation pipeline
            tmp_dir = os.path.join(
                _get_config().get("output_directory", "./downloads"),
                ".zotero-temp",
            )
            os.makedirs(tmp_dir, exist_ok=True)
            tmp_path = os.path.join(
                tmp_dir, f"zotero_{att_key}_{att_filename}",
            )
            try:
                with open(tmp_path, "wb") as fh:
                    fh.write(pdf_bytes)
                result = await _install_and_validate_dropped(
                    paper, tmp_path, "zotero",
                )
                seen_attachments.add(att_key)
                outcome = result.get("outcome")
                if outcome == "complete":
                    state["ingested_count"] = state.get("ingested_count", 0) + 1
                    state["last_message"] = (
                        f"✓ {att_filename} → {paper.title[:50]} (VALID)"
                    )
                elif outcome == "flagged":
                    state["ingested_count"] = state.get("ingested_count", 0) + 1
                    state["last_message"] = (
                        f"⚠ {att_filename} → {paper.title[:50]} (flagged "
                        f"{result.get('identity_status')})"
                    )
                else:
                    state["validation_failed_count"] = (
                        state.get("validation_failed_count", 0) + 1
                    )
                    state["last_message"] = (
                        f"✗ {att_filename}: {outcome}"
                    )
                ingested_this_round += 1

                try:
                    await db.log_audit(AuditLogEntry(
                        canonical_id=paper.canonical_id,
                        outcome=f"ZOTERO_INGEST_{outcome.upper()}",
                        details=json.dumps({
                            "zotero_item_key": item_key,
                            "zotero_attachment_key": att_key,
                            "filename": att_filename,
                            **{k: v for k, v in result.items()
                               if k not in ("paper",)},
                        }),
                    ))
                except Exception: pass
            except Exception as exc:
                logger.error(
                    "Zotero ingest error for %s: %s\n%s",
                    paper.canonical_id, exc, traceback.format_exc(),
                )
            finally:
                try:
                    if os.path.isfile(tmp_path):
                        os.unlink(tmp_path)
                except OSError: pass

        if ingested_this_round > 0:
            state["last_message"] = (
                f"Ingested {ingested_this_round} PDF(s) this poll"
            )

    except ZoteroError as exc:
        state["last_poll_error"] = str(exc)
        state["last_message"] = f"Poll failed: {exc}"
    except Exception as exc:
        state["last_poll_error"] = str(exc)
        state["last_message"] = f"Poll error: {type(exc).__name__}: {exc}"
        logger.error(
            "Zotero poller exception: %s\n%s", exc, traceback.format_exc(),
        )
    finally:
        await client.close()


# ===========================================================================
# BOOKMARKLET — Part 3 of paywalled-retrieval redesign
# ===========================================================================
#
# Goal: the fastest possible path from "user just viewed a paywalled
# paper in their browser" to "PDF validated + filed". User drags a
# bookmarklet to their bookmarks bar once; thereafter, clicking it on
# any publisher page (after they've logged in) extracts the DOI, fetches
# the PDF same-origin, and POSTs the bytes to us. The bookmarklet never
# sees credentials — it just uses the session cookies already on the
# publisher tab.
#
# Security model:
#   - Bookmarklet runs in the publisher's origin. It has that origin's
#     cookies. When it fetches the PDF, that's same-origin — no
#     cross-origin credential leak.
#   - When it POSTs to localhost, we do NOT accept cookies (CORS
#     allow_credentials=False). The PDF bytes + DOI string are all we
#     receive. No auth needed; localhost is the user's own machine.
#   - The endpoint is still rate-limited by our per-host limiter under
#     the "bookmarklet" host key (added below, conservative 2/s).
#
# DOI extraction (6 methods, tried in order):
#   1. <meta name="citation_doi" content="...">   (Highwire + most)
#   2. <meta name="DC.Identifier" content="10.*"> (Dublin Core)
#   3. <meta name="prism.doi">                    (PRISM)
#   4. schema.org JSON-LD  "@id" or "doi"
#   5. URL path matches 10.NNNN/… pattern
#   6. Visible text "DOI: 10.NNNN/…"  (last resort, regex)

@app.post("/api/bookmarklet/submit")
async def bookmarklet_submit(
    file: UploadFile = File(...),
    doi: str = Form(...),
    source_url: Optional[str] = Form(default=None),
) -> JSONResponse:
    """Accept a PDF + DOI from the bookmarklet, match, ingest, validate.

    Idempotent in the sense that re-submitting the same paper when it
    has already left the MANUAL_REQUIRED queue returns
    {"status": "skipped", "reason": "not in manual queue"} instead of
    re-running validation.
    """
    from full_text_acquisition.models import normalize_doi

    norm = normalize_doi(doi)
    if not norm:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid DOI: {doi!r} did not normalize.",
        )

    db = _get_db()
    cid = await db.check_duplicate_doi(norm)
    if not cid:
        raise HTTPException(
            status_code=404,
            detail=(f"DOI {norm} is not in this project's paper set. "
                    "Upload it first, or open the project that contains it."),
        )
    paper = await db.get_paper(cid)
    if paper is None:
        raise HTTPException(status_code=500, detail="Paper lookup failed")

    if paper.state != PaperState.MANUAL_REQUIRED.value:
        return JSONResponse({
            "status": "skipped",
            "reason": f"Paper state is {paper.state}, not MANUAL_REQUIRED.",
            "paper_title": paper.title,
            "canonical_id": paper.canonical_id,
        })

    content = await file.read()
    # Defend against HTML error pages that publishers serve instead of
    # the PDF (Elsevier interstitials, Wiley "access denied", Cloudflare
    # challenges, etc.). The bookmarklet already tries to catch this
    # client-side, but keep server-side validation as a hard boundary.
    if not content or not content.startswith(b"%PDF"):
        received_ct = (file.content_type or "unknown")
        preview = ""
        if content:
            try:
                preview = content[:120].decode("utf-8", errors="replace")
            except Exception:
                preview = repr(content[:60])
        raise HTTPException(
            status_code=400,
            detail=(
                f"Body is not a PDF (got {len(content)} bytes, "
                f"Content-Type: {received_ct}). The publisher likely "
                f"served an HTML wrapper instead of the PDF. Open the "
                f"PDF directly in a tab (URL should end in .pdf or the "
                f"browser should show the PDF viewer), then click the "
                f"bookmarklet on that tab — or paste the direct PDF URL "
                f"into the fallback box."
            ),
            headers={
                "X-Received-Bytes": str(len(content)),
                "X-Received-Content-Type": received_ct,
                "X-Received-Preview": preview[:120].replace("\n", " ").replace("\r", " "),
            },
        )

    # Write to a temp file, hand to the same validation pipeline as
    # Manual Mode. The "bookmarklet" match_type shows up in audit logs
    # so we can count provenance per paper.
    config = _get_config()
    tmp_dir = os.path.join(
        config.get("output_directory", "./downloads"),
        ".bookmarklet-temp",
    )
    os.makedirs(tmp_dir, exist_ok=True)
    safe_name = (file.filename or f"{norm.replace('/', '_')}.pdf").replace(os.sep, "_")
    tmp_path = os.path.join(tmp_dir, f"bm_{paper.canonical_id}_{safe_name}")

    try:
        with open(tmp_path, "wb") as fh:
            fh.write(content)
        result = await _install_and_validate_dropped(
            paper, tmp_path, "bookmarklet",
        )
        outcome = result.get("outcome")

        try:
            await db.log_audit(AuditLogEntry(
                canonical_id=paper.canonical_id,
                outcome=f"BOOKMARKLET_INGEST_{outcome.upper()}",
                details=json.dumps({
                    "doi": norm,
                    "source_url": source_url or "",
                    "filename": file.filename or "",
                    "bytes": len(content),
                    **{k: v for k, v in result.items() if k not in ("paper",)},
                }),
            ))
        except Exception: pass

        return JSONResponse({
            "status": "ok",
            "outcome": outcome,
            "paper_title": paper.title,
            "canonical_id": paper.canonical_id,
            "identity_status": result.get("identity_status"),
            "validation_id": result.get("validation_id"),
            "filename": result.get("filename"),
        })
    finally:
        try:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)
        except OSError:
            pass


# ---------------------------------------------------------------------------
# Bookmarklet source code — generated server-side so the user's server
# URL is baked in. The page that serves the install button wraps this
# into a javascript: URL and provides drag-to-bookmarks instructions.
# ---------------------------------------------------------------------------

_BOOKMARKLET_JS_TEMPLATE = r"""(function(){
var SERVER=__SERVER__;
function $(sel){return document.querySelector(sel);}
function metas(name){return Array.prototype.slice.call(
  document.querySelectorAll('meta[name="'+name+'"], meta[property="'+name+'"]'))
  .map(function(m){return m.content||'';}).filter(Boolean);}
function normDoi(s){if(!s)return '';var m=String(s).match(/10\.\d{4,9}\/[-._;()\/:A-Z0-9]+/i);
  return m?m[0].toLowerCase().replace(/[.,;)\]]+$/,''):'';}
function extractDoi(){
  // 1. citation_doi (Highwire, most publishers)
  var v=metas('citation_doi').concat(metas('citation_DOI'));
  for(var i=0;i<v.length;i++){var d=normDoi(v[i]);if(d)return d;}
  // 2. Dublin Core
  v=metas('DC.Identifier').concat(metas('DC.identifier'),metas('dc.identifier'));
  for(i=0;i<v.length;i++){d=normDoi(v[i]);if(d)return d;}
  // 3. PRISM
  v=metas('prism.doi').concat(metas('PRISM.doi'));
  for(i=0;i<v.length;i++){d=normDoi(v[i]);if(d)return d;}
  // 4. JSON-LD
  try{
    var ld=document.querySelectorAll('script[type="application/ld+json"]');
    for(i=0;i<ld.length;i++){
      try{var o=JSON.parse(ld[i].textContent);
        var cand=o.doi||o['@id']||(o.mainEntity&&o.mainEntity.doi)||'';
        d=normDoi(cand);if(d)return d;}catch(e){}
    }
  }catch(e){}
  // 5. URL
  d=normDoi(location.pathname+location.search);if(d)return d;
  d=normDoi(location.href);if(d)return d;
  // 6. Visible text (last resort)
  var t=(document.body&&document.body.innerText||'').slice(0,50000);
  var m=t.match(/doi[:\s]+10\.\d{4,9}\/[^\s]+/i);
  if(m){d=normDoi(m[0]);if(d)return d;}
  return '';
}
function extractPdfUrl(){
  // citation_pdf_url is the gold standard — Highwire pushes it on
  // publisher-hosted PDFs when the user has entitlement.
  var v=metas('citation_pdf_url');
  if(v[0]) return v[0];
  // Look for <link rel="alternate" type="application/pdf">
  var ls=document.querySelectorAll('link[type="application/pdf"], a[type="application/pdf"]');
  if(ls[0]) return ls[0].href;
  // Publisher-specific patterns — the landing page rarely points at
  // the real PDF, so check a few known-good URL shapes.
  var host=location.hostname.toLowerCase();
  var href=location.href;
  // Elsevier / ScienceDirect: pii-based PDF
  var m=href.match(/sciencedirect\.com\/science\/article\/(?:pii|abs\/pii)\/([A-Z0-9]+)/i);
  if(m){return location.protocol+'//'+host+'/science/article/pii/'+m[1]+'/pdfft?isDTMRedir=true&download=true';}
  // Wiley: /epdf/ → /pdfdirect/
  if(/wiley\.com|onlinelibrary\.wiley/.test(host)){
    var m2=href.match(/\/(?:doi|epdf|full)\/(10\.\d+\/[^?#]+)/);
    if(m2){return location.protocol+'//'+host+'/doi/pdfdirect/'+m2[1]+'?download=true';}
  }
  // Springer / Nature: content/pdf
  if(/springer|springernature|nature\.com/.test(host)){
    var m3=href.match(/\/(?:article|chapter)\/(10\.\d+\/[^?#]+)/);
    if(m3){return location.protocol+'//'+host+'/content/pdf/'+m3[1]+'.pdf';}
  }
  // Tandfonline
  if(/tandfonline\.com/.test(host)){
    var m4=href.match(/\/doi\/(?:abs|full)\/(10\.\d+\/[^?#]+)/);
    if(m4){return location.protocol+'//'+host+'/doi/pdf/'+m4[1]+'?download=true';}
  }
  // Generic fallback: same-page <a href="...pdf"> download link
  var as=document.querySelectorAll('a[href*=".pdf"]');
  for(var i=0;i<as.length;i++){
    var h=as[i].href;
    if(h&&/\.pdf(\?|$)/i.test(h)) return h;
  }
  return '';
}
// Client-side PDF sniff — reads the first 4 bytes of the blob.
// Cheap and prevents round-tripping HTML error pages to the server.
function isPdfBlob(blob){
  return new Promise(function(resolve){
    var fr=new FileReader();
    fr.onload=function(){
      var b=new Uint8Array(fr.result);
      resolve(b.length>=4 && b[0]===0x25 && b[1]===0x50 && b[2]===0x44 && b[3]===0x46);
    };
    fr.onerror=function(){resolve(false);};
    fr.readAsArrayBuffer(blob.slice(0,4));
  });
}
function overlay(){
  var old=document.getElementById('__srma_bm__');if(old)old.remove();
  var d=document.createElement('div');d.id='__srma_bm__';
  d.style.cssText='position:fixed;top:16px;right:16px;z-index:2147483647;'+
    'background:white;color:#1e293b;border:1px solid #cbd5e1;border-radius:10px;'+
    'box-shadow:0 10px 30px rgba(0,0,0,.18);padding:14px 16px;font:13px/1.45 '+
    '-apple-system,system-ui,Segoe UI,Roboto,sans-serif;max-width:380px';
  document.body.appendChild(d);return d;
}
function render(box,html){box.innerHTML=html;}
function esc(s){return String(s||'').replace(/[&<>"']/g,function(c){
  return({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'})[c];});}
async function submit(pdfBlob,doi,srcUrl,box){
  // Client-side %PDF sniff first — saves a round-trip when the
  // "download" the browser fetched is actually an HTML error page
  // (common on Elsevier, Wiley, Springer landing pages).
  var ok=await isPdfBlob(pdfBlob);
  if(!ok){
    pasteUrlUI(doi,box,'Not a PDF: '+Math.round(pdfBlob.size/1024)+' KB starting with non-%PDF bytes.');
    return;
  }
  render(box,'<b>SRMA</b> — uploading '+esc(Math.round(pdfBlob.size/1024))+' KB...');
  var fd=new FormData();
  fd.append('file',pdfBlob,(doi.replace(/[^a-z0-9]+/gi,'_'))+'.pdf');
  fd.append('doi',doi);
  if(srcUrl) fd.append('source_url',srcUrl);
  try{
    var r=await fetch(SERVER+'/api/bookmarklet/submit',
      {method:'POST',mode:'cors',credentials:'omit',body:fd});
    if(!r.ok){
      var t=await r.text();
      var ct=r.headers.get('X-Received-Content-Type')||'';
      var extra=ct?('<br><span style="font-size:11px;color:#64748b">Server saw: '+
        esc(ct)+'</span>'):'';
      render(box,'<b>SRMA</b> — <span style="color:#b91c1c">HTTP '+r.status+'</span><br>'+
        '<span style="font-size:12px">'+esc(t.slice(0,300))+'</span>'+extra+closeBtn());
      return;
    }
    var j=await r.json();
    if(j.status==='ok' && j.outcome==='complete'){
      render(box,'<b>✓ SRMA</b> — Validated<br><span style="font-size:12px">'+
        esc((j.paper_title||'').slice(0,80))+'</span>'+closeBtn());
    } else if(j.status==='ok'){
      render(box,'<b>⚠ SRMA</b> — Ingested but flagged: '+esc(j.outcome)+
        '<br><span style="font-size:12px">'+esc((j.paper_title||'').slice(0,80))+
        '</span>'+closeBtn());
    } else {
      render(box,'<b>SRMA</b> — '+esc(j.status)+': '+esc(j.reason||'')+closeBtn());
    }
    setTimeout(function(){var b=document.getElementById('__srma_bm__');if(b)b.remove();},8000);
  }catch(e){
    render(box,'<b>SRMA</b> — <span style="color:#b91c1c">Network error</span>: '+
      esc(String(e))+'<br><span style="font-size:11px">Is the server running at '+
      esc(SERVER)+' ?</span>'+closeBtn());
  }
}
function closeBtn(){
  return '<br><button onclick="document.getElementById(\'__srma_bm__\').remove()" '+
    'style="margin-top:8px;padding:3px 10px;font-size:11px;border:1px solid #cbd5e1;'+
    'background:#f8fafc;border-radius:4px;cursor:pointer">Dismiss</button>';
}
function pasteUrlUI(doi,box,reason){
  var hdr=reason?('<b>SRMA</b> — <span style="color:#b91c1c">'+esc(reason)+'</span>'):
    '<b>SRMA</b> — No PDF link found on this page.';
  render(box,hdr+'<br>'+
    '<span style="font-size:12px">Tip: right-click the download button → '+
    'Copy Link, then paste below. Or open the PDF in a fresh tab '+
    '(URL ends in <code>.pdf</code> or <code>pdfft</code> / '+
    '<code>/pdf/</code>) and click the bookmarklet there.<br><br>'+
    'Paste the direct PDF URL:</span>'+
    '<input id="__srma_u" style="display:block;width:100%;margin-top:6px;padding:5px;'+
    'border:1px solid #cbd5e1;border-radius:4px;font-size:12px" placeholder="https://...pdf">'+
    '<button id="__srma_go" style="margin-top:8px;padding:4px 12px;font-size:12px;'+
    'background:#3b82f6;color:white;border:none;border-radius:4px;cursor:pointer">'+
    'Fetch &amp; Upload</button>'+closeBtn());
  document.getElementById('__srma_go').addEventListener('click',async function(){
    var u=document.getElementById('__srma_u').value.trim();
    if(!u){return;}
    try{
      render(box,'<b>SRMA</b> — fetching '+esc(u.slice(0,60))+'...');
      var r=await fetch(u,{credentials:'include'});
      if(!r.ok) throw new Error('HTTP '+r.status);
      var b=await r.blob();
      if(b.size<1000) throw new Error('Response too small ('+b.size+' bytes)');
      await submit(b,doi,u,box);
    }catch(e){
      render(box,'<b>SRMA</b> — <span style="color:#b91c1c">Fetch failed</span>: '+
        esc(String(e))+closeBtn());
    }
  });
}
async function run(){
  var box=overlay();
  render(box,'<b>SRMA</b> — scanning page...');
  var doi=extractDoi();
  if(!doi){
    render(box,'<b>SRMA</b> — <span style="color:#b91c1c">No DOI found</span> '+
      'on this page.<br><span style="font-size:11px">Open the paper\'s '+
      'abstract page, then click the bookmarklet again.</span>'+closeBtn());
    return;
  }
  var pdfUrl=extractPdfUrl();
  if(!pdfUrl){pasteUrlUI(doi,box);return;}
  render(box,'<b>SRMA</b> — fetching PDF...<br><span style="font-size:11px">'+
    esc(pdfUrl.slice(0,80))+'</span>');
  try{
    var r=await fetch(pdfUrl,{credentials:'include'});
    if(!r.ok){
      render(box,'<b>SRMA</b> — fetch failed: HTTP '+r.status+
        '<br><span style="font-size:11px">Paste the direct PDF URL below:</span>'+
        closeBtn());
      pasteUrlUI(doi,box);return;
    }
    var blob=await r.blob();
    if(blob.size<1000){pasteUrlUI(doi,box);return;}
    await submit(blob,doi,pdfUrl,box);
  }catch(e){
    pasteUrlUI(doi,box);
  }
}
run();
})();"""


def _render_bookmarklet_js(server_url: str) -> str:
    """Return the bookmarklet JS with server URL baked in as a JSON string."""
    return _BOOKMARKLET_JS_TEMPLATE.replace("__SERVER__", json.dumps(server_url))


@app.get("/api/bookmarklet.js", response_class=PlainTextResponse)
async def bookmarklet_js(server_url: Optional[str] = Query(default=None)) -> PlainTextResponse:
    """Return the bookmarklet JS source, server URL baked in.

    Useful for audit + for users who want to inspect the code before
    installing it. The HTML install page uses the same source.
    """
    url = (server_url or "http://localhost:8000").rstrip("/")
    js = _render_bookmarklet_js(url)
    return PlainTextResponse(
        js,
        headers={"Cache-Control": "no-store"},
        media_type="application/javascript",
    )


@app.get("/api/bookmarklet", response_class=HTMLResponse)
async def bookmarklet_install_page(
    server_url: Optional[str] = Query(default=None),
) -> HTMLResponse:
    """Landing page with install instructions + drag-to-bookmark button."""
    url = (server_url or "http://localhost:8000").rstrip("/")
    js = _render_bookmarklet_js(url)
    # javascript: URL with the whole IIFE URI-encoded
    bm_href = "javascript:" + urllib.parse.quote(js, safe="")
    # The href is huge but that's how bookmarklets work
    html = f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8">
<title>SRMA PDF Bookmarklet — Install</title>
<style>
body{{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;
background:#f8f9fa;color:#1e293b;line-height:1.55;padding:32px;max-width:720px;margin:0 auto}}
h1{{font-size:22px;margin-bottom:6px}}
.subtitle{{color:#64748b;margin-bottom:22px}}
.bm{{display:inline-block;padding:10px 18px;background:#3b82f6;color:white;
text-decoration:none;border-radius:8px;font-weight:600;font-size:15px;
box-shadow:0 4px 12px rgba(59,130,246,.3);margin:12px 0}}
.bm:hover{{background:#2563eb}}
.card{{background:white;border:1px solid #e2e8f0;border-radius:10px;padding:18px 20px;
margin-bottom:16px}}
.card h3{{font-size:15px;margin-bottom:10px}}
ol{{margin-left:20px;line-height:1.8}}
code{{background:#f1f5f9;padding:2px 6px;border-radius:4px;font-size:13px;
font-family:Menlo,Consolas,monospace}}
.warn{{background:#fffbeb;border-left:3px solid #f59e0b;padding:10px 14px;
border-radius:6px;font-size:13px;margin:16px 0}}
details summary{{cursor:pointer;font-size:13px;color:#3b82f6;margin-top:12px}}
pre{{background:#0f172a;color:#e2e8f0;padding:14px 16px;border-radius:6px;
font-size:11px;overflow-x:auto;max-height:340px;line-height:1.5}}
</style></head><body>
<h1>SRMA PDF Bookmarklet</h1>
<div class="subtitle">Drag the button below to your bookmarks bar.
Then click it on any paywalled paper — after logging in — and it will
send the PDF to your local SRMA server.</div>

<a class="bm" href="{bm_href}">📄 Send to SRMA</a>

<div class="card">
  <h3>How it works</h3>
  <ol>
    <li>Drag the button above onto your browser's bookmarks bar
        (show it with <code>Ctrl+Shift+B</code> / <code>⌘⇧B</code>).</li>
    <li>Log into your institution's proxy (EZproxy, OpenAthens, Shibboleth)
        and open the paper's landing page.</li>
    <li>Click the <b>Send to SRMA</b> bookmarklet.</li>
    <li>A small overlay appears in the top-right of the page, extracts
        the DOI from page metadata, fetches the PDF same-origin, and
        posts it to <code>{url}</code>.</li>
    <li>The PDF runs through the same 7-check validation as Manual
        Mode. The overlay shows ✓ or ⚠ with the paper title.</li>
  </ol>
</div>

<div class="warn">
  <b>Security:</b> the bookmarklet uses the cookies already in your
  publisher tab to fetch the PDF — your credentials are never read
  by SRMA. The upload to localhost happens with
  <code>credentials: 'omit'</code> — no cookies are forwarded.
  The server URL (<code>{url}</code>) is baked into this bookmarklet
  at generation time.
</div>

<div class="card">
  <h3>When the automatic PDF fetch fails</h3>
  <p style="font-size:14px">Some publishers hide the PDF behind a
  click-through. If the bookmarklet can't find a direct PDF URL, it
  shows a paste box — right-click the PDF link on the page, copy it,
  paste, and click <b>Fetch &amp; Upload</b>.</p>
</div>

<details>
<summary>Show bookmarklet source code</summary>
<pre>{js.replace('<', '&lt;').replace('>', '&gt;')}</pre>
</details>

</body></html>
"""
    return HTMLResponse(html, headers={"Cache-Control": "no-store"})




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


@app.get("/api/export/queue-pack")
async def export_queue_pack(
    run_id: Optional[str] = Query(default=None),
) -> FileResponse:
    """Self-contained HTML navigation dashboard for MANUAL_REQUIRED papers.

    Philosophy: the user's OWN browser is the retrieval environment.
    This HTML file is a navigation aid that opens EZproxy-wrapped DOI
    links in new tabs. The actual PDF ingestion happens through the
    FIX 4 Manual Mode watched folder — there is NO "Mark as Retrieved
    without file" path. The HTML polls /api/manual/queue every 10s
    and turns cards green as papers leave the queue (validated by
    the watcher).

    REQUIREMENTS:
      - Manual Mode must have a drop folder configured. If not, the
        HTML page renders a banner telling the user what to set.
      - CORS (Part 0) must allow Origin: null so fetch() from file://
        reaches localhost successfully.

    The file is completely self-contained: no CDN fonts, no external
    scripts, no external CSS. Works opened as file://.
    """
    db = _get_db()

    # Validate run_id if given
    if run_id:
        run_record = await db.get_run(run_id)
        if run_record is None:
            raise HTTPException(status_code=404, detail=f"Run {run_id} not found")

    # Build the HTML at request time so the EZproxy prefix and server
    # URL reflect current config/runtime.
    proxy_prefix = _ezproxy_prefix()
    config = _get_config()
    drop_folder = config.get("manual_drop_folder") or ""
    current_project = app_state.get("current_project") or {}
    project_name = current_project.get("project_name", "Default")

    # The server URL is what the USER's file:// page will fetch from.
    # We bake in http://localhost:8000 by convention; if the server
    # runs on a different port, this breaks, but the current launch
    # path (app.main) always uses 8000.
    server_url = "http://localhost:8000"

    html = _render_queue_pack_html(
        run_id=run_id or "",
        project_name=project_name,
        proxy_prefix=proxy_prefix,
        drop_folder=drop_folder,
        server_url=server_url,
    )

    export_dir = os.path.join(
        config.get("output_directory", "./downloads"), "exports"
    )
    os.makedirs(export_dir, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    proj_slug = current_project.get("project_slug") or "project"
    fname = f"{proj_slug}_queue_pack_{ts}.html"
    fpath = os.path.join(export_dir, fname)
    with open(fpath, "w", encoding="utf-8") as fh:
        fh.write(html)

    return FileResponse(fpath, media_type="text/html", filename=fname)


_QUEUE_PACK_HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>SRMA Queue Pack — {project_name}</title>
<style>
*,*::before,*::after{{box-sizing:border-box;margin:0;padding:0}}
body{{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;
background:#f8f9fa;color:#1e293b;line-height:1.5;padding:24px;max-width:1100px;margin:0 auto}}
h1{{font-size:22px;margin-bottom:4px}}
.subtitle{{font-size:13px;color:#64748b;margin-bottom:16px}}
.banner{{padding:12px 16px;border-radius:8px;margin-bottom:16px;font-size:13px;line-height:1.5}}
.banner.err{{background:#fef2f2;border:1px solid #fecaca;color:#991b1b}}
.banner.warn{{background:#fffbeb;border:1px solid #fde68a;color:#92400e}}
.banner.info{{background:#eff6ff;border:1px solid #bfdbfe;color:#1e40af}}
.banner.ok{{background:#f0fdf4;border:1px solid #bbf7d0;color:#166534}}
.progress{{display:flex;align-items:center;gap:12px;padding:14px 18px;background:white;border:1px solid #e2e8f0;border-radius:8px;margin-bottom:16px}}
.progress-bar{{flex:1;height:10px;background:#e2e8f0;border-radius:5px;overflow:hidden}}
.progress-fill{{height:100%;background:#10b981;transition:width 0.3s ease}}
.progress-text{{font-weight:600;font-size:14px;white-space:nowrap}}
.tabs{{display:flex;gap:0;border-bottom:2px solid #e2e8f0;margin-bottom:14px}}
.tab{{padding:8px 14px;cursor:pointer;border-bottom:2px solid transparent;margin-bottom:-2px;
font-size:13px;color:#64748b;font-weight:500}}
.tab.active{{color:#3b82f6;border-bottom-color:#3b82f6}}
.tab .cnt{{background:#f1f5f9;padding:1px 6px;border-radius:8px;font-size:11px;margin-left:6px}}
.card{{background:white;border:1px solid #e2e8f0;border-radius:8px;padding:14px 16px;margin-bottom:10px;
display:flex;flex-direction:column;gap:6px;transition:background 0.3s,opacity 0.3s}}
.card.retrieved{{background:#f0fdf4;border-color:#bbf7d0}}
.card.unavail{{opacity:0.55;background:#fafafa}}
.card-title{{font-weight:600;font-size:14px;color:#1e293b}}
.card-meta{{font-size:12px;color:#64748b}}
.card-doi{{font-family:'SF Mono',Consolas,monospace;font-size:11px;color:#475569}}
.badges{{display:flex;gap:6px;flex-wrap:wrap;margin-top:2px}}
.badge{{display:inline-block;padding:2px 8px;border-radius:10px;font-size:10px;font-weight:600;
text-transform:uppercase;letter-spacing:0.3px}}
.b-success{{background:#d1fae5;color:#065f46}}
.b-warn{{background:#fef3c7;color:#92400e}}
.b-neutral{{background:#f1f5f9;color:#475569}}
.b-info{{background:#cffafe;color:#155e75}}
.actions{{display:flex;gap:6px;margin-top:6px;flex-wrap:wrap}}
.btn{{padding:5px 12px;border:none;border-radius:4px;font-size:12px;font-weight:500;
cursor:pointer;text-decoration:none;display:inline-flex;align-items:center;gap:4px}}
.btn-primary{{background:#3b82f6;color:white}}
.btn-primary:hover{{background:#2563eb}}
.btn-outline{{background:white;border:1px solid #e2e8f0;color:#1e293b}}
.btn-outline:hover{{background:#f1f5f9}}
.btn-danger{{background:white;border:1px solid #fecaca;color:#991b1b}}
.btn-danger:hover{{background:#fef2f2}}
.btn-success{{background:#10b981;color:white;cursor:default}}
#refresh-info{{font-size:11px;color:#94a3b8;margin-top:12px;text-align:right}}
.spinner{{display:inline-block;width:10px;height:10px;border:2px solid #e2e8f0;
border-top-color:#3b82f6;border-radius:50%;animation:spin 0.8s linear infinite}}
@keyframes spin{{to{{transform:rotate(360deg)}}}}
</style>
</head>
<body>

<h1>SRMA Queue Pack</h1>
<div class="subtitle">
  Project: <strong>{project_name}</strong>
  &middot; Run: <span id="run-display">{run_display}</span>
  &middot; Generated: <span id="gen-ts">{gen_ts}</span>
</div>

<div id="status-banner"></div>

<div class="progress">
  <div class="progress-text" id="progress-text">Loading...</div>
  <div class="progress-bar"><div class="progress-fill" id="progress-fill" style="width:0%"></div></div>
  <div class="progress-text" id="progress-pct"></div>
</div>

<div class="tabs">
  <div class="tab active" data-filter="all">All <span class="cnt" id="cnt-all">0</span></div>
  <div class="tab" data-filter="pending">Pending <span class="cnt" id="cnt-pending">0</span></div>
  <div class="tab" data-filter="retrieved">Retrieved <span class="cnt" id="cnt-retrieved">0</span></div>
  <div class="tab" data-filter="unavail">Unavailable <span class="cnt" id="cnt-unavail">0</span></div>
</div>

<div id="queue-list"></div>

<div id="refresh-info">
  <span class="spinner"></span> Auto-refreshing every 10 seconds from <code>{server_url}</code>
</div>

<script>
(function() {{
'use strict';
var SERVER = {server_url_js};
var PROXY_PREFIX = {proxy_prefix_js};
var DROP_FOLDER = {drop_folder_js};
var POLL_INTERVAL_MS = 10000;

var currentFilter = 'all';
var snapshot = {{ papers: [] }};  // last server snapshot

function escHtml(s) {{ if (!s) return ''; var d = document.createElement('div'); d.textContent = s; return d.innerHTML; }}
function escAttr(s) {{ return (s||'').replace(/&/g,'&amp;').replace(/"/g,'&quot;').replace(/'/g,'&#39;').replace(/</g,'&lt;'); }}

function classifyPaper(p) {{
  // Drop-folder handoff means we infer "retrieved" from the paper no
  // longer being in MANUAL_REQUIRED. But for THIS HTML we only fetch
  // /api/manual/queue which is always MANUAL_REQUIRED. So we need a
  // second signal: we track which canonical_ids were in the queue the
  // first time we saw it, and any that subsequently DISAPPEAR from
  // /api/manual/queue are marked retrieved.
  if (p.permanently_unavailable) return 'unavail';
  // ever-seen-and-now-missing is handled in pollQueue()
  return 'pending';
}}

var everSeen = {{}};   // canonical_id -> last-known metadata
var retrievedIds = {{}};  // canonical_id -> true once we infer retrieval
var unavailableIds = {{}}; // canonical_id -> true for permanently unavailable

function render() {{
  var all = Object.keys(everSeen).map(function(cid) {{ return everSeen[cid]; }});
  var list = document.getElementById('queue-list');
  if (all.length === 0) {{
    list.innerHTML = '<div class="banner info">The manual-required queue is empty. Trigger a retrieval run first.</div>';
    document.getElementById('progress-text').textContent = '0 / 0';
    document.getElementById('progress-pct').textContent = '';
    document.getElementById('progress-fill').style.width = '100%';
    document.getElementById('cnt-all').textContent = 0;
    document.getElementById('cnt-pending').textContent = 0;
    document.getElementById('cnt-retrieved').textContent = 0;
    document.getElementById('cnt-unavail').textContent = 0;
    return;
  }}

  var pending = [], retrieved = [], unavail = [];
  for (var i = 0; i < all.length; i++) {{
    var p = all[i];
    if (retrievedIds[p.canonical_id]) retrieved.push(p);
    else if (unavailableIds[p.canonical_id] || p.permanently_unavailable) unavail.push(p);
    else pending.push(p);
  }}
  var total = all.length;
  var done = retrieved.length + unavail.length;
  var pct = total > 0 ? Math.round((done / total) * 100) : 0;

  document.getElementById('progress-text').textContent = done + ' / ' + total + ' done';
  document.getElementById('progress-pct').textContent = pct + '%';
  document.getElementById('progress-fill').style.width = pct + '%';
  document.getElementById('cnt-all').textContent = total;
  document.getElementById('cnt-pending').textContent = pending.length;
  document.getElementById('cnt-retrieved').textContent = retrieved.length;
  document.getElementById('cnt-unavail').textContent = unavail.length;

  var visible;
  if (currentFilter === 'pending') visible = pending;
  else if (currentFilter === 'retrieved') visible = retrieved;
  else if (currentFilter === 'unavail') visible = unavail;
  else visible = all;

  if (visible.length === 0) {{
    list.innerHTML = '<div class="banner info">No papers match this filter.</div>';
    return;
  }}

  list.innerHTML = visible.map(function(p) {{ return renderCard(p); }}).join('');
}}

function renderCard(p) {{
  var isRetrieved = !!retrievedIds[p.canonical_id];
  var isUnavail = !!unavailableIds[p.canonical_id] || !!p.permanently_unavailable;
  var cls = 'card';
  if (isRetrieved) cls += ' retrieved';
  if (isUnavail) cls += ' unavail';

  var badges = [];
  if (isRetrieved) badges.push('<span class="badge b-success">&#10003; Retrieved</span>');
  else if (isUnavail) badges.push('<span class="badge b-neutral">Unavailable</span>');
  else badges.push('<span class="badge b-warn">Pending</span>');
  if (p.publisher) badges.push('<span class="badge b-info">' + escHtml(p.publisher) + '</span>');
  if (p.last_failure_code) badges.push('<span class="badge b-neutral">' + escHtml(p.last_failure_code) + '</span>');

  var ezproxyBtn = p.ezproxy_url
    ? '<a class="btn btn-primary" href="' + escAttr(p.ezproxy_url) + '" target="_blank" rel="noopener">Open via Library</a>' : '';
  var doiBtn = p.doi_url
    ? '<a class="btn btn-outline" href="' + escAttr(p.doi_url) + '" target="_blank" rel="noopener">Open DOI</a>' : '';
  var unavailBtn = (!isRetrieved && !isUnavail)
    ? '<button class="btn btn-danger" onclick="markUnavailable(\\'' + escAttr(p.canonical_id) + '\\')">Mark Unavailable</button>' : '';
  var retrievedMark = isRetrieved
    ? '<span class="btn btn-success">&#10003; File validated in system</span>' : '';

  var meta = [];
  if (p.first_author) meta.push(escHtml(p.first_author));
  if (p.year) meta.push(String(p.year));
  if (p.journal) meta.push(escHtml(p.journal));
  var metaStr = meta.join(' &middot; ');

  return '<div class="' + cls + '" id="card-' + escAttr(p.canonical_id) + '">' +
    '<div class="card-title">' + escHtml(p.title || '(no title)') + '</div>' +
    (metaStr ? '<div class="card-meta">' + metaStr + '</div>' : '') +
    (p.doi ? '<div class="card-doi">DOI: ' + escHtml(p.doi) + '</div>' : '') +
    '<div class="badges">' + badges.join('') + '</div>' +
    '<div class="actions">' + ezproxyBtn + doiBtn + unavailBtn + retrievedMark + '</div>' +
    '</div>';
}}

window.markUnavailable = function(cid) {{
  var note = prompt('Reason this paper is permanently unavailable? (optional)') || '';
  fetch(SERVER + '/api/manual/paper/' + encodeURIComponent(cid) + '/mark-unobtainable?note=' + encodeURIComponent(note), {{method:'POST'}})
    .then(function(r) {{
      if (!r.ok) throw new Error('HTTP ' + r.status);
      unavailableIds[cid] = true;
      render();
    }})
    .catch(function(err) {{
      alert('Failed to mark unavailable: ' + err.message);
    }});
}};

async function pollQueue() {{
  try {{
    var r = await fetch(SERVER + '/api/manual/queue');
    if (!r.ok) throw new Error('HTTP ' + r.status);
    var data = await r.json();

    // Clear prior banner on success
    document.getElementById('status-banner').innerHTML = '';

    var currentIds = {{}};
    for (var i = 0; i < (data.papers || []).length; i++) {{
      var p = data.papers[i];
      currentIds[p.canonical_id] = true;
      everSeen[p.canonical_id] = p;
      if (p.permanently_unavailable) unavailableIds[p.canonical_id] = true;
    }}
    // Inference: any paper we saw before but is now gone from the
    // queue has left MANUAL_REQUIRED. That happens when the FIX 4
    // watcher validates a drop-folder file and marks COMPLETE (or
    // when the paper goes to any non-MANUAL_REQUIRED state).
    Object.keys(everSeen).forEach(function(cid) {{
      if (!currentIds[cid] && !unavailableIds[cid]) {{
        retrievedIds[cid] = true;
      }}
    }});

    render();
  }} catch (err) {{
    showBanner('err',
      'Cannot reach SRMA server at <code>' + SERVER + '</code>. ' +
      'Make sure the app is running. (' + escHtml(err.message) + ')');
  }}
}}

function showBanner(kind, html) {{
  document.getElementById('status-banner').innerHTML =
    '<div class="banner ' + kind + '">' + html + '</div>';
}}

// Guard: if drop folder isn't configured, show a sticky error.
if (!DROP_FOLDER) {{
  showBanner('warn',
    '<strong>&#9888; Manual Mode drop folder is not configured.</strong> ' +
    'Open the app, go to SSO → Manual Mode, set a drop folder, ' +
    'and enable Manual Mode. The HTML pack is useless without the watcher.');
}} else {{
  showBanner('info',
    'Drop folder: <code>' + escHtml(DROP_FOLDER) + '</code> &nbsp;·&nbsp; ' +
    'Download PDFs here; the system validates automatically.');
}}

// Tab switching
var tabs = document.querySelectorAll('.tab');
for (var i = 0; i < tabs.length; i++) {{
  tabs[i].addEventListener('click', function(e) {{
    for (var j = 0; j < tabs.length; j++) tabs[j].classList.remove('active');
    e.currentTarget.classList.add('active');
    currentFilter = e.currentTarget.getAttribute('data-filter');
    render();
  }});
}}

// Kick off polling
pollQueue();
setInterval(pollQueue, POLL_INTERVAL_MS);
}})();
</script>
</body>
</html>
"""


def _render_queue_pack_html(
    run_id: str,
    project_name: str,
    proxy_prefix: str,
    drop_folder: str,
    server_url: str,
) -> str:
    """Produce the self-contained HTML queue pack.

    All user-controlled strings go through json.dumps() for safe
    interpolation into inline JavaScript, and through html.escape()
    for interpolation into HTML text.
    """
    import html as _html

    return _QUEUE_PACK_HTML_TEMPLATE.format(
        project_name=_html.escape(project_name),
        run_display=_html.escape(run_id or "(all)"),
        gen_ts=_html.escape(datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")),
        server_url=_html.escape(server_url),
        # JS-safe (JSON-quoted) — injects safely inside <script>
        server_url_js=json.dumps(server_url),
        proxy_prefix_js=json.dumps(proxy_prefix),
        drop_folder_js=json.dumps(drop_folder),
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
    # Include project slug so bundles from different projects are
    # immediately distinguishable when collected in a drop folder.
    current_proj = app_state.get("current_project") or {}
    proj_slug = current_proj.get("project_slug") or "project"
    bundle_filename = f"{proj_slug}_submission_bundle_{run_id}_{timestamp}.zip"
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


def _mask_secret(s: Optional[str]) -> str:
    """Mask all but last 4 chars of a secret. Empty stays empty."""
    if not s:
        return ""
    s = str(s)
    if len(s) <= 4:
        return "****"
    return "*" * (len(s) - 4) + s[-4:]


def _redact_config(config: Dict[str, Any]) -> Dict[str, Any]:
    """Return a copy of config with secrets masked, and a set_flag
    for each secret indicating whether it's currently configured.
    """
    redacted = dict(config)
    key = redacted.get("zotero_api_key") or ""
    redacted["zotero_api_key"] = _mask_secret(key)
    redacted["zotero_api_key_set"] = bool(key)
    return redacted


@app.get("/api/settings")
async def get_settings() -> JSONResponse:
    """Get current system settings and runtime status.

    The Zotero API key is masked (last 4 chars only) and a boolean
    `zotero_api_key_set` is added alongside it. The client must never
    see the full key after it's persisted.
    """
    config = _get_config()
    wizard: Optional[WizardResult] = app_state.get("wizard_result")

    resp = SettingsResponse(
        config=_redact_config(config),
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

    # Secrets that should never echo back to the client and should be
    # skipped if the UI re-submits the masked form by mistake.
    secret_fields = {"zotero_api_key"}
    audit_values: Dict[str, Any] = {}

    for key, value in update_dict.items():
        if key in secret_fields and isinstance(value, str):
            stripped = value.strip()
            # Skip if: empty string, or looks like the masked form (starts
            # with asterisks) — the client is confirming the existing value,
            # not setting a new one.
            if not stripped or stripped.startswith("*"):
                continue

        if key in config and config[key] != value:
            config[key] = value
            updated_fields.append(key)
        elif key not in config:
            config[key] = value
            updated_fields.append(key)

        if key in updated_fields:
            audit_values[key] = "***REDACTED***" if key in secret_fields else config[key]

    if updated_fields:
        save_config(config)
        app_state["config"] = config

        await _get_db().log_audit(AuditLogEntry(
            outcome="SETTINGS_UPDATED",
            details=json.dumps({
                "updated_fields": updated_fields,
                "new_values": audit_values,
            }),
        ))

        logger.info("Settings updated: %s", updated_fields)

    return JSONResponse({
        "status": "updated",
        "fields_changed": updated_fields,
        "config": _redact_config(config),
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

    # Current project (per-project isolation)
    cp = app_state.get("current_project") or {}
    payload["current_project"] = {
        "project_id": cp.get("project_id"),
        "project_name": cp.get("project_name"),
        "project_slug": cp.get("project_slug"),
    }

    # Manual Orchestration Mode — drop-folder watcher progress
    mm = app_state.get("manual_mode") or {}
    payload["manual_mode"] = {
        "enabled": bool(mm.get("enabled")),
        "drop_folder": mm.get("drop_folder", ""),
        "watcher_running": bool(mm.get("watcher_running")),
        "last_scan_at": mm.get("last_scan_at"),
        "last_file_seen": mm.get("last_file_seen"),
        "last_file_seen_at": mm.get("last_file_seen_at"),
        "last_message": mm.get("last_message", ""),
        "matched_and_validated": mm.get("matched_and_validated", 0),
        "validation_failed": mm.get("validation_failed", 0),
        "unmatched_count": mm.get("unmatched_count", 0),
        "permanently_unavailable": mm.get("permanently_unavailable", 0),
        "pending_disambiguation": bool(mm.get("pending_disambiguation")),
    }

    # Zotero integration — poller + push status (Part 2 paywalled
    # redesign). Credentials are never included in the SSE payload;
    # only the presence-flag `configured` is surfaced so the UI can
    # render the right control set.
    zt = app_state.get("zotero") or {}
    cfg = _get_config()
    payload["zotero"] = {
        "configured": bool(
            cfg.get("zotero_api_key")
            and cfg.get("zotero_user_id")
        ),
        "enabled": bool(zt.get("enabled")),
        "collection_key": zt.get("collection_key", ""),
        "collection_name": zt.get("collection_name", ""),
        "poller_running": bool(zt.get("poller_running")),
        "last_poll_at": zt.get("last_poll_at"),
        "last_poll_version": zt.get("last_poll_version", 0),
        "last_poll_items": zt.get("last_poll_items", 0),
        "last_poll_error": zt.get("last_poll_error"),
        "last_message": zt.get("last_message", ""),
        "ingested_count": zt.get("ingested_count", 0),
        "validation_failed_count": zt.get("validation_failed_count", 0),
        "pushed_last_run": zt.get("pushed_last_run", 0),
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
# BACKUP & RESTORE — per-project, SQLite backup API
# ===========================================================================
#
# Uses stdlib sqlite3.Connection.backup() (NOT file copy) so manual
# backups are consistent even while writers are active. Restore is a
# teardown + overwrite + rebuild operation analogous to project switch;
# requires quiescent state.

BACKUP_KIND_MANUAL = "manual"
BACKUP_KIND_AUTO = "auto"
BACKUP_KIND_SAFETY = "safety"
_AUTO_BACKUP_KEEP = 5
_BACKUP_STALE_DAYS = 7


def _project_backups_dir(slug: str, kind: str) -> str:
    return os.path.join(_project_dir(slug), "backups", kind)


def _ensure_backup_dirs(slug: str) -> None:
    for kind in (BACKUP_KIND_MANUAL, BACKUP_KIND_AUTO, BACKUP_KIND_SAFETY):
        os.makedirs(_project_backups_dir(slug, kind), exist_ok=True)


def _sqlite_backup_sync(source_path: str, target_path: str) -> None:
    """Blocking SQLite backup — run in executor.

    Opens a SEPARATE sqlite3 connection to the source file so we don't
    interfere with the aiosqlite write queue. In WAL mode, the backup
    API proceeds without blocking the writer.
    """
    import sqlite3 as _sqlite3
    src = _sqlite3.connect(source_path)
    try:
        dst = _sqlite3.connect(target_path)
        try:
            # pages=500 keeps each lock window short under heavy write load
            src.backup(dst, pages=500)
        finally:
            dst.close()
    finally:
        src.close()


def _sqlite_read_counts_sync(db_path: str) -> Dict[str, int]:
    """Synchronously read paper/run/complete counts from an arbitrary DB."""
    import sqlite3 as _sqlite3
    counts = {"paper_count": 0, "run_count": 0, "complete_count": 0}
    try:
        conn = _sqlite3.connect(db_path)
        try:
            try:
                counts["paper_count"] = conn.execute(
                    "SELECT COUNT(*) FROM papers"
                ).fetchone()[0]
            except _sqlite3.OperationalError: pass
            try:
                counts["run_count"] = conn.execute(
                    "SELECT COUNT(*) FROM runs"
                ).fetchone()[0]
            except _sqlite3.OperationalError: pass
            try:
                counts["complete_count"] = conn.execute(
                    "SELECT COUNT(*) FROM papers WHERE state = 'COMPLETE'"
                ).fetchone()[0]
            except _sqlite3.OperationalError: pass
        finally:
            conn.close()
    except Exception as exc:
        logger.debug("Read counts failed for %s: %s", db_path, exc)
    return counts


async def _create_backup(
    slug: str,
    kind: str = BACKUP_KIND_MANUAL,
) -> Dict[str, Any]:
    """Create a backup + write metadata sidecar. Safe during active workers."""
    if kind not in (BACKUP_KIND_MANUAL, BACKUP_KIND_AUTO, BACKUP_KIND_SAFETY):
        raise ValueError(f"Invalid backup kind: {kind}")

    _ensure_backup_dirs(slug)
    src_db = _project_db_path(slug)
    if not os.path.isfile(src_db):
        raise HTTPException(
            status_code=400,
            detail=f"Project DB does not exist yet: {src_db}",
        )

    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    suffix = uuid.uuid4().hex[:8]
    fname = f"acquisition_backup_{ts}_{suffix}.db"
    target = os.path.join(_project_backups_dir(slug, kind), fname)

    # Flush live write queue so recent ops are on disk before backup
    db = app_state.get("db")
    cp = app_state.get("current_project") or {}
    if db is not None and cp.get("project_slug") == slug:
        try:
            await db.flush_write_queue()
        except Exception: pass

    loop = asyncio.get_running_loop()
    try:
        await loop.run_in_executor(None, _sqlite_backup_sync, src_db, target)
    except Exception as exc:
        try:
            if os.path.exists(target): os.unlink(target)
        except OSError: pass
        raise HTTPException(
            status_code=500,
            detail=f"SQLite backup failed: {exc}",
        )

    counts = await loop.run_in_executor(None, _sqlite_read_counts_sync, target)
    size_bytes = os.path.getsize(target)
    meta = {
        "kind": kind,
        "filename": fname,
        "path": target,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "size_bytes": size_bytes,
        "size_human": _human_bytes(size_bytes),
        **counts,
    }
    with open(target + ".meta.json", "w") as fh:
        json.dump(meta, fh, indent=2)

    try:
        if db is not None:
            await db.log_audit(AuditLogEntry(
                outcome=f"BACKUP_{kind.upper()}",
                details=json.dumps({
                    "filename": fname,
                    "size_bytes": size_bytes,
                    **counts,
                }),
            ))
    except Exception: pass

    logger.info(
        "Created %s backup for project %s: %s (%d bytes)",
        kind, slug, fname, size_bytes,
    )
    return meta


def _read_backup_meta(backup_path: str) -> Dict[str, Any]:
    """Read sidecar metadata; fall back to os.stat if absent."""
    meta_path = backup_path + ".meta.json"
    if os.path.isfile(meta_path):
        try:
            with open(meta_path, "r") as fh:
                data = json.load(fh)
            try: data["size_bytes"] = os.path.getsize(backup_path)
            except OSError: pass
            data["size_human"] = _human_bytes(data.get("size_bytes", 0))
            return data
        except (OSError, json.JSONDecodeError):
            pass
    try:
        st = os.stat(backup_path)
        return {
            "kind": "unknown",
            "filename": os.path.basename(backup_path),
            "path": backup_path,
            "created_at": datetime.fromtimestamp(
                st.st_mtime, tz=timezone.utc
            ).isoformat(),
            "size_bytes": st.st_size,
            "size_human": _human_bytes(st.st_size),
            "paper_count": None, "run_count": None, "complete_count": None,
        }
    except OSError:
        return {}


def _list_backups_for_project(slug: str) -> Dict[str, List[Dict[str, Any]]]:
    """List backups by kind via sidecar metadata only — no DB opens."""
    _ensure_backup_dirs(slug)
    out: Dict[str, List[Dict[str, Any]]] = {
        BACKUP_KIND_MANUAL: [], BACKUP_KIND_AUTO: [], BACKUP_KIND_SAFETY: [],
    }
    for kind in out.keys():
        d = _project_backups_dir(slug, kind)
        if not os.path.isdir(d): continue
        for name in os.listdir(d):
            if not name.endswith(".db"): continue
            meta = _read_backup_meta(os.path.join(d, name))
            if meta: out[kind].append(meta)
        out[kind].sort(key=lambda m: m.get("created_at", ""), reverse=True)
    return out


def _rotate_auto_backups(slug: str, keep: int = _AUTO_BACKUP_KEEP) -> int:
    """Delete oldest auto backups beyond the keep limit."""
    d = _project_backups_dir(slug, BACKUP_KIND_AUTO)
    if not os.path.isdir(d): return 0
    entries: List[tuple] = []
    for name in os.listdir(d):
        if not name.endswith(".db"): continue
        full = os.path.join(d, name)
        try: entries.append((os.path.getmtime(full), full))
        except OSError: continue
    entries.sort(reverse=True)  # newest first

    deleted = 0
    for _mtime, full in entries[keep:]:
        try: os.unlink(full); deleted += 1
        except OSError: continue
        meta_path = full + ".meta.json"
        try:
            if os.path.isfile(meta_path): os.unlink(meta_path)
        except OSError: pass
    if deleted:
        logger.info("Rotated %d old auto backups for project %s", deleted, slug)
    return deleted


def _last_backup_info(slug: str) -> Dict[str, Any]:
    """Summary for UI: last backup + stale flag."""
    groups = _list_backups_for_project(slug)
    candidates: List[Dict[str, Any]] = []
    for k in (BACKUP_KIND_MANUAL, BACKUP_KIND_AUTO):
        candidates.extend(groups[k])
    if not candidates:
        return {
            "any_backup_exists": False, "stale": True,
            "last_backup_at": None, "last_backup_age_days": None,
            "last_backup_size_bytes": 0,
            "last_backup_filename": None, "last_backup_kind": None,
        }
    candidates.sort(key=lambda m: m.get("created_at", ""), reverse=True)
    latest = candidates[0]
    try:
        created = datetime.fromisoformat(latest["created_at"])
        if created.tzinfo is None:
            created = created.replace(tzinfo=timezone.utc)
        age_days = (datetime.now(timezone.utc) - created).total_seconds() / 86400.0
    except (ValueError, TypeError):
        age_days = None
    return {
        "any_backup_exists": True,
        "stale": age_days is None or age_days > _BACKUP_STALE_DAYS,
        "last_backup_at": latest.get("created_at"),
        "last_backup_age_days": round(age_days, 2) if age_days is not None else None,
        "last_backup_size_bytes": latest.get("size_bytes", 0),
        "last_backup_filename": latest.get("filename"),
        "last_backup_kind": latest.get("kind"),
    }


async def _restore_database_from_backup(
    slug: str, filename: str,
) -> Dict[str, Any]:
    """Restore the project's DB from a backup file.

    CALLERS MUST have verified _check_busy_reasons() == [].
    Sequence:
      1. Locate backup + verify path stays inside project backups/
      2. Flush + close current DB
      3. Safety backup of current DB (kind=safety)
      4. Delete -wal and -shm sidecars (CRITICAL)
      5. Copy backup -> acquisition.db
      6. Rebind project (reopens DB, migrations, reconciliation)
    """
    import shutil as _shutil

    project_backup_root = os.path.abspath(
        os.path.join(_project_dir(slug), "backups")
    )
    candidate: Optional[str] = None
    for kind in (BACKUP_KIND_MANUAL, BACKUP_KIND_AUTO, BACKUP_KIND_SAFETY):
        p = os.path.join(_project_backups_dir(slug, kind), filename)
        if os.path.isfile(p):
            candidate = p
            break
    if candidate is None:
        raise HTTPException(
            status_code=404, detail=f"Backup not found: {filename}",
        )
    # Defense in depth: ensure candidate stays inside project_backup_root
    if os.path.commonpath([
        os.path.abspath(candidate), project_backup_root,
    ]) != project_backup_root:
        raise HTTPException(
            status_code=403, detail="Backup path escapes project directory",
        )

    current_db = app_state.get("db")
    if current_db is not None:
        try:
            await current_db.flush_write_queue()
            await current_db.close()
        except Exception as exc:
            logger.warning("Error closing DB before restore: %s", exc)

    src_db = _project_db_path(slug)
    safety_info: Optional[Dict[str, Any]] = None
    if os.path.isfile(src_db):
        try:
            safety_info = await _create_backup(slug, kind=BACKUP_KIND_SAFETY)
        except Exception as exc:
            logger.error("Safety backup failed: %s — aborting restore", exc)
            # Try to restore app to a working state
            try: await _rebind_to_project(slug)
            except Exception: pass
            raise HTTPException(
                status_code=500,
                detail=f"Safety backup failed, restore aborted: {exc}",
            )

    # CRITICAL: delete WAL/SHM sidecars before overwriting
    for sidecar in (src_db + "-wal", src_db + "-shm"):
        try:
            if os.path.isfile(sidecar):
                os.unlink(sidecar)
                logger.info("Removed stale sidecar: %s", sidecar)
        except OSError as exc:
            logger.warning("Could not remove sidecar %s: %s", sidecar, exc)

    tmp_path = src_db + ".restoring"
    try:
        _shutil.copy2(candidate, tmp_path)
        os.replace(tmp_path, src_db)
    except OSError as exc:
        # Attempt rollback from safety
        if safety_info and os.path.isfile(safety_info["path"]):
            try:
                _shutil.copy2(safety_info["path"], src_db)
            except OSError: pass
        try:
            if os.path.exists(tmp_path): os.unlink(tmp_path)
        except OSError: pass
        raise HTTPException(
            status_code=500,
            detail=f"Restore copy failed: {exc}",
        )

    try:
        await _rebind_to_project(slug)
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"DB restored but rebind failed: {exc}",
        )

    return {
        "status": "restored",
        "restored_from": filename,
        "backup_meta": _read_backup_meta(candidate),
        "safety_backup_filename": (safety_info or {}).get("filename"),
    }


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


def _current_project_slug_or_404() -> str:
    cp = app_state.get("current_project") or {}
    slug = cp.get("project_slug")
    if not slug:
        raise HTTPException(status_code=503, detail="No active project")
    return slug


@app.post("/api/backup")
async def backup_create() -> JSONResponse:
    """Manual backup of the current project's DB.

    Safe during active workers (SQLite backup API on a separate
    connection). Writes a sidecar .meta.json for cheap listing.
    """
    slug = _current_project_slug_or_404()
    meta = await _create_backup(slug, kind=BACKUP_KIND_MANUAL)
    return JSONResponse({"status": "created", **meta})


@app.get("/api/backup/list")
async def backup_list() -> JSONResponse:
    """List backups grouped by kind. Reads sidecar metadata only."""
    slug = _current_project_slug_or_404()
    groups = _list_backups_for_project(slug)
    return JSONResponse({
        "manual": groups[BACKUP_KIND_MANUAL],
        "auto": groups[BACKUP_KIND_AUTO],
        "safety": groups[BACKUP_KIND_SAFETY],
        "last_backup": _last_backup_info(slug),
        "auto_keep_limit": _AUTO_BACKUP_KEEP,
        "stale_after_days": _BACKUP_STALE_DAYS,
    })


@app.post("/api/backup/restore")
async def backup_restore(filename: str = Query(...)) -> JSONResponse:
    """Restore current project's DB from a backup. Refuses while busy."""
    slug = _current_project_slug_or_404()
    busy = _check_busy_reasons()
    if busy:
        raise HTTPException(
            status_code=409,
            detail={
                "message": "Cannot restore while tasks are active",
                "active": busy,
                "guidance": "Stop running tasks, then retry restore.",
            },
        )
    return JSONResponse(await _restore_database_from_backup(slug, filename))


@app.delete("/api/backup/{filename}")
async def backup_delete(filename: str) -> JSONResponse:
    """Delete a specific backup + its metadata sidecar."""
    slug = _current_project_slug_or_404()
    project_backup_root = os.path.abspath(
        os.path.join(_project_dir(slug), "backups")
    )
    target: Optional[str] = None
    for kind in (BACKUP_KIND_MANUAL, BACKUP_KIND_AUTO, BACKUP_KIND_SAFETY):
        p = os.path.join(_project_backups_dir(slug, kind), filename)
        if os.path.isfile(p):
            if os.path.commonpath([
                os.path.abspath(p), project_backup_root,
            ]) != project_backup_root:
                raise HTTPException(403, "Path escapes project directory")
            target = p
            break
    if target is None:
        raise HTTPException(status_code=404, detail="Backup not found")
    try:
        os.unlink(target)
        mp = target + ".meta.json"
        if os.path.isfile(mp): os.unlink(mp)
    except OSError as exc:
        raise HTTPException(status_code=500, detail=str(exc))
    return JSONResponse({"status": "deleted", "filename": filename})


# ===========================================================================
# PROJECTS ENDPOINTS — per-project isolation
# ===========================================================================


@app.get("/api/projects")
async def list_projects() -> JSONResponse:
    """List every project + which one is current."""
    manifest = _load_projects_manifest()
    current = manifest.get("current_project_slug")
    items: List[Dict[str, Any]] = []
    for p in manifest.get("projects", []):
        items.append({
            **p,
            "is_current": p.get("project_slug") == current,
            "db_path": _project_db_path(p["project_slug"]),
            "output_dir": _project_output_dir(p["project_slug"]),
        })
    # Sort: current first, then by created_at
    items.sort(key=lambda x: (not x["is_current"], x.get("created_at", "")))
    return JSONResponse({
        "projects": items,
        "current_slug": current,
        "migration_notice": app_state.get("migration_notice") or {},
    })


@app.post("/api/projects")
async def create_project(body: "ProjectCreate") -> JSONResponse:
    """Create a new project: fresh slug, directory tree, empty DB with schema."""
    from full_text_acquisition.models import ProjectCreate as _PC
    # Validate via Pydantic (body already enforced by FastAPI, but be defensive)
    name = (body.project_name or "").strip()
    if not name:
        raise HTTPException(status_code=400, detail="project_name is required")

    manifest = _load_projects_manifest()
    existing_slugs = {p.get("project_slug") for p in manifest.get("projects", [])}
    slug = _slugify_project_name(name, existing_slugs)

    # Create directory tree
    os.makedirs(_project_dir(slug), exist_ok=True)
    os.makedirs(_project_output_dir(slug), exist_ok=True)
    os.makedirs(_project_supplement_dir(slug), exist_ok=True)
    _save_project_config_overlay(slug, {})  # empty overlay — inherits all

    # Initialize empty SQLite DB with full schema
    try:
        new_db = Database(_project_db_path(slug))
        await new_db.initialize()
        await new_db.close()
    except Exception as exc:
        # Roll back directory creation on DB init failure
        import shutil as _shutil
        try: _shutil.rmtree(_project_dir(slug))
        except OSError: pass
        raise HTTPException(
            status_code=500,
            detail=f"Failed to initialize project DB: {exc}",
        )

    project = {
        "project_id": str(uuid.uuid4()),
        "project_name": name,
        "project_slug": slug,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    manifest.setdefault("projects", []).append(project)
    _save_projects_manifest(manifest)

    return JSONResponse({
        **project,
        "is_current": False,
        "db_path": _project_db_path(slug),
        "output_dir": _project_output_dir(slug),
    })


@app.post("/api/projects/activate")
async def activate_project(slug: str = Query(...)) -> JSONResponse:
    """Switch to a different project.

    Refuses with 409 if any background task is active (retrieval run,
    enrichment, SSO session, or Manual Mode watcher) — user must stop
    those first to avoid data corruption.
    """
    manifest = _load_projects_manifest()
    project = _find_project(manifest, slug)
    if project is None:
        raise HTTPException(status_code=404, detail=f"Project '{slug}' not found")

    if slug == manifest.get("current_project_slug"):
        return JSONResponse({
            "status": "already_current",
            "current": dict(project),
        })

    busy = _check_busy_reasons()
    if busy:
        raise HTTPException(
            status_code=409,
            detail={
                "message": "Cannot switch projects while any of these are running",
                "active": busy,
                "guidance": "Stop each task from its own panel, then try again.",
            },
        )

    new_project = await _rebind_to_project(slug)
    return JSONResponse({
        "status": "switched",
        "current": new_project,
    })


@app.put("/api/projects/{slug}")
async def rename_project(
    slug: str,
    body: "ProjectCreate",
) -> JSONResponse:
    """Rename a project's DISPLAY NAME. Slug never changes (immutable path)."""
    name = (body.project_name or "").strip()
    if not name:
        raise HTTPException(status_code=400, detail="project_name is required")

    manifest = _load_projects_manifest()
    project = _find_project(manifest, slug)
    if project is None:
        raise HTTPException(status_code=404, detail="Project not found")

    project["project_name"] = name
    _save_projects_manifest(manifest)

    if slug == manifest.get("current_project_slug"):
        app_state["current_project"] = dict(project)

    return JSONResponse({
        **project,
        "is_current": slug == manifest.get("current_project_slug"),
    })


@app.delete("/api/projects/{slug}")
async def delete_project(slug: str) -> JSONResponse:
    """Delete a project (directory + DB). Refuses on current project.

    DESTRUCTIVE: this wipes the project's entire data. Frontend must
    confirm before calling this.
    """
    manifest = _load_projects_manifest()
    current = manifest.get("current_project_slug")
    project = _find_project(manifest, slug)
    if project is None:
        raise HTTPException(status_code=404, detail="Project not found")
    if slug == current:
        raise HTTPException(
            status_code=409,
            detail="Cannot delete the current project — switch to another first.",
        )
    if slug == DEFAULT_PROJECT_SLUG:
        raise HTTPException(
            status_code=409,
            detail="The Default project cannot be deleted.",
        )

    pdir = _project_dir(slug)
    import shutil as _shutil
    if os.path.isdir(pdir):
        try:
            _shutil.rmtree(pdir)
        except OSError as exc:
            raise HTTPException(status_code=500, detail=f"rmtree failed: {exc}")

    manifest["projects"] = [
        p for p in manifest.get("projects", [])
        if p.get("project_slug") != slug
    ]
    _save_projects_manifest(manifest)

    return JSONResponse({"status": "deleted", "slug": slug})


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
        "backup": _safe_backup_summary(),
    })


def _safe_backup_summary() -> Dict[str, Any]:
    """Helper: last-backup info for the current project, tolerating no-project."""
    cp = app_state.get("current_project") or {}
    slug = cp.get("project_slug")
    if not slug:
        return {"any_backup_exists": False, "stale": True,
                "last_backup_at": None, "last_backup_age_days": None,
                "last_backup_size_bytes": 0,
                "last_backup_filename": None, "last_backup_kind": None}
    try:
        return _last_backup_info(slug)
    except Exception:
        return {"any_backup_exists": False, "stale": True,
                "last_backup_at": None, "last_backup_age_days": None,
                "last_backup_size_bytes": 0,
                "last_backup_filename": None, "last_backup_kind": None}


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

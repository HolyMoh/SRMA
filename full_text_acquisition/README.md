# Full-Text Acquisition System

A **local-first, research-grade** full-text PDF acquisition engine for Systematic Reviews and Meta-Analyses (SRMAs). Deterministic, auditable, and designed for PRISMA 2020 compliance.

This is not a downloader. It is an **evidence acquisition engine**: reproducible, reviewer-proof, and suitable for submission alongside a published systematic review.

## Core Principles

- **FALSE NEGATIVE preferred over FALSE POSITIVE** — a missed paper is always safer than a wrong paper in the dataset
- **Every action logged** with timestamp, duration, and retry count to a SQLite audit trail
- **No credentials ever touch the system** — institutional login is user-mediated in a headed browser
- **PRISMA 2020 compliance** is a hard requirement — all reports are per-run and subcategorized
- **Crash-safe** — the system can be stopped, resumed, and re-run at any point without data loss
- **Reproducible** — every source of randomness is seeded per `run_id` for audit reproducibility

## Stack

| Component | Technology |
|-----------|-----------|
| Backend | Python + FastAPI (async-first) |
| Frontend | Single HTML file + vanilla JS |
| Automation | Playwright + stealth scripts |
| Database | SQLite with WAL mode |
| Launch | `python -m full_text_acquisition.app` → auto-opens localhost:8000 |
| Platforms | Windows, macOS, Linux |

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    FastAPI Application                    │
│  Upload │ Settings │ Health │ Run │ SSO │ Results        │
├─────────────────────────────────────────────────────────┤
│                                                          │
│  ┌──────────────┐    ┌──────────────────────────────┐   │
│  │ Worker Pool   │    │ Retrieval Engine              │   │
│  │              │    │                               │   │
│  │ Retrieval ×2 │───▶│ Tier 0: Unpaywall            │   │
│  │ Validation×2 │    │ Tier 1: PMC/EuroPMC/S2/OAlex │   │
│  │              │    │ Tier 2: Publisher Direct       │   │
│  │ Backpressure │    │ Tier 3: Institutional SSO     │   │
│  │ Disk Safety  │    │ Tier 3.5: Scholar-Assisted    │   │
│  └──────┬───────┘    │ Tier 4: Manual Flag           │   │
│         │            └──────────────┬────────────────┘   │
│         │                           │                    │
│  ┌──────▼───────────────────────────▼────────────────┐   │
│  │              SQLite (WAL mode)                     │   │
│  │  papers │ runs │ paper_runs │ audit_log            │   │
│  │  api_cache │ publisher_cooldowns │ supplements     │   │
│  │                                                    │   │
│  │  Single write queue │ Free reads │ Atomic claims   │   │
│  └────────────────────────────────────────────────────┘   │
│                                                          │
│  ┌────────────────────────────────────────────────────┐   │
│  │           Browser Manager (Singleton)              │   │
│  │  Disposable contexts (Tier 2, headless)            │   │
│  │  Persistent context  (Tier 3/3.5, headed)          │   │
│  │  Stealth injection │ atexit safety net             │   │
│  └────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────┘
```

## Features

### Multi-Tier Retrieval

| Tier | Method | Browser | Description |
|------|--------|---------|-------------|
| 0 | Unpaywall | None (HTTP) | Query Unpaywall API for open-access PDF URLs |
| 1 | OA APIs | None (HTTP) | PubMed Central, Europe PMC, Semantic Scholar, OpenAlex |
| 2 | Publisher Direct | Headless + Stealth | Per-publisher methods (Elsevier, Springer, Wiley, Nature, BMJ, Lancet, T&F, SAGE) with paywall detection |
| 3 | Institutional SSO | Headed + Stealth | User-mediated login via EZproxy/OpenAthens/resolver |
| 3.5 | Scholar-Assisted | SSO Session | Google Scholar search with safe-domain-only link following |
| 4 | Manual Flag | None | ILL / author contact / ResearchGate suggestions |

### PDF Validation (7 Checks)

1. **File size** > 50KB
2. **Magic bytes** (%PDF header)
3. **HTML disguise** detection
4. **PDF parsability** (page count via PyMuPDF/pypdf)
5. **Text extraction** from first 3 pages + **OCR fallback** (tesseract/ocrmypdf)
6. **Metadata-DOI cross-check** — exact DOI match, fuzzy title match (≥85%), different DOI detection
7. **SHA-256 content drift** detection with automatic versioning

### Hard Constraint

**VERSION_MISMATCH** or **CONTENT_UNVERIFIED** papers never auto-succeed. User override is required. False negative > false positive. Absolute.

### State Machine

```
INGESTED → NORMALIZED → ENRICHED → IDENTITY_ASSIGNED → READY_FOR_RETRIEVAL
    → RETRIEVING → RETRIEVED → VALIDATING → VALIDATED → COMPLETE

RETRIEVING → FAILED | MANUAL_REQUIRED
FAILED → READY_FOR_RETRIEVAL (on retry)
MANUAL_REQUIRED → READY_FOR_RETRIEVAL (on retry)
IDENTITY_ASSIGNED → DUPLICATE
```

All transitions are atomic SQLite transactions. No step executes unless the paper is in the correct preceding state.

### Safety & Compliance

- **No paywall bypass** without authenticated user session
- **No credential capture, injection, or storage**
- **No scraping behind login walls** without user-mediated interaction
- **Publisher rate limits and robots.txt respected**
- **Scholar links filtered** to known-safe domains only (publisher, PMC, arXiv, .edu, .ac.uk, etc.)
- **Designed for lawful academic use only**

### Run Isolation

- Each retrieval run gets a unique `run_id` with a config snapshot
- Papers link to runs via `paper_runs` table
- Papers completed in prior runs are detected as `ALREADY_RETRIEVED`
- All reports (PRISMA, Excel, audit) are filterable by run
- State machine operates globally; reporting operates per-run

### Publisher Adaptive Cooldown

- Tracks failure rates per publisher (rolling window of last 20 Tier 2 attempts)
- If failure rate > 70%: pauses Tier 2 for that publisher, routes to Tier 3
- Exponential cooldown extension (up to 60 minutes) if failures persist
- Manual override from Health Dashboard

### Integrity Scoring (0–100)

| Component | Points |
|-----------|--------|
| Source: OA API | 40 |
| Source: Publisher | 35 |
| Source: SSO / Scholar | 30 |
| Version: Published | +25 |
| Version: Accepted | +15 |
| Version: Preprint | +5 |
| Identity: DOI Verified | +25 |
| Identity: Title Verified | +15 |
| Supplements | +10 |

**Cap**: If identity is CONTENT_UNVERIFIED or VERSION_MISMATCH, total capped at 40.

**Bands**: HIGH 80–100 (green) | MEDIUM 50–79 (amber) | LOW 0–49 (red)

### Exports

- **Excel** (.xlsx) — Full results + summary statistics
- **PRISMA 2020** (.csv) — Per-run compliance report with subcategorized counts
- **Audit Log** (.jsonl) — Complete retrieval/validation event trail
- **Integration JSON** — Per-paper export for ASReview, extraction pipelines, bias assessment

---

## Installation

### Prerequisites

- **Python 3.9+**
- **pip** (or your preferred package manager)
- **Playwright Chromium** (auto-installed on first launch if missing)

Optional for OCR:
- **Tesseract** — install via system package manager (`apt install tesseract-ocr`, `brew install tesseract`, or download from GitHub)
- **Ghostscript** — install via system package manager (`apt install ghostscript`, `brew install ghostscript`)

### Install Steps

```bash
# 1. Clone the repository
git clone <repo-url>
cd SRMA

# 2. Create a virtual environment (recommended)
python -m venv venv
source venv/bin/activate   # Linux/macOS
# venv\Scripts\activate    # Windows

# 3. Install dependencies
pip install -r full_text_acquisition/requirements.txt

# 4. Install Playwright Chromium
playwright install chromium

# 5. Launch
python -m full_text_acquisition.app
```

The system auto-opens `http://localhost:8000` in your default browser.

### First-Launch Wizard

On first run (and re-runnable from Settings), the wizard checks:

| Check | Required | Action on Failure |
|-------|----------|-------------------|
| Python >= 3.9 | Yes | Exit with error |
| pip dependencies | Yes | List missing packages |
| Playwright + Chromium | Yes | Auto-install Chromium if Playwright present |
| Tesseract on PATH | No | OCR disabled gracefully |
| Ghostscript on PATH | No | ocrmypdf disabled, falls back to pytesseract |
| config.json current | Yes | Auto-migrate from older versions |

OCR availability is permanently visible in the Settings panel.

---

## Configuration

All settings are stored in `config.json` (created automatically on first run). Every setting is also editable from the Settings panel in the UI.

### config.json Reference

```json
{
  "config_version": 1,
  "output_directory": "./downloads",
  "supplement_directory": "./downloads/Supplements",
  "database_path": "./acquisition.db",

  "api_timeout_s": 10,
  "pdf_download_timeout_s": 30,
  "page_load_timeout_s": 20,
  "selector_timeout_s": 15,
  "pdf_validation_timeout_s": 10,
  "ocr_per_page_timeout_s": 30,

  "max_retries": 3,
  "max_total_attempts": 10,
  "retrieval_concurrency": 2,
  "validation_concurrency": 2,
  "backpressure_threshold": 20,
  "min_disk_space_bytes": 1073741824,

  "cache_ttl_days": 7,
  "cooldown_failure_threshold": 0.70,
  "cooldown_window_size": 20,
  "cooldown_minutes": 10,

  "unpaywall_email": "",
  "sso_proxy_url": "",
  "openathens_url": "",
  "institutional_resolver_url": "",
  "scholar_min_delay_s": 10,
  "scholar_max_queries_per_paper": 3
}
```

### Config Versioning

- `config_version` is checked on every startup
- Migrations are **additive only** — new keys are added with defaults, existing keys are never deleted
- If config version is newer than the application supports, the system warns and exits

---

## Usage

### 1. Upload

- Drag and drop a CSV or Excel file exported from Rayyan or Covidence
- The system auto-detects DOI, Title, Authors, Year, and Journal columns
- Review the 10-row preview and column mapping
- Deduplication report shows exact DOI and fuzzy title/author matches
- Metadata enrichment fills missing fields from OpenAlex and CrossRef
- Click **Start Retrieval** to begin

### 2. Run Control

- **Start** — begins retrieval workers for the selected run
- **Pause** — pauses retrieval workers (validation continues to drain)
- **Resume** — resumes paused retrieval
- **Cancel** — stops all workers and resets in-progress papers
- **Retry Failed / Mismatch / Manual / All** — re-queues eligible papers

### 3. SSO Access (Tier 3)

1. Configure at least one SSO URL in Settings (EZproxy, OpenAthens, or institutional resolver)
2. Click **Start SSO Login** — a headed browser opens to your institution's login page
3. Complete authentication manually (username, password, MFA)
4. Click **Continue** — the system verifies your session
5. Tier 3 and 3.5 retrieval will use your authenticated session
6. If the session expires, click **Re-authenticate**

The system **never accesses anything you type**.

### 4. CAPTCHA Handling

If Google Scholar detects automated access during Tier 3.5:
- The retrieval queue pauses
- A full-screen overlay appears: "CAPTCHA Detected"
- Solve the CAPTCHA in the headed browser window
- Click **CAPTCHA Resolved** to resume

### 5. Results & Overrides

- Filter by: All | Downloaded | Partial | Mismatch | Failed | Manual
- Sort by any column header
- Filter by specific run ID
- **Mismatch / Unverified papers** (amber highlight) require user action:
  - **Confirm Correct** — free-text reason required, counted separately in PRISMA
  - **Re-retrieve** — resets paper to READY_FOR_RETRIEVAL
- Manual tab sorted by integrity score ascending (lowest confidence first)

### 6. Exports

| Export | Format | Scope |
|--------|--------|-------|
| Excel Report | .xlsx (2 sheets) | Current run / specific run / all |
| PRISMA 2020 | .csv | Per run_id (required) |
| Audit Log | .jsonl | Current run / all |
| Integration | .json | Current run / all |

All exports respect the run_id filter selected in the Results panel.

### 7. Health Dashboard

Live SSE-powered metrics visible during any active run:
- Retrieval success rate (rolling 50)
- Average attempts per paper
- API failure rates by source tier
- Queue depths with backpressure indicator
- Active publisher cooldowns with manual reset
- Paywall detection counts by publisher
- Cache hit rate
- Disk space remaining
- Worker status (active/paused)

---

## File Structure

```
full_text_acquisition/
    __init__.py
    models.py              # Enums, dataclasses, Pydantic schemas, helpers
    database.py            # SQLite with WAL, write queue, state machine
    browser_manager.py     # Playwright singleton, stealth, CAPTCHA detection
    retrieval_engine.py    # Multi-tier retrieval, validation, scoring
    workers.py             # Worker pools, backpressure, disk safety
    app.py                 # FastAPI endpoints, lifespan, startup sequence
    requirements.txt       # Python dependencies
    README.md              # This file
    templates/
        index.html         # Single-page UI (HTML + CSS + vanilla JS)
```

## Database Tables

| Table | Purpose |
|-------|---------|
| `papers` | All paper metadata, state, validation results, file paths |
| `runs` | Run records with config snapshots |
| `paper_runs` | Per-run paper linkage and outcomes |
| `audit_log` | Every retrieval attempt, validation check, state transition |
| `api_cache` | Cached API responses (shared across runs, TTL-based) |
| `publisher_cooldowns` | Adaptive cooldown state per publisher |
| `supplements` | Downloaded supplement file metadata |

---

## Troubleshooting

### "Playwright Chromium not installed"

```bash
playwright install chromium
```

### "OCR unavailable"

Install system packages:
```bash
# Ubuntu/Debian
sudo apt install tesseract-ocr ghostscript

# macOS
brew install tesseract ghostscript

# Windows
# Download from: https://github.com/tesseract-ocr/tesseract
# Download from: https://ghostscript.com/releases/gsdnld.html
```

Then re-run the wizard from Settings.

### Database locked errors

The system uses WAL mode and a single write queue to prevent this. If you see lock errors:
1. Ensure only one instance of the application is running
2. Delete `acquisition.db-wal` and `acquisition.db-shm` files if they exist after a crash
3. Restart the application

### Papers stuck in RETRIEVING/VALIDATING

The system automatically resets these on startup (RETRIEVING → READY_FOR_RETRIEVAL, VALIDATING → RETRIEVED). Simply restart the application.

### Low disk space warning

The system pauses all workers when disk space falls below the configured threshold (default 1 GB). Free disk space and click Resume in the Run panel.

---

## License

This system is designed for lawful academic use only. No telemetry. No external data transmission. Local-first by design.

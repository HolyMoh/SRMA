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

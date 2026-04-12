"""
Full-Text Acquisition System — Models & Domain Types

Core data structures, enums, validation schemas, and helper functions
for a deterministic, auditable evidence acquisition engine designed for
Systematic Reviews and Meta-Analyses (SRMAs).
"""

from __future__ import annotations

import enum
import hashlib
import json
import re
import unicodedata
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set, Tuple

from pydantic import BaseModel, Field, field_validator

# ---------------------------------------------------------------------------
# Configuration version
# ---------------------------------------------------------------------------
CURRENT_CONFIG_VERSION: int = 1

# ---------------------------------------------------------------------------
# Timeout defaults (milliseconds unless noted)
# ---------------------------------------------------------------------------
DEFAULT_API_TIMEOUT_S: float = 10.0
DEFAULT_PDF_DOWNLOAD_TIMEOUT_S: float = 30.0
DEFAULT_PAGE_LOAD_TIMEOUT_S: float = 20.0
DEFAULT_SELECTOR_TIMEOUT_S: float = 15.0
DEFAULT_PDF_VALIDATION_TIMEOUT_S: float = 10.0
DEFAULT_OCR_PER_PAGE_TIMEOUT_S: float = 30.0
SHUTDOWN_DRAIN_TIMEOUT_S: float = 30.0

# ---------------------------------------------------------------------------
# Retry & concurrency defaults
# ---------------------------------------------------------------------------
DEFAULT_MAX_RETRIES: int = 3
DEFAULT_MAX_TOTAL_ATTEMPTS: int = 10
DEFAULT_RETRIEVAL_CONCURRENCY: int = 2
MAX_RETRIEVAL_CONCURRENCY: int = 5
DEFAULT_VALIDATION_CONCURRENCY: int = 2
MAX_VALIDATION_CONCURRENCY: int = 3
DEFAULT_BACKPRESSURE_THRESHOLD: int = 20
OCR_BACKPRESSURE_WEIGHT: int = 5
STANDARD_BACKPRESSURE_WEIGHT: int = 1

# ---------------------------------------------------------------------------
# Disk space safety
# ---------------------------------------------------------------------------
DEFAULT_MIN_DISK_SPACE_BYTES: int = 1_073_741_824  # 1 GB

# ---------------------------------------------------------------------------
# PDF validation thresholds
# ---------------------------------------------------------------------------
MIN_PDF_SIZE_BYTES: int = 51_200  # 50 KB
PDF_MAGIC_BYTES: bytes = b"%PDF"
TITLE_FUZZY_THRESHOLD: float = 0.85

# ---------------------------------------------------------------------------
# Scholar rate-limiting
# ---------------------------------------------------------------------------
SCHOLAR_MIN_DELAY_S: float = 10.0
SCHOLAR_MAX_QUERIES_PER_PAPER: int = 3

# ---------------------------------------------------------------------------
# Publisher cooldown defaults
# ---------------------------------------------------------------------------
DEFAULT_COOLDOWN_FAILURE_THRESHOLD: float = 0.70
DEFAULT_COOLDOWN_WINDOW_SIZE: int = 20
DEFAULT_COOLDOWN_MINUTES: int = 10
MAX_COOLDOWN_MINUTES: int = 60
COOLDOWN_POST_RETRY_COUNT: int = 3

# ---------------------------------------------------------------------------
# API cache TTL
# ---------------------------------------------------------------------------
DEFAULT_CACHE_TTL_DAYS: int = 7

# ---------------------------------------------------------------------------
# File naming
# ---------------------------------------------------------------------------
MAX_FILENAME_LENGTH: int = 60

STOPWORDS: Set[str] = {
    "a", "an", "the", "and", "or", "but", "in", "on", "at", "to", "for",
    "of", "with", "by", "from", "is", "it", "as", "be", "was", "were",
    "are", "been", "being", "have", "has", "had", "do", "does", "did",
    "will", "would", "shall", "should", "may", "might", "can", "could",
    "not", "no", "nor", "so", "if", "then", "than", "that", "this",
    "these", "those", "its", "into", "upon", "about", "between", "through",
    "during", "before", "after", "above", "below", "each", "every", "both",
    "few", "more", "most", "other", "some", "such", "only", "own", "same",
    "also", "just", "over", "very", "using", "based", "among", "versus",
    "via", "within", "without", "across", "along", "against",
}

# ---------------------------------------------------------------------------
# DOI prefix → publisher lookup (Signal 1)
# ---------------------------------------------------------------------------
DOI_PREFIX_PUBLISHER_MAP: Dict[str, str] = {
    "10.1016": "ELSEVIER",
    "10.1006": "ELSEVIER",
    "10.1053": "ELSEVIER",
    "10.1067": "ELSEVIER",
    "10.1078": "ELSEVIER",
    "10.1007": "SPRINGER",
    "10.1038": "NATURE",
    "10.1057": "NATURE",
    "10.1002": "WILEY",
    "10.1111": "WILEY",
    "10.1136": "BMJ",
    "10.1080": "TAYLOR_FRANCIS",
    "10.1081": "TAYLOR_FRANCIS",
    "10.1177": "SAGE",
    "10.1243": "SAGE",
    "10.1016/S0140-6736": "LANCET",
}

# ---------------------------------------------------------------------------
# Safe domains for Scholar-Assisted Retrieval (Tier 3.5)
# ---------------------------------------------------------------------------
SAFE_SCHOLAR_DOMAINS: Set[str] = {
    # Publisher domains
    "sciencedirect.com",
    "springer.com",
    "springerlink.com",
    "link.springer.com",
    "nature.com",
    "wiley.com",
    "onlinelibrary.wiley.com",
    "bmj.com",
    "thelancet.com",
    "tandfonline.com",
    "sagepub.com",
    "journals.sagepub.com",
    # Open-access repositories
    "ncbi.nlm.nih.gov",
    "pmc.ncbi.nlm.nih.gov",
    "europepmc.org",
    "arxiv.org",
    "medrxiv.org",
    "biorxiv.org",
    # Domain suffixes for institutional repositories
    ".edu",
    ".ac.uk",
    ".ac.jp",
    ".edu.au",
    ".ac.in",
    ".edu.cn",
}

# ---------------------------------------------------------------------------
# Paywall indicator strings (Tier 2 paywall detection)
# ---------------------------------------------------------------------------
PAYWALL_INDICATORS: List[str] = [
    "Purchase PDF",
    "Buy Article",
    "Subscribe to read",
    "Rent this article",
    "Get access",
    "Buy this article",
    "Purchase this article",
    "Sign in to access",
    "Institutional access",
    "Buy single article",
]

# ---------------------------------------------------------------------------
# Column detection mappings for CSV/Excel ingestion
# ---------------------------------------------------------------------------
COLUMN_ALIASES: Dict[str, List[str]] = {
    "doi": ["DOI", "doi", "Digital Object Identifier", "DI"],
    "title": ["Title", "title", "Article Title", "Primary Title", "TI", "T1"],
    "authors": ["Authors", "Author", "AU", "Author List", "A1"],
    "year": ["Year", "Publication Year", "PY", "year", "Y1"],
    "journal": ["Journal", "Source", "SO", "Publication", "JO", "JF", "T2"],
}

# ---------------------------------------------------------------------------
# State transition map — defines all valid (from_state → to_state) pairs
# ---------------------------------------------------------------------------
VALID_TRANSITIONS: Dict[str, Set[str]] = {
    "INGESTED": {"NORMALIZED"},
    "NORMALIZED": {"ENRICHED"},
    "ENRICHED": {"IDENTITY_ASSIGNED"},
    "IDENTITY_ASSIGNED": {"READY_FOR_RETRIEVAL", "DUPLICATE"},
    "READY_FOR_RETRIEVAL": {"RETRIEVING"},
    "RETRIEVING": {"RETRIEVED", "FAILED", "MANUAL_REQUIRED"},
    "RETRIEVED": {"VALIDATING"},
    "VALIDATING": {"VALIDATED", "FAILED"},
    "VALIDATED": {"COMPLETE"},
    "FAILED": {"READY_FOR_RETRIEVAL"},
    "MANUAL_REQUIRED": {"READY_FOR_RETRIEVAL"},
    "DUPLICATE": set(),  # terminal unless user explicitly unlocks
}

# ---------------------------------------------------------------------------
# Integrity score weights
# ---------------------------------------------------------------------------
SOURCE_SCORES: Dict[str, int] = {
    "TIER_0": 40,   # OA API (Unpaywall)
    "TIER_1": 40,   # Open Access APIs
    "TIER_2": 35,   # Publisher direct
    "TIER_3": 30,   # Institutional SSO
    "TIER_3_5": 30, # Scholar-assisted
    "TIER_4": 0,    # Manual
}

VERSION_SCORES: Dict[str, int] = {
    "PUBLISHED_VERSION": 25,
    "ACCEPTED_MANUSCRIPT": 15,
    "PREPRINT": 5,
    "SUPPLEMENTARY": 0,
    "UNKNOWN": 0,
}

IDENTITY_SCORES: Dict[str, int] = {
    "DOI_VERIFIED": 25,
    "TITLE_VERIFIED": 15,
    "CONTENT_UNVERIFIED": 0,
    "VERSION_MISMATCH": 0,
}

SUPPLEMENT_BONUS: int = 10
INTEGRITY_SCORE_MAX: int = 100
INTEGRITY_SCORE_MIN: int = 0

# Identity-unverified cap: if identity is CONTENT_UNVERIFIED or
# VERSION_MISMATCH, total score capped at this value
IDENTITY_UNVERIFIED_CAP: int = 40

# Confidence level thresholds
CONFIDENCE_HIGH_THRESHOLD: int = 80
CONFIDENCE_MEDIUM_THRESHOLD: int = 50

# ---------------------------------------------------------------------------
# Default configuration (written to config.json on first launch)
# ---------------------------------------------------------------------------
DEFAULT_CONFIG: Dict[str, Any] = {
    "config_version": CURRENT_CONFIG_VERSION,
    "output_directory": "./downloads",
    "supplement_directory": "./downloads/Supplements",
    "database_path": "./acquisition.db",
    "api_timeout_s": DEFAULT_API_TIMEOUT_S,
    "pdf_download_timeout_s": DEFAULT_PDF_DOWNLOAD_TIMEOUT_S,
    "page_load_timeout_s": DEFAULT_PAGE_LOAD_TIMEOUT_S,
    "selector_timeout_s": DEFAULT_SELECTOR_TIMEOUT_S,
    "pdf_validation_timeout_s": DEFAULT_PDF_VALIDATION_TIMEOUT_S,
    "ocr_per_page_timeout_s": DEFAULT_OCR_PER_PAGE_TIMEOUT_S,
    "max_retries": DEFAULT_MAX_RETRIES,
    "max_total_attempts": DEFAULT_MAX_TOTAL_ATTEMPTS,
    "retrieval_concurrency": DEFAULT_RETRIEVAL_CONCURRENCY,
    "validation_concurrency": DEFAULT_VALIDATION_CONCURRENCY,
    "backpressure_threshold": DEFAULT_BACKPRESSURE_THRESHOLD,
    "min_disk_space_bytes": DEFAULT_MIN_DISK_SPACE_BYTES,
    "cache_ttl_days": DEFAULT_CACHE_TTL_DAYS,
    "cooldown_failure_threshold": DEFAULT_COOLDOWN_FAILURE_THRESHOLD,
    "cooldown_window_size": DEFAULT_COOLDOWN_WINDOW_SIZE,
    "cooldown_minutes": DEFAULT_COOLDOWN_MINUTES,
    "unpaywall_email": "",
    "sso_proxy_url": "",
    "openathens_url": "",
    "institutional_resolver_url": "",
    "scholar_min_delay_s": SCHOLAR_MIN_DELAY_S,
    "scholar_max_queries_per_paper": SCHOLAR_MAX_QUERIES_PER_PAPER,
}

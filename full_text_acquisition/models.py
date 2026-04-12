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


# ===========================================================================
# ENUMS
# ===========================================================================


class PaperState(str, enum.Enum):
    """Global state machine states for each paper."""

    INGESTED = "INGESTED"
    NORMALIZED = "NORMALIZED"
    ENRICHED = "ENRICHED"
    IDENTITY_ASSIGNED = "IDENTITY_ASSIGNED"
    READY_FOR_RETRIEVAL = "READY_FOR_RETRIEVAL"
    RETRIEVING = "RETRIEVING"
    RETRIEVED = "RETRIEVED"
    VALIDATING = "VALIDATING"
    VALIDATED = "VALIDATED"
    COMPLETE = "COMPLETE"
    FAILED = "FAILED"
    MANUAL_REQUIRED = "MANUAL_REQUIRED"
    DUPLICATE = "DUPLICATE"
    ALREADY_RETRIEVED = "ALREADY_RETRIEVED"


class PublisherEnum(str, enum.Enum):
    """Known publisher identifiers for Tier 2 routing."""

    ELSEVIER = "ELSEVIER"
    SPRINGER = "SPRINGER"
    WILEY = "WILEY"
    NATURE = "NATURE"
    BMJ = "BMJ"
    LANCET = "LANCET"
    TAYLOR_FRANCIS = "TAYLOR_FRANCIS"
    SAGE = "SAGE"
    OTHER = "OTHER"


class TierEnum(str, enum.Enum):
    """Retrieval tier identifiers."""

    TIER_0 = "TIER_0"      # Unpaywall (pure HTTP)
    TIER_1 = "TIER_1"      # Open Access APIs (pure HTTP)
    TIER_2 = "TIER_2"      # Publisher-aware direct (Playwright headless)
    TIER_3 = "TIER_3"      # Institutional SSO (Playwright headed)
    TIER_3_5 = "TIER_3_5"  # Scholar-assisted retrieval (SSO session)
    TIER_4 = "TIER_4"      # Manual flag


class FailureCode(str, enum.Enum):
    """Complete failure taxonomy — every possible failure reason."""

    NO_DOI = "NO_DOI"
    NO_OA_SOURCE = "NO_OA_SOURCE"
    PUBLISHER_SOFT_BLOCK = "PUBLISHER_SOFT_BLOCK"
    PAYWALL_DETECTED = "PAYWALL_DETECTED"
    ACCESS_DENIED = "ACCESS_DENIED"
    NOT_FOUND = "NOT_FOUND"
    RATE_LIMITED = "RATE_LIMITED"
    SSO_FAILED = "SSO_FAILED"
    SESSION_EXPIRED = "SESSION_EXPIRED"
    SCHOLAR_BLOCKED = "SCHOLAR_BLOCKED"
    CAPTCHA_ENCOUNTERED = "CAPTCHA_ENCOUNTERED"
    PDF_INVALID = "PDF_INVALID"
    SIZE_TOO_SMALL = "SIZE_TOO_SMALL"
    IMAGE_ONLY_SCAN = "IMAGE_ONLY_SCAN"
    IMAGE_UNREADABLE = "IMAGE_UNREADABLE"
    CORRUPTED = "CORRUPTED"
    HTML_DISGUISED_AS_PDF = "HTML_DISGUISED_AS_PDF"
    VERSION_MISMATCH = "VERSION_MISMATCH"
    CONTENT_UNVERIFIED = "CONTENT_UNVERIFIED"
    CONTENT_UPDATED = "CONTENT_UPDATED"
    CONTENT_UNCHANGED = "CONTENT_UNCHANGED"
    INCOMPLETE_DOWNLOAD = "INCOMPLETE_DOWNLOAD"
    FILE_MISSING = "FILE_MISSING"
    METADATA_MISSING = "METADATA_MISSING"
    TIMEOUT = "TIMEOUT"
    MANUAL_REQUIRED = "MANUAL_REQUIRED"
    INTERRUPTED_RESET = "INTERRUPTED_RESET"
    STATE_MACHINE_ERROR = "STATE_MACHINE_ERROR"
    SUPPLEMENT_NON_PDF = "SUPPLEMENT_NON_PDF"
    ALREADY_RETRIEVED = "ALREADY_RETRIEVED"
    REUSED = "REUSED"
    PUBLISHER_COOLDOWN_ACTIVE = "PUBLISHER_COOLDOWN_ACTIVE"
    COOLDOWN_EXTENDED = "COOLDOWN_EXTENDED"


class ValidationStatus(str, enum.Enum):
    """Final validation status after Step 3 checks."""

    VALID = "VALID"                          # DOI verified
    VALID_TITLE = "VALID_TITLE"              # Title verified (≥85% fuzzy)
    VALID_OCR = "VALID_OCR"                  # OCR applied, then verified
    PARTIAL_SIZE = "PARTIAL_SIZE"            # File size < 50KB
    PARTIAL_IMAGE = "PARTIAL_IMAGE"          # Image-only, OCR unavailable
    PARTIAL_OCR = "PARTIAL_OCR"              # OCR applied, confirmed
    PARTIAL_IDENTITY = "PARTIAL_IDENTITY"    # Title match only
    VERSION_MISMATCH = "VERSION_MISMATCH"    # Different DOI found in PDF
    CONTENT_UNVERIFIED = "CONTENT_UNVERIFIED"  # No DOI or title match
    CONTENT_UPDATED = "CONTENT_UPDATED"      # Hash differs from baseline
    INVALID = "INVALID"                      # Terminal failure (corrupt/HTML/wrong)


class IdentityStatus(str, enum.Enum):
    """Result of metadata-DOI cross-check (Check 6)."""

    DOI_VERIFIED = "DOI_VERIFIED"
    TITLE_VERIFIED = "TITLE_VERIFIED"
    VERSION_MISMATCH = "VERSION_MISMATCH"
    CONTENT_UNVERIFIED = "CONTENT_UNVERIFIED"


class VersionType(str, enum.Enum):
    """Detected version classification (Step 3.5)."""

    PREPRINT = "PREPRINT"
    ACCEPTED_MANUSCRIPT = "ACCEPTED_MANUSCRIPT"
    PUBLISHED_VERSION = "PUBLISHED_VERSION"
    SUPPLEMENTARY = "SUPPLEMENTARY"
    UNKNOWN = "UNKNOWN"


class ContentDriftStatus(str, enum.Enum):
    """SHA-256 content drift detection result (Check 7)."""

    CONTENT_UNCHANGED = "CONTENT_UNCHANGED"
    CONTENT_UPDATED = "CONTENT_UPDATED"
    REUSED = "REUSED"
    FIRST_RETRIEVAL = "FIRST_RETRIEVAL"


class ConfidenceLevel(str, enum.Enum):
    """Integrity score confidence band."""

    HIGH = "HIGH"      # 80–100 (green)
    MEDIUM = "MEDIUM"  # 50–79  (amber)
    LOW = "LOW"        # 0–49   (red)


class RunStatus(str, enum.Enum):
    """Status of a retrieval run."""

    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    CANCELLED = "CANCELLED"
    FAILED = "FAILED"


# ===========================================================================
# DATACLASSES
# ===========================================================================


@dataclass
class Paper:
    """Core paper record persisted in SQLite papers table.

    Holds all metadata, retrieval state, validation results, and
    file-system references for a single paper throughout the pipeline.
    """

    # --- Identity ---
    canonical_id: str
    doi: Optional[str] = None
    pmid: Optional[str] = None
    openalex_id: Optional[str] = None
    title_hash: Optional[str] = None

    # --- Bibliographic metadata ---
    title: str = ""
    authors: str = ""
    first_author_lastname: str = ""
    year: Optional[int] = None
    journal: Optional[str] = None

    # --- State machine ---
    state: str = PaperState.INGESTED.value
    previous_state: Optional[str] = None

    # --- Publisher resolution ---
    publisher: str = PublisherEnum.OTHER.value
    publisher_signal_source: Optional[str] = None  # "doi_prefix" | "crossref" | "redirect_domain"

    # --- Retrieval metadata ---
    retrieval_tier: Optional[str] = None
    retrieval_method: Optional[str] = None
    retrieval_url: Optional[str] = None
    attempt_count: int = 0
    last_failure_code: Optional[str] = None

    # --- File storage ---
    pdf_path: Optional[str] = None
    pdf_filename: Optional[str] = None
    sha256_checksum: Optional[str] = None
    pdf_size_bytes: Optional[int] = None
    pdf_page_count: Optional[int] = None

    # --- Validation results ---
    validation_status: Optional[str] = None
    identity_status: Optional[str] = None
    version_type: str = VersionType.UNKNOWN.value
    is_primary: bool = True
    integrity_score: Optional[int] = None
    confidence_level: Optional[str] = None

    # --- OCR ---
    ocr_applied: bool = False
    ocr_text_extracted: bool = False

    # --- Content drift ---
    content_drift_status: Optional[str] = None
    content_drift_version: int = 1

    # --- Supplements ---
    supplement_count: int = 0

    # --- User override ---
    user_override: Optional[str] = None  # "CONFIRMED_CORRECT" | "RE_RETRIEVE"
    override_reason: Optional[str] = None
    override_timestamp: Optional[str] = None

    # --- Worker tracking ---
    worker_id: Optional[str] = None
    claimed_at: Optional[str] = None

    # --- Run linkage ---
    run_id: Optional[str] = None
    run_id_of_first_success: Optional[str] = None

    # --- Timestamps ---
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    updated_at: Optional[str] = None

    # --- Raw ingestion data ---
    raw_doi: Optional[str] = None
    raw_title: Optional[str] = None
    raw_authors: Optional[str] = None

    # --- Enrichment source tracking ---
    enrichment_source: Optional[str] = None  # "openalex" | "crossref"
    enriched_fields: Optional[str] = None    # JSON list of field names auto-filled

    def to_dict(self) -> Dict[str, Any]:
        """Serialize to dictionary for SQLite insertion."""
        return asdict(self)


@dataclass
class AuditLogEntry:
    """Single audit log entry written to SQLite and exported to JSONL.

    Every retrieval attempt, validation check, state transition,
    and system event produces one of these.
    """

    id: Optional[int] = None
    timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    canonical_id: Optional[str] = None
    run_id: Optional[str] = None
    tier: Optional[str] = None
    method: Optional[str] = None
    url_attempted: Optional[str] = None
    http_status: Optional[int] = None
    content_type_received: Optional[str] = None
    outcome: Optional[str] = None
    failure_code: Optional[str] = None
    execution_time_ms: Optional[float] = None
    retry_count: int = 0
    cache_hit: bool = False
    details: Optional[str] = None  # JSON string for extra context
    exception_type: Optional[str] = None
    exception_message: Optional[str] = None
    exception_traceback: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Serialize to dictionary for SQLite insertion."""
        return asdict(self)


@dataclass
class RunRecord:
    """Metadata for a single retrieval run (runs table)."""

    run_id: str
    started_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    completed_at: Optional[str] = None
    status: str = RunStatus.RUNNING.value
    total_submitted: int = 0
    config_snapshot: Optional[str] = None  # JSON copy of config at run start

    def to_dict(self) -> Dict[str, Any]:
        """Serialize to dictionary for SQLite insertion."""
        return asdict(self)


@dataclass
class PaperRun:
    """Links a paper to a specific run (paper_runs table).

    Tracks per-run outcomes independently from the global paper state.
    """

    canonical_id: str
    run_id: str
    submitted_in_this_run: bool = True
    retrieval_attempted_in_this_run: bool = False
    outcome_in_this_run: Optional[str] = None  # e.g. "RETRIEVED", "FAILED", "ALREADY_RETRIEVED"

    def to_dict(self) -> Dict[str, Any]:
        """Serialize to dictionary for SQLite insertion."""
        return asdict(self)


@dataclass
class PublisherCooldown:
    """Tracks adaptive cooldown state per publisher (publisher_cooldowns table)."""

    publisher: str
    failure_count: int = 0
    success_count: int = 0
    failure_rate: float = 0.0
    cooldown_until: Optional[str] = None  # ISO timestamp
    extended_count: int = 0
    last_updated: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    @property
    def is_in_cooldown(self) -> bool:
        """Check whether this publisher is currently in cooldown."""
        if self.cooldown_until is None:
            return False
        now = datetime.now(timezone.utc)
        try:
            expiry = datetime.fromisoformat(self.cooldown_until)
            if expiry.tzinfo is None:
                expiry = expiry.replace(tzinfo=timezone.utc)
            return now < expiry
        except (ValueError, TypeError):
            return False

    def to_dict(self) -> Dict[str, Any]:
        """Serialize to dictionary for SQLite insertion."""
        result = asdict(self)
        result.pop("is_in_cooldown", None)
        return result


@dataclass
class ApiCacheEntry:
    """Cached API response (api_cache table).

    Keyed by normalized DOI + api_name. Shared across all runs.
    """

    doi: str
    api_name: str  # "unpaywall" | "openalex" | "crossref" | "semantic_scholar" | "pmc" | "europepmc"
    response_json: str  # JSON string
    cached_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    expires_at: Optional[str] = None

    @property
    def is_expired(self) -> bool:
        """Check whether this cache entry has expired."""
        if self.expires_at is None:
            return False
        now = datetime.now(timezone.utc)
        try:
            expiry = datetime.fromisoformat(self.expires_at)
            if expiry.tzinfo is None:
                expiry = expiry.replace(tzinfo=timezone.utc)
            return now >= expiry
        except (ValueError, TypeError):
            return True

    def to_dict(self) -> Dict[str, Any]:
        """Serialize to dictionary for SQLite insertion."""
        result = asdict(self)
        result.pop("is_expired", None)
        return result


@dataclass
class SupplementFile:
    """Metadata for a downloaded supplement file."""

    canonical_id: str
    supplement_index: int  # 1-based
    filename: str
    file_path: str
    file_extension: str
    file_size_bytes: Optional[int] = None
    sha256_checksum: Optional[str] = None
    validation_status: Optional[str] = None  # "VALID" for PDFs, "SUPPLEMENT_NON_PDF" for others
    source_url: Optional[str] = None
    downloaded_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def to_dict(self) -> Dict[str, Any]:
        """Serialize to dictionary for SQLite insertion."""
        return asdict(self)


@dataclass
class ConfigSnapshot:
    """Immutable snapshot of configuration at run start.

    Stored as JSON in RunRecord.config_snapshot for audit reproducibility.
    """

    config_version: int
    output_directory: str
    supplement_directory: str
    database_path: str
    api_timeout_s: float
    pdf_download_timeout_s: float
    page_load_timeout_s: float
    selector_timeout_s: float
    pdf_validation_timeout_s: float
    ocr_per_page_timeout_s: float
    max_retries: int
    max_total_attempts: int
    retrieval_concurrency: int
    validation_concurrency: int
    backpressure_threshold: int
    min_disk_space_bytes: int
    cache_ttl_days: int
    cooldown_failure_threshold: float
    cooldown_window_size: int
    cooldown_minutes: int
    unpaywall_email: str
    sso_proxy_url: str
    openathens_url: str
    institutional_resolver_url: str
    scholar_min_delay_s: float
    scholar_max_queries_per_paper: int

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> ConfigSnapshot:
        """Create a ConfigSnapshot from a configuration dictionary.

        Ignores unknown keys so forward-compatible configs don't break.
        """
        known_fields = {f.name for f in cls.__dataclass_fields__.values()}
        filtered = {k: v for k, v in data.items() if k in known_fields}
        return cls(**filtered)

    def to_json(self) -> str:
        """Serialize to JSON string for storage in runs table."""
        return json.dumps(asdict(self), indent=2)

    @classmethod
    def from_json(cls, json_str: str) -> ConfigSnapshot:
        """Deserialize from JSON string."""
        data = json.loads(json_str)
        return cls.from_dict(data)


# ===========================================================================
# PYDANTIC MODELS — API Request / Response Schemas
# ===========================================================================


class ColumnMapping(BaseModel):
    """User-confirmed column mapping from uploaded CSV/Excel."""

    doi_column: Optional[str] = None
    title_column: Optional[str] = None
    authors_column: Optional[str] = None
    year_column: Optional[str] = None
    journal_column: Optional[str] = None


class UploadRequest(BaseModel):
    """Request body for file upload endpoint."""

    filename: str = Field(..., description="Name of the uploaded file")
    column_mapping: ColumnMapping = Field(
        default_factory=ColumnMapping,
        description="User-confirmed column mapping overrides",
    )
    auto_start: bool = Field(
        default=False,
        description="Whether to start retrieval immediately after ingestion",
    )


class DeduplicationMatch(BaseModel):
    """A single duplicate pair found during deduplication."""

    paper_a_title: str
    paper_a_doi: Optional[str] = None
    paper_b_title: str
    paper_b_doi: Optional[str] = None
    match_type: str  # "exact_doi" | "fuzzy_title_author"
    similarity_score: Optional[float] = None


class DeduplicationReport(BaseModel):
    """Report of duplicates found during ingestion."""

    total_submitted: int
    unique_papers: int
    duplicates_removed: int
    matches: List[DeduplicationMatch] = Field(default_factory=list)


class EnrichmentResult(BaseModel):
    """Summary of metadata enrichment from external APIs."""

    total_papers: int
    enriched_count: int
    enrichment_sources: Dict[str, int] = Field(
        default_factory=dict,
        description="Count per source: {'openalex': N, 'crossref': M}",
    )
    fields_filled: Dict[str, int] = Field(
        default_factory=dict,
        description="Count of papers where each field was auto-filled",
    )
    failed_count: int = 0


class UploadResponse(BaseModel):
    """Response after file upload and ingestion."""

    run_id: str
    total_submitted: int
    total_after_dedup: int
    preview_rows: List[Dict[str, Any]] = Field(
        default_factory=list,
        description="First 10 rows for user confirmation",
    )
    detected_columns: ColumnMapping = Field(default_factory=ColumnMapping)
    deduplication_report: DeduplicationReport
    enrichment_result: Optional[EnrichmentResult] = None
    already_retrieved_count: int = 0
    ready_for_retrieval_count: int = 0


class RetryRequest(BaseModel):
    """Request to retry failed or mismatched papers."""

    canonical_ids: Optional[List[str]] = Field(
        default=None,
        description="Specific papers to retry. None = all eligible.",
    )
    retry_type: str = Field(
        default="failed",
        description="'failed' | 'mismatch' | 'manual' | 'all_eligible'",
    )

    @field_validator("retry_type")
    @classmethod
    def validate_retry_type(cls, v: str) -> str:
        allowed = {"failed", "mismatch", "manual", "all_eligible"}
        if v not in allowed:
            raise ValueError(f"retry_type must be one of {allowed}")
        return v


class OverrideRequest(BaseModel):
    """Request to override a VERSION_MISMATCH or CONTENT_UNVERIFIED paper."""

    canonical_id: str
    action: str = Field(..., description="'confirm_correct' | 're_retrieve'")
    reason: str = Field(
        default="",
        description="Free-text reason (required for confirm_correct)",
    )

    @field_validator("action")
    @classmethod
    def validate_action(cls, v: str) -> str:
        allowed = {"confirm_correct", "re_retrieve"}
        if v not in allowed:
            raise ValueError(f"action must be one of {allowed}")
        return v


class PaperResponse(BaseModel):
    """API response schema for a single paper in the results table."""

    canonical_id: str
    doi: Optional[str] = None
    pmid: Optional[str] = None
    openalex_id: Optional[str] = None
    title: str = ""
    authors: str = ""
    year: Optional[int] = None
    journal: Optional[str] = None
    state: str
    validation_status: Optional[str] = None
    identity_status: Optional[str] = None
    version_type: Optional[str] = None
    integrity_score: Optional[int] = None
    confidence_level: Optional[str] = None
    retrieval_tier: Optional[str] = None
    attempt_count: int = 0
    last_failure_code: Optional[str] = None
    pdf_filename: Optional[str] = None
    pdf_size_bytes: Optional[int] = None
    pdf_page_count: Optional[int] = None
    ocr_applied: bool = False
    supplement_count: int = 0
    user_override: Optional[str] = None
    override_reason: Optional[str] = None
    run_id: Optional[str] = None
    content_drift_version: int = 1

    @classmethod
    def from_paper(cls, paper: Paper) -> PaperResponse:
        """Create a PaperResponse from a Paper dataclass."""
        return cls(
            canonical_id=paper.canonical_id,
            doi=paper.doi,
            pmid=paper.pmid,
            openalex_id=paper.openalex_id,
            title=paper.title,
            authors=paper.authors,
            year=paper.year,
            journal=paper.journal,
            state=paper.state,
            validation_status=paper.validation_status,
            identity_status=paper.identity_status,
            version_type=paper.version_type,
            integrity_score=paper.integrity_score,
            confidence_level=paper.confidence_level,
            retrieval_tier=paper.retrieval_tier,
            attempt_count=paper.attempt_count,
            last_failure_code=paper.last_failure_code,
            pdf_filename=paper.pdf_filename,
            pdf_size_bytes=paper.pdf_size_bytes,
            pdf_page_count=paper.pdf_page_count,
            ocr_applied=paper.ocr_applied,
            supplement_count=paper.supplement_count,
            user_override=paper.user_override,
            override_reason=paper.override_reason,
            run_id=paper.run_id,
            content_drift_version=paper.content_drift_version,
        )


class SettingsUpdate(BaseModel):
    """Request to update system configuration."""

    api_timeout_s: Optional[float] = None
    pdf_download_timeout_s: Optional[float] = None
    page_load_timeout_s: Optional[float] = None
    selector_timeout_s: Optional[float] = None
    pdf_validation_timeout_s: Optional[float] = None
    ocr_per_page_timeout_s: Optional[float] = None
    max_retries: Optional[int] = None
    max_total_attempts: Optional[int] = None
    retrieval_concurrency: Optional[int] = None
    validation_concurrency: Optional[int] = None
    backpressure_threshold: Optional[int] = None
    min_disk_space_bytes: Optional[int] = None
    cache_ttl_days: Optional[int] = None
    cooldown_failure_threshold: Optional[float] = None
    cooldown_window_size: Optional[int] = None
    cooldown_minutes: Optional[int] = None
    unpaywall_email: Optional[str] = None
    sso_proxy_url: Optional[str] = None
    openathens_url: Optional[str] = None
    institutional_resolver_url: Optional[str] = None
    output_directory: Optional[str] = None
    scholar_min_delay_s: Optional[float] = None
    scholar_max_queries_per_paper: Optional[int] = None

    @field_validator("retrieval_concurrency")
    @classmethod
    def validate_retrieval_concurrency(cls, v: Optional[int]) -> Optional[int]:
        if v is not None and (v < 1 or v > MAX_RETRIEVAL_CONCURRENCY):
            raise ValueError(
                f"retrieval_concurrency must be between 1 and {MAX_RETRIEVAL_CONCURRENCY}"
            )
        return v

    @field_validator("validation_concurrency")
    @classmethod
    def validate_validation_concurrency(cls, v: Optional[int]) -> Optional[int]:
        if v is not None and (v < 1 or v > MAX_VALIDATION_CONCURRENCY):
            raise ValueError(
                f"validation_concurrency must be between 1 and {MAX_VALIDATION_CONCURRENCY}"
            )
        return v

    @field_validator("cooldown_failure_threshold")
    @classmethod
    def validate_cooldown_threshold(cls, v: Optional[float]) -> Optional[float]:
        if v is not None and (v < 0.0 or v > 1.0):
            raise ValueError("cooldown_failure_threshold must be between 0.0 and 1.0")
        return v


class SettingsResponse(BaseModel):
    """Current system settings plus runtime status."""

    config: Dict[str, Any]
    ocr_available: bool
    tesseract_available: bool
    ghostscript_available: bool
    playwright_installed: bool
    python_version: str
    platform: str


class HealthMetrics(BaseModel):
    """Live system health metrics for SSE dashboard."""

    retrieval_success_rate: float = 0.0
    avg_attempts_per_paper: float = 0.0
    api_failure_rates: Dict[str, float] = Field(default_factory=dict)
    avg_retrieval_time_ms: float = 0.0
    retrieval_queue_depth: int = 0
    validation_queue_depth: int = 0
    backpressure_weighted_depth: int = 0
    backpressure_threshold: int = DEFAULT_BACKPRESSURE_THRESHOLD
    active_cooldowns: List[Dict[str, Any]] = Field(default_factory=list)
    paywall_counts_by_publisher: Dict[str, int] = Field(default_factory=dict)
    cache_hit_rate: float = 0.0
    disk_space_remaining_bytes: int = 0
    already_retrieved_count: int = 0
    retrieval_workers_active: int = 0
    retrieval_workers_paused: bool = False
    validation_workers_active: int = 0
    validation_workers_paused: bool = False
    total_papers: int = 0
    completed_papers: int = 0
    failed_papers: int = 0
    manual_required_papers: int = 0


class PrismaReport(BaseModel):
    """PRISMA 2020 compliance report per run_id."""

    run_id: str
    total_sought: int = 0
    verified: int = 0
    flagged: int = 0
    not_retrieved_no_oa: int = 0
    not_retrieved_paywall: int = 0
    not_retrieved_access_denied: int = 0
    not_retrieved_not_found: int = 0
    not_retrieved_timeout: int = 0
    not_retrieved_other: int = 0
    manual_required: int = 0
    already_retrieved: int = 0
    user_overrides_confirmed: int = 0
    user_overrides_reretried: int = 0


class ExportRequest(BaseModel):
    """Request for data export."""

    export_type: str = Field(
        ...,
        description="'excel' | 'prisma' | 'audit_json' | 'integration_json'",
    )
    run_id: Optional[str] = Field(
        default=None,
        description="Filter by run_id. None = all runs.",
    )
    filter_state: Optional[str] = Field(
        default=None,
        description="Filter by paper state.",
    )

    @field_validator("export_type")
    @classmethod
    def validate_export_type(cls, v: str) -> str:
        allowed = {"excel", "prisma", "audit_json", "integration_json"}
        if v not in allowed:
            raise ValueError(f"export_type must be one of {allowed}")
        return v


class IntegrationExport(BaseModel):
    """Per-paper integration export for downstream tools (ASReview, etc.)."""

    canonical_id: str
    doi: Optional[str] = None
    pmid: Optional[str] = None
    openalex_id: Optional[str] = None
    title: str = ""
    authors: str = ""
    year: Optional[int] = None
    journal: Optional[str] = None
    pdf_path: Optional[str] = None
    sha256_checksum: Optional[str] = None
    pdf_size_bytes: Optional[int] = None
    pdf_page_count: Optional[int] = None
    version_type: Optional[str] = None
    is_primary: bool = True
    confidence_level: Optional[str] = None
    integrity_score: Optional[int] = None
    validation_status: Optional[str] = None
    identity_status: Optional[str] = None
    ocr_applied: bool = False
    content_drift_version: int = 1
    user_override: Optional[str] = None
    override_reason: Optional[str] = None
    run_id_of_first_success: Optional[str] = None
    supplement_paths: List[str] = Field(default_factory=list)
    retrieval_log: List[Dict[str, Any]] = Field(default_factory=list)

    @classmethod
    def from_paper(
        cls,
        paper: Paper,
        supplements: List[SupplementFile],
        audit_entries: List[AuditLogEntry],
    ) -> IntegrationExport:
        """Build integration export from paper + related data."""
        return cls(
            canonical_id=paper.canonical_id,
            doi=paper.doi,
            pmid=paper.pmid,
            openalex_id=paper.openalex_id,
            title=paper.title,
            authors=paper.authors,
            year=paper.year,
            journal=paper.journal,
            pdf_path=paper.pdf_path,
            sha256_checksum=paper.sha256_checksum,
            pdf_size_bytes=paper.pdf_size_bytes,
            pdf_page_count=paper.pdf_page_count,
            version_type=paper.version_type,
            is_primary=paper.is_primary,
            confidence_level=paper.confidence_level,
            integrity_score=paper.integrity_score,
            validation_status=paper.validation_status,
            identity_status=paper.identity_status,
            ocr_applied=paper.ocr_applied,
            content_drift_version=paper.content_drift_version,
            user_override=paper.user_override,
            override_reason=paper.override_reason,
            run_id_of_first_success=paper.run_id_of_first_success,
            supplement_paths=[s.file_path for s in supplements],
            retrieval_log=[e.to_dict() for e in audit_entries],
        )

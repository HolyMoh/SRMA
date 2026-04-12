"""
Full-Text Acquisition System — Retrieval Engine

Multi-tier retrieval engine with publisher resolution, PDF validation,
version detection, and integrity scoring. Deterministic, auditable,
and designed for PRISMA 2020 compliance.
"""

from __future__ import annotations

import asyncio
import hashlib
import io
import json
import logging
import os
import random
import re
import shutil
import struct
import tempfile
import time
import traceback
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import urljoin, urlparse

import httpx

from full_text_acquisition.models import (
    DEFAULT_API_TIMEOUT_S,
    DEFAULT_CACHE_TTL_DAYS,
    DEFAULT_MAX_RETRIES,
    DEFAULT_MAX_TOTAL_ATTEMPTS,
    DEFAULT_OCR_PER_PAGE_TIMEOUT_S,
    DEFAULT_PAGE_LOAD_TIMEOUT_S,
    DEFAULT_PDF_DOWNLOAD_TIMEOUT_S,
    DEFAULT_PDF_VALIDATION_TIMEOUT_S,
    DEFAULT_SELECTOR_TIMEOUT_S,
    DOI_PREFIX_PUBLISHER_MAP,
    IDENTITY_UNVERIFIED_CAP,
    MIN_PDF_SIZE_BYTES,
    PDF_MAGIC_BYTES,
    SAFE_SCHOLAR_DOMAINS,
    SCHOLAR_MAX_QUERIES_PER_PAPER,
    SCHOLAR_MIN_DELAY_S,
    STOPWORDS,
    TITLE_FUZZY_THRESHOLD,
    AuditLogEntry,
    ConfidenceLevel,
    ContentDriftStatus,
    FailureCode,
    IdentityStatus,
    Paper,
    PaperState,
    PublisherEnum,
    SupplementFile,
    TierEnum,
    ValidationStatus,
    VersionType,
    calculate_integrity_score,
    generate_canonical_id,
    generate_filename,
    generate_seeded_delay,
    generate_supplement_filename,
    is_safe_scholar_domain,
    looks_like_html,
    normalize_doi,
    normalize_title,
    pdf_magic_bytes_valid,
    resolve_publisher_from_doi,
    score_to_confidence,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# HTTP constants
# ---------------------------------------------------------------------------

DEFAULT_HTTP_HEADERS: Dict[str, str] = {
    "Accept": "application/pdf,application/octet-stream,*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
}

UNPAYWALL_BASE_URL = "https://api.unpaywall.org/v2/"
OPENALEX_BASE_URL = "https://api.openalex.org/works/doi:"
CROSSREF_BASE_URL = "https://api.crossref.org/works/"
SEMANTIC_SCHOLAR_BASE_URL = "https://api.semanticscholar.org/graph/v1/paper/DOI:"
PMC_BASE_URL = "https://www.ncbi.nlm.nih.gov/pmc/articles/"
EUROPEPMC_API_URL = "https://www.europepmc.org/webservices/rest/search"

# Supplement detection keywords
SUPPLEMENT_KEYWORDS: List[str] = [
    "supplementary",
    "supplemental",
    "supporting information",
    "supporting material",
    "appendix",
    "additional file",
    "online resource",
    "extended data",
    "data supplement",
    "supplementary table",
    "supplementary figure",
]

# Preprint server domains for version detection
PREPRINT_DOMAINS: Set[str] = {
    "arxiv.org",
    "medrxiv.org",
    "biorxiv.org",
    "ssrn.com",
    "preprints.org",
    "osf.io",
}

# Accepted manuscript indicators in PDF text
ACCEPTED_MANUSCRIPT_PATTERNS: List[str] = [
    "accepted manuscript",
    "author manuscript",
    "peer-reviewed accepted",
    "post-print",
    "postprint",
    "author accepted manuscript",
    "aam",
]

# Published version indicators
PUBLISHED_VERSION_PATTERNS: List[str] = [
    "published version",
    "version of record",
    "final published",
    "publisher's version",
    "©",  # Copyright symbol indicates published
]


# ---------------------------------------------------------------------------
# PublisherResolver
# ---------------------------------------------------------------------------


class PublisherResolver:
    """Multi-signal publisher detection.

    Signals (priority order):
        Signal 3: Domain after DOI redirect (authoritative)
        Signal 2: CrossRef publisher field (from enrichment cache)
        Signal 1: DOI prefix (local lookup table)

    On conflict: Signal 3 wins, logged with details.
    """

    # Domain → PublisherEnum mapping for Signal 3
    DOMAIN_PUBLISHER_MAP: Dict[str, str] = {
        "sciencedirect.com": PublisherEnum.ELSEVIER.value,
        "elsevier.com": PublisherEnum.ELSEVIER.value,
        "linkinghub.elsevier.com": PublisherEnum.ELSEVIER.value,
        "springer.com": PublisherEnum.SPRINGER.value,
        "springerlink.com": PublisherEnum.SPRINGER.value,
        "link.springer.com": PublisherEnum.SPRINGER.value,
        "wiley.com": PublisherEnum.WILEY.value,
        "onlinelibrary.wiley.com": PublisherEnum.WILEY.value,
        "nature.com": PublisherEnum.NATURE.value,
        "bmj.com": PublisherEnum.BMJ.value,
        "thelancet.com": PublisherEnum.LANCET.value,
        "tandfonline.com": PublisherEnum.TAYLOR_FRANCIS.value,
        "sagepub.com": PublisherEnum.SAGE.value,
        "journals.sagepub.com": PublisherEnum.SAGE.value,
    }

    # CrossRef publisher name → PublisherEnum mapping for Signal 2
    CROSSREF_PUBLISHER_MAP: Dict[str, str] = {
        "elsevier": PublisherEnum.ELSEVIER.value,
        "springer": PublisherEnum.SPRINGER.value,
        "springer nature": PublisherEnum.SPRINGER.value,
        "wiley": PublisherEnum.WILEY.value,
        "john wiley": PublisherEnum.WILEY.value,
        "nature publishing": PublisherEnum.NATURE.value,
        "nature portfolio": PublisherEnum.NATURE.value,
        "bmj": PublisherEnum.BMJ.value,
        "british medical journal": PublisherEnum.BMJ.value,
        "lancet": PublisherEnum.LANCET.value,
        "taylor & francis": PublisherEnum.TAYLOR_FRANCIS.value,
        "taylor and francis": PublisherEnum.TAYLOR_FRANCIS.value,
        "informa uk": PublisherEnum.TAYLOR_FRANCIS.value,
        "sage": PublisherEnum.SAGE.value,
        "sage publications": PublisherEnum.SAGE.value,
    }

    @classmethod
    def resolve_from_domain(cls, url: str) -> Optional[str]:
        """Signal 3: Resolve publisher from the domain of a URL.

        This is the most authoritative signal — used after DOI redirect.
        """
        try:
            parsed = urlparse(url)
            domain = parsed.hostname
            if not domain:
                return None
            domain = domain.lower()

            # Check exact match first
            if domain in cls.DOMAIN_PUBLISHER_MAP:
                return cls.DOMAIN_PUBLISHER_MAP[domain]

            # Check subdomain match
            for known_domain, publisher in cls.DOMAIN_PUBLISHER_MAP.items():
                if domain.endswith(f".{known_domain}"):
                    return publisher

            return None
        except Exception:
            return None

    @classmethod
    def resolve_from_crossref(cls, publisher_name: Optional[str]) -> Optional[str]:
        """Signal 2: Resolve publisher from CrossRef publisher field."""
        if not publisher_name:
            return None

        name_lower = publisher_name.lower().strip()

        # Exact match
        if name_lower in cls.CROSSREF_PUBLISHER_MAP:
            return cls.CROSSREF_PUBLISHER_MAP[name_lower]

        # Substring match
        for key, publisher in cls.CROSSREF_PUBLISHER_MAP.items():
            if key in name_lower:
                return publisher

        return None

    @classmethod
    def resolve_from_doi_prefix(cls, doi: Optional[str]) -> str:
        """Signal 1: Resolve publisher from DOI prefix (least authoritative)."""
        return resolve_publisher_from_doi(doi)

    @classmethod
    def resolve(
        cls,
        doi: Optional[str] = None,
        crossref_publisher: Optional[str] = None,
        redirect_url: Optional[str] = None,
    ) -> Tuple[str, str]:
        """Resolve publisher using all available signals.

        Priority: Signal 3 > Signal 2 > Signal 1.

        Returns:
            Tuple of (publisher_enum_value, signal_source) where
            signal_source is "redirect_domain" | "crossref" | "doi_prefix".
        """
        signal_3 = cls.resolve_from_domain(redirect_url) if redirect_url else None
        signal_2 = cls.resolve_from_crossref(crossref_publisher)
        signal_1 = cls.resolve_from_doi_prefix(doi)

        # Conflict detection and logging
        signals = {
            "doi_prefix": signal_1,
            "crossref": signal_2,
            "redirect_domain": signal_3,
        }
        non_none = {k: v for k, v in signals.items() if v is not None and v != PublisherEnum.OTHER.value}
        unique_values = set(non_none.values())

        if len(unique_values) > 1:
            logger.info(
                "Publisher signal conflict for DOI %s: %s — using Signal 3 (domain)",
                doi,
                {k: v for k, v in non_none.items()},
            )

        # Priority resolution
        if signal_3 is not None:
            return signal_3, "redirect_domain"
        if signal_2 is not None:
            return signal_2, "crossref"
        return signal_1, "doi_prefix"


# ===========================================================================
# RetrievalEngine
# ===========================================================================


class RetrievalEngine:
    """Multi-tier retrieval engine with dependency-injected database.

    Handles Tier 0–4 retrieval, PDF validation, version detection,
    integrity scoring, and file storage. All operations logged to
    the audit trail via the database write queue.
    """

    def __init__(
        self,
        db: Any,  # database.Database
        browser_manager: Any,  # browser_manager.BrowserManager
        config: Dict[str, Any],
        output_directory: str = "./downloads",
        supplement_directory: str = "./downloads/Supplements",
    ) -> None:
        self._db = db
        self._browser = browser_manager
        self._config = config
        self._output_dir = output_directory
        self._supplement_dir = supplement_directory

        # Shared HTTP client — reused across all pure-HTTP tiers
        self._http_client: Optional[httpx.AsyncClient] = None

        # Ensure output directories exist
        os.makedirs(self._output_dir, exist_ok=True)
        os.makedirs(self._supplement_dir, exist_ok=True)

        # OCR availability (set during wizard checks)
        self.tesseract_available: bool = False
        self.ghostscript_available: bool = False

    async def get_http_client(self) -> httpx.AsyncClient:
        """Get or create the shared async HTTP client."""
        if self._http_client is None or self._http_client.is_closed:
            self._http_client = httpx.AsyncClient(
                headers=DEFAULT_HTTP_HEADERS,
                follow_redirects=True,
                timeout=httpx.Timeout(
                    connect=10.0,
                    read=self._config.get(
                        "pdf_download_timeout_s", DEFAULT_PDF_DOWNLOAD_TIMEOUT_S
                    ),
                    write=10.0,
                    pool=10.0,
                ),
                limits=httpx.Limits(
                    max_connections=10,
                    max_keepalive_connections=5,
                ),
            )
        return self._http_client

    async def close(self) -> None:
        """Close the HTTP client. Called during shutdown."""
        if self._http_client and not self._http_client.is_closed:
            await self._http_client.aclose()
            self._http_client = None

    # -----------------------------------------------------------------------
    # Audit logging helper
    # -----------------------------------------------------------------------

    async def _log(
        self,
        canonical_id: Optional[str] = None,
        run_id: Optional[str] = None,
        tier: Optional[str] = None,
        method: Optional[str] = None,
        url_attempted: Optional[str] = None,
        http_status: Optional[int] = None,
        content_type: Optional[str] = None,
        outcome: Optional[str] = None,
        failure_code: Optional[str] = None,
        execution_time_ms: Optional[float] = None,
        retry_count: int = 0,
        cache_hit: bool = False,
        details: Optional[Dict[str, Any]] = None,
        exc: Optional[Exception] = None,
    ) -> None:
        """Write an audit log entry through the database write queue."""
        entry = AuditLogEntry(
            canonical_id=canonical_id,
            run_id=run_id,
            tier=tier,
            method=method,
            url_attempted=url_attempted,
            http_status=http_status,
            content_type_received=content_type,
            outcome=outcome,
            failure_code=failure_code,
            execution_time_ms=execution_time_ms,
            retry_count=retry_count,
            cache_hit=cache_hit,
            details=json.dumps(details) if details else None,
        )
        if exc is not None:
            entry.exception_type = type(exc).__name__
            entry.exception_message = str(exc)
            entry.exception_traceback = traceback.format_exc()

        try:
            await self._db.log_audit(entry)
        except Exception as log_exc:
            logger.error("Failed to write audit log: %s", log_exc)

    # -----------------------------------------------------------------------
    # Stream integrity check
    # -----------------------------------------------------------------------

    async def _stream_download(
        self,
        url: str,
        timeout_s: Optional[float] = None,
    ) -> Tuple[Optional[bytes], Optional[int], Optional[str], Optional[str]]:
        """Download a file via HTTP streaming with Content-Length integrity check.

        Returns:
            Tuple of (data_bytes, http_status, content_type, error_string).
            data_bytes is None on failure.
        """
        if timeout_s is None:
            timeout_s = self._config.get(
                "pdf_download_timeout_s", DEFAULT_PDF_DOWNLOAD_TIMEOUT_S
            )

        client = await self.get_http_client()
        chunks: List[bytes] = []
        bytes_received = 0
        http_status: Optional[int] = None
        content_type: Optional[str] = None

        try:
            async with client.stream("GET", url, timeout=timeout_s) as response:
                http_status = response.status_code
                content_type = response.headers.get("content-type", "")
                content_length_str = response.headers.get("content-length")
                expected_length: Optional[int] = None

                if content_length_str:
                    try:
                        expected_length = int(content_length_str)
                    except ValueError:
                        expected_length = None

                async for chunk in response.aiter_bytes(chunk_size=65536):
                    chunks.append(chunk)
                    bytes_received += len(chunk)

            # Integrity check: compare to Content-Length if present
            if expected_length is not None and bytes_received != expected_length:
                return (
                    None,
                    http_status,
                    content_type,
                    f"INCOMPLETE_DOWNLOAD: received {bytes_received} "
                    f"of {expected_length} bytes",
                )

            data = b"".join(chunks)
            return data, http_status, content_type, None

        except httpx.TimeoutException:
            return None, http_status, content_type, "TIMEOUT"
        except httpx.HTTPStatusError as exc:
            return None, exc.response.status_code, content_type, f"HTTP_{exc.response.status_code}"
        except Exception as exc:
            return (
                None,
                http_status,
                content_type,
                f"{type(exc).__name__}: {exc}",
            )

    # -----------------------------------------------------------------------
    # Response validation gate
    # -----------------------------------------------------------------------

    def _validate_response_content_type(
        self,
        content_type: Optional[str],
        http_status: Optional[int],
    ) -> Tuple[bool, Optional[str]]:
        """Apply the HTTP status code logic gate and content-type validation.

        Returns:
            Tuple of (is_valid, failure_code).
            is_valid=True means the response should be treated as a PDF.
        """
        if http_status is None:
            return False, FailureCode.TIMEOUT.value

        # 4xx / 5xx status codes
        if http_status == 403:
            return False, FailureCode.ACCESS_DENIED.value
        if http_status == 404:
            return False, FailureCode.NOT_FOUND.value
        if http_status == 429:
            return False, FailureCode.RATE_LIMITED.value
        if http_status >= 500:
            return False, FailureCode.PUBLISHER_SOFT_BLOCK.value
        if http_status >= 400:
            return False, FailureCode.ACCESS_DENIED.value

        # 200 OK — check content type
        if http_status == 200 or (200 <= http_status < 300):
            ct = (content_type or "").lower()
            if "application/pdf" in ct or "application/octet-stream" in ct:
                return True, None
            if "text/html" in ct:
                return False, FailureCode.PUBLISHER_SOFT_BLOCK.value
            # Unknown content type — allow but log
            if ct:
                logger.debug(
                    "Unexpected content-type %s with status %d",
                    ct, http_status,
                )
                return True, None
            return True, None

        # 3xx should be handled by follow_redirects
        return False, FailureCode.PUBLISHER_SOFT_BLOCK.value

    # -----------------------------------------------------------------------
    # Atomic temp file write
    # -----------------------------------------------------------------------

    async def _atomic_write_pdf(
        self,
        data: bytes,
        final_path: str,
    ) -> Tuple[bool, Optional[str]]:
        """Write PDF data atomically: write to .tmp, verify, os.replace().

        Returns:
            Tuple of (success, error_string).
        """
        tmp_path = f"{final_path}.tmp"

        try:
            # Ensure directory exists
            os.makedirs(os.path.dirname(final_path), exist_ok=True)

            # Write to temp file
            with open(tmp_path, "wb") as f:
                f.write(data)

            # Verify temp file integrity
            tmp_size = os.path.getsize(tmp_path)
            if tmp_size != len(data):
                os.unlink(tmp_path)
                return False, (
                    f"Temp file size mismatch: wrote {len(data)} "
                    f"but file is {tmp_size}"
                )

            # Verify starts with PDF magic bytes
            with open(tmp_path, "rb") as f:
                header = f.read(4)
            if not pdf_magic_bytes_valid(header):
                os.unlink(tmp_path)
                return False, "Temp file does not start with PDF magic bytes"

            # Atomic replace
            os.replace(tmp_path, final_path)
            return True, None

        except Exception as exc:
            # Clean up temp file on failure
            try:
                if os.path.exists(tmp_path):
                    os.unlink(tmp_path)
            except OSError:
                pass
            return False, f"{type(exc).__name__}: {exc}"

    # -----------------------------------------------------------------------
    # Retry-After header parsing
    # -----------------------------------------------------------------------

    @staticmethod
    def _parse_retry_after(headers: Dict[str, str]) -> Optional[float]:
        """Parse a Retry-After header value into seconds.

        Handles both delta-seconds and HTTP-date formats.
        Returns None if header is absent or unparseable.
        """
        value = headers.get("retry-after") or headers.get("Retry-After")
        if not value:
            return None

        # Try integer seconds
        try:
            return float(value)
        except ValueError:
            pass

        # Try HTTP-date format
        try:
            from email.utils import parsedate_to_datetime
            target_dt = parsedate_to_datetime(value)
            delta = (target_dt - datetime.now(timezone.utc)).total_seconds()
            return max(0.0, delta)
        except Exception:
            return None

    # -----------------------------------------------------------------------
    # Exponential backoff helper
    # -----------------------------------------------------------------------

    @staticmethod
    def _backoff_delay(attempt: int, base: float = 1.0, max_delay: float = 30.0) -> float:
        """Calculate exponential backoff delay with jitter.

        delay = min(base * 2^attempt + jitter, max_delay)
        """
        delay = min(base * (2 ** attempt), max_delay)
        jitter = random.uniform(0, delay * 0.1)
        return delay + jitter

    # -----------------------------------------------------------------------
    # HTTP API request helper (with cache + retry)
    # -----------------------------------------------------------------------

    async def _api_request(
        self,
        url: str,
        doi: Optional[str],
        api_name: str,
        canonical_id: Optional[str] = None,
        run_id: Optional[str] = None,
        tier: Optional[str] = None,
        timeout_s: Optional[float] = None,
    ) -> Tuple[Optional[Dict[str, Any]], bool]:
        """Make an API request with cache check and retry logic.

        Returns:
            Tuple of (response_dict_or_none, cache_hit).
        """
        if timeout_s is None:
            timeout_s = self._config.get("api_timeout_s", DEFAULT_API_TIMEOUT_S)

        # Check cache first
        if doi:
            cached = await self._db.get_cached_response(doi, api_name)
            if cached is not None:
                await self._log(
                    canonical_id=canonical_id,
                    run_id=run_id,
                    tier=tier,
                    method=f"{api_name}_cache",
                    url_attempted=url,
                    outcome="CACHE_HIT",
                    cache_hit=True,
                )
                return cached, True

        # Make request with retry
        client = await self.get_http_client()
        max_retries = self._config.get("max_retries", DEFAULT_MAX_RETRIES)
        last_exc: Optional[Exception] = None

        for attempt in range(max_retries + 1):
            start_ms = time.monotonic() * 1000
            try:
                response = await client.get(url, timeout=timeout_s)
                elapsed_ms = (time.monotonic() * 1000) - start_ms

                if response.status_code == 200:
                    data = response.json()
                    # Cache the response
                    if doi:
                        ttl = self._config.get("cache_ttl_days", DEFAULT_CACHE_TTL_DAYS)
                        await self._db.set_cached_response(
                            doi, api_name, data, ttl_days=ttl
                        )
                    await self._log(
                        canonical_id=canonical_id,
                        run_id=run_id,
                        tier=tier,
                        method=api_name,
                        url_attempted=url,
                        http_status=200,
                        content_type=response.headers.get("content-type"),
                        outcome="API_SUCCESS",
                        execution_time_ms=elapsed_ms,
                        retry_count=attempt,
                    )
                    return data, False

                if response.status_code == 429:
                    retry_after = self._parse_retry_after(
                        dict(response.headers)
                    )
                    wait = retry_after or self._backoff_delay(attempt)
                    await self._log(
                        canonical_id=canonical_id,
                        run_id=run_id,
                        tier=tier,
                        method=api_name,
                        url_attempted=url,
                        http_status=429,
                        outcome="RATE_LIMITED",
                        failure_code=FailureCode.RATE_LIMITED.value,
                        execution_time_ms=elapsed_ms,
                        retry_count=attempt,
                    )
                    await asyncio.sleep(wait)
                    continue

                if response.status_code >= 500:
                    await self._log(
                        canonical_id=canonical_id,
                        run_id=run_id,
                        tier=tier,
                        method=api_name,
                        url_attempted=url,
                        http_status=response.status_code,
                        outcome="SERVER_ERROR",
                        execution_time_ms=elapsed_ms,
                        retry_count=attempt,
                    )
                    await asyncio.sleep(self._backoff_delay(attempt))
                    continue

                # 4xx (not 429) — don't retry
                await self._log(
                    canonical_id=canonical_id,
                    run_id=run_id,
                    tier=tier,
                    method=api_name,
                    url_attempted=url,
                    http_status=response.status_code,
                    outcome="API_FAILED",
                    failure_code=FailureCode.NOT_FOUND.value,
                    execution_time_ms=elapsed_ms,
                    retry_count=attempt,
                )
                return None, False

            except httpx.TimeoutException as exc:
                last_exc = exc
                elapsed_ms = (time.monotonic() * 1000) - start_ms
                await self._log(
                    canonical_id=canonical_id,
                    run_id=run_id,
                    tier=tier,
                    method=api_name,
                    url_attempted=url,
                    outcome="TIMEOUT",
                    failure_code=FailureCode.TIMEOUT.value,
                    execution_time_ms=elapsed_ms,
                    retry_count=attempt,
                    exc=exc,
                )
                if attempt < max_retries:
                    await asyncio.sleep(self._backoff_delay(attempt))
                continue

            except Exception as exc:
                last_exc = exc
                elapsed_ms = (time.monotonic() * 1000) - start_ms
                await self._log(
                    canonical_id=canonical_id,
                    run_id=run_id,
                    tier=tier,
                    method=api_name,
                    url_attempted=url,
                    outcome="API_ERROR",
                    execution_time_ms=elapsed_ms,
                    retry_count=attempt,
                    exc=exc,
                )
                if attempt < max_retries:
                    await asyncio.sleep(self._backoff_delay(attempt))
                continue

        return None, False

    # -----------------------------------------------------------------------
    # PDF download helper (stream + validate + atomic write)
    # -----------------------------------------------------------------------

    async def _download_pdf(
        self,
        url: str,
        final_path: str,
        canonical_id: str,
        run_id: str,
        tier: str,
        method: str,
        retry_count: int = 0,
    ) -> Tuple[bool, Optional[str], Optional[bytes]]:
        """Download a PDF, validate the response, and write atomically.

        Returns:
            Tuple of (success, failure_code, pdf_bytes).
        """
        start_ms = time.monotonic() * 1000

        data, http_status, content_type, error = await self._stream_download(
            url
        )
        elapsed_ms = (time.monotonic() * 1000) - start_ms

        # Stream error
        if error:
            failure_code = FailureCode.TIMEOUT.value
            if "INCOMPLETE_DOWNLOAD" in error:
                failure_code = FailureCode.INCOMPLETE_DOWNLOAD.value
            elif "HTTP_" in error:
                code = error.replace("HTTP_", "")
                if code == "403":
                    failure_code = FailureCode.ACCESS_DENIED.value
                elif code == "404":
                    failure_code = FailureCode.NOT_FOUND.value
                elif code == "429":
                    failure_code = FailureCode.RATE_LIMITED.value
                else:
                    failure_code = FailureCode.PUBLISHER_SOFT_BLOCK.value

            await self._log(
                canonical_id=canonical_id,
                run_id=run_id,
                tier=tier,
                method=method,
                url_attempted=url,
                http_status=http_status,
                content_type=content_type,
                outcome="DOWNLOAD_FAILED",
                failure_code=failure_code,
                execution_time_ms=elapsed_ms,
                retry_count=retry_count,
            )
            return False, failure_code, None

        # Response validation gate
        is_valid, failure_code = self._validate_response_content_type(
            content_type, http_status
        )
        if not is_valid:
            await self._log(
                canonical_id=canonical_id,
                run_id=run_id,
                tier=tier,
                method=method,
                url_attempted=url,
                http_status=http_status,
                content_type=content_type,
                outcome="RESPONSE_REJECTED",
                failure_code=failure_code,
                execution_time_ms=elapsed_ms,
                retry_count=retry_count,
            )
            return False, failure_code, None

        # Check for HTML disguise before writing
        if data and looks_like_html(data):
            await self._log(
                canonical_id=canonical_id,
                run_id=run_id,
                tier=tier,
                method=method,
                url_attempted=url,
                http_status=http_status,
                content_type=content_type,
                outcome="HTML_DISGUISED",
                failure_code=FailureCode.HTML_DISGUISED_AS_PDF.value,
                execution_time_ms=elapsed_ms,
                retry_count=retry_count,
            )
            return False, FailureCode.HTML_DISGUISED_AS_PDF.value, None

        # Atomic write
        write_ok, write_err = await self._atomic_write_pdf(data, final_path)
        if not write_ok:
            await self._log(
                canonical_id=canonical_id,
                run_id=run_id,
                tier=tier,
                method=method,
                url_attempted=url,
                http_status=http_status,
                content_type=content_type,
                outcome="WRITE_FAILED",
                failure_code=FailureCode.CORRUPTED.value,
                execution_time_ms=elapsed_ms,
                retry_count=retry_count,
                details={"write_error": write_err},
            )
            return False, FailureCode.CORRUPTED.value, None

        await self._log(
            canonical_id=canonical_id,
            run_id=run_id,
            tier=tier,
            method=method,
            url_attempted=url,
            http_status=http_status,
            content_type=content_type,
            outcome="RETRIEVED",
            execution_time_ms=elapsed_ms,
            retry_count=retry_count,
            details={"size_bytes": len(data)},
        )
        return True, None, data

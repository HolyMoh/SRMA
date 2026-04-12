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

    # ===================================================================
    # TIER 0 — Unpaywall (pure HTTP)
    # ===================================================================

    async def tier0_unpaywall(
        self,
        paper: Paper,
        run_id: str,
    ) -> Tuple[bool, Optional[str]]:
        """Tier 0: Query Unpaywall API for an open-access PDF URL.

        Returns:
            Tuple of (success, failure_code).
        """
        if not paper.doi:
            await self._log(
                canonical_id=paper.canonical_id,
                run_id=run_id,
                tier=TierEnum.TIER_0.value,
                method="unpaywall",
                outcome="SKIPPED",
                failure_code=FailureCode.NO_DOI.value,
            )
            return False, FailureCode.NO_DOI.value

        email = self._config.get("unpaywall_email", "")
        if not email:
            email = "srma-acquisition@example.com"

        url = f"{UNPAYWALL_BASE_URL}{paper.doi}?email={email}"

        data, cache_hit = await self._api_request(
            url=url,
            doi=paper.doi,
            api_name="unpaywall",
            canonical_id=paper.canonical_id,
            run_id=run_id,
            tier=TierEnum.TIER_0.value,
        )

        if data is None:
            await self._log(
                canonical_id=paper.canonical_id,
                run_id=run_id,
                tier=TierEnum.TIER_0.value,
                method="unpaywall",
                outcome="NO_RESULT",
                failure_code=FailureCode.NO_OA_SOURCE.value,
                cache_hit=cache_hit,
            )
            return False, FailureCode.NO_OA_SOURCE.value

        # Extract best OA location PDF URL
        pdf_url = self._extract_unpaywall_pdf_url(data)
        if not pdf_url:
            await self._log(
                canonical_id=paper.canonical_id,
                run_id=run_id,
                tier=TierEnum.TIER_0.value,
                method="unpaywall",
                outcome="NO_PDF_URL",
                failure_code=FailureCode.NO_OA_SOURCE.value,
                cache_hit=cache_hit,
            )
            return False, FailureCode.NO_OA_SOURCE.value

        # Generate file path
        filename = generate_filename(
            paper.first_author_lastname,
            paper.year,
            paper.title,
        )
        final_path = os.path.join(self._output_dir, filename)

        # Download
        success, failure_code, pdf_data = await self._download_pdf(
            url=pdf_url,
            final_path=final_path,
            canonical_id=paper.canonical_id,
            run_id=run_id,
            tier=TierEnum.TIER_0.value,
            method="unpaywall",
        )

        if success:
            await self._db.update_paper_fields(
                paper.canonical_id,
                pdf_path=final_path,
                pdf_filename=filename,
                retrieval_tier=TierEnum.TIER_0.value,
                retrieval_method="unpaywall",
                retrieval_url=pdf_url,
                pdf_size_bytes=len(pdf_data) if pdf_data else None,
            )

        return success, failure_code

    @staticmethod
    def _extract_unpaywall_pdf_url(data: Dict[str, Any]) -> Optional[str]:
        """Extract the best PDF URL from an Unpaywall API response.

        Prefers the best_oa_location, falls back to other oa_locations.
        Only returns direct PDF URLs (url_for_pdf field).
        """
        # Best OA location
        best = data.get("best_oa_location")
        if best:
            pdf_url = best.get("url_for_pdf")
            if pdf_url:
                return pdf_url

        # Fallback: iterate all OA locations
        locations = data.get("oa_locations", [])
        for loc in locations:
            pdf_url = loc.get("url_for_pdf")
            if pdf_url:
                return pdf_url

        # Last resort: use landing page URL (may not be direct PDF)
        if best:
            return best.get("url")

        return None

    # ===================================================================
    # TIER 1 — Open Access APIs (pure HTTP)
    # ===================================================================

    async def tier1_open_access(
        self,
        paper: Paper,
        run_id: str,
    ) -> Tuple[bool, Optional[str]]:
        """Tier 1: Try multiple OA APIs in sequence.

        Order: PMC → Europe PMC → Semantic Scholar → OpenAlex.

        Returns:
            Tuple of (success, failure_code).
        """
        methods: List[
            Tuple[str, Any]
        ] = [
            ("pmc", self._tier1_pmc),
            ("europepmc", self._tier1_europepmc),
            ("semantic_scholar", self._tier1_semantic_scholar),
            ("openalex", self._tier1_openalex),
        ]

        last_failure: Optional[str] = None
        for method_name, method_fn in methods:
            success, failure_code = await method_fn(paper, run_id)
            if success:
                return True, None
            last_failure = failure_code

        return False, last_failure or FailureCode.NO_OA_SOURCE.value

    async def _tier1_pmc(
        self,
        paper: Paper,
        run_id: str,
    ) -> Tuple[bool, Optional[str]]:
        """Tier 1a: PubMed Central full-text PDF."""
        pmid = paper.pmid
        doi = paper.doi

        # Try to find PMC ID via PMID or DOI
        pmc_id = None

        if pmid:
            # Query NCBI E-utilities to convert PMID → PMCID
            eutils_url = (
                f"https://www.ncbi.nlm.nih.gov/pmc/utils/idconv/v1.0/"
                f"?ids={pmid}&format=json"
            )
            data, cache_hit = await self._api_request(
                url=eutils_url,
                doi=doi,
                api_name="pmc_idconv",
                canonical_id=paper.canonical_id,
                run_id=run_id,
                tier=TierEnum.TIER_1.value,
            )
            if data:
                records = data.get("records", [])
                if records:
                    pmc_id = records[0].get("pmcid")

        if not pmc_id and doi:
            # Try DOI-based lookup
            eutils_url = (
                f"https://www.ncbi.nlm.nih.gov/pmc/utils/idconv/v1.0/"
                f"?ids={doi}&format=json"
            )
            data, cache_hit = await self._api_request(
                url=eutils_url,
                doi=doi,
                api_name="pmc_idconv_doi",
                canonical_id=paper.canonical_id,
                run_id=run_id,
                tier=TierEnum.TIER_1.value,
            )
            if data:
                records = data.get("records", [])
                if records:
                    pmc_id = records[0].get("pmcid")

        if not pmc_id:
            return False, FailureCode.NO_OA_SOURCE.value

        # Construct PDF URL
        pdf_url = f"{PMC_BASE_URL}{pmc_id}/pdf/"

        filename = generate_filename(
            paper.first_author_lastname, paper.year, paper.title,
        )
        final_path = os.path.join(self._output_dir, filename)

        success, failure_code, pdf_data = await self._download_pdf(
            url=pdf_url,
            final_path=final_path,
            canonical_id=paper.canonical_id,
            run_id=run_id,
            tier=TierEnum.TIER_1.value,
            method="pmc",
        )

        if success:
            await self._db.update_paper_fields(
                paper.canonical_id,
                pdf_path=final_path,
                pdf_filename=filename,
                retrieval_tier=TierEnum.TIER_1.value,
                retrieval_method="pmc",
                retrieval_url=pdf_url,
                pdf_size_bytes=len(pdf_data) if pdf_data else None,
            )

        return success, failure_code

    async def _tier1_europepmc(
        self,
        paper: Paper,
        run_id: str,
    ) -> Tuple[bool, Optional[str]]:
        """Tier 1b: Europe PMC full-text PDF."""
        if not paper.doi and not paper.pmid:
            return False, FailureCode.NO_OA_SOURCE.value

        # Search Europe PMC
        query = paper.doi if paper.doi else paper.pmid
        search_url = (
            f"{EUROPEPMC_API_URL}?query={query}"
            f"&resultType=core&format=json&pageSize=1"
        )

        data, cache_hit = await self._api_request(
            url=search_url,
            doi=paper.doi,
            api_name="europepmc",
            canonical_id=paper.canonical_id,
            run_id=run_id,
            tier=TierEnum.TIER_1.value,
        )

        if not data:
            return False, FailureCode.NO_OA_SOURCE.value

        results = data.get("resultList", {}).get("result", [])
        if not results:
            return False, FailureCode.NO_OA_SOURCE.value

        result = results[0]
        pmcid = result.get("pmcid")

        if not pmcid:
            # Try fullTextUrlList for direct PDF
            url_list = result.get("fullTextUrlList", {}).get("fullTextUrl", [])
            for url_entry in url_list:
                if (
                    url_entry.get("documentStyle") == "pdf"
                    and url_entry.get("availabilityCode") == "OA"
                ):
                    pdf_url = url_entry.get("url")
                    if pdf_url:
                        return await self._try_download_tier1(
                            paper, run_id, pdf_url, "europepmc"
                        )
            return False, FailureCode.NO_OA_SOURCE.value

        # Use PMC PDF URL
        pdf_url = f"https://europepmc.org/backend/ptpmcrender.fcgi?accid={pmcid}&blobtype=pdf"
        return await self._try_download_tier1(
            paper, run_id, pdf_url, "europepmc"
        )

    async def _tier1_semantic_scholar(
        self,
        paper: Paper,
        run_id: str,
    ) -> Tuple[bool, Optional[str]]:
        """Tier 1c: Semantic Scholar open-access PDF links."""
        if not paper.doi:
            return False, FailureCode.NO_OA_SOURCE.value

        url = (
            f"{SEMANTIC_SCHOLAR_BASE_URL}{paper.doi}"
            f"?fields=openAccessPdf,externalIds"
        )

        data, cache_hit = await self._api_request(
            url=url,
            doi=paper.doi,
            api_name="semantic_scholar",
            canonical_id=paper.canonical_id,
            run_id=run_id,
            tier=TierEnum.TIER_1.value,
        )

        if not data:
            return False, FailureCode.NO_OA_SOURCE.value

        oa_pdf = data.get("openAccessPdf")
        if not oa_pdf:
            return False, FailureCode.NO_OA_SOURCE.value

        pdf_url = oa_pdf.get("url")
        if not pdf_url:
            return False, FailureCode.NO_OA_SOURCE.value

        return await self._try_download_tier1(
            paper, run_id, pdf_url, "semantic_scholar"
        )

    async def _tier1_openalex(
        self,
        paper: Paper,
        run_id: str,
    ) -> Tuple[bool, Optional[str]]:
        """Tier 1d: OpenAlex PDF URLs."""
        if not paper.doi:
            return False, FailureCode.NO_OA_SOURCE.value

        url = f"{OPENALEX_BASE_URL}{paper.doi}"

        data, cache_hit = await self._api_request(
            url=url,
            doi=paper.doi,
            api_name="openalex",
            canonical_id=paper.canonical_id,
            run_id=run_id,
            tier=TierEnum.TIER_1.value,
        )

        if not data:
            return False, FailureCode.NO_OA_SOURCE.value

        # Check primary_location and best_oa_location
        for location_key in ("best_oa_location", "primary_location"):
            location = data.get(location_key)
            if location:
                pdf_url = location.get("pdf_url")
                if pdf_url:
                    success, fc = await self._try_download_tier1(
                        paper, run_id, pdf_url, "openalex"
                    )
                    if success:
                        return True, None

        # Check all locations
        locations = data.get("locations", [])
        for loc in locations:
            pdf_url = loc.get("pdf_url")
            if pdf_url:
                success, fc = await self._try_download_tier1(
                    paper, run_id, pdf_url, "openalex"
                )
                if success:
                    return True, None

        return False, FailureCode.NO_OA_SOURCE.value

    async def _try_download_tier1(
        self,
        paper: Paper,
        run_id: str,
        pdf_url: str,
        method_name: str,
    ) -> Tuple[bool, Optional[str]]:
        """Shared download helper for Tier 1 methods."""
        filename = generate_filename(
            paper.first_author_lastname, paper.year, paper.title,
        )
        final_path = os.path.join(self._output_dir, filename)

        success, failure_code, pdf_data = await self._download_pdf(
            url=pdf_url,
            final_path=final_path,
            canonical_id=paper.canonical_id,
            run_id=run_id,
            tier=TierEnum.TIER_1.value,
            method=method_name,
        )

        if success:
            await self._db.update_paper_fields(
                paper.canonical_id,
                pdf_path=final_path,
                pdf_filename=filename,
                retrieval_tier=TierEnum.TIER_1.value,
                retrieval_method=method_name,
                retrieval_url=pdf_url,
                pdf_size_bytes=len(pdf_data) if pdf_data else None,
            )

        return success, failure_code

    # ===================================================================
    # TIER 2 — Publisher-Aware Direct Retrieval (Playwright headless)
    # ===================================================================

    async def tier2_publisher_direct(
        self,
        paper: Paper,
        run_id: str,
    ) -> Tuple[bool, Optional[str]]:
        """Tier 2: Publisher-aware direct retrieval with Playwright headless.

        Checks publisher cooldown first. Routes to publisher-specific method.
        Uses a disposable browser context per paper.

        Returns:
            Tuple of (success, failure_code).
        """
        publisher = paper.publisher or PublisherEnum.OTHER.value

        # Check adaptive cooldown
        cooldown = await self._db.get_publisher_cooldown(publisher)
        if cooldown and cooldown.is_in_cooldown:
            await self._log(
                canonical_id=paper.canonical_id,
                run_id=run_id,
                tier=TierEnum.TIER_2.value,
                method=f"publisher_{publisher.lower()}",
                outcome="COOLDOWN_ACTIVE",
                failure_code=FailureCode.PUBLISHER_COOLDOWN_ACTIVE.value,
                details={
                    "publisher": publisher,
                    "failure_rate": cooldown.failure_rate,
                    "cooldown_until": cooldown.cooldown_until,
                },
            )
            return False, FailureCode.PUBLISHER_COOLDOWN_ACTIVE.value

        if not paper.doi:
            await self._log(
                canonical_id=paper.canonical_id,
                run_id=run_id,
                tier=TierEnum.TIER_2.value,
                method="publisher_direct",
                outcome="SKIPPED",
                failure_code=FailureCode.NO_DOI.value,
            )
            return False, FailureCode.NO_DOI.value

        doi_url = f"https://doi.org/{paper.doi}"
        start_ms = time.monotonic() * 1000
        success = False
        failure_code: Optional[str] = None

        try:
            async with self._browser.disposable_context() as ctx:
                page = await ctx.new_page()

                # Navigate to DOI (follows redirect to publisher)
                nav_result = await self._browser.safe_navigate(
                    page, doi_url,
                    timeout_s=self._config.get(
                        "page_load_timeout_s", DEFAULT_PAGE_LOAD_TIMEOUT_S
                    ),
                )

                if not nav_result["success"]:
                    failure_code = self._map_nav_error(nav_result)
                    elapsed_ms = (time.monotonic() * 1000) - start_ms
                    await self._log(
                        canonical_id=paper.canonical_id,
                        run_id=run_id,
                        tier=TierEnum.TIER_2.value,
                        method="publisher_navigate",
                        url_attempted=doi_url,
                        http_status=nav_result.get("status"),
                        outcome="NAVIGATION_FAILED",
                        failure_code=failure_code,
                        execution_time_ms=elapsed_ms,
                    )
                    await self._record_tier2_attempt(publisher, False)
                    return False, failure_code

                # Update publisher from redirect domain (Signal 3)
                final_url = nav_result["url"]
                resolved_pub, signal = PublisherResolver.resolve(
                    doi=paper.doi,
                    redirect_url=final_url,
                )
                if resolved_pub != publisher:
                    await self._db.update_paper_fields(
                        paper.canonical_id,
                        publisher=resolved_pub,
                        publisher_signal_source=signal,
                    )
                    publisher = resolved_pub

                # Paywall detection
                if await self._browser.detect_paywall(page):
                    elapsed_ms = (time.monotonic() * 1000) - start_ms
                    await self._log(
                        canonical_id=paper.canonical_id,
                        run_id=run_id,
                        tier=TierEnum.TIER_2.value,
                        method=f"publisher_{publisher.lower()}",
                        url_attempted=final_url,
                        outcome="PAYWALL_DETECTED",
                        failure_code=FailureCode.PAYWALL_DETECTED.value,
                        execution_time_ms=elapsed_ms,
                        details={"publisher": publisher},
                    )
                    await self._record_tier2_attempt(publisher, False)
                    return False, FailureCode.PAYWALL_DETECTED.value

                # Route to publisher-specific method
                publisher_methods = {
                    PublisherEnum.ELSEVIER.value: self._pub_elsevier,
                    PublisherEnum.SPRINGER.value: self._pub_springer,
                    PublisherEnum.WILEY.value: self._pub_wiley,
                    PublisherEnum.NATURE.value: self._pub_nature,
                    PublisherEnum.BMJ.value: self._pub_bmj,
                    PublisherEnum.LANCET.value: self._pub_lancet,
                    PublisherEnum.TAYLOR_FRANCIS.value: self._pub_taylor_francis,
                    PublisherEnum.SAGE.value: self._pub_sage,
                }

                method_fn = publisher_methods.get(
                    publisher, self._pub_generic_fallback
                )
                pdf_url = await method_fn(page, paper, final_url)

                if not pdf_url:
                    # Fallback to generic if specific method failed
                    if publisher != PublisherEnum.OTHER.value:
                        pdf_url = await self._pub_generic_fallback(
                            page, paper, final_url
                        )

                if not pdf_url:
                    elapsed_ms = (time.monotonic() * 1000) - start_ms
                    await self._log(
                        canonical_id=paper.canonical_id,
                        run_id=run_id,
                        tier=TierEnum.TIER_2.value,
                        method=f"publisher_{publisher.lower()}",
                        url_attempted=final_url,
                        outcome="NO_PDF_LINK",
                        failure_code=FailureCode.PUBLISHER_SOFT_BLOCK.value,
                        execution_time_ms=elapsed_ms,
                    )
                    await self._record_tier2_attempt(publisher, False)
                    return False, FailureCode.PUBLISHER_SOFT_BLOCK.value

                # Resolve relative PDF URL
                if pdf_url.startswith("/"):
                    pdf_url = urljoin(final_url, pdf_url)

                # Download the PDF
                filename = generate_filename(
                    paper.first_author_lastname, paper.year, paper.title,
                )
                final_path = os.path.join(self._output_dir, filename)

                success, failure_code, pdf_data = await self._download_pdf(
                    url=pdf_url,
                    final_path=final_path,
                    canonical_id=paper.canonical_id,
                    run_id=run_id,
                    tier=TierEnum.TIER_2.value,
                    method=f"publisher_{publisher.lower()}",
                )

                if success:
                    await self._db.update_paper_fields(
                        paper.canonical_id,
                        pdf_path=final_path,
                        pdf_filename=filename,
                        retrieval_tier=TierEnum.TIER_2.value,
                        retrieval_method=f"publisher_{publisher.lower()}",
                        retrieval_url=pdf_url,
                        pdf_size_bytes=len(pdf_data) if pdf_data else None,
                    )

                await self._record_tier2_attempt(publisher, success)
                return success, failure_code

        except Exception as exc:
            elapsed_ms = (time.monotonic() * 1000) - start_ms
            await self._log(
                canonical_id=paper.canonical_id,
                run_id=run_id,
                tier=TierEnum.TIER_2.value,
                method=f"publisher_{publisher.lower()}",
                url_attempted=doi_url,
                outcome="EXCEPTION",
                failure_code=FailureCode.PUBLISHER_SOFT_BLOCK.value,
                execution_time_ms=elapsed_ms,
                exc=exc,
            )
            await self._record_tier2_attempt(publisher, False)
            return False, FailureCode.PUBLISHER_SOFT_BLOCK.value

    async def _record_tier2_attempt(
        self, publisher: str, success: bool
    ) -> None:
        """Record a Tier 2 attempt for publisher cooldown tracking."""
        try:
            await self._db.record_publisher_attempt(
                publisher=publisher,
                success=success,
                cooldown_failure_threshold=self._config.get(
                    "cooldown_failure_threshold", 0.70
                ),
                cooldown_window_size=self._config.get(
                    "cooldown_window_size", 20
                ),
                cooldown_minutes=self._config.get(
                    "cooldown_minutes", 10
                ),
            )
        except Exception as exc:
            logger.error("Failed to record publisher attempt: %s", exc)

    def _map_nav_error(self, nav_result: Dict[str, Any]) -> str:
        """Map a safe_navigate error to a FailureCode."""
        error = nav_result.get("error", "")
        status = nav_result.get("status")

        if error == "TIMEOUT":
            return FailureCode.TIMEOUT.value
        if status == 403:
            return FailureCode.ACCESS_DENIED.value
        if status == 404:
            return FailureCode.NOT_FOUND.value
        if status == 429:
            return FailureCode.RATE_LIMITED.value
        if status and status >= 500:
            return FailureCode.PUBLISHER_SOFT_BLOCK.value
        return FailureCode.PUBLISHER_SOFT_BLOCK.value

    # -----------------------------------------------------------------------
    # Publisher-specific methods (Tier 2)
    # -----------------------------------------------------------------------

    async def _pub_elsevier(
        self, page: Any, paper: Paper, current_url: str
    ) -> Optional[str]:
        """Elsevier / ScienceDirect — shadow DOM + iframe handling."""
        # Strategy 1: Look for PDF link in shadow DOM
        shadow_el = await self._browser.traverse_shadow_dom(
            page,
            "#pdfLink",
            "a[href*='pdf']",
        )
        if shadow_el:
            try:
                href = await shadow_el.get_attribute("href")
                if href:
                    return href
            except Exception:
                pass

        # Strategy 2: Direct PDF download link selectors
        selectors = [
            "a.pdf-download[href*='pdf']",
            "a[id='pdfLink']",
            "#pdfLink",
            "a[class*='pdf-download']",
            "a[href*='/pdfft?']",
            "a[href*='pii'][href*='pdf']",
        ]
        pdf_url = await self._browser.wait_for_pdf_link(
            page, selectors,
            timeout_s=self._config.get("selector_timeout_s", DEFAULT_SELECTOR_TIMEOUT_S),
        )
        if pdf_url:
            return pdf_url

        # Strategy 3: Check iframe for embedded PDF viewer
        iframe_url = await self._browser.extract_from_iframe(
            page,
            "iframe[src*='pdf']",
            "embed[src*='pdf'], object[data*='pdf']",
            "src",
        )
        if iframe_url:
            return iframe_url

        # Strategy 4: Meta tag fallback
        return await self._browser.extract_pdf_url_from_page(page)

    async def _pub_springer(
        self, page: Any, paper: Paper, current_url: str
    ) -> Optional[str]:
        """Springer / SpringerLink — PDF link extraction."""
        selectors = [
            "a[data-article-pdf]",
            "a[href*='/content/pdf/']",
            "a.c-pdf-download__link",
            "a[title='Download PDF']",
            "a[data-test='pdf-link']",
            "a[href*='.pdf']",
        ]
        pdf_url = await self._browser.wait_for_pdf_link(
            page, selectors,
            timeout_s=self._config.get("selector_timeout_s", DEFAULT_SELECTOR_TIMEOUT_S),
        )
        if pdf_url:
            return pdf_url

        # Construct PDF URL from article URL
        if "/article/" in current_url:
            article_path = current_url.split("/article/")[-1]
            constructed = f"https://link.springer.com/content/pdf/{article_path}.pdf"
            return constructed

        return await self._browser.extract_pdf_url_from_page(page)

    async def _pub_wiley(
        self, page: Any, paper: Paper, current_url: str
    ) -> Optional[str]:
        """Wiley Online Library — PDF link extraction."""
        selectors = [
            "a.pdf-download",
            "a[href*='/pdfdirect/']",
            "a[href*='/epdf/']",
            "a[title*='PDF']",
            "a[class*='epub-section__item__link']",
            "a[href*='.pdf']",
        ]
        pdf_url = await self._browser.wait_for_pdf_link(
            page, selectors,
            timeout_s=self._config.get("selector_timeout_s", DEFAULT_SELECTOR_TIMEOUT_S),
        )
        if pdf_url:
            # Convert epdf to pdfdirect for direct download
            if "/epdf/" in pdf_url:
                pdf_url = pdf_url.replace("/epdf/", "/pdfdirect/")
            return pdf_url

        # Construct from DOI
        if paper.doi:
            constructed = f"https://onlinelibrary.wiley.com/doi/pdfdirect/{paper.doi}"
            return constructed

        return await self._browser.extract_pdf_url_from_page(page)

    async def _pub_nature(
        self, page: Any, paper: Paper, current_url: str
    ) -> Optional[str]:
        """Nature Publishing Group — PDF link extraction."""
        selectors = [
            "a[data-article-pdf]",
            "a[href*='.pdf']",
            "a.c-pdf-download__link",
            "a[data-track-action='download pdf']",
            "a[class*='download-pdf']",
        ]
        pdf_url = await self._browser.wait_for_pdf_link(
            page, selectors,
            timeout_s=self._config.get("selector_timeout_s", DEFAULT_SELECTOR_TIMEOUT_S),
        )
        if pdf_url:
            return pdf_url

        # Construct from article URL
        if "/articles/" in current_url:
            return current_url.rstrip("/") + ".pdf"

        return await self._browser.extract_pdf_url_from_page(page)

    async def _pub_bmj(
        self, page: Any, paper: Paper, current_url: str
    ) -> Optional[str]:
        """BMJ — PDF link extraction."""
        selectors = [
            "a.article-pdf-download",
            "a[href*='.full.pdf']",
            "a[href*='/pdf/']",
            "a[data-trigger='full-pdf']",
            "a[class*='pdf']",
        ]
        pdf_url = await self._browser.wait_for_pdf_link(
            page, selectors,
            timeout_s=self._config.get("selector_timeout_s", DEFAULT_SELECTOR_TIMEOUT_S),
        )
        if pdf_url:
            return pdf_url

        # Construct from content URL
        if "/content/" in current_url:
            return current_url.rstrip("/") + ".full.pdf"

        return await self._browser.extract_pdf_url_from_page(page)

    async def _pub_lancet(
        self, page: Any, paper: Paper, current_url: str
    ) -> Optional[str]:
        """The Lancet — PDF link extraction (Elsevier-owned)."""
        selectors = [
            "a.pdf-download",
            "a[href*='pdfft']",
            "a[href*='pdf'][class*='download']",
            "a[id*='pdf']",
            "a[href*='.pdf']",
        ]
        pdf_url = await self._browser.wait_for_pdf_link(
            page, selectors,
            timeout_s=self._config.get("selector_timeout_s", DEFAULT_SELECTOR_TIMEOUT_S),
        )
        if pdf_url:
            return pdf_url

        # Lancet uses similar structure to ScienceDirect
        return await self._pub_elsevier(page, paper, current_url)

    async def _pub_taylor_francis(
        self, page: Any, paper: Paper, current_url: str
    ) -> Optional[str]:
        """Taylor & Francis Online — PDF link extraction."""
        selectors = [
            "a.show-pdf",
            "a[href*='/pdf/']",
            "a[class*='pdf-download']",
            "a[data-id='pdf-link']",
            "a[href*='.pdf']",
        ]
        pdf_url = await self._browser.wait_for_pdf_link(
            page, selectors,
            timeout_s=self._config.get("selector_timeout_s", DEFAULT_SELECTOR_TIMEOUT_S),
        )
        if pdf_url:
            return pdf_url

        # Construct PDF URL from DOI page
        if paper.doi and "/doi/" in current_url:
            base = current_url.split("/doi/")[0]
            return f"{base}/doi/pdf/{paper.doi}?needAccess=true"

        return await self._browser.extract_pdf_url_from_page(page)

    async def _pub_sage(
        self, page: Any, paper: Paper, current_url: str
    ) -> Optional[str]:
        """SAGE Publications — PDF link extraction."""
        selectors = [
            "a.pdf-download",
            "a[href*='/doi/pdf/']",
            "a[data-item-name='download-PDF']",
            "a[class*='pdf']",
            "a[href*='.pdf']",
        ]
        pdf_url = await self._browser.wait_for_pdf_link(
            page, selectors,
            timeout_s=self._config.get("selector_timeout_s", DEFAULT_SELECTOR_TIMEOUT_S),
        )
        if pdf_url:
            return pdf_url

        # Construct from DOI
        if paper.doi:
            return f"https://journals.sagepub.com/doi/pdf/{paper.doi}"

        return await self._browser.extract_pdf_url_from_page(page)

    async def _pub_generic_fallback(
        self, page: Any, paper: Paper, current_url: str
    ) -> Optional[str]:
        """Generic fallback — meta tags + common anchor patterns.

        Used when the publisher is OTHER or when publisher-specific
        methods fail.
        """
        # Strategy 1: Meta tags (most reliable across publishers)
        pdf_url = await self._browser.extract_pdf_url_from_page(page)
        if pdf_url:
            return pdf_url

        # Strategy 2: Common PDF link selectors
        generic_selectors = [
            "a[href*='.pdf']",
            "a[href*='/pdf/']",
            "a[href*='pdf?']",
            "a[class*='pdf']",
            "a[title*='PDF']",
            "a[title*='pdf']",
            "a[data-format='pdf']",
            "button[data-format='pdf']",
        ]
        pdf_url = await self._browser.wait_for_pdf_link(
            page, generic_selectors,
            timeout_s=self._config.get("selector_timeout_s", DEFAULT_SELECTOR_TIMEOUT_S),
        )
        if pdf_url:
            return pdf_url

        # Strategy 3: Look for download buttons that might trigger JS
        download_selectors = [
            "a[download]",
            "a[href*='download']",
            "button[class*='download']",
        ]
        for selector in download_selectors:
            try:
                element = await page.query_selector(selector)
                if element:
                    href = await element.get_attribute("href")
                    if href:
                        return href
            except Exception:
                continue

        return None

    # ===================================================================
    # TIER 3 — Institutional SSO (Playwright headed, persistent context)
    # ===================================================================

    async def tier3_institutional_sso(
        self,
        paper: Paper,
        run_id: str,
    ) -> Tuple[bool, Optional[str]]:
        """Tier 3: Retrieve via institutional SSO with headed browser.

        Uses a persistent browser context with user-mediated login.
        Tries in order: EZproxy → OpenAthens → institutional resolver → doi.org

        ABSOLUTE RULE: Never access anything the user types.

        Returns:
            Tuple of (success, failure_code).
        """
        if not paper.doi:
            await self._log(
                canonical_id=paper.canonical_id,
                run_id=run_id,
                tier=TierEnum.TIER_3.value,
                method="sso",
                outcome="SKIPPED",
                failure_code=FailureCode.NO_DOI.value,
            )
            return False, FailureCode.NO_DOI.value

        # Check if SSO session is valid
        session_valid = await self._browser.check_sso_session_valid()
        if not session_valid:
            await self._log(
                canonical_id=paper.canonical_id,
                run_id=run_id,
                tier=TierEnum.TIER_3.value,
                method="sso",
                outcome="SSO_NOT_AUTHENTICATED",
                failure_code=FailureCode.SSO_FAILED.value,
                details={"reason": "No active SSO session. User must authenticate."},
            )
            return False, FailureCode.SSO_FAILED.value

        # Build access URLs in priority order
        access_urls = self._build_sso_access_urls(paper.doi)
        if not access_urls:
            return False, FailureCode.SSO_FAILED.value

        start_ms = time.monotonic() * 1000

        try:
            page = await self._browser.get_sso_page()

            for url_method, url in access_urls:
                nav_result = await self._browser.safe_navigate(
                    page, url,
                    timeout_s=self._config.get(
                        "page_load_timeout_s", DEFAULT_PAGE_LOAD_TIMEOUT_S
                    ),
                )

                if not nav_result["success"]:
                    # Check for SESSION_EXPIRED (403 in SSO context)
                    if nav_result.get("status") == 403:
                        await self._log(
                            canonical_id=paper.canonical_id,
                            run_id=run_id,
                            tier=TierEnum.TIER_3.value,
                            method=url_method,
                            url_attempted=url,
                            http_status=403,
                            outcome="SESSION_EXPIRED",
                            failure_code=FailureCode.SESSION_EXPIRED.value,
                        )
                        # Attempt re-authentication
                        sso_url = self._get_primary_sso_url()
                        if sso_url:
                            await self._browser.handle_session_expired(sso_url)
                        return False, FailureCode.SESSION_EXPIRED.value
                    continue

                # Paywall detection on the resolved page
                if await self._browser.detect_paywall(page):
                    await self._log(
                        canonical_id=paper.canonical_id,
                        run_id=run_id,
                        tier=TierEnum.TIER_3.value,
                        method=url_method,
                        url_attempted=url,
                        outcome="PAYWALL_DETECTED",
                        failure_code=FailureCode.PAYWALL_DETECTED.value,
                    )
                    continue

                # Try to find PDF URL on this page
                pdf_url = await self._browser.extract_pdf_url_from_page(page)
                if not pdf_url:
                    pdf_url = await self._pub_generic_fallback(
                        page, paper, nav_result["url"]
                    )

                if not pdf_url:
                    continue

                # Resolve relative URL
                if pdf_url.startswith("/"):
                    pdf_url = urljoin(nav_result["url"], pdf_url)

                # Download the PDF
                filename = generate_filename(
                    paper.first_author_lastname, paper.year, paper.title,
                )
                final_path = os.path.join(self._output_dir, filename)

                success, failure_code, pdf_data = await self._download_pdf(
                    url=pdf_url,
                    final_path=final_path,
                    canonical_id=paper.canonical_id,
                    run_id=run_id,
                    tier=TierEnum.TIER_3.value,
                    method=url_method,
                )

                if success:
                    elapsed_ms = (time.monotonic() * 1000) - start_ms
                    await self._db.update_paper_fields(
                        paper.canonical_id,
                        pdf_path=final_path,
                        pdf_filename=filename,
                        retrieval_tier=TierEnum.TIER_3.value,
                        retrieval_method=url_method,
                        retrieval_url=pdf_url,
                        pdf_size_bytes=len(pdf_data) if pdf_data else None,
                    )
                    return True, None

            # All SSO methods exhausted
            elapsed_ms = (time.monotonic() * 1000) - start_ms
            await self._log(
                canonical_id=paper.canonical_id,
                run_id=run_id,
                tier=TierEnum.TIER_3.value,
                method="sso_all",
                outcome="ALL_SSO_FAILED",
                failure_code=FailureCode.SSO_FAILED.value,
                execution_time_ms=elapsed_ms,
            )
            return False, FailureCode.SSO_FAILED.value

        except Exception as exc:
            elapsed_ms = (time.monotonic() * 1000) - start_ms
            await self._log(
                canonical_id=paper.canonical_id,
                run_id=run_id,
                tier=TierEnum.TIER_3.value,
                method="sso",
                outcome="EXCEPTION",
                failure_code=FailureCode.SSO_FAILED.value,
                execution_time_ms=elapsed_ms,
                exc=exc,
            )
            return False, FailureCode.SSO_FAILED.value

    def _build_sso_access_urls(self, doi: str) -> List[Tuple[str, str]]:
        """Build ordered list of SSO access URLs from config.

        Returns list of (method_name, url) tuples.
        """
        urls: List[Tuple[str, str]] = []
        doi_url = f"https://doi.org/{doi}"

        # EZproxy
        ezproxy = self._config.get("sso_proxy_url", "")
        if ezproxy:
            ezproxy_clean = ezproxy.rstrip("/")
            urls.append((
                "ezproxy",
                f"{ezproxy_clean}/login?url={doi_url}",
            ))

        # OpenAthens
        openathens = self._config.get("openathens_url", "")
        if openathens:
            openathens_clean = openathens.rstrip("/")
            urls.append((
                "openathens",
                f"{openathens_clean}?url={doi_url}",
            ))

        # Institutional resolver
        resolver = self._config.get("institutional_resolver_url", "")
        if resolver:
            resolver_clean = resolver.rstrip("/")
            urls.append((
                "resolver",
                f"{resolver_clean}?doi={doi}",
            ))

        # Direct DOI fallback (may work if institutional IP is recognized)
        urls.append(("doi_direct", doi_url))

        return urls

    def _get_primary_sso_url(self) -> Optional[str]:
        """Get the primary SSO login URL for re-authentication."""
        for key in ("sso_proxy_url", "openathens_url", "institutional_resolver_url"):
            url = self._config.get(key, "")
            if url:
                return url
        return None

    # ===================================================================
    # TIER 3.5 — Scholar-Assisted Retrieval (SSO session only)
    # ===================================================================

    async def tier3_5_scholar_assisted(
        self,
        paper: Paper,
        run_id: str,
    ) -> Tuple[bool, Optional[str]]:
        """Tier 3.5: Search Google Scholar for PDF links using SSO session.

        SAFETY FILTER: Only follows links to known-safe domains.
        Rate limited: 10s minimum (seeded delay). Max 3 queries per paper.

        Returns:
            Tuple of (success, failure_code).
        """
        # Must have active SSO session
        session_valid = await self._browser.check_sso_session_valid()
        if not session_valid:
            await self._log(
                canonical_id=paper.canonical_id,
                run_id=run_id,
                tier=TierEnum.TIER_3_5.value,
                method="scholar",
                outcome="SSO_NOT_AUTHENTICATED",
                failure_code=FailureCode.SSO_FAILED.value,
            )
            return False, FailureCode.SSO_FAILED.value

        if not paper.title:
            return False, FailureCode.METADATA_MISSING.value

        max_queries = self._config.get(
            "scholar_max_queries_per_paper", SCHOLAR_MAX_QUERIES_PER_PAPER
        )
        min_delay = self._config.get(
            "scholar_min_delay_s", SCHOLAR_MIN_DELAY_S
        )

        page = await self._browser.get_sso_page()

        for query_attempt in range(max_queries):
            start_ms = time.monotonic() * 1000

            # Seeded delay for audit reproducibility
            delay = generate_seeded_delay(
                run_id,
                f"{paper.canonical_id}_{query_attempt}",
                min_delay,
                jitter_range=3.0,
            )
            await asyncio.sleep(delay)

            # Build search query — title-based
            search_title = paper.title[:200]  # Truncate very long titles
            if paper.authors and query_attempt > 0:
                # Add author on retry for more specific results
                search_query = f'"{search_title}" {paper.first_author_lastname}'
            else:
                search_query = f'"{search_title}"'

            scholar_url = (
                f"https://scholar.google.com/scholar?"
                f"q={search_query.replace(' ', '+')}"
            )

            # Navigate to Scholar
            nav_result = await self._browser.safe_navigate(
                page, scholar_url,
                timeout_s=self._config.get(
                    "page_load_timeout_s", DEFAULT_PAGE_LOAD_TIMEOUT_S
                ),
            )

            elapsed_ms = (time.monotonic() * 1000) - start_ms

            if not nav_result["success"]:
                await self._log(
                    canonical_id=paper.canonical_id,
                    run_id=run_id,
                    tier=TierEnum.TIER_3_5.value,
                    method="scholar",
                    url_attempted=scholar_url,
                    http_status=nav_result.get("status"),
                    outcome="SCHOLAR_NAV_FAILED",
                    failure_code=FailureCode.SCHOLAR_BLOCKED.value,
                    execution_time_ms=elapsed_ms,
                    retry_count=query_attempt,
                )
                continue

            # CAPTCHA detection
            if await self._browser.detect_captcha(page):
                await self._log(
                    canonical_id=paper.canonical_id,
                    run_id=run_id,
                    tier=TierEnum.TIER_3_5.value,
                    method="scholar",
                    url_attempted=scholar_url,
                    outcome="CAPTCHA_ENCOUNTERED",
                    failure_code=FailureCode.CAPTCHA_ENCOUNTERED.value,
                    execution_time_ms=elapsed_ms,
                    retry_count=query_attempt,
                )
                return False, FailureCode.CAPTCHA_ENCOUNTERED.value

            # Extract PDF links from Scholar results
            pdf_links = await self._extract_scholar_pdf_links(page)

            for link_url in pdf_links:
                # SAFETY FILTER: known-safe domains only
                if not is_safe_scholar_domain(link_url):
                    logger.debug(
                        "Scholar link rejected (unsafe domain): %s", link_url
                    )
                    continue

                # Try downloading
                filename = generate_filename(
                    paper.first_author_lastname, paper.year, paper.title,
                )
                final_path = os.path.join(self._output_dir, filename)

                success, failure_code, pdf_data = await self._download_pdf(
                    url=link_url,
                    final_path=final_path,
                    canonical_id=paper.canonical_id,
                    run_id=run_id,
                    tier=TierEnum.TIER_3_5.value,
                    method="scholar",
                    retry_count=query_attempt,
                )

                if success:
                    await self._db.update_paper_fields(
                        paper.canonical_id,
                        pdf_path=final_path,
                        pdf_filename=filename,
                        retrieval_tier=TierEnum.TIER_3_5.value,
                        retrieval_method="scholar",
                        retrieval_url=link_url,
                        pdf_size_bytes=len(pdf_data) if pdf_data else None,
                    )
                    await self._log(
                        canonical_id=paper.canonical_id,
                        run_id=run_id,
                        tier=TierEnum.TIER_3_5.value,
                        method="scholar",
                        url_attempted=link_url,
                        outcome="SCHOLAR_FALLBACK",
                        execution_time_ms=elapsed_ms,
                        retry_count=query_attempt,
                    )
                    return True, None

        # All queries exhausted
        await self._log(
            canonical_id=paper.canonical_id,
            run_id=run_id,
            tier=TierEnum.TIER_3_5.value,
            method="scholar",
            outcome="ALL_SCHOLAR_FAILED",
            failure_code=FailureCode.SCHOLAR_BLOCKED.value,
            details={"queries_attempted": max_queries},
        )
        return False, FailureCode.SCHOLAR_BLOCKED.value

    async def _extract_scholar_pdf_links(self, page: Any) -> List[str]:
        """Extract PDF links from a Google Scholar results page.

        Looks for:
            - [PDF] links on the right side of results
            - Direct PDF href attributes
        """
        pdf_links: List[str] = []

        try:
            # Scholar shows [PDF] links with class "gs_or_ggsm"
            # or as direct links with "[PDF]" text
            pdf_selectors = [
                "a[href*='.pdf']",
                ".gs_or_ggsm a",
                ".gs_ggsd a",
                "a[data-clk-atid]",
            ]

            seen_urls: Set[str] = set()
            for selector in pdf_selectors:
                elements = await page.query_selector_all(selector)
                for element in elements:
                    href = await element.get_attribute("href")
                    if href and href not in seen_urls:
                        # Prioritize actual PDF URLs
                        if ".pdf" in href.lower() or "pdf" in href.lower():
                            pdf_links.append(href)
                            seen_urls.add(href)

            # Also check for links with "[PDF]" text content
            all_links = await page.query_selector_all("a")
            for link in all_links:
                try:
                    text = await link.inner_text()
                    if "[PDF]" in text.upper():
                        href = await link.get_attribute("href")
                        if href and href not in seen_urls:
                            pdf_links.append(href)
                            seen_urls.add(href)
                except Exception:
                    continue

        except Exception as exc:
            logger.debug("Error extracting Scholar PDF links: %s", exc)

        return pdf_links

    # ===================================================================
    # TIER 4 — Manual Flag
    # ===================================================================

    async def tier4_manual_flag(
        self,
        paper: Paper,
        run_id: str,
    ) -> Tuple[bool, Optional[str]]:
        """Tier 4: Flag paper for manual retrieval.

        State: RETRIEVING → MANUAL_REQUIRED.
        Logs all previous endpoints and failure codes.
        Suggests action: ILL | corresponding author | ResearchGate.
        """
        # Gather retrieval history for this paper
        audit_entries = await self._db.get_audit_log_for_paper(
            paper.canonical_id, limit=50
        )

        failed_endpoints: List[Dict[str, Any]] = []
        failure_codes: List[str] = []
        for entry in audit_entries:
            if entry.failure_code:
                failure_codes.append(entry.failure_code)
            if entry.url_attempted:
                failed_endpoints.append({
                    "tier": entry.tier,
                    "method": entry.method,
                    "url": entry.url_attempted,
                    "status": entry.http_status,
                    "failure_code": entry.failure_code,
                })

        # Build suggested actions
        suggestions: List[str] = []
        if paper.doi:
            suggestions.append(
                f"Request via Inter-Library Loan (ILL) using DOI: {paper.doi}"
            )
            suggestions.append(
                "Contact corresponding author directly"
            )
            suggestions.append(
                f"Search ResearchGate: https://www.researchgate.net/search?q={paper.doi}"
            )
        else:
            suggestions.append(
                f"Search by title: \"{paper.title[:100]}\""
            )
            suggestions.append(
                "Request via Inter-Library Loan (ILL)"
            )

        await self._log(
            canonical_id=paper.canonical_id,
            run_id=run_id,
            tier=TierEnum.TIER_4.value,
            method="manual_flag",
            outcome="MANUAL_REQUIRED",
            failure_code=FailureCode.MANUAL_REQUIRED.value,
            details={
                "failed_endpoints": failed_endpoints,
                "failure_codes": list(set(failure_codes)),
                "suggested_actions": suggestions,
                "total_attempts": paper.attempt_count,
            },
        )

        return False, FailureCode.MANUAL_REQUIRED.value

    # ===================================================================
    # Adaptive Retry Engine
    # ===================================================================

    async def orchestrate_retrieval(
        self,
        paper: Paper,
        run_id: str,
    ) -> Tuple[bool, Optional[str]]:
        """Master retrieval orchestrator — runs through tiers adaptively.

        Tier progression: 0 → 1 → 2 → 3 → 3.5 → 4.
        Skips tiers intelligently based on previous failures.
        Tracks retry history. Hard cap on total attempts.

        Returns:
            Tuple of (success, final_failure_code).
        """
        max_attempts = self._config.get(
            "max_total_attempts", DEFAULT_MAX_TOTAL_ATTEMPTS
        )

        # Check attempt cap
        if paper.attempt_count >= max_attempts:
            await self._log(
                canonical_id=paper.canonical_id,
                run_id=run_id,
                tier=None,
                method="orchestrator",
                outcome="ATTEMPT_CAP_REACHED",
                failure_code=FailureCode.MANUAL_REQUIRED.value,
                details={"attempt_count": paper.attempt_count, "cap": max_attempts},
            )
            return await self.tier4_manual_flag(paper, run_id)

        # Increment attempt count
        new_count = paper.attempt_count + 1
        await self._db.update_paper_fields(
            paper.canonical_id, attempt_count=new_count
        )

        # Get previous failure history to make adaptive decisions
        prev_failures = await self._get_failure_history(paper.canonical_id)

        # Tier 0 — Unpaywall (pure HTTP)
        if not self._should_skip_tier(TierEnum.TIER_0.value, prev_failures):
            success, fc = await self.tier0_unpaywall(paper, run_id)
            if success:
                return True, None

        # Tier 1 — Open Access APIs (pure HTTP)
        if not self._should_skip_tier(TierEnum.TIER_1.value, prev_failures):
            success, fc = await self.tier1_open_access(paper, run_id)
            if success:
                return True, None

        # Tier 2 — Publisher Direct (Playwright headless)
        # Skip if NO_DOI or if publisher in cooldown
        if (
            paper.doi
            and not self._should_skip_tier(TierEnum.TIER_2.value, prev_failures)
        ):
            success, fc = await self.tier2_publisher_direct(paper, run_id)
            if success:
                return True, None
            # If PAYWALL_DETECTED, go straight to Tier 3
            if fc == FailureCode.PAYWALL_DETECTED.value:
                pass  # Fall through to Tier 3

        # Tier 3 — Institutional SSO (requires active session)
        if (
            self._browser.has_persistent_context
            and not self._should_skip_tier(TierEnum.TIER_3.value, prev_failures)
        ):
            success, fc = await self.tier3_institutional_sso(paper, run_id)
            if success:
                return True, None

            # Tier 3.5 — Scholar-Assisted (only with active SSO)
            if fc != FailureCode.SSO_FAILED.value:
                success, fc = await self.tier3_5_scholar_assisted(paper, run_id)
                if success:
                    return True, None

        # Tier 4 — Manual Flag
        return await self.tier4_manual_flag(paper, run_id)

    async def _get_failure_history(
        self, canonical_id: str
    ) -> Dict[str, List[str]]:
        """Get failure history grouped by tier for adaptive decisions."""
        entries = await self._db.get_audit_log_for_paper(canonical_id, limit=100)
        history: Dict[str, List[str]] = {}
        for entry in entries:
            if entry.tier and entry.failure_code:
                if entry.tier not in history:
                    history[entry.tier] = []
                history[entry.tier].append(entry.failure_code)
        return history

    def _should_skip_tier(
        self,
        tier: str,
        failure_history: Dict[str, List[str]],
    ) -> bool:
        """Decide whether to skip a tier based on previous failures.

        Skip rules:
            - Tier had 3+ failures with the same endpoint → skip
            - Tier 0/1 returned NO_OA_SOURCE on all attempts → skip on retry
            - Tier 2 returned PAYWALL_DETECTED → skip (go to Tier 3)
            - Tier 3 returned SSO_FAILED → skip 3 and 3.5
        """
        tier_failures = failure_history.get(tier, [])
        if not tier_failures:
            return False

        max_retries = self._config.get("max_retries", DEFAULT_MAX_RETRIES)

        # If all attempts for this tier returned the same terminal failure
        if len(tier_failures) >= max_retries:
            unique_failures = set(tier_failures[-max_retries:])
            terminal_failures = {
                FailureCode.NO_OA_SOURCE.value,
                FailureCode.NO_DOI.value,
                FailureCode.PAYWALL_DETECTED.value,
                FailureCode.SSO_FAILED.value,
            }
            if unique_failures.issubset(terminal_failures):
                return True

        return False

    # ===================================================================
    # Supplement Sniffer
    # ===================================================================

    async def sniff_supplements(
        self,
        paper: Paper,
        page: Any,
        run_id: str,
    ) -> int:
        """Scan for supplementary materials after successful Tier 2 download.

        Searches:
            1. HTML page for supplement links
            2. First 5 pages of the PDF for supplement keywords

        Downloads each to /Supplements/{canonical_id}/.
        PDFs get full Step 3 validation. Others get size-only check.

        Returns count of supplements found and downloaded.
        """
        supplement_count = 0
        supplement_urls: List[Tuple[str, str]] = []  # (url, extension)

        # Scan HTML page for supplement links
        try:
            supplement_selectors = [
                "a[href*='supplement']",
                "a[href*='supporting']",
                "a[href*='appendix']",
                "a[href*='additional']",
                "a[class*='supplement']",
                "a[data-type='supplementary']",
            ]

            seen: Set[str] = set()
            for selector in supplement_selectors:
                elements = await page.query_selector_all(selector)
                for element in elements:
                    href = await element.get_attribute("href")
                    if href and href not in seen:
                        seen.add(href)
                        # Determine extension
                        ext = self._guess_extension(href)
                        supplement_urls.append((href, ext))
        except Exception as exc:
            logger.debug("Error scanning HTML for supplements: %s", exc)

        # Download each supplement
        for idx, (supp_url, ext) in enumerate(supplement_urls, start=1):
            # Resolve relative URL
            if supp_url.startswith("/"):
                supp_url = urljoin(page.url, supp_url)

            try:
                supp_filename = generate_supplement_filename(
                    paper.first_author_lastname,
                    paper.year,
                    idx,
                    ext,
                )
                supp_dir = os.path.join(
                    self._supplement_dir, paper.canonical_id.replace(":", "_")
                )
                os.makedirs(supp_dir, exist_ok=True)
                supp_path = os.path.join(supp_dir, supp_filename)

                # Download
                data, http_status, content_type, error = await self._stream_download(
                    supp_url
                )
                if data is None or error:
                    continue

                # Validate based on type
                if ext == "pdf":
                    # Full validation: magic bytes + size
                    if not pdf_magic_bytes_valid(data) or len(data) < MIN_PDF_SIZE_BYTES:
                        continue
                    write_ok, _ = await self._atomic_write_pdf(data, supp_path)
                    validation_status = "VALID" if write_ok else None
                else:
                    # Non-PDF: size > 0 check only
                    if len(data) == 0:
                        continue
                    with open(supp_path, "wb") as f:
                        f.write(data)
                    validation_status = "SUPPLEMENT_NON_PDF"

                if not os.path.isfile(supp_path):
                    continue

                # Record supplement
                supp_hash = hashlib.sha256(data).hexdigest()
                supp_record = SupplementFile(
                    canonical_id=paper.canonical_id,
                    supplement_index=idx,
                    filename=supp_filename,
                    file_path=supp_path,
                    file_extension=ext,
                    file_size_bytes=len(data),
                    sha256_checksum=supp_hash,
                    validation_status=validation_status,
                    source_url=supp_url,
                )
                await self._db.insert_supplement(supp_record)
                supplement_count += 1

                await self._log(
                    canonical_id=paper.canonical_id,
                    run_id=run_id,
                    tier=paper.retrieval_tier,
                    method="supplement_sniffer",
                    url_attempted=supp_url,
                    outcome="SUPPLEMENT_DOWNLOADED",
                    details={
                        "index": idx,
                        "extension": ext,
                        "size_bytes": len(data),
                        "validation": validation_status,
                    },
                )

            except Exception as exc:
                logger.debug(
                    "Error downloading supplement %d for %s: %s",
                    idx, paper.canonical_id, exc,
                )
                continue

        # Update paper supplement count
        if supplement_count > 0:
            await self._db.update_paper_fields(
                paper.canonical_id,
                supplement_count=supplement_count,
            )

        return supplement_count

    @staticmethod
    def _guess_extension(url: str) -> str:
        """Guess file extension from a URL."""
        parsed = urlparse(url)
        path = parsed.path.lower()
        if path.endswith(".pdf"):
            return "pdf"
        if path.endswith(".docx") or path.endswith(".doc"):
            return "docx"
        if path.endswith(".xlsx") or path.endswith(".xls"):
            return "xlsx"
        if path.endswith(".csv"):
            return "csv"
        if path.endswith(".zip"):
            return "zip"
        if path.endswith(".pptx") or path.endswith(".ppt"):
            return "pptx"
        if path.endswith(".txt"):
            return "txt"
        # Default to pdf for unknown
        return "pdf"

    # ===================================================================
    # STEP 3 — PDF Validation & Integrity
    # ===================================================================

    async def validate_pdf(
        self,
        paper: Paper,
        run_id: str,
    ) -> Tuple[str, str]:
        """Run the full 7-check validation pipeline on a retrieved PDF.

        State: RETRIEVED → VALIDATING → VALIDATED (or FAILED).

        Returns:
            Tuple of (validation_status, identity_status).
        """
        pdf_path = paper.pdf_path
        if not pdf_path or not os.path.isfile(pdf_path):
            await self._log(
                canonical_id=paper.canonical_id,
                run_id=run_id,
                method="validate",
                outcome="FILE_MISSING",
                failure_code=FailureCode.FILE_MISSING.value,
            )
            return ValidationStatus.INVALID.value, IdentityStatus.CONTENT_UNVERIFIED.value

        start_ms = time.monotonic() * 1000

        try:
            with open(pdf_path, "rb") as f:
                pdf_bytes = f.read()
        except Exception as exc:
            await self._log(
                canonical_id=paper.canonical_id,
                run_id=run_id,
                method="validate_read",
                outcome="READ_FAILED",
                failure_code=FailureCode.CORRUPTED.value,
                exc=exc,
            )
            return ValidationStatus.INVALID.value, IdentityStatus.CONTENT_UNVERIFIED.value

        # CHECK 1: File size > 50KB
        check1_ok, check1_detail = self._check1_file_size(pdf_bytes)
        await self._log_validation_check(
            paper.canonical_id, run_id, "CHECK_1_SIZE",
            check1_ok, check1_detail, start_ms,
        )
        if not check1_ok:
            return ValidationStatus.PARTIAL_SIZE.value, IdentityStatus.CONTENT_UNVERIFIED.value

        # CHECK 2: Magic bytes (%PDF)
        check2_ok, check2_detail = self._check2_magic_bytes(pdf_bytes)
        await self._log_validation_check(
            paper.canonical_id, run_id, "CHECK_2_MAGIC",
            check2_ok, check2_detail, start_ms,
        )
        if not check2_ok:
            return ValidationStatus.INVALID.value, IdentityStatus.CONTENT_UNVERIFIED.value

        # CHECK 3: HTML disguise
        check3_ok, check3_detail = self._check3_html_disguise(pdf_bytes)
        await self._log_validation_check(
            paper.canonical_id, run_id, "CHECK_3_HTML",
            check3_ok, check3_detail, start_ms,
        )
        if not check3_ok:
            return ValidationStatus.INVALID.value, IdentityStatus.CONTENT_UNVERIFIED.value

        # CHECK 4: PDF parsability
        check4_ok, page_count, check4_detail = await self._check4_parsability(
            pdf_path, paper.canonical_id, run_id, start_ms
        )
        if not check4_ok:
            return ValidationStatus.INVALID.value, IdentityStatus.CONTENT_UNVERIFIED.value

        # Update page count
        await self._db.update_paper_fields(
            paper.canonical_id, pdf_page_count=page_count
        )

        # CHECK 5: Text extraction
        extracted_text, check5_ok = await self._check5_text_extraction(
            pdf_path, paper.canonical_id, run_id, start_ms
        )

        ocr_applied = False
        if not check5_ok:
            # CHECK 5b: OCR fallback
            if self.tesseract_available and self.ghostscript_available:
                extracted_text, ocr_ok = await self._check5b_ocr(
                    pdf_path, paper.canonical_id, run_id, start_ms
                )
                if ocr_ok and extracted_text:
                    ocr_applied = True
                    await self._db.update_paper_fields(
                        paper.canonical_id,
                        ocr_applied=True,
                        ocr_text_extracted=True,
                    )
                else:
                    # IMAGE_UNREADABLE
                    await self._log_validation_check(
                        paper.canonical_id, run_id, "CHECK_5B_OCR",
                        False, "OCR produced no text", start_ms,
                    )
                    await self._db.update_paper_fields(
                        paper.canonical_id,
                        ocr_applied=True,
                        ocr_text_extracted=False,
                    )
                    return (
                        ValidationStatus.PARTIAL_IMAGE.value,
                        IdentityStatus.CONTENT_UNVERIFIED.value,
                    )
            elif self.tesseract_available:
                # ghostscript missing — try pytesseract directly
                extracted_text, ocr_ok = await self._check5b_ocr_tesseract_only(
                    pdf_path, paper.canonical_id, run_id, start_ms
                )
                if ocr_ok and extracted_text:
                    ocr_applied = True
                    await self._db.update_paper_fields(
                        paper.canonical_id,
                        ocr_applied=True,
                        ocr_text_extracted=True,
                    )
                else:
                    return (
                        ValidationStatus.PARTIAL_IMAGE.value,
                        IdentityStatus.CONTENT_UNVERIFIED.value,
                    )
            else:
                # OCR unavailable
                await self._log_validation_check(
                    paper.canonical_id, run_id, "CHECK_5_TEXT",
                    False, "No text extracted, OCR unavailable", start_ms,
                )
                return (
                    ValidationStatus.PARTIAL_IMAGE.value,
                    IdentityStatus.CONTENT_UNVERIFIED.value,
                )

        # CHECK 6: Metadata-DOI cross-check
        identity_status = await self._check6_identity(
            extracted_text, paper, run_id, start_ms
        )

        # CHECK 7: SHA-256 + content drift detection
        content_drift = await self._check7_content_drift(
            pdf_bytes, paper, run_id, start_ms
        )

        # Determine final validation status
        validation_status = self._determine_validation_status(
            identity_status, ocr_applied, content_drift
        )

        elapsed_ms = (time.monotonic() * 1000) - start_ms
        await self._log(
            canonical_id=paper.canonical_id,
            run_id=run_id,
            method="validate_complete",
            outcome=validation_status,
            execution_time_ms=elapsed_ms,
            details={
                "identity": identity_status,
                "ocr_applied": ocr_applied,
                "content_drift": content_drift,
                "page_count": page_count,
            },
        )

        return validation_status, identity_status

    # -----------------------------------------------------------------------
    # Individual validation checks
    # -----------------------------------------------------------------------

    @staticmethod
    def _check1_file_size(data: bytes) -> Tuple[bool, str]:
        """CHECK 1: File size > 50KB."""
        size = len(data)
        if size < MIN_PDF_SIZE_BYTES:
            return False, f"File size {size} bytes < minimum {MIN_PDF_SIZE_BYTES}"
        return True, f"File size {size} bytes OK"

    @staticmethod
    def _check2_magic_bytes(data: bytes) -> Tuple[bool, str]:
        """CHECK 2: PDF magic bytes (%PDF)."""
        if pdf_magic_bytes_valid(data):
            return True, "Magic bytes %PDF present"
        header = data[:4].hex() if len(data) >= 4 else "empty"
        return False, f"Invalid magic bytes: {header}"

    @staticmethod
    def _check3_html_disguise(data: bytes) -> Tuple[bool, str]:
        """CHECK 3: HTML disguised as PDF."""
        if looks_like_html(data):
            return False, "File contains HTML content disguised as PDF"
        return True, "No HTML disguise detected"

    async def _check4_parsability(
        self,
        pdf_path: str,
        canonical_id: str,
        run_id: str,
        start_ms: float,
    ) -> Tuple[bool, int, str]:
        """CHECK 4: PDF parsability — can we open and count pages?

        Returns (ok, page_count, detail).
        """
        try:
            import fitz  # PyMuPDF

            doc = fitz.open(pdf_path)
            page_count = len(doc)
            doc.close()

            if page_count == 0:
                await self._log_validation_check(
                    canonical_id, run_id, "CHECK_4_PARSE",
                    False, "PDF has 0 pages", start_ms,
                )
                return False, 0, "PDF has 0 pages"

            await self._log_validation_check(
                canonical_id, run_id, "CHECK_4_PARSE",
                True, f"PDF parsable, {page_count} pages", start_ms,
            )
            return True, page_count, f"{page_count} pages"

        except ImportError:
            # PyMuPDF not available, try pypdf
            try:
                from pypdf import PdfReader

                reader = PdfReader(pdf_path)
                page_count = len(reader.pages)

                if page_count == 0:
                    await self._log_validation_check(
                        canonical_id, run_id, "CHECK_4_PARSE",
                        False, "PDF has 0 pages", start_ms,
                    )
                    return False, 0, "PDF has 0 pages"

                await self._log_validation_check(
                    canonical_id, run_id, "CHECK_4_PARSE",
                    True, f"PDF parsable, {page_count} pages", start_ms,
                )
                return True, page_count, f"{page_count} pages"

            except Exception as exc:
                await self._log_validation_check(
                    canonical_id, run_id, "CHECK_4_PARSE",
                    False, f"Parse failed: {type(exc).__name__}: {exc}", start_ms,
                )
                return False, 0, f"CORRUPTED: {exc}"

        except Exception as exc:
            await self._log_validation_check(
                canonical_id, run_id, "CHECK_4_PARSE",
                False, f"Parse failed: {type(exc).__name__}: {exc}", start_ms,
            )
            return False, 0, f"CORRUPTED: {exc}"

    async def _check5_text_extraction(
        self,
        pdf_path: str,
        canonical_id: str,
        run_id: str,
        start_ms: float,
    ) -> Tuple[str, bool]:
        """CHECK 5: Extract text from first 3 pages.

        Returns (extracted_text, has_text).
        """
        text = ""
        try:
            try:
                import fitz

                doc = fitz.open(pdf_path)
                pages_to_read = min(3, len(doc))
                for i in range(pages_to_read):
                    page_text = doc[i].get_text()
                    text += page_text + "\n"
                doc.close()

            except ImportError:
                from pypdf import PdfReader

                reader = PdfReader(pdf_path)
                pages_to_read = min(3, len(reader.pages))
                for i in range(pages_to_read):
                    page_text = reader.pages[i].extract_text() or ""
                    text += page_text + "\n"

        except Exception as exc:
            await self._log_validation_check(
                canonical_id, run_id, "CHECK_5_TEXT",
                False, f"Text extraction error: {exc}", start_ms,
            )
            return "", False

        text = text.strip()
        has_text = len(text) > 50  # More than trivial content

        await self._log_validation_check(
            canonical_id, run_id, "CHECK_5_TEXT",
            has_text,
            f"Extracted {len(text)} chars" if has_text else "No meaningful text",
            start_ms,
        )

        return text, has_text

    async def _check5b_ocr(
        self,
        pdf_path: str,
        canonical_id: str,
        run_id: str,
        start_ms: float,
    ) -> Tuple[str, bool]:
        """CHECK 5b: OCR via ocrmypdf (requires tesseract + ghostscript).

        Runs ocrmypdf on page 1, saves searchable PDF as stored copy.
        Returns (extracted_text, success).
        """
        try:
            import subprocess

            output_path = pdf_path  # Overwrite with searchable version
            ocr_timeout = self._config.get(
                "ocr_per_page_timeout_s", DEFAULT_OCR_PER_PAGE_TIMEOUT_S
            )

            process = await asyncio.create_subprocess_exec(
                "ocrmypdf",
                "--pages", "1",
                "--skip-text",
                "--force-ocr",
                "--output-type", "pdf",
                pdf_path, output_path,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await asyncio.wait_for(
                process.communicate(), timeout=ocr_timeout
            )

            if process.returncode != 0:
                await self._log_validation_check(
                    canonical_id, run_id, "CHECK_5B_OCR",
                    False,
                    f"ocrmypdf failed (exit {process.returncode}): "
                    f"{stderr.decode('utf-8', errors='replace')[:200]}",
                    start_ms,
                )
                return "", False

            # Extract text from the now-searchable PDF
            text, has_text = await self._check5_text_extraction(
                output_path, canonical_id, run_id, start_ms
            )

            await self._log_validation_check(
                canonical_id, run_id, "CHECK_5B_OCR",
                has_text,
                f"OCR extracted {len(text)} chars" if has_text else "OCR: no text",
                start_ms,
            )

            return text, has_text

        except asyncio.TimeoutError:
            await self._log_validation_check(
                canonical_id, run_id, "CHECK_5B_OCR",
                False, "OCR timed out", start_ms,
            )
            return "", False
        except Exception as exc:
            await self._log_validation_check(
                canonical_id, run_id, "CHECK_5B_OCR",
                False, f"OCR error: {type(exc).__name__}: {exc}", start_ms,
            )
            return "", False

    async def _check5b_ocr_tesseract_only(
        self,
        pdf_path: str,
        canonical_id: str,
        run_id: str,
        start_ms: float,
    ) -> Tuple[str, bool]:
        """CHECK 5b fallback: OCR with pytesseract only (no ghostscript).

        Converts first page to image, then runs tesseract.
        Returns (extracted_text, success).
        """
        try:
            try:
                import fitz

                doc = fitz.open(pdf_path)
                if len(doc) == 0:
                    doc.close()
                    return "", False

                page = doc[0]
                pix = page.get_pixmap(dpi=300)
                img_bytes = pix.tobytes("png")
                doc.close()

            except ImportError:
                # Cannot render without PyMuPDF
                await self._log_validation_check(
                    canonical_id, run_id, "CHECK_5B_TESSERACT",
                    False, "PyMuPDF not available for image rendering", start_ms,
                )
                return "", False

            # Run tesseract on the image
            try:
                from PIL import Image
                import pytesseract

                img = Image.open(io.BytesIO(img_bytes))
                text = pytesseract.image_to_string(img)
                text = text.strip()
                has_text = len(text) > 50

                await self._log_validation_check(
                    canonical_id, run_id, "CHECK_5B_TESSERACT",
                    has_text,
                    f"Tesseract extracted {len(text)} chars"
                    if has_text else "Tesseract: no text",
                    start_ms,
                )
                return text, has_text

            except ImportError:
                await self._log_validation_check(
                    canonical_id, run_id, "CHECK_5B_TESSERACT",
                    False, "pytesseract not installed", start_ms,
                )
                return "", False

        except Exception as exc:
            await self._log_validation_check(
                canonical_id, run_id, "CHECK_5B_TESSERACT",
                False, f"Tesseract error: {exc}", start_ms,
            )
            return "", False

    async def _check6_identity(
        self,
        extracted_text: str,
        paper: Paper,
        run_id: str,
        start_ms: float,
    ) -> str:
        """CHECK 6: Metadata-DOI cross-check.

        Priority:
            1. Target DOI literally present in first-3-pages text    → DOI_VERIFIED
            2. Target DOI URL form present                            → DOI_VERIFIED
            3. A different DOI present                                → VERSION_MISMATCH
            4. rapidfuzz.token_set_ratio(title, first_3_pages) ≥ 85   → TITLE_VERIFIED
            5. rapidfuzz.token_set_ratio(title, later_pages)   ≥ 85   → TITLE_VERIFIED_WEAK
            6. Otherwise                                              → CONTENT_UNVERIFIED

        Returns IdentityStatus value.
        """
        text_lower = extracted_text.lower()

        # Search for target DOI in text
        if paper.doi:
            doi_lower = paper.doi.lower()
            if doi_lower in text_lower:
                await self._log_validation_check(
                    paper.canonical_id, run_id, "CHECK_6_IDENTITY",
                    True, "DOI found in PDF text", start_ms,
                )
                return IdentityStatus.DOI_VERIFIED.value

            # Check for DOI URL form
            doi_url_form = f"doi.org/{doi_lower}"
            if doi_url_form in text_lower:
                await self._log_validation_check(
                    paper.canonical_id, run_id, "CHECK_6_IDENTITY",
                    True, "DOI URL found in PDF text", start_ms,
                )
                return IdentityStatus.DOI_VERIFIED.value

        # Check if a DIFFERENT DOI is found (VERSION_MISMATCH)
        doi_pattern = re.compile(r'10\.\d{4,9}/[^\s]+')
        found_dois = doi_pattern.findall(text_lower)
        if paper.doi and found_dois:
            # Filter out the target DOI
            other_dois = [d for d in found_dois if d != paper.doi.lower()]
            if other_dois:
                await self._log_validation_check(
                    paper.canonical_id, run_id, "CHECK_6_IDENTITY",
                    False,
                    f"Different DOI found: {other_dois[0][:50]}",
                    start_ms,
                )
                return IdentityStatus.VERSION_MISMATCH.value

        # Fuzzy title match (rapidfuzz.token_set_ratio, threshold 85)
        if paper.title:
            title_norm = normalize_title(paper.title)
            if title_norm and len(title_norm) > 10:
                # Phase 1: match against the first-3-pages text (the caller
                # passes this in via `extracted_text`).
                score_first3 = self._fuzzy_title_match(title_norm, text_lower)
                if score_first3 >= TITLE_FUZZY_THRESHOLD:
                    await self._log_validation_check(
                        paper.canonical_id, run_id, "CHECK_6_IDENTITY",
                        True,
                        f"Title fuzzy match (first 3 pages): "
                        f"{score_first3 * 100:.1f}%",
                        start_ms,
                    )
                    return IdentityStatus.TITLE_VERIFIED.value

                # Phase 2: extract later pages and retry. If the title only
                # appears in the body/references, it's likely an incidental
                # mention rather than the actual title → downgrade confidence.
                later_text = await self._extract_later_pages_text(
                    paper.pdf_path
                )
                if later_text:
                    score_later = self._fuzzy_title_match(
                        title_norm, later_text.lower()
                    )
                    if score_later >= TITLE_FUZZY_THRESHOLD:
                        await self._log(
                            canonical_id=paper.canonical_id,
                            run_id=run_id,
                            method="CHECK_6_TITLE_WEAK",
                            outcome=IdentityStatus.TITLE_VERIFIED_WEAK.value,
                            details={
                                "score_first_3_pages_pct": round(score_first3 * 100, 1),
                                "score_later_pages_pct": round(score_later * 100, 1),
                                "reason": (
                                    "Title matched only outside first 3 pages "
                                    "— likely incidental (references/body) "
                                    "rather than actual title."
                                ),
                            },
                        )
                        await self._log_validation_check(
                            paper.canonical_id, run_id, "CHECK_6_IDENTITY",
                            True,
                            f"Title fuzzy match (WEAK, later pages only): "
                            f"{score_later * 100:.1f}%",
                            start_ms,
                        )
                        return IdentityStatus.TITLE_VERIFIED_WEAK.value

        # No match at all
        await self._log_validation_check(
            paper.canonical_id, run_id, "CHECK_6_IDENTITY",
            False, "No DOI or title match found", start_ms,
        )
        return IdentityStatus.CONTENT_UNVERIFIED.value

    async def _extract_later_pages_text(
        self,
        pdf_path: Optional[str],
        skip_first: int = 3,
        max_pages: int = 50,
    ) -> str:
        """Extract text from pages AFTER the first `skip_first` pages.

        Used by Check 6 to detect titles that appear only in the body or
        references (indicating an incidental match, not the real title).
        Capped at `max_pages` pages to avoid runaway extraction on huge PDFs.
        """
        if not pdf_path or not os.path.isfile(pdf_path):
            return ""

        text = ""
        try:
            try:
                import fitz  # PyMuPDF

                doc = fitz.open(pdf_path)
                total = len(doc)
                end = min(total, skip_first + max_pages)
                for i in range(skip_first, end):
                    text += doc[i].get_text() + "\n"
                doc.close()
            except ImportError:
                from pypdf import PdfReader

                reader = PdfReader(pdf_path)
                total = len(reader.pages)
                end = min(total, skip_first + max_pages)
                for i in range(skip_first, end):
                    text += (reader.pages[i].extract_text() or "") + "\n"
        except Exception as exc:
            logger.debug(
                "Later-pages text extraction failed for %s: %s",
                pdf_path, exc,
            )
            return ""

        return text

    @staticmethod
    def _fuzzy_title_match(title_norm: str, text_lower: str) -> float:
        """Fuzzy similarity between a paper title and a body of text.

        Uses rapidfuzz.fuzz.token_set_ratio and returns a value in [0.0, 1.0]
        to preserve the original function signature (callers compare against
        TITLE_FUZZY_THRESHOLD = 0.85). token_set_ratio is robust to token
        reordering and insertions (publisher headers, footnotes), whereas
        the old token-overlap implementation over-accepted any document
        containing the title words scattered anywhere.

        If rapidfuzz is unavailable, falls back to a conservative
        SequenceMatcher-based ratio that is stricter than pure token
        overlap — we err on the side of FALSE NEGATIVE.
        """
        if not title_norm or not text_lower:
            return 0.0

        try:
            from rapidfuzz import fuzz
            # rapidfuzz returns 0–100; normalize to 0.0–1.0 for the
            # existing 0.85 threshold in TITLE_FUZZY_THRESHOLD.
            return float(fuzz.token_set_ratio(title_norm, text_lower)) / 100.0
        except ImportError:
            # Fallback: difflib SequenceMatcher over a sliding window of
            # len(title)*2 characters — still much stricter than pure
            # token overlap.
            import difflib
            title_len = len(title_norm)
            best = 0.0
            window = max(title_len * 2, 200)
            step = max(title_len, 100)
            for start in range(0, max(1, len(text_lower) - title_len), step):
                snippet = text_lower[start:start + window]
                ratio = difflib.SequenceMatcher(
                    None, title_norm, snippet
                ).ratio()
                if ratio > best:
                    best = ratio
                if best >= 1.0:
                    break
            return best

    async def _check7_content_drift(
        self,
        pdf_bytes: bytes,
        paper: Paper,
        run_id: str,
        start_ms: float,
    ) -> str:
        """CHECK 7: SHA-256 + content drift detection.

        First retrieval: store hash as baseline.
        Subsequent: compare hash, version if changed.

        Returns ContentDriftStatus value.
        """
        current_hash = hashlib.sha256(pdf_bytes).hexdigest()

        if not paper.sha256_checksum:
            # First retrieval — store as baseline
            await self._db.update_paper_fields(
                paper.canonical_id,
                sha256_checksum=current_hash,
                pdf_size_bytes=len(pdf_bytes),
            )
            await self._log_validation_check(
                paper.canonical_id, run_id, "CHECK_7_DRIFT",
                True, f"First retrieval, baseline hash: {current_hash[:16]}...",
                start_ms,
            )
            return ContentDriftStatus.FIRST_RETRIEVAL.value

        if current_hash == paper.sha256_checksum:
            await self._log_validation_check(
                paper.canonical_id, run_id, "CHECK_7_DRIFT",
                True, "Hash matches stored baseline", start_ms,
            )
            return ContentDriftStatus.CONTENT_UNCHANGED.value

        # Hash differs — content updated
        # Save as versioned file
        new_version = paper.content_drift_version + 1
        base_name = os.path.splitext(paper.pdf_path or "unknown.pdf")[0]
        versioned_path = f"{base_name}_v{new_version}.pdf"

        write_ok, write_err = await self._atomic_write_pdf(
            pdf_bytes, versioned_path
        )

        await self._db.update_paper_fields(
            paper.canonical_id,
            content_drift_status=ContentDriftStatus.CONTENT_UPDATED.value,
            content_drift_version=new_version,
        )

        await self._log_validation_check(
            paper.canonical_id, run_id, "CHECK_7_DRIFT",
            False,
            f"Content changed: old={paper.sha256_checksum[:16]}... "
            f"new={current_hash[:16]}... saved as v{new_version}",
            start_ms,
        )

        return ContentDriftStatus.CONTENT_UPDATED.value

    # -----------------------------------------------------------------------
    # Validation logging helper
    # -----------------------------------------------------------------------

    async def _log_validation_check(
        self,
        canonical_id: str,
        run_id: str,
        check_name: str,
        passed: bool,
        detail: str,
        start_ms: float,
    ) -> None:
        """Log a single validation check result."""
        elapsed_ms = (time.monotonic() * 1000) - start_ms
        await self._log(
            canonical_id=canonical_id,
            run_id=run_id,
            method=check_name,
            outcome="PASS" if passed else "FAIL",
            failure_code=None if passed else check_name,
            execution_time_ms=elapsed_ms,
            details={"detail": detail},
        )

    @staticmethod
    def _determine_validation_status(
        identity_status: str,
        ocr_applied: bool,
        content_drift: str,
    ) -> str:
        """Determine the final validation status from check results."""
        # Content drift takes precedence for flagging
        if content_drift == ContentDriftStatus.CONTENT_UPDATED.value:
            return ValidationStatus.CONTENT_UPDATED.value

        # Identity-based status
        if identity_status == IdentityStatus.DOI_VERIFIED.value:
            if ocr_applied:
                return ValidationStatus.VALID_OCR.value
            return ValidationStatus.VALID.value

        if identity_status == IdentityStatus.TITLE_VERIFIED.value:
            if ocr_applied:
                return ValidationStatus.PARTIAL_OCR.value
            return ValidationStatus.VALID_TITLE.value

        if identity_status == IdentityStatus.TITLE_VERIFIED_WEAK.value:
            # Title only found outside the first 3 pages — treat as
            # partial identity regardless of OCR. Flagged for review;
            # does not auto-complete per the VERSION_MISMATCH /
            # CONTENT_UNVERIFIED hard-constraint rule.
            return ValidationStatus.VALID_TITLE_WEAK.value

        if identity_status == IdentityStatus.VERSION_MISMATCH.value:
            return ValidationStatus.VERSION_MISMATCH.value

        # CONTENT_UNVERIFIED
        return ValidationStatus.CONTENT_UNVERIFIED.value

    # ===================================================================
    # STEP 3.5 — Version Detection
    # ===================================================================

    async def detect_version(
        self,
        paper: Paper,
        extracted_text: str,
        run_id: str,
    ) -> str:
        """Classify the PDF version type.

        Signals (priority order):
            1. Source domain → preprint servers
            2. PDF text patterns → accepted manuscript / published indicators
            3. Journal branding → presence of publisher formatting

        Returns VersionType value.
        """
        version = VersionType.UNKNOWN.value

        # Signal 1: Source domain
        retrieval_url = paper.retrieval_url or ""
        try:
            domain = urlparse(retrieval_url).hostname or ""
            domain_lower = domain.lower()

            for preprint_domain in PREPRINT_DOMAINS:
                if preprint_domain in domain_lower:
                    version = VersionType.PREPRINT.value
                    await self._log(
                        canonical_id=paper.canonical_id,
                        run_id=run_id,
                        method="version_detect",
                        outcome="VERSION_DETECTED",
                        details={
                            "version": version,
                            "signal": "source_domain",
                            "domain": domain_lower,
                        },
                    )
                    await self._db.update_paper_fields(
                        paper.canonical_id, version_type=version
                    )
                    return version
        except Exception:
            pass

        text_lower = extracted_text.lower() if extracted_text else ""

        # Signal 2: Accepted manuscript patterns
        for pattern in ACCEPTED_MANUSCRIPT_PATTERNS:
            if pattern in text_lower:
                version = VersionType.ACCEPTED_MANUSCRIPT.value
                await self._log(
                    canonical_id=paper.canonical_id,
                    run_id=run_id,
                    method="version_detect",
                    outcome="VERSION_DETECTED",
                    details={
                        "version": version,
                        "signal": "text_pattern",
                        "pattern": pattern,
                    },
                )
                await self._db.update_paper_fields(
                    paper.canonical_id, version_type=version
                )
                return version

        # Signal 3: Published version patterns
        for pattern in PUBLISHED_VERSION_PATTERNS:
            if pattern in text_lower:
                version = VersionType.PUBLISHED_VERSION.value
                await self._log(
                    canonical_id=paper.canonical_id,
                    run_id=run_id,
                    method="version_detect",
                    outcome="VERSION_DETECTED",
                    details={
                        "version": version,
                        "signal": "text_pattern",
                        "pattern": pattern,
                    },
                )
                await self._db.update_paper_fields(
                    paper.canonical_id, version_type=version
                )
                return version

        # Signal 4: Journal branding heuristics
        # If from a known publisher and has DOI, likely published
        if paper.publisher and paper.publisher != PublisherEnum.OTHER.value:
            if paper.identity_status == IdentityStatus.DOI_VERIFIED.value:
                version = VersionType.PUBLISHED_VERSION.value
                await self._log(
                    canonical_id=paper.canonical_id,
                    run_id=run_id,
                    method="version_detect",
                    outcome="VERSION_DETECTED",
                    details={
                        "version": version,
                        "signal": "publisher_doi_heuristic",
                        "publisher": paper.publisher,
                    },
                )
                await self._db.update_paper_fields(
                    paper.canonical_id, version_type=version
                )
                return version

        # Could not determine — stay UNKNOWN
        await self._log(
            canonical_id=paper.canonical_id,
            run_id=run_id,
            method="version_detect",
            outcome="VERSION_UNKNOWN",
            details={"version": version},
        )
        await self._db.update_paper_fields(
            paper.canonical_id, version_type=version
        )
        return version

    # ===================================================================
    # STEP 3.6 — Integrity Score Calculation
    # ===================================================================

    async def compute_integrity_score(
        self,
        paper: Paper,
        run_id: str,
    ) -> Tuple[int, str]:
        """Compute the integrity score (0–100) and confidence level.

        Uses source tier, version type, identity status, and supplements.

        Returns (score, confidence_level).
        """
        has_supplements = paper.supplement_count > 0

        score, confidence = calculate_integrity_score(
            source_tier=paper.retrieval_tier,
            version_type=paper.version_type,
            identity_status=paper.identity_status,
            has_supplements=has_supplements,
        )

        await self._db.update_paper_fields(
            paper.canonical_id,
            integrity_score=score,
            confidence_level=confidence,
        )

        await self._log(
            canonical_id=paper.canonical_id,
            run_id=run_id,
            method="integrity_score",
            outcome=f"SCORE_{confidence}",
            details={
                "score": score,
                "confidence": confidence,
                "source_tier": paper.retrieval_tier,
                "version_type": paper.version_type,
                "identity_status": paper.identity_status,
                "has_supplements": has_supplements,
            },
        )

        return score, confidence

    # ===================================================================
    # STEP 4 — File Naming & Collision Handling
    # ===================================================================

    async def finalize_file_storage(
        self,
        paper: Paper,
        run_id: str,
    ) -> Optional[str]:
        """Ensure the PDF is stored with its final canonical filename.

        Handles collision detection via SHA-256 comparison:
            Same hash → REUSED (skip save)
            Different hash → append _v2, _v3, ...

        Returns the final file path, or None on failure.
        """
        if not paper.pdf_path or not os.path.isfile(paper.pdf_path):
            return None

        # Generate canonical filename
        filename = generate_filename(
            paper.first_author_lastname,
            paper.year,
            paper.title,
        )
        final_path = os.path.join(self._output_dir, filename)

        # If already at the correct path, nothing to do
        current_path = paper.pdf_path
        if os.path.abspath(current_path) == os.path.abspath(final_path):
            return final_path

        # Read current file hash
        try:
            with open(current_path, "rb") as f:
                current_hash = hashlib.sha256(f.read()).hexdigest()
        except Exception as exc:
            logger.error("Cannot read PDF for finalization: %s", exc)
            return current_path

        # Check for collision
        if os.path.isfile(final_path):
            try:
                with open(final_path, "rb") as f:
                    existing_hash = hashlib.sha256(f.read()).hexdigest()

                if existing_hash == current_hash:
                    # Same file — REUSED
                    await self._log(
                        canonical_id=paper.canonical_id,
                        run_id=run_id,
                        method="file_storage",
                        outcome="REUSED",
                        failure_code=FailureCode.REUSED.value,
                        details={
                            "path": final_path,
                            "hash": current_hash[:16],
                        },
                    )
                    # Clean up duplicate if different path
                    if os.path.abspath(current_path) != os.path.abspath(final_path):
                        try:
                            os.unlink(current_path)
                        except OSError:
                            pass
                    await self._db.update_paper_fields(
                        paper.canonical_id,
                        pdf_path=final_path,
                        pdf_filename=filename,
                        sha256_checksum=current_hash,
                    )
                    return final_path
                else:
                    # Different content — version the filename
                    base, ext = os.path.splitext(final_path)
                    version = 2
                    while os.path.isfile(f"{base}_v{version}{ext}"):
                        version += 1
                    versioned_path = f"{base}_v{version}{ext}"
                    versioned_filename = os.path.basename(versioned_path)

                    try:
                        os.replace(current_path, versioned_path)
                    except OSError as exc:
                        logger.error("Failed to move to versioned path: %s", exc)
                        return current_path

                    await self._db.update_paper_fields(
                        paper.canonical_id,
                        pdf_path=versioned_path,
                        pdf_filename=versioned_filename,
                        sha256_checksum=current_hash,
                        content_drift_version=version,
                    )

                    await self._log(
                        canonical_id=paper.canonical_id,
                        run_id=run_id,
                        method="file_storage",
                        outcome="VERSIONED",
                        details={
                            "path": versioned_path,
                            "version": version,
                            "hash": current_hash[:16],
                        },
                    )
                    return versioned_path

            except Exception as exc:
                logger.error("Collision check error: %s", exc)
                return current_path
        else:
            # No collision — move to final path
            try:
                os.makedirs(os.path.dirname(final_path), exist_ok=True)
                os.replace(current_path, final_path)
            except OSError as exc:
                logger.error("Failed to move to final path: %s", exc)
                return current_path

            await self._db.update_paper_fields(
                paper.canonical_id,
                pdf_path=final_path,
                pdf_filename=filename,
                sha256_checksum=current_hash,
            )
            return final_path

    # ===================================================================
    # Full validation + finalization pipeline
    # ===================================================================

    async def run_validation_pipeline(
        self,
        paper: Paper,
        run_id: str,
    ) -> Tuple[str, str, int]:
        """Run the complete post-retrieval pipeline:

        1. PDF Validation (Step 3)
        2. Version Detection (Step 3.5)
        3. Integrity Score (Step 3.6)
        4. File Naming & Storage (Step 4)

        Returns (validation_status, identity_status, integrity_score).
        """
        # Step 3: Validate
        validation_status, identity_status = await self.validate_pdf(
            paper, run_id
        )

        # Update paper with validation results
        await self._db.update_paper_fields(
            paper.canonical_id,
            validation_status=validation_status,
            identity_status=identity_status,
        )

        # Refresh paper to get updated fields
        paper = await self._db.get_paper(paper.canonical_id) or paper

        # HARD CONSTRAINT: VERSION_MISMATCH / CONTENT_UNVERIFIED /
        # TITLE_VERIFIED_WEAK → never auto-succeed. User must override.
        if identity_status in (
            IdentityStatus.VERSION_MISMATCH.value,
            IdentityStatus.CONTENT_UNVERIFIED.value,
            IdentityStatus.TITLE_VERIFIED_WEAK.value,
        ):
            # Still continue with scoring and storage, but don't mark COMPLETE
            pass

        # Step 3.5: Version Detection
        extracted_text = ""
        if paper.pdf_path and os.path.isfile(paper.pdf_path):
            try:
                try:
                    import fitz

                    doc = fitz.open(paper.pdf_path)
                    pages_to_read = min(3, len(doc))
                    for i in range(pages_to_read):
                        extracted_text += doc[i].get_text() + "\n"
                    doc.close()
                except ImportError:
                    from pypdf import PdfReader

                    reader = PdfReader(paper.pdf_path)
                    pages_to_read = min(3, len(reader.pages))
                    for i in range(pages_to_read):
                        extracted_text += (reader.pages[i].extract_text() or "") + "\n"
            except Exception:
                pass

        version_type = await self.detect_version(paper, extracted_text, run_id)

        # Refresh after version update
        paper = await self._db.get_paper(paper.canonical_id) or paper

        # Step 3.6: Integrity Score
        score, confidence = await self.compute_integrity_score(paper, run_id)

        # Step 4: File Naming & Storage
        await self.finalize_file_storage(paper, run_id)

        return validation_status, identity_status, score
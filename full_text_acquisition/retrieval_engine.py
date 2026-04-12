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
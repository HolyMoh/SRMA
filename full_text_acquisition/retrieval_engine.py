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

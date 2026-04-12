"""
Full-Text Acquisition System — Workers

Retrieval and validation worker pools with backpressure control,
disk space safety, and graceful shutdown support. All task claiming
uses database-level atomicity (no in-memory locks).
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
import traceback
import uuid
from typing import Any, Dict, List, Optional

from full_text_acquisition.models import (
    DEFAULT_BACKPRESSURE_THRESHOLD,
    DEFAULT_MIN_DISK_SPACE_BYTES,
    DEFAULT_RETRIEVAL_CONCURRENCY,
    DEFAULT_VALIDATION_CONCURRENCY,
    MAX_RETRIEVAL_CONCURRENCY,
    MAX_VALIDATION_CONCURRENCY,
    OCR_BACKPRESSURE_WEIGHT,
    STANDARD_BACKPRESSURE_WEIGHT,
    FailureCode,
    PaperState,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

POLL_INTERVAL_S: float = 2.0
BACKPRESSURE_CHECK_INTERVAL_S: float = 5.0
DISK_CHECK_INTERVAL_PAPERS: int = 50
DISK_CHECK_INTERVAL_S: float = 60.0


def generate_worker_id(prefix: str = "w") -> str:
    """Generate a unique worker ID for task claiming."""
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


def check_disk_space(path: str = ".") -> int:
    """Return available disk space in bytes for the given path.

    Returns 0 on error or unsupported platforms.
    """
    try:
        stat = os.statvfs(path)
        return stat.f_bavail * stat.f_frsize
    except (OSError, AttributeError):
        # Windows fallback
        try:
            import shutil
            total, used, free = shutil.disk_usage(path)
            return free
        except Exception:
            return 0


def compute_weighted_backpressure(
    retrieved_count: int,
    ocr_pending_count: int,
) -> int:
    """Compute the weighted backpressure depth.

    Standard PDF = 1 slot. IMAGE_ONLY_SCAN (OCR) = 5 slots.
    """
    non_ocr = max(0, retrieved_count - ocr_pending_count)
    return (
        non_ocr * STANDARD_BACKPRESSURE_WEIGHT
        + ocr_pending_count * OCR_BACKPRESSURE_WEIGHT
    )

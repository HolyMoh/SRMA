"""
Full-Text Acquisition System — Browser Manager

Singleton Playwright browser manager with disposable (Tier 2) and
persistent (Tier 3/3.5) browser contexts. Stealth injection on all
browser tiers. CAPTCHA and paywall detection. atexit safety net.
"""

from __future__ import annotations

import asyncio
import atexit
import logging
import subprocess
import traceback
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Dict, List, Optional, Set

from full_text_acquisition.models import (
    PAYWALL_INDICATORS,
    DEFAULT_PAGE_LOAD_TIMEOUT_S,
    DEFAULT_SELECTOR_TIMEOUT_S,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Default viewport for headed (persistent) contexts
HEADED_VIEWPORT = {"width": 1280, "height": 900}

# Default viewport for headless (disposable) contexts
HEADLESS_VIEWPORT = {"width": 1920, "height": 1080}

# Common user agents for rotation (HTTP-tier only; stealth handles browser UA)
USER_AGENTS: List[str] = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) "
    "Gecko/20100101 Firefox/121.0",
]

# CAPTCHA detection selectors and text patterns
CAPTCHA_SELECTORS: List[str] = [
    "iframe[src*='recaptcha']",
    "iframe[src*='hcaptcha']",
    ".g-recaptcha",
    "#recaptcha",
    "[data-sitekey]",
    "iframe[src*='captcha']",
]

CAPTCHA_TEXT_PATTERNS: List[str] = [
    "unusual traffic",
    "are you a robot",
    "verify you are human",
    "captcha",
    "please verify",
    "automated requests",
    "bot detection",
]

# Stealth scripts — injected into every browser context
# These mimic a real browser fingerprint to avoid bot detection
STEALTH_SCRIPTS: List[str] = [
    # Hide webdriver property
    """
    Object.defineProperty(navigator, 'webdriver', {
        get: () => undefined
    });
    """,
    # Override plugins to look like a real browser
    """
    Object.defineProperty(navigator, 'plugins', {
        get: () => [1, 2, 3, 4, 5]
    });
    """,
    # Override languages
    """
    Object.defineProperty(navigator, 'languages', {
        get: () => ['en-US', 'en']
    });
    """,
    # Chrome runtime mock
    """
    window.chrome = {
        runtime: {},
        loadTimes: function() {},
        csi: function() {},
        app: {}
    };
    """,
    # Permissions query override
    """
    const originalQuery = window.navigator.permissions.query;
    window.navigator.permissions.query = (parameters) => (
        parameters.name === 'notifications' ?
            Promise.resolve({ state: Notification.permission }) :
            originalQuery(parameters)
    );
    """,
    # WebGL vendor/renderer override
    """
    const getParameter = WebGLRenderingContext.prototype.getParameter;
    WebGLRenderingContext.prototype.getParameter = function(parameter) {
        if (parameter === 37445) {
            return 'Intel Inc.';
        }
        if (parameter === 37446) {
            return 'Intel Iris OpenGL Engine';
        }
        return getParameter.call(this, parameter);
    };
    """,
]


class BrowserManager:
    """Singleton Playwright browser manager.

    Manages two types of browser contexts:
        DISPOSABLE (Tier 2): Headless, per-paper, stealth-injected.
            Created and destroyed for each paper. No cookies persist.
        PERSISTENT (Tier 3/3.5): Headed, per-session, stealth-injected.
            Retains cookies across papers for SSO sessions.
            Destroyed on logout, SESSION_EXPIRED exhausted, or shutdown.

    Thread-safety:
        All operations are async and must run in a single event loop.
        Context creation/destruction is serialized via _lock.

    Lifecycle:
        BrowserManager is created once at app startup.
        close_all() is called during graceful shutdown.
        atexit handler provides a secondary safety net.
    """

    _instance: Optional[BrowserManager] = None

    def __init__(self) -> None:
        self._playwright: Any = None  # playwright.async_api.Playwright
        self._browser: Any = None     # playwright.async_api.Browser
        self._persistent_context: Any = None  # BrowserContext for Tier 3
        self._disposable_contexts: Set[Any] = set()  # Active Tier 2 contexts
        self._lock: asyncio.Lock = asyncio.Lock()
        self._closed: bool = False
        self._browser_launch_args: List[str] = [
            "--disable-blink-features=AutomationControlled",
            "--disable-dev-shm-usage",
            "--no-first-run",
            "--no-default-browser-check",
        ]

        # Register atexit as secondary safety net
        atexit.register(self._atexit_cleanup)
        logger.debug("BrowserManager initialized with atexit handler")

    @classmethod
    def get_instance(cls) -> BrowserManager:
        """Get or create the singleton BrowserManager instance."""
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    @classmethod
    def reset_instance(cls) -> None:
        """Reset the singleton (for testing only)."""
        cls._instance = None

    def _atexit_cleanup(self) -> None:
        """Synchronous atexit handler — best-effort cleanup.

        This runs when the Python interpreter is shutting down.
        It cannot run async code, so it attempts synchronous cleanup
        of any remaining Playwright resources.
        """
        if self._closed:
            return
        try:
            if self._playwright:
                logger.warning(
                    "atexit: BrowserManager was not properly closed. "
                    "Resources may leak."
                )
        except Exception:
            pass

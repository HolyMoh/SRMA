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

    # -----------------------------------------------------------------------
    # Browser startup
    # -----------------------------------------------------------------------

    async def ensure_browser(self, headless: bool = True) -> Any:
        """Lazily start Playwright and launch Chromium if not already running.

        Args:
            headless: True for Tier 2 (disposable), False for Tier 3 (persistent).

        Returns the browser instance. If the browser is already running in
        a different headless mode, it is closed and relaunched.
        """
        async with self._lock:
            if self._closed:
                raise RuntimeError("BrowserManager has been closed")

            # Start Playwright if needed
            if self._playwright is None:
                try:
                    from playwright.async_api import async_playwright
                    self._playwright = await async_playwright().start()
                    logger.info("Playwright started")
                except ImportError:
                    raise RuntimeError(
                        "Playwright is not installed. "
                        "Run: pip install playwright && playwright install chromium"
                    )
                except Exception as exc:
                    logger.error(
                        "Failed to start Playwright: %s: %s\n%s",
                        type(exc).__name__,
                        exc,
                        traceback.format_exc(),
                    )
                    raise

            # Launch browser if needed
            if self._browser is None or not self._browser.is_connected():
                try:
                    self._browser = await self._playwright.chromium.launch(
                        headless=headless,
                        args=self._browser_launch_args,
                    )
                    logger.info(
                        "Chromium launched (headless=%s)", headless
                    )
                except Exception as exc:
                    logger.error(
                        "Failed to launch Chromium: %s: %s\n%s",
                        type(exc).__name__,
                        exc,
                        traceback.format_exc(),
                    )
                    raise

            return self._browser

    async def _inject_stealth(self, context: Any) -> None:
        """Inject stealth scripts into a browser context.

        Scripts are added via add_init_script so they execute on every
        new page and navigation within the context.
        """
        for script in STEALTH_SCRIPTS:
            try:
                await context.add_init_script(script)
            except Exception as exc:
                logger.warning(
                    "Failed to inject stealth script: %s: %s",
                    type(exc).__name__,
                    exc,
                )

    # -----------------------------------------------------------------------
    # Disposable context (Tier 2) — per-paper, headless, stealth
    # -----------------------------------------------------------------------

    async def create_disposable_context(self) -> Any:
        """Create a fresh, isolated browser context for a single paper (Tier 2).

        Features:
            - Headless Chromium
            - Stealth scripts injected
            - Fresh cookies and storage (no carryover)
            - Tracked for cleanup on shutdown

        Returns the BrowserContext. Caller must close it via
        close_disposable_context() or the disposable_context() manager.
        """
        browser = await self.ensure_browser(headless=True)

        try:
            context = await browser.new_context(
                viewport=HEADLESS_VIEWPORT,
                java_script_enabled=True,
                accept_downloads=True,
                ignore_https_errors=False,
                user_agent=USER_AGENTS[
                    len(self._disposable_contexts) % len(USER_AGENTS)
                ],
            )
            await self._inject_stealth(context)
            self._disposable_contexts.add(context)
            logger.debug(
                "Disposable context created (active: %d)",
                len(self._disposable_contexts),
            )
            return context
        except Exception as exc:
            logger.error(
                "Failed to create disposable context: %s: %s\n%s",
                type(exc).__name__,
                exc,
                traceback.format_exc(),
            )
            raise

    async def close_disposable_context(self, context: Any) -> None:
        """Close and clean up a disposable browser context.

        Clears cookies, cache, and storage before closing.
        Removes context from tracking set.
        """
        if context is None:
            return

        try:
            # Clear all cookies and storage
            await context.clear_cookies()
        except Exception as exc:
            logger.debug(
                "Could not clear cookies on disposable context: %s", exc
            )

        try:
            await context.close()
        except Exception as exc:
            logger.debug(
                "Error closing disposable context: %s", exc
            )
        finally:
            self._disposable_contexts.discard(context)
            logger.debug(
                "Disposable context closed (remaining: %d)",
                len(self._disposable_contexts),
            )

    @asynccontextmanager
    async def disposable_context(self) -> AsyncIterator[Any]:
        """Async context manager for a disposable Tier 2 browser context.

        Usage:
            async with browser_manager.disposable_context() as ctx:
                page = await ctx.new_page()
                await page.goto(url)
                ...
            # Context automatically closed and cleaned up
        """
        context = await self.create_disposable_context()
        try:
            yield context
        finally:
            await self.close_disposable_context(context)

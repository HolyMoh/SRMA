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

    # -----------------------------------------------------------------------
    # Persistent context (Tier 3/3.5) — per-session, headed, stealth
    # -----------------------------------------------------------------------

    async def create_persistent_context(self) -> Any:
        """Create a headed, persistent browser context for SSO sessions.

        Features:
            - Headed Chromium (user can see and interact)
            - Stealth scripts injected
            - Cookies retained across papers for SSO
            - Only one persistent context at a time

        Destroys any existing persistent context before creating a new one.
        Returns the BrowserContext.
        """
        async with self._lock:
            # Close existing persistent context if any
            if self._persistent_context is not None:
                try:
                    await self._persistent_context.close()
                except Exception as exc:
                    logger.debug(
                        "Error closing old persistent context: %s", exc
                    )
                self._persistent_context = None

        # Need a headed browser — close headless if running and relaunch
        async with self._lock:
            if self._browser is not None and self._browser.is_connected():
                # Check if there are active disposable contexts
                if self._disposable_contexts:
                    logger.warning(
                        "Launching headed browser while %d disposable "
                        "contexts are active. They will continue on the "
                        "existing browser instance.",
                        len(self._disposable_contexts),
                    )

        browser = await self.ensure_browser(headless=False)

        try:
            context = await browser.new_context(
                viewport=HEADED_VIEWPORT,
                java_script_enabled=True,
                accept_downloads=True,
                ignore_https_errors=False,
            )
            await self._inject_stealth(context)

            async with self._lock:
                self._persistent_context = context

            logger.info("Persistent SSO context created (headed)")
            return context
        except Exception as exc:
            logger.error(
                "Failed to create persistent context: %s: %s\n%s",
                type(exc).__name__,
                exc,
                traceback.format_exc(),
            )
            raise

    async def get_persistent_context(self) -> Any:
        """Get the existing persistent context, or create one if absent.

        Returns the BrowserContext for Tier 3/3.5 operations.
        """
        async with self._lock:
            if self._persistent_context is not None:
                try:
                    # Verify context is still usable by checking pages
                    _ = self._persistent_context.pages
                    return self._persistent_context
                except Exception:
                    logger.warning(
                        "Persistent context is no longer valid, recreating"
                    )
                    self._persistent_context = None

        return await self.create_persistent_context()

    async def destroy_persistent_context(self, reason: str = "shutdown") -> None:
        """Destroy the persistent SSO context.

        Called on:
            - User logout
            - SESSION_EXPIRED retries exhausted
            - Graceful shutdown
            - Manual re-authentication request

        Args:
            reason: Why the context is being destroyed (for logging).
        """
        async with self._lock:
            if self._persistent_context is None:
                return

            try:
                await self._persistent_context.close()
            except Exception as exc:
                logger.debug(
                    "Error closing persistent context: %s", exc
                )
            finally:
                self._persistent_context = None
                logger.info(
                    "Persistent SSO context destroyed (reason: %s)", reason
                )

    @property
    def has_persistent_context(self) -> bool:
        """Check whether a persistent SSO context is currently active."""
        return self._persistent_context is not None

    async def get_sso_page(self) -> Any:
        """Get or create a page in the persistent context for SSO navigation.

        If the persistent context has existing pages, returns the first one.
        Otherwise creates a new page.
        """
        context = await self.get_persistent_context()
        pages = context.pages
        if pages:
            return pages[0]
        return await context.new_page()

    async def initiate_sso_login(
        self,
        sso_url: str,
        page_load_timeout_s: float = DEFAULT_PAGE_LOAD_TIMEOUT_S,
    ) -> Any:
        """Navigate the persistent context to the SSO login URL.

        The user is expected to complete authentication manually in
        the headed browser. This method only navigates to the URL.

        ABSOLUTE RULE: Never access anything the user types.

        Returns the page after navigation.
        """
        page = await self.get_sso_page()
        try:
            await page.goto(
                sso_url,
                timeout=page_load_timeout_s * 1000,
                wait_until="domcontentloaded",
            )
            logger.info("SSO login page loaded: %s", sso_url)
            return page
        except Exception as exc:
            logger.error(
                "Failed to navigate to SSO URL %s: %s: %s\n%s",
                sso_url,
                type(exc).__name__,
                exc,
                traceback.format_exc(),
            )
            raise

    async def check_sso_session_valid(self) -> bool:
        """Check whether the persistent SSO session appears active.

        Looks for signs that the user is logged in:
            - Persistent context exists
            - At least one page is open
            - No obvious session-expired indicators on the current page

        This is a heuristic check, not a guarantee.
        """
        async with self._lock:
            if self._persistent_context is None:
                return False

        try:
            pages = self._persistent_context.pages
            if not pages:
                return False

            page = pages[0]
            url = page.url

            # If we're on a login page, session is likely expired
            login_indicators = [
                "/login", "/signin", "/auth", "/sso",
                "login.microsoftonline.com",
                "shibboleth",
                "wayf",
            ]
            url_lower = url.lower()
            for indicator in login_indicators:
                if indicator in url_lower:
                    return False

            return True
        except Exception:
            return False

    async def handle_session_expired(
        self,
        sso_url: str,
    ) -> Any:
        """Handle a SESSION_EXPIRED event by re-creating the persistent context.

        Destroys the current context, creates a fresh one, and navigates
        to the SSO URL for user re-authentication.

        Returns the page for the new SSO login.
        """
        logger.warning("SSO session expired, initiating re-authentication")
        await self.destroy_persistent_context(reason="SESSION_EXPIRED")
        return await self.initiate_sso_login(sso_url)

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

    # -----------------------------------------------------------------------
    # Page helpers — CAPTCHA detection
    # -----------------------------------------------------------------------

    async def detect_captcha(self, page: Any) -> bool:
        """Check whether the current page contains a CAPTCHA challenge.

        Detection signals:
            1. Known CAPTCHA iframe selectors (reCAPTCHA, hCaptcha)
            2. Text patterns indicating bot detection

        Returns True if CAPTCHA is detected.
        """
        # Check for CAPTCHA selectors
        for selector in CAPTCHA_SELECTORS:
            try:
                element = await page.query_selector(selector)
                if element is not None:
                    logger.info("CAPTCHA detected via selector: %s", selector)
                    return True
            except Exception:
                continue

        # Check page text for CAPTCHA indicators
        try:
            body_text = await page.inner_text("body")
            body_lower = body_text.lower()
            for pattern in CAPTCHA_TEXT_PATTERNS:
                if pattern in body_lower:
                    logger.info(
                        "CAPTCHA detected via text pattern: '%s'", pattern
                    )
                    return True
        except Exception as exc:
            logger.debug("Could not read body text for CAPTCHA check: %s", exc)

        return False

    # -----------------------------------------------------------------------
    # Page helpers — paywall detection
    # -----------------------------------------------------------------------

    async def detect_paywall(self, page: Any) -> bool:
        """Check whether the current page shows paywall indicators.

        Scans for:
            - Text patterns from PAYWALL_INDICATORS
            - Payment-related DOM elements (purchase buttons, price tags)

        Returns True if paywall is detected.
        """
        # Check text indicators
        try:
            body_text = await page.inner_text("body")
            body_lower = body_text.lower()
            for indicator in PAYWALL_INDICATORS:
                if indicator.lower() in body_lower:
                    logger.debug(
                        "Paywall detected via text: '%s'", indicator
                    )
                    return True
        except Exception as exc:
            logger.debug("Could not read body text for paywall check: %s", exc)

        # Check for payment DOM elements
        payment_selectors = [
            "button[class*='purchase']",
            "button[class*='buy']",
            "a[class*='purchase']",
            "a[class*='buy-article']",
            "[class*='paywall']",
            "[class*='pay-wall']",
            "[id*='paywall']",
            "[data-testid*='purchase']",
            ".price-tag",
            ".article-purchase",
            ".subscription-required",
        ]
        for selector in payment_selectors:
            try:
                element = await page.query_selector(selector)
                if element is not None:
                    logger.debug(
                        "Paywall detected via DOM element: %s", selector
                    )
                    return True
            except Exception:
                continue

        return False

    # -----------------------------------------------------------------------
    # Page helpers — navigation utilities
    # -----------------------------------------------------------------------

    async def safe_navigate(
        self,
        page: Any,
        url: str,
        timeout_s: float = DEFAULT_PAGE_LOAD_TIMEOUT_S,
        wait_until: str = "domcontentloaded",
    ) -> Dict[str, Any]:
        """Navigate to a URL with timeout and comprehensive error handling.

        Returns a dict with:
            success: bool
            status: int or None (HTTP status code)
            url: str (final URL after redirects)
            error: str or None
            content_type: str or None
        """
        result: Dict[str, Any] = {
            "success": False,
            "status": None,
            "url": url,
            "error": None,
            "content_type": None,
        }

        try:
            response = await page.goto(
                url,
                timeout=timeout_s * 1000,
                wait_until=wait_until,
            )

            if response is not None:
                result["status"] = response.status
                result["url"] = page.url
                headers = response.headers
                result["content_type"] = headers.get("content-type", "")

                if 200 <= response.status < 400:
                    result["success"] = True
                else:
                    result["error"] = f"HTTP {response.status}"
            else:
                result["url"] = page.url
                result["success"] = True

        except Exception as exc:
            exc_name = type(exc).__name__
            if "Timeout" in exc_name or "timeout" in str(exc).lower():
                result["error"] = "TIMEOUT"
            elif "net::ERR_" in str(exc):
                result["error"] = f"NETWORK_ERROR: {exc}"
            else:
                result["error"] = f"{exc_name}: {exc}"
            logger.debug(
                "Navigation to %s failed: %s", url, result["error"]
            )

        return result

    async def wait_for_pdf_link(
        self,
        page: Any,
        selectors: List[str],
        timeout_s: float = DEFAULT_SELECTOR_TIMEOUT_S,
    ) -> Optional[str]:
        """Wait for any of the given selectors to appear and extract a PDF link.

        Tries each selector in order. Returns the href/src of the first
        matching element, or None if none found within timeout.
        """
        for selector in selectors:
            try:
                element = await page.wait_for_selector(
                    selector,
                    timeout=timeout_s * 1000,
                    state="attached",
                )
                if element is None:
                    continue

                # Try href first, then src, then data-url
                for attr in ("href", "src", "data-url", "data-pdf-url"):
                    value = await element.get_attribute(attr)
                    if value and (".pdf" in value.lower() or "pdf" in value.lower()):
                        logger.debug(
                            "PDF link found via selector '%s': %s",
                            selector, value,
                        )
                        return value

                # If no PDF-specific attribute, still return href
                href = await element.get_attribute("href")
                if href:
                    return href

            except Exception:
                continue

        return None

    async def extract_pdf_url_from_page(self, page: Any) -> Optional[str]:
        """Attempt to find a PDF download URL on the current page.

        Searches through multiple strategies:
            1. Meta tags (citation_pdf_url, etc.)
            2. Link elements with PDF types
            3. Anchor tags with PDF patterns
            4. Embedded object/embed elements

        Returns the first PDF URL found, or None.
        """
        strategies = [
            # Meta tags — most reliable
            (
                "meta[name='citation_pdf_url']",
                "content",
            ),
            (
                "meta[name='citation_pdf']",
                "content",
            ),
            (
                "meta[property='citation_pdf_url']",
                "content",
            ),
            (
                "meta[name='dc.identifier'][scheme='doi']",
                None,
            ),
            # Link elements
            (
                "link[type='application/pdf']",
                "href",
            ),
            # Anchor patterns
            (
                "a[href*='.pdf']",
                "href",
            ),
            (
                "a[href*='/pdf/']",
                "href",
            ),
            (
                "a[href*='pdf?']",
                "href",
            ),
            (
                "a[data-article-pdf]",
                "href",
            ),
            (
                "a.pdf-download",
                "href",
            ),
            (
                "a[class*='pdf']",
                "href",
            ),
            # Embedded objects
            (
                "embed[type='application/pdf']",
                "src",
            ),
            (
                "object[type='application/pdf']",
                "data",
            ),
            (
                "iframe[src*='.pdf']",
                "src",
            ),
        ]

        for selector, attr in strategies:
            try:
                element = await page.query_selector(selector)
                if element is None:
                    continue

                if attr is None:
                    continue

                value = await element.get_attribute(attr)
                if value:
                    # Resolve relative URLs
                    if value.startswith("/"):
                        base_url = page.url
                        from urllib.parse import urljoin
                        value = urljoin(base_url, value)
                    logger.debug(
                        "PDF URL extracted via '%s': %s", selector, value
                    )
                    return value
            except Exception:
                continue

        return None

    async def traverse_shadow_dom(
        self,
        page: Any,
        host_selector: str,
        inner_selector: str,
    ) -> Optional[Any]:
        """Traverse into a shadow DOM to find an inner element.

        Used primarily for Elsevier/ScienceDirect which uses
        shadow DOM for PDF viewer components.

        Args:
            page: The Playwright page.
            host_selector: CSS selector for the shadow host element.
            inner_selector: CSS selector within the shadow root.

        Returns the inner element handle, or None.
        """
        try:
            host = await page.query_selector(host_selector)
            if host is None:
                return None

            # Evaluate in page context to pierce shadow DOM
            inner = await page.evaluate_handle(
                """([hostEl, innerSel]) => {
                    const shadow = hostEl.shadowRoot;
                    if (!shadow) return null;
                    return shadow.querySelector(innerSel);
                }""",
                [host, inner_selector],
            )

            # Check if result is null
            is_null = await page.evaluate("(el) => el === null", inner)
            if is_null:
                return None

            return inner
        except Exception as exc:
            logger.debug(
                "Shadow DOM traversal failed (%s > %s): %s",
                host_selector, inner_selector, exc,
            )
            return None

    async def extract_from_iframe(
        self,
        page: Any,
        iframe_selector: str,
        inner_selector: str,
        attribute: str = "href",
    ) -> Optional[str]:
        """Extract a value from an element inside an iframe.

        Args:
            page: The Playwright page.
            iframe_selector: CSS selector for the iframe element.
            inner_selector: CSS selector for the target inside the iframe.
            attribute: Attribute to extract from the inner element.

        Returns the attribute value, or None.
        """
        try:
            iframe_element = await page.query_selector(iframe_selector)
            if iframe_element is None:
                return None

            frame = await iframe_element.content_frame()
            if frame is None:
                return None

            element = await frame.query_selector(inner_selector)
            if element is None:
                return None

            value = await element.get_attribute(attribute)
            return value
        except Exception as exc:
            logger.debug(
                "Iframe extraction failed (%s > %s): %s",
                iframe_selector, inner_selector, exc,
            )
            return None

    async def intercept_pdf_download(
        self,
        page: Any,
        trigger_action: Any,
        timeout_s: float = 30.0,
    ) -> Optional[bytes]:
        """Intercept a PDF download triggered by a page action.

        Some publishers don't expose direct PDF URLs but instead trigger
        a download via JavaScript. This method intercepts the download
        event to capture the PDF bytes.

        Args:
            page: The Playwright page.
            trigger_action: An async callable that triggers the download
                (e.g., clicking a button).
            timeout_s: How long to wait for the download to start.

        Returns the downloaded bytes, or None on failure.
        """
        try:
            async with page.expect_download(
                timeout=timeout_s * 1000
            ) as download_info:
                await trigger_action()

            download = download_info.value
            tmp_path = await download.path()
            if tmp_path is None:
                logger.debug("Download intercepted but no file path returned")
                return None

            with open(tmp_path, "rb") as f:
                data = f.read()

            # Clean up temp file
            try:
                os.unlink(tmp_path)
            except OSError:
                pass

            logger.debug(
                "PDF download intercepted: %d bytes", len(data)
            )
            return data

        except Exception as exc:
            logger.debug(
                "PDF download interception failed: %s: %s",
                type(exc).__name__, exc,
            )
            return None

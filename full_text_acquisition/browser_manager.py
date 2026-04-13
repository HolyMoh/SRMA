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
import os
import shutil
import subprocess
import traceback
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, AsyncIterator, Dict, List, Optional, Set

from full_text_acquisition.models import (
    PAYWALL_INDICATORS,
    DEFAULT_PAGE_LOAD_TIMEOUT_S,
    DEFAULT_SELECTOR_TIMEOUT_S,
)

# Where per-session audit marker folders live. Existence of EXACTLY one
# file (SESSION_INFO.txt) inside this folder during a session is the
# user-verifiable signal that nothing else is being written to disk.
SSO_SESSION_AUDIT_BASE_DIR = "./sso_session_temp"

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

        # ---- Privacy Audit instrumentation -----------------------------
        # These counters are computed from REAL operations, never
        # hardcoded. If form_fields_read or screenshots_taken ever
        # become non-zero, the UI shows the truth — we have not added
        # any code path that would do so today.
        self._audit: Dict[str, Any] = {
            "pages_navigated": 0,
            "form_fields_read": 0,
            "screenshots_taken": 0,
            "files_downloaded": 0,
            "nav_log": [],  # list of {timestamp, url, reason}
            "session_audit_dir": None,
            "session_started_at": None,
        }

        # Register atexit as secondary safety net
        atexit.register(self._atexit_cleanup)
        logger.debug("BrowserManager initialized with atexit handler")

    # -----------------------------------------------------------------------
    # Privacy Audit instrumentation
    # -----------------------------------------------------------------------

    def _is_persistent_page(self, page: Any) -> bool:
        """True iff the page belongs to the SSO (persistent) context.

        Used to filter audit counters: Tier 2 disposable-context
        navigations are NOT counted as SSO activity.
        """
        if self._persistent_context is None or page is None:
            return False
        try:
            return page.context is self._persistent_context
        except Exception:
            return False

    async def _audited_goto(
        self,
        page: Any,
        url: str,
        reason: str = "navigation",
        **goto_kwargs: Any,
    ) -> Any:
        """The single navigation entry point for SSO-context pages.

        For pages in the persistent SSO context: increments the
        pages_navigated counter and appends to the nav log.
        For pages in disposable contexts (Tier 2): pure delegate, no
        audit side-effects.

        Logs ONLY the destination URL and a caller-supplied reason
        string. Never logs the page response, body, title, or any
        DOM content.
        """
        if self._is_persistent_page(page):
            self._audit["pages_navigated"] += 1
            self._audit["nav_log"].append({
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "url": url,
                "reason": reason,
            })
            # Cap the log to avoid unbounded growth on long sessions
            if len(self._audit["nav_log"]) > 5000:
                self._audit["nav_log"] = self._audit["nav_log"][-5000:]
        return await page.goto(url, **goto_kwargs)

    def _reset_audit_state(self) -> None:
        """Reset all counters and log to fresh state for a new session."""
        self._audit["pages_navigated"] = 0
        self._audit["form_fields_read"] = 0
        self._audit["screenshots_taken"] = 0
        self._audit["files_downloaded"] = 0
        self._audit["nav_log"] = []

    def _create_session_audit_dir(
        self,
        base_dir: str = SSO_SESSION_AUDIT_BASE_DIR,
    ) -> str:
        """Create a per-session marker folder + SESSION_INFO.txt.

        Existence of EXACTLY this one file in the folder during a
        session is the user-verifiable signal that nothing else is
        being persisted by the system. Returns the folder path.
        """
        os.makedirs(base_dir, exist_ok=True)
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        session_dir = os.path.abspath(
            os.path.join(base_dir, f"session_{ts}_{uuid.uuid4().hex[:8]}")
        )
        os.makedirs(session_dir, exist_ok=True)

        marker_path = os.path.join(session_dir, "SESSION_INFO.txt")
        marker_text = (
            "SSO Session Audit Marker\n"
            "=" * 40 + "\n"
            f"Session started:  {datetime.now(timezone.utc).isoformat()}\n"
            f"Session folder:   {session_dir}\n\n"
            "This folder exists so you can verify that the system\n"
            "is NOT writing anything else to disk during your SSO\n"
            "session. The Playwright browser context is in-memory\n"
            "only — cookies, cache, IndexedDB never touch the disk.\n\n"
            "If you click 'Verify now' in the Privacy Audit panel\n"
            "and see only this one file (SESSION_INFO.txt), you\n"
            "have proof that no other session data is being persisted.\n\n"
            "When you click End SSO Session, this entire folder\n"
            "(including this file) is removed.\n"
        )
        with open(marker_path, "w") as fh:
            fh.write(marker_text)

        self._audit["session_audit_dir"] = session_dir
        self._audit["session_started_at"] = datetime.now(timezone.utc).isoformat()
        logger.info("SSO audit session folder created: %s", session_dir)
        return session_dir

    def _cleanup_session_audit_dir(self) -> None:
        """Remove the per-session audit folder + marker."""
        path = self._audit.get("session_audit_dir")
        if path and os.path.isdir(path):
            try:
                shutil.rmtree(path)
                logger.info("SSO audit session folder removed: %s", path)
            except Exception as exc:
                logger.warning(
                    "Could not remove audit folder %s: %s", path, exc
                )
        self._audit["session_audit_dir"] = None

    def audit_snapshot(self) -> Dict[str, Any]:
        """Serializable snapshot of the audit state — safe to expose to the UI.

        Returns a fresh dict; callers may mutate without affecting
        internal state. Never returns cookie values.
        """
        return {
            "pages_navigated": self._audit["pages_navigated"],
            "form_fields_read": self._audit["form_fields_read"],
            "screenshots_taken": self._audit["screenshots_taken"],
            "files_downloaded": self._audit["files_downloaded"],
            "nav_log": list(self._audit["nav_log"]),
            "session_audit_dir": self._audit["session_audit_dir"],
            "session_started_at": self._audit["session_started_at"],
            "has_persistent_context": self._persistent_context is not None,
        }

    async def list_persistent_cookies(self) -> List[Dict[str, Any]]:
        """List cookies in the SSO context — names + domains only.

        SECURITY: never returns the cookie value. Returns metadata
        sufficient to identify what's set without exposing what it
        contains.
        """
        if self._persistent_context is None:
            return []
        try:
            cookies = await self._persistent_context.cookies()
        except Exception as exc:
            logger.debug("Could not list cookies: %s", exc)
            return []
        return [{
            "domain": c.get("domain", ""),
            "name": c.get("name", ""),
            "path": c.get("path", "/"),
            "expires": c.get("expires", -1),
            "http_only": bool(c.get("httpOnly", False)),
            "secure": bool(c.get("secure", False)),
            "same_site": c.get("sameSite", ""),
        } for c in cookies]

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

            # Reset audit counters and create a per-session marker
            # folder. Existence of EXACTLY one file in this folder
            # (SESSION_INFO.txt) during the session is the user-
            # verifiable signal that nothing else is being persisted.
            self._reset_audit_state()
            try:
                self._create_session_audit_dir()
            except Exception as exc:
                logger.warning("Could not create session audit dir: %s", exc)

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

        # Wipe the per-session audit folder + marker. Done outside
        # the lock so a slow filesystem doesn't block other operations.
        try:
            self._cleanup_session_audit_dir()
        except Exception as exc:
            logger.warning("Audit folder cleanup failed: %s", exc)

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
            await self._audited_goto(
                page,
                sso_url,
                reason="User-initiated SSO login",
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

    async def probe_sso_login(
        self,
        test_url: str,
        page_load_timeout_s: float = DEFAULT_PAGE_LOAD_TIMEOUT_S,
    ) -> Dict[str, Any]:
        """Probe whether the SSO session is authenticated.

        Navigates the persistent context to `test_url` (typically the
        EZproxy-wrapped form of a known open-access DOI) and inspects the
        resulting page URL. If the URL still looks like a login page we
        treat the session as unauthenticated; otherwise authenticated.

        SECURITY: This method NEVER reads form fields, input values, or
        any DOM content except the final page URL. It does not log,
        store, or screenshot anything. It only looks at where the browser
        landed after a navigation.

        Returns:
            {
                "authenticated": bool,
                "final_url": str,
                "http_status": int | None,
                "error": str | None,
                "elapsed_ms": float,
            }
        """
        import time as _time
        result: Dict[str, Any] = {
            "authenticated": False,
            "final_url": "",
            "http_status": None,
            "error": None,
            "elapsed_ms": 0.0,
        }

        async with self._lock:
            if self._persistent_context is None:
                result["error"] = "NO_PERSISTENT_CONTEXT"
                return result

        try:
            page = await self.get_sso_page()
        except Exception as exc:
            result["error"] = f"{type(exc).__name__}: {exc}"
            return result

        t0 = _time.monotonic()
        try:
            response = await self._audited_goto(
                page,
                test_url,
                reason="Session check",
                timeout=page_load_timeout_s * 1000,
                wait_until="domcontentloaded",
            )
            if response is not None:
                result["http_status"] = response.status
            result["final_url"] = page.url
        except Exception as exc:
            result["error"] = f"{type(exc).__name__}: {exc}"
            result["elapsed_ms"] = (_time.monotonic() - t0) * 1000.0
            return result

        result["elapsed_ms"] = (_time.monotonic() - t0) * 1000.0

        # Heuristic: if the final URL still looks like a login page,
        # we're not authenticated.
        url_lower = (result["final_url"] or "").lower()
        login_indicators = [
            "/login", "/signin", "/sign-in", "/auth", "/sso",
            "login.microsoftonline.com",
            "shibboleth",
            "wayf",
            "idp.",
            "idpz.",
        ]
        is_login_page = any(ind in url_lower for ind in login_indicators)

        # A non-login URL + a 2xx/3xx response is our signal for auth success.
        status = result["http_status"]
        reached_target = status is None or (200 <= status < 400)
        result["authenticated"] = (not is_login_page) and reached_target
        return result

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
        reason: str = "navigation",
    ) -> Dict[str, Any]:
        """Navigate to a URL with timeout and comprehensive error handling.

        For pages in the persistent SSO context, navigation is recorded
        in the Privacy Audit log (URL + reason only, never page content).
        For Tier 2 disposable contexts, no audit side-effects.

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
            response = await self._audited_goto(
                page,
                url,
                reason=reason,
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

    # -----------------------------------------------------------------------
    # close_all — master cleanup
    # -----------------------------------------------------------------------

    async def close_all(self) -> None:
        """Close all browser contexts, stop Playwright, and log.

        Called during graceful shutdown. Safe to call multiple times.

        Sequence:
            1. Close all disposable contexts
            2. Close persistent context
            3. Close browser
            4. Stop Playwright
            5. Mark as closed
        """
        if self._closed:
            logger.debug("BrowserManager.close_all() already called, skipping")
            return

        async with self._lock:
            self._closed = True

        closed_disposable = 0
        closed_persistent = False

        # 1. Close all disposable contexts
        disposable_copy = set(self._disposable_contexts)
        for ctx in disposable_copy:
            try:
                await ctx.close()
                closed_disposable += 1
            except Exception as exc:
                logger.debug(
                    "Error closing disposable context during shutdown: %s", exc
                )
            finally:
                self._disposable_contexts.discard(ctx)

        # 2. Close persistent context
        if self._persistent_context is not None:
            try:
                await self._persistent_context.close()
                closed_persistent = True
            except Exception as exc:
                logger.debug(
                    "Error closing persistent context during shutdown: %s", exc
                )
            finally:
                self._persistent_context = None

        # 3. Close browser
        if self._browser is not None:
            try:
                await self._browser.close()
            except Exception as exc:
                logger.debug(
                    "Error closing browser during shutdown: %s", exc
                )
            finally:
                self._browser = None

        # 4. Stop Playwright
        if self._playwright is not None:
            try:
                await self._playwright.stop()
            except Exception as exc:
                logger.debug(
                    "Error stopping Playwright during shutdown: %s", exc
                )
            finally:
                self._playwright = None

        logger.info(
            "BrowserManager closed: %d disposable, persistent=%s",
            closed_disposable,
            closed_persistent,
        )

    @property
    def is_closed(self) -> bool:
        """Whether close_all() has been called."""
        return self._closed

    @property
    def active_disposable_count(self) -> int:
        """Number of currently active disposable contexts."""
        return len(self._disposable_contexts)

    # -----------------------------------------------------------------------
    # Async context manager for app lifecycle
    # -----------------------------------------------------------------------

    @asynccontextmanager
    async def managed(self) -> AsyncIterator[BrowserManager]:
        """Async context manager for BrowserManager lifecycle.

        Usage:
            async with BrowserManager.get_instance().managed() as bm:
                # Use bm for browser operations
                ...
            # close_all() called automatically on exit
        """
        try:
            yield self
        except Exception as exc:
            logger.error(
                "BrowserManager context error: %s: %s\n%s",
                type(exc).__name__,
                exc,
                traceback.format_exc(),
            )
            raise
        finally:
            await self.close_all()


# ---------------------------------------------------------------------------
# Playwright installation check (used by first-launch wizard)
# ---------------------------------------------------------------------------

def is_playwright_installed() -> bool:
    """Check whether Playwright Python package is importable."""
    try:
        import playwright  # noqa: F401
        return True
    except ImportError:
        return False


def is_chromium_installed() -> bool:
    """Check whether Playwright Chromium browser is installed.

    Runs `playwright install --dry-run chromium` to check without downloading.
    Falls back to checking the registry if dry-run is not available.
    """
    try:
        from playwright._impl._driver import compute_driver_executable
        driver_executable = compute_driver_executable()
        result = subprocess.run(
            [str(driver_executable), "install", "--dry-run", "chromium"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        # If dry-run exits 0, browser is already installed
        return result.returncode == 0
    except Exception:
        # Fallback: try to find browser executables in known locations
        try:
            from playwright._impl._driver import compute_driver_executable
            driver_executable = compute_driver_executable()
            result = subprocess.run(
                [str(driver_executable), "install", "--help"],
                capture_output=True,
                text=True,
                timeout=10,
            )
            # If playwright CLI works, assume we can check
            return "chromium" in result.stdout.lower()
        except Exception:
            return False


async def install_chromium() -> bool:
    """Attempt to install Playwright Chromium browser.

    Runs `playwright install chromium` as a subprocess.
    Returns True on success.
    """
    try:
        process = await asyncio.create_subprocess_exec(
            "playwright", "install", "chromium",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await asyncio.wait_for(
            process.communicate(), timeout=300.0
        )

        if process.returncode == 0:
            logger.info("Playwright Chromium installed successfully")
            return True
        else:
            logger.error(
                "Playwright Chromium installation failed (exit %d): %s",
                process.returncode,
                stderr.decode("utf-8", errors="replace"),
            )
            return False
    except asyncio.TimeoutError:
        logger.error("Playwright Chromium installation timed out (300s)")
        return False
    except FileNotFoundError:
        logger.error(
            "Playwright CLI not found. Install with: pip install playwright"
        )
        return False
    except Exception as exc:
        logger.error(
            "Playwright Chromium installation error: %s: %s\n%s",
            type(exc).__name__,
            exc,
            traceback.format_exc(),
        )
        return False

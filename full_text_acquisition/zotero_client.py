"""
Full-Text Acquisition System — Zotero Web API v3 client

Thin async wrapper around the subset of the Zotero API we need for
the Part 2 integration:

    - Create / find a collection by name
    - Push items (DOIs) into that collection in batches of 50
    - List items in the collection (filtering by modification version)
    - List child attachments for an item + pull PDF bytes

All calls go through the shared RateLimiter (host="zotero", 5 req/s).
The client is stateless other than its constructor args (API key,
user ID). Instantiate per-operation; don't singleton.

DESIGN NOTES
------------
- User libraries only in v1. Group libraries work with the same API
  paths substituting /groups/{id}/ for /users/{id}/, and could be
  added by extending this class.
- We read Last-Modified-Version on GET to support incremental polling
  (only fetching items that changed). Write responses update the same
  version counter; we don't enforce If-Unmodified-Since-Version on
  writes because we only push new items and never update existing ones.
- Zotero returns a 429 with Retry-After and Backoff headers under
  load. The rate limiter caps us at 5/s which should keep us well
  below any such threshold, but we respect Retry-After defensively.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict, List, Optional, Tuple

import httpx

from full_text_acquisition.rate_limiter import RateLimiter

logger = logging.getLogger(__name__)

ZOTERO_API_BASE = "https://api.zotero.org"


class ZoteroError(Exception):
    """Raised when the Zotero API returns a non-retryable error."""

    def __init__(self, message: str, status_code: Optional[int] = None):
        super().__init__(message)
        self.status_code = status_code


class ZoteroClient:
    """Async Zotero Web API v3 client for personal libraries."""

    def __init__(
        self,
        api_key: str,
        user_id: str,
        client: Optional[httpx.AsyncClient] = None,
    ) -> None:
        if not api_key or not api_key.strip():
            raise ValueError("Zotero API key is required")
        if not user_id or not str(user_id).strip():
            raise ValueError("Zotero user ID is required")
        self._api_key = api_key.strip()
        self._user_id = str(user_id).strip()
        self._external_client = client is not None
        self._client = client or httpx.AsyncClient(timeout=30.0)
        self._limiter = RateLimiter.get_instance()

    async def close(self) -> None:
        if not self._external_client:
            await self._client.aclose()

    @property
    def library_prefix(self) -> str:
        return f"/users/{self._user_id}"

    def _headers(self, extra: Optional[Dict[str, str]] = None) -> Dict[str, str]:
        h = {
            "Zotero-API-Version": "3",
            "Zotero-API-Key": self._api_key,
            "Accept": "application/json",
        }
        if extra:
            h.update(extra)
        return h

    async def _request(
        self,
        method: str,
        path: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        json_body: Optional[Any] = None,
        extra_headers: Optional[Dict[str, str]] = None,
        max_retries: int = 3,
    ) -> httpx.Response:
        """Rate-limited request with Retry-After / Backoff handling."""
        url = f"{ZOTERO_API_BASE}{path}"
        attempt = 0
        while True:
            async with self._limiter.acquire("zotero") as _delay:
                headers = self._headers(extra_headers)
                resp = await self._client.request(
                    method, url,
                    params=params,
                    headers=headers,
                    json=json_body,
                )

            # Retryable: 429 Too Many Requests, 503 Service Unavailable,
            # and an explicit Backoff header (Zotero custom).
            retry_after = resp.headers.get("Retry-After")
            backoff = resp.headers.get("Backoff")
            should_retry = resp.status_code in (429, 503) or backoff is not None

            if should_retry and attempt < max_retries:
                attempt += 1
                delay = 1.0
                if retry_after:
                    try: delay = max(delay, float(retry_after))
                    except ValueError: pass
                if backoff:
                    try: delay = max(delay, float(backoff))
                    except ValueError: pass
                logger.info(
                    "Zotero %s %s: %d — backoff %.1fs (attempt %d/%d)",
                    method, path, resp.status_code, delay, attempt, max_retries,
                )
                await asyncio.sleep(delay)
                continue

            return resp

    # -----------------------------------------------------------------------
    # Verification
    # -----------------------------------------------------------------------

    async def verify(self) -> Dict[str, Any]:
        """Confirm the API key + user ID pair works.

        Returns the Zotero /keys/{key} response, which includes the
        username, access scopes, and library permissions.
        """
        resp = await self._request("GET", f"/keys/{self._api_key}")
        if resp.status_code == 200:
            data = resp.json()
            return {"ok": True, "data": data}
        if resp.status_code == 403:
            return {"ok": False, "error": "Invalid API key", "status": 403}
        if resp.status_code == 404:
            return {"ok": False, "error": "Key not found", "status": 404}
        return {
            "ok": False,
            "error": f"HTTP {resp.status_code}: {resp.text[:200]}",
            "status": resp.status_code,
        }

    # -----------------------------------------------------------------------
    # Collections
    # -----------------------------------------------------------------------

    async def find_collection_by_name(self, name: str) -> Optional[Dict[str, Any]]:
        """Return the first collection whose name exactly matches, or None."""
        resp = await self._request(
            "GET",
            f"{self.library_prefix}/collections",
            params={"limit": 100},
        )
        if resp.status_code != 200:
            raise ZoteroError(
                f"List collections failed: HTTP {resp.status_code}",
                resp.status_code,
            )
        for col in resp.json():
            if col.get("data", {}).get("name") == name:
                return col
        return None

    async def create_collection(self, name: str) -> Dict[str, Any]:
        """Create a top-level collection with the given display name."""
        body = [{"name": name}]
        resp = await self._request(
            "POST",
            f"{self.library_prefix}/collections",
            json_body=body,
            extra_headers={"Content-Type": "application/json"},
        )
        if resp.status_code not in (200, 204):
            raise ZoteroError(
                f"Create collection failed: HTTP {resp.status_code}: "
                f"{resp.text[:200]}",
                resp.status_code,
            )
        data = resp.json()
        successful = data.get("successful", {})
        if not successful:
            raise ZoteroError(
                f"Zotero reported no successful collection create: {data}"
            )
        # {"0": {...collection obj...}}
        return next(iter(successful.values()))

    async def get_or_create_collection(
        self, name: str,
    ) -> Dict[str, Any]:
        """Idempotent: return the collection if it exists, else create it."""
        found = await self.find_collection_by_name(name)
        if found is not None:
            return found
        return await self.create_collection(name)

    async def get_collection(self, collection_key: str) -> Optional[Dict[str, Any]]:
        """Fetch one collection by key. Returns None on 404."""
        resp = await self._request(
            "GET",
            f"{self.library_prefix}/collections/{collection_key}",
        )
        if resp.status_code == 404:
            return None
        if resp.status_code != 200:
            raise ZoteroError(
                f"Get collection failed: HTTP {resp.status_code}",
                resp.status_code,
            )
        return resp.json()

    # -----------------------------------------------------------------------
    # Items
    # -----------------------------------------------------------------------

    async def add_items_to_collection(
        self,
        collection_key: str,
        items: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """Push items into a collection. Batches of up to 50 per call.

        Each item dict must include at minimum {"itemType": "journalArticle"}.
        We set the collection via "collections": [collection_key].

        Returns aggregated counts:
          {"created": N, "unchanged": M, "failed": [reasons...]}
        """
        if not items:
            return {"created": 0, "unchanged": 0, "failed": []}

        aggregated = {"created": 0, "unchanged": 0, "failed": []}
        # Zotero allows 50 items per write request
        for batch_start in range(0, len(items), 50):
            batch = items[batch_start : batch_start + 50]
            for item in batch:
                item.setdefault("collections", [])
                if collection_key not in item["collections"]:
                    item["collections"].append(collection_key)

            resp = await self._request(
                "POST",
                f"{self.library_prefix}/items",
                json_body=batch,
                extra_headers={"Content-Type": "application/json"},
            )
            if resp.status_code not in (200, 204):
                raise ZoteroError(
                    f"Add items failed: HTTP {resp.status_code}: "
                    f"{resp.text[:200]}",
                    resp.status_code,
                )
            data = resp.json()
            aggregated["created"] += len(data.get("successful", {}))
            aggregated["unchanged"] += len(data.get("unchanged", {}))
            # Zotero returns failed as {"0": {"code": N, "message": "..."}}
            for f in data.get("failed", {}).values():
                aggregated["failed"].append(f.get("message") or str(f))
        return aggregated

    async def list_collection_items(
        self,
        collection_key: str,
        *,
        since_version: int = 0,
        include_trashed: bool = False,
    ) -> Tuple[List[Dict[str, Any]], int]:
        """List top-level items (articles) in a collection.

        Returns (items, last_modified_version). Pass the returned
        version as since_version on the next call for incremental
        polling.
        """
        params: Dict[str, Any] = {
            "limit": 100,
            "itemType": "-attachment",  # top-level articles only
        }
        if since_version > 0:
            params["since"] = since_version

        all_items: List[Dict[str, Any]] = []
        start = 0
        last_version = since_version
        while True:
            params["start"] = start
            resp = await self._request(
                "GET",
                f"{self.library_prefix}/collections/{collection_key}/items/top",
                params=params,
            )
            if resp.status_code == 404:
                raise ZoteroError("Collection not found", 404)
            if resp.status_code != 200:
                raise ZoteroError(
                    f"List items failed: HTTP {resp.status_code}",
                    resp.status_code,
                )

            try:
                v = int(resp.headers.get("Last-Modified-Version", "0"))
                if v > last_version:
                    last_version = v
            except ValueError:
                pass

            page = resp.json()
            if not page:
                break
            for item in page:
                if include_trashed or not item.get("data", {}).get("deleted"):
                    all_items.append(item)
            if len(page) < 100:
                break
            start += 100

        return all_items, last_version

    async def list_child_attachments(
        self,
        item_key: str,
    ) -> List[Dict[str, Any]]:
        """List child attachments (PDF etc.) for an item."""
        resp = await self._request(
            "GET",
            f"{self.library_prefix}/items/{item_key}/children",
        )
        if resp.status_code != 200:
            raise ZoteroError(
                f"List children failed: HTTP {resp.status_code}",
                resp.status_code,
            )
        return [
            c for c in resp.json()
            if c.get("data", {}).get("itemType") == "attachment"
        ]

    async def download_attachment(
        self,
        item_key: str,
    ) -> Optional[bytes]:
        """Download an attachment's file bytes. Returns None on non-file
        attachments (e.g. web links with no local storage)."""
        # /file 302-redirects to S3. httpx.follow_redirects=True by default
        # is False, so we pass it explicitly.
        url = f"{ZOTERO_API_BASE}{self.library_prefix}/items/{item_key}/file"
        async with self._limiter.acquire("zotero") as _delay:
            resp = await self._client.get(
                url,
                headers=self._headers(),
                follow_redirects=True,
            )
        if resp.status_code == 404:
            return None
        if resp.status_code != 200:
            raise ZoteroError(
                f"Download attachment failed: HTTP {resp.status_code}",
                resp.status_code,
            )
        return resp.content


def build_item_from_paper(
    doi: str,
    title: str = "",
    authors: str = "",
    year: Optional[int] = None,
    journal: Optional[str] = None,
) -> Dict[str, Any]:
    """Compose a Zotero 'journalArticle' item dict from our Paper record.

    We keep the payload minimal: DOI is the required anchor (Zotero's
    Connector and our poller both match by DOI). Title + creators help
    the user recognize items in their Zotero window.
    """
    item: Dict[str, Any] = {
        "itemType": "journalArticle",
        "DOI": doi,
        "url": f"https://doi.org/{doi}" if doi else "",
    }
    if title:
        item["title"] = title
    if journal:
        item["publicationTitle"] = journal
    if year is not None:
        item["date"] = str(year)
    if authors:
        creators: List[Dict[str, str]] = []
        # Authors come as "Lastname, Firstname; Other, Name" from our
        # ingestion; split naively and hand to Zotero
        for a in authors.split(";"):
            a = a.strip()
            if not a:
                continue
            if "," in a:
                last, _, first = a.partition(",")
                creators.append({
                    "creatorType": "author",
                    "firstName": first.strip(),
                    "lastName": last.strip(),
                })
            else:
                creators.append({
                    "creatorType": "author",
                    "name": a,
                })
        if creators:
            item["creators"] = creators
    return item


def extract_doi_from_zotero_item(item: Dict[str, Any]) -> Optional[str]:
    """Pull the normalized DOI from a Zotero item, in priority order.

    Zotero items store DOI in the top-level 'DOI' field for
    journalArticle types. Some items might have it in 'extra' or
    'url' only — check those too.
    """
    from full_text_acquisition.models import normalize_doi

    data = item.get("data") or item

    # Helper: first non-empty value across data dict AND top-level item
    # (defensive — accepts flat or nested shapes, or partial data)
    def _val(key: str) -> str:
        return (data.get(key) or item.get(key) or "") if isinstance(item, dict) else ""

    doi = _val("DOI")
    if doi:
        norm = normalize_doi(doi)
        if norm:
            return norm

    # Fallback: url might be https://doi.org/10.XXX
    url = _val("url")
    if "doi.org/" in url:
        norm = normalize_doi(url)
        if norm:
            return norm

    # Fallback: 'extra' field sometimes has 'DOI: 10.XXX'
    extra = _val("extra")
    for line in extra.splitlines():
        if line.lower().startswith("doi:"):
            norm = normalize_doi(line.split(":", 1)[1])
            if norm:
                return norm

    return None

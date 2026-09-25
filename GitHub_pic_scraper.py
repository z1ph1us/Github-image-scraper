#!/usr/bin/env python3
"""
GitHub Multi-Source Image Scraper
Scrapes issues, PRs, commit messages, and discussions for images
matching keyword queries, then downloads them.

Architecture:
    GitHubClient   REST + GraphQL search, token rotation, rate-limit handling
    UrlExtractor   pull image URLs out of text
    Downloader     async concurrent download with size/magic-byte/SHA256 filtering
    Store          SQLite persistence + resume (seen_urls)
    Orchestrator   wires the above together, drives workers, renders live UI

Tokens and keywords live in a `config.json` file beside this script — you edit
that file by hand. On first run, if no config.json exists, a
template is written next to the script for you to fill in. Any key in config.json overrides the matching default in DEFAULT_CONFIG below (so you can also tweak download_dir, max_pages, blacklist_domains, etc. without touching the code).

    config.json (minimum):
        {
            "tokens":  ["ghp_...", "ghp_..."],
            "queries": ["dog", "cat"]
        }
"""

import os
import re
import sys
import json
import time
import sqlite3
import asyncio
import hashlib
import logging
import argparse
import threading
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Set, Tuple, Optional
from urllib.parse import urlparse

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import aiohttp
import aiofiles
from rich.console import Console
from rich.live import Live
from rich.table import Table


# ─────────────────────────────────────────────
#  CONFIG
# ─────────────────────────────────────────────
# config.json lives in the same folder as this script. You edit it by hand to
# add your tokens and keywords; everything else has a sensible default here and
# can optionally be overridden from the same file.
SCRIPT_DIR = Path(__file__).resolve().parent
CONFIG_PATH = SCRIPT_DIR / "config.json"

DEFAULT_CONFIG = {
    # Filled in from config.json — leave empty here so credentials never live
    # in the code.
    "tokens": [],
    "queries": [],

    "download_dir": "Scraped_Images",
    "db_path": "scraper_state.db",
    "max_workers": 20,
    "search_workers": 5,
    "search_sleep": 2.5,
    "page_sleep": 1.0,
    "parallel_search": True,

    "min_image_bytes": 20 * 1024,
    "max_image_bytes": 20 * 1024 * 1024,

    "scrape_issues":      True,
    "scrape_prs":         True,
    "scrape_commits":     True,
    "scrape_discussions": True,

    "max_pages": 10,

    # Domains to never attempt downloading (junk placeholders, badges, etc.)
    "blacklist_domains": [
        "cursor.com",
        "img.shields.io",
        "shields.io",
        "yourlink.com",
        "developer.mend.io",
        "pr-comments-assets.blacksmith.sh",
        "hub-image.moreve.net",
        "example.com",
        "static.trunk.io",
    ],

    # URL substrings that mark CI badges / workflow banners regardless of host
    # (e.g. github.com/.../actions/workflows/xxx/badge.svg). Matched anywhere in
    # the lowercased URL.
    "blacklist_substrings": [
        "/badge.svg",
        "/actions/workflows/",
    ],
}

# Written to config.json on first run when the file is missing, so you have
# something to fill in by hand.
CONFIG_TEMPLATE = {
    "tokens": [
        "ghp_REPLACE_WITH_YOUR_FIRST_TOKEN",
        "ghp_REPLACE_WITH_YOUR_SECOND_TOKEN"
    ],
    "queries": [
        "dog",
        "cat"
    ],
}


def load_config() -> Tuple[dict, bool]:
    """
    Merge config.json (beside the script) over DEFAULT_CONFIG.

    Returns (config, created_template). If config.json is missing, a template is
    written and (defaults, True) is returned so the caller can tell the user to
    fill it in. Unknown keys in the file are ignored with a warning printed to
    stderr; tokens/queries and any known setting are accepted.
    """
    cfg = dict(DEFAULT_CONFIG)

    if not CONFIG_PATH.exists():
        try:
            CONFIG_PATH.write_text(json.dumps(CONFIG_TEMPLATE, indent=4) + "\n")
        except OSError as e:
            print(f"Could not write config template to {CONFIG_PATH}: {e}",
                  file=sys.stderr)
        return cfg, True

    try:
        raw = json.loads(CONFIG_PATH.read_text())
    except (OSError, json.JSONDecodeError) as e:
        print(f"Failed to read {CONFIG_PATH}: {e}", file=sys.stderr)
        return cfg, False

    if not isinstance(raw, dict):
        print(f"{CONFIG_PATH} must contain a JSON object.", file=sys.stderr)
        return cfg, False

    for key, value in raw.items():
        if key in DEFAULT_CONFIG:
            cfg[key] = value
        else:
            print(f"Ignoring unknown config key: {key!r}", file=sys.stderr)

    # Normalize: allow tokens/queries given as a comma-separated string too.
    if isinstance(cfg.get("tokens"), str):
        cfg["tokens"] = [t.strip() for t in cfg["tokens"].split(",") if t.strip()]
    if isinstance(cfg.get("queries"), str):
        cfg["queries"] = [q.strip() for q in cfg["queries"].split(",") if q.strip()]

    # Drop any leftover template placeholders so they aren't treated as real.
    cfg["tokens"] = [t for t in (cfg.get("tokens") or [])
                     if t and "REPLACE_WITH_YOUR" not in t]

    return cfg, False
# ─────────────────────────────────────────────

IMAGE_EXTENSIONS = ('.png', '.jpg', '.jpeg', '.webp', '.bmp', '.svg')

GITHUB_API = "https://api.github.com"
GITHUB_GRAPHQL = "https://api.github.com/graphql"
SEARCH_ISSUES  = f"{GITHUB_API}/search/issues"
SEARCH_COMMITS = f"{GITHUB_API}/search/commits"

DISCUSSION_QUERY = """
query($q: String!, $first: Int!) {
  search(query: $q, type: DISCUSSION, first: $first) {
    discussionCount
    nodes {
      ... on Discussion {
        title
        body
        url
        createdAt
      }
    }
  }
}
"""


# ── Logging ───────────────────────────────────────────────────────────────────

def setup_logging(log_dir: Path) -> logging.Logger:
    log_dir.mkdir(exist_ok=True)
    log_file = log_dir / f"scraper_{datetime.now():%Y%m%d_%H%M%S}.log"
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[logging.FileHandler(log_file)],
    )
    return logging.getLogger("GitHubScraper")


# ── Store: SQLite persistence + resume ────────────────────────────────────────

class Store:
    """
    Persists every URL we have seen so subsequent runs skip already-downloaded
    images and don't waste API quota. Table:

        seen_urls(url PRIMARY KEY, keyword, source, date, context, status)
        status ∈ {downloaded, failed, skipped}

    Thread-safe: a single connection guarded by a lock (check_same_thread=False).
    """

    def __init__(self, db_path: str):
        self.path = db_path
        self._lock = threading.Lock()
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS seen_urls (
                url      TEXT PRIMARY KEY,
                keyword  TEXT,
                source   TEXT,
                date     TEXT,
                context  TEXT,
                status   TEXT
            )
        """)
        self.conn.commit()

    def load_downloaded(self) -> Set[str]:
        with self._lock:
            rows = self.conn.execute(
                "SELECT url FROM seen_urls WHERE status = 'downloaded'"
            ).fetchall()
        return {r[0] for r in rows}

    def load_all_seen(self) -> Set[str]:
        with self._lock:
            rows = self.conn.execute("SELECT url FROM seen_urls").fetchall()
        return {r[0] for r in rows}

    def record(self, url: str, keyword: str, source: str,
               date: str, context: str, status: str):
        with self._lock:
            self.conn.execute(
                """INSERT INTO seen_urls (url, keyword, source, date, context, status)
                   VALUES (?, ?, ?, ?, ?, ?)
                   ON CONFLICT(url) DO UPDATE SET
                       keyword=excluded.keyword,
                       source=excluded.source,
                       date=excluded.date,
                       context=excluded.context,
                       status=excluded.status""",
                (url, keyword, source, date, context, status),
            )
            self.conn.commit()

    def set_status(self, url: str, status: str):
        with self._lock:
            self.conn.execute(
                "UPDATE seen_urls SET status = ? WHERE url = ?", (status, url)
            )
            self.conn.commit()

    def close(self):
        with self._lock:
            self.conn.close()


# ── UrlExtractor ──────────────────────────────────────────────────────────────

class UrlExtractor:
    """Pulls image URLs out of arbitrary text and applies a domain blacklist."""

    def __init__(self, blacklist_domains: List[str],
                 blacklist_substrings: Optional[List[str]] = None):
        # Compare against urlparse(url).netloc, not substring over the whole URL.
        self.blacklist = {d.lower() for d in blacklist_domains}
        # Substrings matched anywhere in the URL (for badges/banners on github.com).
        self.blacklist_substrings = tuple(
            s.lower() for s in (blacklist_substrings or [])
        )
        ext_group = "|".join(e[1:] for e in IMAGE_EXTENSIONS)
        self.patterns = [
            rf'https?://[^\s\)\"\'<>]+\.(?:{ext_group})(?:\?[^\s\"\'<>]*)?',
            r'https?://user-images\.githubusercontent\.com/[^\s\)\"\'<>]+',
            r'https?://github\.com/user-attachments/assets/[A-Za-z0-9\-]+',
            r'https?://objects\.githubusercontent\.com/[^\s\)\"\'<>]+',
            r'!\[.*?\]\((https?://[^\s\)]+)\)',
            r'<img[^>]+src=["\']([^"\']+)["\']',
        ]

    def _blacklisted(self, url: str) -> bool:
        netloc = urlparse(url).netloc.lower()
        if not netloc:
            return True
        # Match exact host or any parent domain (e.g. foo.shields.io → shields.io).
        parts = netloc.split(".")
        candidates = {".".join(parts[i:]) for i in range(len(parts))}
        candidates.add(netloc)
        if candidates & self.blacklist:
            return True
        low = url.lower()
        return any(s in low for s in self.blacklist_substrings)

    def extract(self, text: str) -> List[str]:
        if not text:
            return []
        found: Set[str] = set()
        for pat in self.patterns:
            for match in re.findall(pat, text, re.IGNORECASE):
                url = match if isinstance(match, str) else match
                url = re.sub(r'[\)\.,;:\'\"!?\[\]]+$', '', url).strip()
                if len(url) > 20 and not self._blacklisted(url):
                    found.add(url)
        return list(found)


# ── GitHubClient: REST + GraphQL ──────────────────────────────────────────────

class GitHubClient:
    """
    Owns a requests.Session for one token slot, handles pagination, rate limits,
    token rotation, and GraphQL discussion search.
    """

    def __init__(self, tokens: List[str], token_index: int,
                 logger: logging.Logger, cfg: dict):
        self.tokens = tokens
        self.tok_idx = [token_index % max(len(tokens), 1)] if tokens else [0]
        self.logger = logger
        self.cfg = cfg
        self.session = self._make_session(tokens[self.tok_idx[0]] if tokens else "")

    @staticmethod
    def _make_session(token: str) -> requests.Session:
        session = requests.Session()
        retries = Retry(
            total=5,
            backoff_factor=1.5,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST"],
        )
        adapter = HTTPAdapter(max_retries=retries, pool_connections=20, pool_maxsize=20)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        session.headers.update({
            "User-Agent": "GitHub-Image-Scraper/5.0",
            "Accept": "application/vnd.github.v3+json",
        })
        if token:
            session.headers["Authorization"] = f"token {token}"
        return session

    @property
    def worker_id(self) -> int:
        return self.tok_idx[0]

    @staticmethod
    def _parse_rate_headers(resp: requests.Response) -> Tuple[int, int, int]:
        try:
            remaining = int(resp.headers.get("X-RateLimit-Remaining", "0"))
            limit     = int(resp.headers.get("X-RateLimit-Limit", "0"))
            reset     = int(resp.headers.get("X-RateLimit-Reset", "0"))
            return remaining, limit, reset
        except (ValueError, TypeError):
            return 0, 0, int(time.time()) + 60

    def _rotate_token(self) -> bool:
        if len(self.tokens) <= 1:
            return False
        next_idx = (self.tok_idx[0] + 1) % len(self.tokens)
        if next_idx == self.tok_idx[0]:
            return False
        self.tok_idx[0] = next_idx
        self.session.headers["Authorization"] = f"token {self.tokens[next_idx]}"
        self.logger.info(f"[w{self.worker_id}] rotated to token "
                         f"{next_idx + 1}/{len(self.tokens)}")
        return True

    def _paginate(self, url: str, params: dict, label: str,
                  accept_header: str = "application/vnd.github.v3+json") -> List[Dict]:
        results: List[Dict] = []
        old_accept = self.session.headers.get("Accept")
        self.session.headers["Accept"] = accept_header
        sort_key = "committer-date" if "commits" in url else "created"
        wid = self.worker_id
        max_pages = self.cfg["max_pages"]
        page_sleep = self.cfg["page_sleep"]

        for page in range(1, max_pages + 1):
            self.logger.info(f"[w{wid}] {label} → page {page}")
            try:
                r = self.session.get(
                    url,
                    params={**params, "per_page": 100, "page": page,
                            "sort": sort_key, "order": "desc"},
                    timeout=15,
                )
                rem, _lim, reset = self._parse_rate_headers(r)

                if r.status_code == 401:
                    if self._rotate_token():
                        time.sleep(2); continue
                    break

                if r.status_code == 403:
                    if "rate limit" in r.text.lower():
                        wait = max(reset - int(time.time()) + 3, 0)
                        self.logger.info(f"[w{wid}] rate limit, sleeping {min(wait,65)}s")
                        time.sleep(min(wait, 65)); continue
                    if self._rotate_token():
                        time.sleep(2); continue
                    break

                if r.status_code == 422:
                    self.logger.warning(f"[w{wid}] 422 on {label}: {r.text[:120]}")
                    break

                r.raise_for_status()
                data = r.json()
                items = data.get("items", [])
                total = data.get("total_count", 0)
                self.logger.info(f"[w{wid}] {label} p{page}: {len(items)} items, total {total}")
                if not items:
                    break
                results.extend(items)

                if rem < 3:
                    wait = max(reset - int(time.time()) + 3, 0)
                    if wait > 0:
                        time.sleep(min(wait, 65))

                time.sleep(page_sleep)
            except requests.RequestException as e:
                self.logger.error(f"[w{wid}] req error {label} p{page}: {e}")
                time.sleep(2)

        self.session.headers["Accept"] = old_accept or "application/vnd.github.v3+json"
        return results

    def search(self, endpoint: str, query: str, extra_q: str = "",
               label: str = "",
               accept: str = "application/vnd.github.v3+json") -> List[Dict]:
        q = f"{query} {extra_q}".strip()
        time.sleep(self.cfg["search_sleep"])
        return self._paginate(endpoint, {"q": q}, label or query, accept)

    def search_discussions(self, query: str, first: int = 50) -> List[Dict]:
        try:
            auth = self.session.headers.get("Authorization", "")
            bearer = auth.replace("token ", "bearer ") if auth.startswith("token ") else auth
            r = self.session.post(
                GITHUB_GRAPHQL,
                json={"query": DISCUSSION_QUERY, "variables": {"q": query, "first": first}},
                headers={"Authorization": bearer, "Content-Type": "application/json"},
                timeout=20,
            )
            if r.status_code != 200:
                self.logger.warning(f"GraphQL discussions {r.status_code}: {r.text[:200]}")
                return []
            data = r.json().get("data", {}).get("search", {}) or {}
            nodes = data.get("nodes", []) or []
            self.logger.info(f"Discussions '{query}': "
                             f"{data.get('discussionCount', 0)} total, got {len(nodes)}")
            return nodes
        except Exception as e:
            self.logger.warning(f"GraphQL discussions error: {e}")
            return []

    @staticmethod
    def validate_token(token: str, timeout: int = 10) -> Optional[Tuple[int, int]]:
        """Return (remaining, limit) for a valid token, else None."""
        try:
            r = requests.get(
                "https://api.github.com/user",
                headers={"Authorization": f"token {token}"},
                timeout=timeout,
            )
            if r.status_code != 200:
                return None
            remaining = int(r.headers.get("X-RateLimit-Remaining", "0"))
            limit     = int(r.headers.get("X-RateLimit-Limit", "0"))
            return remaining, limit
        except Exception:
            return None


# ── Downloader (async) ────────────────────────────────────────────────────────

class Downloader:
    """
    Async concurrent downloader. Filters by Content-Length, actual size,
    magic bytes, and SHA256 dedupe. Runs on a background event loop so the
    synchronous worker threads can submit batches.

    Each URL resolves to one of three outcomes:
        "new"     a file was actually written to disk this run
        "exists"  already on disk / duplicate content — nothing written
        "failed"  rejected (bad status, wrong size, not an image, error)
    """

    def __init__(self, dest: Path, workers: int, logger: logging.Logger,
                 min_bytes: int, max_bytes: int):
        self.dest = dest
        self.workers = workers
        self.logger = logger
        self.min_bytes = min_bytes
        self.max_bytes = max_bytes
        self.content_hashes: Set[str] = set()
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._thread: Optional[threading.Thread] = None

    def start(self):
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._loop.run_forever, daemon=True)
        self._thread.start()

    def stop(self):
        if self._loop is not None:
            self._loop.call_soon_threadsafe(self._loop.stop)

    @staticmethod
    def _looks_like_image(data: bytes) -> bool:
        if len(data) < 12:
            return False
        sigs = (
            b"\x89PNG\r\n\x1a\n",
            b"\xff\xd8\xff",
            b"GIF87a", b"GIF89a",
            b"BM",
            b"RIFF",
        )
        return any(data.startswith(s) for s in sigs)

    async def _download_one(self, url: str, sem: asyncio.Semaphore,
                            session: aiohttp.ClientSession,
                            lock: asyncio.Lock) -> Tuple[str, str]:
        async with sem:
            uid = hashlib.md5(url.encode()).hexdigest()[:10]
            parsed = urlparse(url)
            basename = os.path.basename(parsed.path) or "image"
            ext = os.path.splitext(basename)[1].lower()
            if ext not in IMAGE_EXTENSIONS:
                ext = ".jpg"
            filepath = self.dest / f"{uid}_{basename}"
            if not filepath.suffix:
                filepath = filepath.with_suffix(ext)

            if filepath.exists() and filepath.stat().st_size >= self.min_bytes:
                return url, "exists"

            try:
                async with session.get(url, allow_redirects=True) as resp:
                    if resp.status != 200:
                        return url, "failed"

                    cl = resp.headers.get("Content-Length")
                    if cl and cl.isdigit():
                        size = int(cl)
                        if size < self.min_bytes or size > self.max_bytes:
                            return url, "failed"

                    content = await resp.read()
                    if len(content) < self.min_bytes or len(content) > self.max_bytes:
                        return url, "failed"
                    if not self._looks_like_image(content):
                        return url, "failed"

                    sha = hashlib.sha256(content).hexdigest()
                    async with lock:
                        if sha in self.content_hashes:
                            return url, "exists"
                        self.content_hashes.add(sha)

                    async with aiofiles.open(filepath, "wb") as f:
                        await f.write(content)
                    self.logger.info(f"Saved {filepath.name} ({len(content):,} bytes)")
                    return url, "new"
            except Exception as e:
                self.logger.debug(f"Download failed {url[:80]}: {e}")
                return url, "failed"

    async def _download_batch_async(self, urls: Set[str]) -> Dict[str, str]:
        sem = asyncio.Semaphore(self.workers)
        lock = asyncio.Lock()
        timeout = aiohttp.ClientTimeout(total=60, connect=10)
        connector = aiohttp.TCPConnector(limit=self.workers * 2, ttl_dns_cache=300)
        headers = {"User-Agent": "GitHub-Image-Scraper/5.0"}
        results: Dict[str, str] = {}
        async with aiohttp.ClientSession(timeout=timeout, connector=connector,
                                         headers=headers) as session:
            tasks = [self._download_one(u, sem, session, lock) for u in urls]
            for coro in asyncio.as_completed(tasks):
                try:
                    url, outcome = await coro
                    results[url] = outcome
                except Exception:
                    pass
        return results

    def download(self, urls: Set[str]) -> Dict[str, str]:
        """Blocking call from a worker thread. Returns {url: outcome}."""
        if not urls or self._loop is None:
            return {}
        fut = asyncio.run_coroutine_threadsafe(
            self._download_batch_async(urls), self._loop
        )
        return fut.result()


# ── Live UI ───────────────────────────────────────────────────────────────────

class WorkerBoard:
    """
    One row per worker in a rich.live.Live panel. Console stays minimal;
    detailed logging goes to the log file.
    """

    def __init__(self, console: Console, worker_ids: List[int]):
        self.console = console
        self._lock = threading.Lock()
        self.rows: Dict[int, Dict] = {
            wid: {"keyword": "-", "source": "-", "found": 0, "saved": 0, "failed": 0}
            for wid in worker_ids
        }
        self._live = Live(
            self._render(), console=console,
            refresh_per_second=4, auto_refresh=True,
            screen=True, vertical_overflow="crop",
        )

    def __enter__(self):
        self._live.start()
        return self

    def __exit__(self, *exc):
        self._live.update(self._render())
        self._live.stop()

    def _render(self) -> Table:
        table = Table(title="GitHub Image Scraper", expand=False)
        table.add_column("Worker", justify="right")
        table.add_column("Keyword")
        table.add_column("Source")
        table.add_column("Found", justify="right")
        table.add_column("Saved", justify="right")
        table.add_column("Failed", justify="right")
        for wid in sorted(self.rows):
            r = self.rows[wid]
            table.add_row(
                f"w{wid}", str(r["keyword"]), str(r["source"]),
                str(r["found"]), str(r["saved"]), str(r["failed"]),
            )
        return table

    def update(self, worker_id: int, **fields):
        with self._lock:
            row = self.rows.setdefault(
                worker_id,
                {"keyword": "-", "source": "-", "found": 0, "saved": 0, "failed": 0},
            )
            row.update(fields)
            self._live.update(self._render(), refresh=False)

    def add(self, worker_id: int, **deltas):
        with self._lock:
            row = self.rows.setdefault(
                worker_id,
                {"keyword": "-", "source": "-", "found": 0, "saved": 0, "failed": 0},
            )
            for k, v in deltas.items():
                row[k] = row.get(k, 0) + v
            self._live.update(self._render(), refresh=False)


# ── Orchestrator ──────────────────────────────────────────────────────────────

class Orchestrator:
    def __init__(self, cfg: dict, dry_run: bool = False, limit: Optional[int] = None):
        self.cfg = cfg
        self.dry_run = dry_run
        self.limit = limit
        self.dest = SCRIPT_DIR / cfg["download_dir"]
        self.dest.mkdir(parents=True, exist_ok=True)
        self.logger = setup_logging(SCRIPT_DIR / "logs")
        self.console = Console()
        self.tokens = list(cfg["tokens"])

        self.extractor = UrlExtractor(
            cfg.get("blacklist_domains", []),
            cfg.get("blacklist_substrings", []),
        )
        self.store = Store(str(SCRIPT_DIR / cfg["db_path"]))
        self.downloader = Downloader(
            self.dest, cfg["max_workers"], self.logger,
            cfg["min_image_bytes"], cfg["max_image_bytes"],
        )

        # Shared mutable state, guarded by a single lock.
        self._lock = threading.Lock()
        self.found: Set[str] = set()
        self.downloaded: Set[str] = self.store.load_downloaded()  # resume
        self.failed: Set[str] = set()
        self.seen: Set[str] = self.store.load_all_seen()          # resume
        self.entries: List[Tuple[str, str, str, str]] = []
        self.stats = {src: {"processed": 0, "images": 0}
                      for src in ("issues", "prs", "commits", "discussions")}
        self.newly_downloaded = 0  # files actually written this run

        self.board: Optional[WorkerBoard] = None

    # ── URL recording (thread-safe) ───────────

    def _record_urls(self, urls: List[str], source: str, keyword: str,
                     date: str = "", context: str = "") -> int:
        """Record newly-seen URLs. Returns count of new URLs added."""
        with self._lock:
            # Skip URLs already downloaded in a previous run (resume) and ones
            # already handled this run. `downloaded` was seeded from the DB at
            # startup, so previously-downloaded URLs are skipped automatically.
            new = [u for u in dict.fromkeys(urls)  # dedupe, preserve order
                   if u not in self.found
                   and u not in self.failed
                   and u not in self.downloaded]
            for u in new:
                self.found.add(u)
                self.seen.add(u)
                self.stats[source]["images"] += 1
                self.entries.append((date or "unknown", source, u, context or ""))
            return len(new)

    def _download_pending(self, keyword: str):
        with self._lock:
            pending = self.found - self.downloaded - self.failed

        if not pending:
            return

        if self.dry_run:
            # Record intent without fetching; mark as skipped.
            for u in pending:
                self.store.record(u, keyword, "dryrun", "", "", "skipped")
            with self._lock:
                self.failed.update(pending)  # remove from pending set this run
            return

        results = self.downloader.download(pending)
        entry_lookup = {}
        with self._lock:
            for date, source, u, ctx in self.entries:
                entry_lookup.setdefault(u, (date, source, ctx))

        for url, outcome in results.items():
            date, source, ctx = entry_lookup.get(url, ("", "", ""))
            db_status = "failed" if outcome == "failed" else "downloaded"
            self.store.record(url, keyword, source, date, ctx, db_status)
            with self._lock:
                if outcome == "failed":
                    self.failed.add(url)
                else:
                    self.downloaded.add(url)
                    if outcome == "new":
                        self.newly_downloaded += 1

    # ── per-source scrapers ───────────────────

    def _scrape_issues(self, client: GitHubClient, query: str) -> int:
        if not self.cfg["scrape_issues"]:
            return 0
        items = client.search(SEARCH_ISSUES, query, label=f"Issues: {query}")
        with self._lock:
            self.stats["issues"]["processed"] += len(items)
        added = 0
        for item in items:
            date = item.get("created_at", "")
            ctx = item.get("html_url", "")
            added += self._record_urls(
                self.extractor.extract(f"{item.get('title','')} {item.get('body','')}"),
                "issues", query, date, ctx,
            )
        return added

    def _scrape_prs(self, client: GitHubClient, query: str) -> int:
        if not self.cfg["scrape_prs"]:
            return 0
        items = client.search(SEARCH_ISSUES, query, extra_q="type:pr",
                              label=f"PRs: {query}")
        with self._lock:
            self.stats["prs"]["processed"] += len(items)
        added = 0
        for item in items:
            date = item.get("created_at", "")
            ctx = item.get("html_url", "")
            added += self._record_urls(
                self.extractor.extract(f"{item.get('title','')} {item.get('body','')}"),
                "prs", query, date, ctx,
            )
        return added

    def _scrape_commits(self, client: GitHubClient, query: str) -> int:
        if not self.cfg["scrape_commits"]:
            return 0
        items = client.search(
            SEARCH_COMMITS, query, label=f"Commits: {query}",
            accept="application/vnd.github.cloak-preview+json",
        )
        with self._lock:
            self.stats["commits"]["processed"] += len(items)
        added = 0
        for commit in items:
            msg = commit.get("commit", {}).get("message", "")
            date = commit.get("commit", {}).get("committer", {}).get("date", "")
            ctx = commit.get("html_url", "")
            added += self._record_urls(self.extractor.extract(msg),
                                       "commits", query, date, ctx)
        return added

    def _scrape_discussions(self, client: GitHubClient, query: str) -> int:
        if not self.cfg["scrape_discussions"]:
            return 0
        nodes = client.search_discussions(query)
        with self._lock:
            self.stats["discussions"]["processed"] += len(nodes)
        added = 0
        for d in nodes:
            date = d.get("createdAt", "")
            ctx = d.get("url", "")
            added += self._record_urls(
                self.extractor.extract(f"{d.get('title','')} {d.get('body','')}"),
                "discussions", query, date, ctx,
            )
        return added

    # ── worker ────────────────────────────────

    def _worker(self, worker_id: int, token: str, queries: List[str]):
        client = GitHubClient(self.tokens, worker_id, self.logger, self.cfg)
        # pin this client's session to its assigned token
        client.tok_idx = [worker_id % max(len(self.tokens), 1)]
        client.session.headers["Authorization"] = f"token {token}"

        for q in queries:
            self.logger.info(f"[w{worker_id}] === keyword: {q} ===")
            if self.board:
                self.board.update(worker_id, keyword=q, source="starting")
            try:
                for source, fn in (
                    ("issues", self._scrape_issues),
                    ("prs", self._scrape_prs),
                    ("commits", self._scrape_commits),
                    ("discussions", self._scrape_discussions),
                ):
                    if self.board:
                        self.board.update(worker_id, source=source)
                    added = fn(client, q)
                    if self.board:
                        self.board.add(worker_id, found=added)

                if self.board:
                    self.board.update(worker_id, source="downloading")
                before = len(self.downloaded)
                before_failed = len(self.failed)
                self._download_pending(q)
                if self.board:
                    self.board.add(
                        worker_id,
                        saved=len(self.downloaded) - before,
                        failed=len(self.failed) - before_failed,
                    )
            except Exception as e:
                self.logger.exception(f"w{worker_id} error on '{q}': {e}")

        if self.board:
            self.board.update(worker_id, keyword="done", source="-")

    # ── report ────────────────────────────────

    def save_report(self):
        report = self.dest / "report.txt"
        urls_file = self.dest / "image_urls.txt"
        dated_file = self.dest / "image_dates.txt"

        with open(report, "w") as f:
            f.write(f"GitHub Image Scraper Report\n{'='*50}\n")
            f.write(f"Date     : {datetime.now():%Y-%m-%d %H:%M:%S}\n")
            f.write(f"Keywords : {len(self.cfg['queries'])} queries\n")
            f.write(f"Output   : {self.dest.absolute()}\n")
            f.write(f"Dry run  : {self.dry_run}\n\n")
            for src, s in self.stats.items():
                f.write(f"{src:12s}  processed={s['processed']:5d}  images={s['images']:5d}\n")
            f.write(f"\nTotal unique URLs : {len(self.found)}\n")
            f.write(f"Newly downloaded  : {self.newly_downloaded}\n")
            f.write(f"Already present   : {len(self.downloaded) - self.newly_downloaded}\n")
            f.write(f"Downloaded (total): {len(self.downloaded)}\n")
            f.write(f"Failed            : {len(self.failed)}\n")

        with open(urls_file, "w") as f:
            f.write("\n".join(sorted(self.found)))

        with open(dated_file, "w") as f:
            for d, src, u, ctx in sorted(self.entries, key=lambda x: x[0], reverse=True):
                f.write(f"{d}\t{src}\t{u}\t{ctx}\n")

        self.console.print(f"[green]Report → {report}[/green]")
        self.console.print(f"[green]URLs   → {urls_file}[/green]")
        self.console.print(f"[green]Dates  → {dated_file}[/green]")

    # ── run ───────────────────────────────────

    def run(self):
        if not self.tokens:
            self.console.print(
                f"[bold red]No GitHub tokens found.[/bold red] Add them to the "
                f'"tokens" list in [cyan]{CONFIG_PATH}[/cyan] (next to the script).'
            )
            return

        if not self.cfg.get("queries"):
            self.console.print(
                f"[bold red]No keywords found.[/bold red] Add them to the "
                f'"queries" list in [cyan]{CONFIG_PATH}[/cyan].'
            )
            return

        queries = list(self.cfg["queries"])
        if self.limit is not None:
            queries = queries[:self.limit]
        self.cfg = {**self.cfg, "queries": queries}

        self.console.print("[cyan]Validating tokens...[/cyan]")
        valid = []
        for i, token in enumerate(self.tokens, 1):
            info = GitHubClient.validate_token(token)
            if info is None:
                self.console.print(f"[red]✗ Token {i} invalid[/red]")
                continue
            remaining, limit = info
            self.console.print(
                f"[green]✓ Token {i} valid — {remaining}/{limit} requests left[/green]"
            )
            valid.append(token)
        if not valid:
            self.console.print("[bold red]No valid tokens! Exiting.[/bold red]")
            return
        self.tokens = valid

        if self.dry_run:
            self.console.print("[yellow]DRY RUN — no images will be downloaded.[/yellow]")

        self.downloader.start()
        t0 = time.time()

        n_workers = min(self.cfg["search_workers"], len(self.tokens), len(queries))
        n_workers = max(n_workers, 1)

        worker_ids = list(range(n_workers))
        self.board = WorkerBoard(self.console, worker_ids)

        with self.board:
            if not self.cfg["parallel_search"] or n_workers <= 1:
                self._worker(0, self.tokens[0], queries)
            else:
                slices: List[List[str]] = [[] for _ in range(n_workers)]
                for i, q in enumerate(queries):
                    slices[i % n_workers].append(q)
                threads = []
                for w in range(n_workers):
                    th = threading.Thread(
                        target=self._worker,
                        args=(w, self.tokens[w % len(self.tokens)], slices[w]),
                        daemon=True,
                    )
                    th.start()
                    threads.append(th)
                for th in threads:
                    th.join()

        self.downloader.stop()
        self.store.close()

        self.save_report()
        elapsed = time.time() - t0
        already = len(self.downloaded) - self.newly_downloaded
        self.console.print(
            f"\n[bold]Done in {elapsed:.1f}s — {self.newly_downloaded} new, "
            f"{already} already present, {len(self.failed)} failed[/bold]"
        )
        self.console.print(f"[bold]Saved to: {self.dest.absolute()}[/bold]")


# ── Backwards-compatible entry point ──────────────────────────────────────────

class Scraper:
    """Thin shim so the original entry point keeps working."""

    def __init__(self, cfg: dict, dry_run: bool = False, limit: Optional[int] = None):
        self._orch = Orchestrator(cfg, dry_run=dry_run, limit=limit)

    def run(self):
        self._orch.run()


def _parse_args(argv: List[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="GitHub multi-source image scraper")
    p.add_argument("--dry-run", action="store_true",
                   help="Search and record URLs but do not download images.")
    p.add_argument("--limit", type=int, default=None, metavar="N",
                   help="Only process the first N keyword queries.")
    return p.parse_args(argv)


if __name__ == "__main__":
    args = _parse_args(sys.argv[1:])
    config, created_template = load_config()
    if created_template:
        print(f"Created a config template at:\n    {CONFIG_PATH}\n"
              f"Open it, paste your GitHub tokens into \"tokens\" and your "
              f"keywords into \"queries\", then run this script again.")
        sys.exit(0)
    try:
        Scraper(config, dry_run=args.dry_run, limit=args.limit).run()
    except KeyboardInterrupt:
        print("\nInterrupted.")
        sys.exit(0)
    except Exception as e:
        logging.exception(f"Fatal: {e}")
        sys.exit(1)

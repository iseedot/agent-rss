#!/usr/bin/env python3
"""pi-agent-rss - single-file RSS aggregator (backend for the pi agent extension)

Design:
- Depends only on the Python standard library + feedparser (pure Python, no compilation)
- Fixed database schema: rss_feeds / rss_items / rss_items_fts
- Database defaults to <pi config dir>/rss-data/rss.db; override with RSS_DB_PATH

Commands: add / fetch / unread / search / markread / list / remove / tag / tags / stats / recent
Report pipeline (v0.5, for the scheduled short-report task):
  brief / payload / annotate / check / commit / ledger
Feeds carry comma-separated tags (rss_feeds.tags) for category filtering
(unread/search/list accept -t to filter by tag). markread accepts tag/time
filters for batch marking; stats reports per-feed and per-tag health.
"""
from __future__ import annotations

import argparse
import difflib
import gzip
import hashlib
import html
import json
import os
import re
import sqlite3
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

try:
    import fcntl  # POSIX only; Windows falls back to no lock
except ImportError:  # pragma: no cover
    fcntl = None

try:
    import feedparser
except ImportError:
    print(
        "❌ Missing Python package 'feedparser'.\n"
        "Run: python3 -m pip install --user feedparser\n"
        "(if pip is unavailable, run python3 -m ensurepip --user first)\n"
        "See INSTALL.md in the plugin directory.",
        file=sys.stderr,
    )
    sys.exit(2)

# ---------------------------------------------------------------------------
# Paths & constants
# ---------------------------------------------------------------------------

MODULE_DIR = Path(__file__).resolve().parent


def get_pi_config_dir() -> Path:
    """pi config directory: $PI_CODING_AGENT_DIR, else ~/.pi/agent"""
    override = os.getenv("PI_CODING_AGENT_DIR", "").strip()
    if override:
        return Path(override).expanduser().resolve()
    return Path.home() / ".pi" / "agent"


def get_db_path() -> Path:
    """Database path: $RSS_DB_PATH if set, else <pi config dir>/rss-data/rss.db.

    Lives outside the plugin directory on purpose: plugin directories are
    reset by package updates (git installs), this location is stable.
    """
    override = os.getenv("RSS_DB_PATH", "").strip()
    if override:
        p = Path(override).expanduser().resolve()
    else:
        p = get_pi_config_dir() / "rss-data" / "rss.db"
    p.parent.mkdir(parents=True, exist_ok=True)
    return p


DB_PATH = get_db_path()

MAX_FEED_SIZE = 10 * 1024 * 1024        # 10 MB
FETCH_TIMEOUT = 30                      # seconds
MAX_ITEM_LIMIT = 1000
MAX_SEARCH_QUERY = 500

# ---------------------------------------------------------------------------
# Schema (stable, documented)
# ---------------------------------------------------------------------------

SCHEMAS = [
    """
    CREATE TABLE IF NOT EXISTS rss_feeds (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        url TEXT NOT NULL UNIQUE,
        title TEXT,
        link TEXT,
        description TEXT,
        tags TEXT NOT NULL DEFAULT '',
        last_modified TEXT,
        etag TEXT,
        last_fetch_time INTEGER,
        fetch_error TEXT,
        fetch_count INTEGER DEFAULT 0,
        enabled INTEGER DEFAULT 1,
        created_at INTEGER NOT NULL,
        updated_at INTEGER NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_rss_feeds_enabled ON rss_feeds(enabled);
    CREATE INDEX IF NOT EXISTS idx_rss_feeds_url ON rss_feeds(url);
    """,
    """
    CREATE TABLE IF NOT EXISTS rss_items (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        guid TEXT NOT NULL UNIQUE,
        feed_id INTEGER NOT NULL,
        title TEXT,
        link TEXT,
        author TEXT,
        published INTEGER,
        updated INTEGER,
        content TEXT,
        summary TEXT,
        categories TEXT,
        is_read INTEGER DEFAULT 0,
        is_starred INTEGER DEFAULT 0,
        related_session_id INTEGER,
        created_at INTEGER NOT NULL,
        FOREIGN KEY (feed_id) REFERENCES rss_feeds(id) ON DELETE CASCADE
    );
    CREATE INDEX IF NOT EXISTS idx_rss_items_feed_id ON rss_items(feed_id);
    CREATE INDEX IF NOT EXISTS idx_rss_items_published ON rss_items(published DESC);
    CREATE INDEX IF NOT EXISTS idx_rss_items_is_read ON rss_items(is_read);
    CREATE INDEX IF NOT EXISTS idx_rss_items_guid ON rss_items(guid);
    """,
    """
    CREATE VIRTUAL TABLE IF NOT EXISTS rss_items_fts USING fts5(
        title,
        content,
        summary,
        content=rss_items,
        content_rowid=id
    );
    CREATE TRIGGER IF NOT EXISTS rss_items_fts_insert AFTER INSERT ON rss_items BEGIN
        INSERT INTO rss_items_fts(rowid, title, content, summary)
        VALUES (new.id, new.title, new.content, new.summary);
    END;
    CREATE TRIGGER IF NOT EXISTS rss_items_fts_delete AFTER DELETE ON rss_items BEGIN
        DELETE FROM rss_items_fts WHERE rowid = old.id;
    END;
    CREATE TRIGGER IF NOT EXISTS rss_items_fts_update AFTER UPDATE ON rss_items BEGIN
        UPDATE rss_items_fts SET
            title = new.title,
            content = new.content,
            summary = new.summary
        WHERE rowid = new.id;
    END;
    """,
    """
    CREATE TABLE IF NOT EXISTS rss_ledger_facts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        text TEXT NOT NULL,
        entities TEXT,
        numbers TEXT,
        status TEXT NOT NULL DEFAULT 'confirmed',
        supersedes_id INTEGER,
        source_item_ids TEXT,
        first_seen_run INTEGER,
        last_seen_run INTEGER,
        created_at INTEGER NOT NULL,
        updated_at INTEGER NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_rss_ledger_facts_status ON rss_ledger_facts(status);
    """,
    """
    CREATE TABLE IF NOT EXISTS rss_ledger_opinions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        source TEXT,
        claim TEXT,
        evidence TEXT,
        checks TEXT,
        conclusion TEXT,
        trend TEXT,
        signal TEXT,
        time_window TEXT,
        change TEXT,
        status TEXT NOT NULL DEFAULT 'active',
        source_item_ids TEXT,
        first_seen_run INTEGER,
        last_seen_run INTEGER,
        created_at INTEGER NOT NULL,
        updated_at INTEGER NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_rss_ledger_opinions_status ON rss_ledger_opinions(status);
    """,
    """
    CREATE TABLE IF NOT EXISTS rss_runs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        slot TEXT,
        slot_date TEXT,
        report_name TEXT,
        prev_report TEXT,
        fetched_new INTEGER DEFAULT 0,
        briefed INTEGER DEFAULT 0,
        facts_used INTEGER DEFAULT 0,
        opinions_used INTEGER DEFAULT 0,
        report_path TEXT,
        status TEXT,
        jev_stats TEXT,
        websearch_used INTEGER DEFAULT 0,
        violations TEXT,
        started_at INTEGER,
        finished_at INTEGER
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS rss_state (
        key TEXT PRIMARY KEY,
        value TEXT,
        updated_at INTEGER
    );
    """,
]


# ---------------------------------------------------------------------------
# Database layer
# ---------------------------------------------------------------------------

def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=5000")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def migrate_legacy_db():
    """One-time migration: move plugin-dir data/rss.db to the new stable location.

    Only runs when RSS_DB_PATH is unset (explicit override means the user
    manages the location) and the legacy file exists.
    """
    if os.getenv("RSS_DB_PATH", "").strip():
        return
    legacy = MODULE_DIR / "data" / "rss.db"
    if legacy.exists() and not DB_PATH.exists():
        import shutil

        shutil.copy2(legacy, DB_PATH)
        print(f"ℹ️ Migrated database from {legacy} to {DB_PATH}")


def migrate_schema(conn: sqlite3.Connection):
    """Additive migrations for existing databases: CREATE IF NOT EXISTS only
    covers new databases, so column additions must be applied explicitly."""
    cols = {r["name"] for r in conn.execute("PRAGMA table_info(rss_feeds)").fetchall()}
    if "tags" not in cols:
        conn.execute("ALTER TABLE rss_feeds ADD COLUMN tags TEXT NOT NULL DEFAULT ''")

    item_cols = {r["name"] for r in conn.execute("PRAGMA table_info(rss_items)").fetchall()}
    for name, decl in (
        ("brief_run_id", "INTEGER"),
        ("kind", "TEXT"),
        ("topic", "TEXT"),
        ("has_number", "INTEGER"),
        ("novel", "INTEGER"),
        ("change", "TEXT"),
        ("verify_needed", "INTEGER"),
        ("conf_json", "TEXT"),
        ("triage_json", "TEXT"),
        ("triaged_at", "INTEGER"),
        ("is_skipped", "INTEGER NOT NULL DEFAULT 0"),
    ):
        if name not in item_cols:
            conn.execute(f"ALTER TABLE rss_items ADD COLUMN {name} {decl}")

    run_cols = {r["name"] for r in conn.execute("PRAGMA table_info(rss_runs)").fetchall()}
    for name, decl in (("report_name", "TEXT"),):
        if name not in run_cols:
            conn.execute(f"ALTER TABLE rss_runs ADD COLUMN {name} {decl}")
    conn.commit()


def init_db():
    migrate_legacy_db()
    conn = get_conn()
    try:
        conn.executescript("".join(SCHEMAS))
        migrate_schema(conn)
    finally:
        conn.close()


@dataclass
class Feed:
    id: Optional[int]
    url: str
    title: Optional[str] = None
    link: Optional[str] = None
    description: Optional[str] = None
    tags: str = ""
    last_modified: Optional[str] = None
    etag: Optional[str] = None
    last_fetch_time: Optional[datetime] = None
    fetch_error: Optional[str] = None
    fetch_count: int = 0
    enabled: bool = True


@dataclass
class Item:
    id: Optional[int]
    guid: str
    feed_id: int
    title: Optional[str] = None
    link: Optional[str] = None
    author: Optional[str] = None
    published: Optional[datetime] = None
    updated: Optional[datetime] = None
    content: Optional[str] = None
    summary: Optional[str] = None
    categories: list[str] = field(default_factory=list)
    is_read: bool = False


def _feed_from_row(row: sqlite3.Row) -> Feed:
    return Feed(
        id=row["id"],
        url=row["url"],
        title=row["title"],
        link=row["link"],
        description=row["description"],
        tags=row["tags"] or "",
        last_modified=row["last_modified"],
        etag=row["etag"],
        last_fetch_time=datetime.fromtimestamp(row["last_fetch_time"]) if row["last_fetch_time"] else None,
        fetch_error=row["fetch_error"],
        fetch_count=row["fetch_count"],
        enabled=bool(row["enabled"]),
    )


def _item_from_row(row: sqlite3.Row) -> Item:
    return Item(
        id=row["id"],
        guid=row["guid"],
        feed_id=row["feed_id"],
        title=row["title"],
        link=row["link"],
        author=row["author"],
        published=datetime.fromtimestamp(row["published"]) if row["published"] else None,
        updated=datetime.fromtimestamp(row["updated"]) if row["updated"] else None,
        content=row["content"],
        summary=row["summary"],
        categories=json.loads(row["categories"]) if row["categories"] else [],
        is_read=bool(row["is_read"]),
    )


# ---- feeds ----

def _norm_tags(tags: str) -> str:
    """Normalize a comma-separated tag string: trim each tag, drop empties."""
    return ",".join(t.strip() for t in tags.split(",") if t.strip())


def _tag_match_sql(column: str) -> str:
    """SQL fragment matching a feed whose comma-separated tags contain the
    bound parameter exactly (case-insensitive, no substring false positives)."""
    return f"',' || lower({column}) || ',' LIKE '%,' || lower(?) || ',%'"


def add_feed(url: str, tags: str = "") -> int:
    now = int(time.time())
    tags = _norm_tags(tags)
    conn = get_conn()
    try:
        cur = conn.execute(
            "INSERT OR IGNORE INTO rss_feeds (url, tags, created_at, updated_at) VALUES (?, ?, ?, ?)",
            (url, tags, now, now),
        )
        conn.commit()
        if cur.rowcount == 0:
            row = conn.execute("SELECT id FROM rss_feeds WHERE url = ?", (url,)).fetchone()
            return row["id"] if row else 0
        return cur.lastrowid
    finally:
        conn.close()


def get_all_feeds(enabled_only: bool = True, tag: Optional[str] = None) -> list[Feed]:
    conn = get_conn()
    try:
        sql = "SELECT * FROM rss_feeds"
        where, params = [], []
        if enabled_only:
            where.append("enabled = 1")
        if tag:
            where.append(_tag_match_sql("tags"))
            params.append(tag)
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY id"
        return [_feed_from_row(r) for r in conn.execute(sql, params).fetchall()]
    finally:
        conn.close()


def get_feed(feed_id: int) -> Optional[Feed]:
    conn = get_conn()
    try:
        row = conn.execute("SELECT * FROM rss_feeds WHERE id = ?", (feed_id,)).fetchone()
        return _feed_from_row(row) if row else None
    finally:
        conn.close()


def update_feed_metadata(feed_id: int, **fields: Any):
    if not fields:
        return
    conn = get_conn()
    try:
        sets = ", ".join(f"{k} = ?" for k in fields)
        conn.execute(
            f"UPDATE rss_feeds SET {sets}, updated_at = ? WHERE id = ?",
            (*fields.values(), int(time.time()), feed_id),
        )
        conn.commit()
    finally:
        conn.close()


def update_fetch_status(feed_id: int, error: Optional[str] = None,
                        last_modified: Optional[str] = None, etag: Optional[str] = None):
    conn = get_conn()
    try:
        conn.execute(
            """UPDATE rss_feeds SET last_fetch_time = ?, fetch_count = fetch_count + 1,
               fetch_error = ?, last_modified = ?, etag = ?, updated_at = ?
               WHERE id = ?""",
            (int(time.time()), error, last_modified, etag, int(time.time()), feed_id),
        )
        conn.commit()
    finally:
        conn.close()


def delete_feed(feed_id: int) -> bool:
    conn = get_conn()
    try:
        cur = conn.execute("DELETE FROM rss_feeds WHERE id = ?", (feed_id,))
        conn.commit()
        return cur.rowcount > 0
    finally:
        conn.close()


# ---- items ----

def add_item(item: Item) -> Optional[int]:
    conn = get_conn()
    try:
        cur = conn.execute(
            """INSERT OR IGNORE INTO rss_items
               (guid, feed_id, title, link, author, published, updated,
                content, summary, categories, is_read, is_starred, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?)""",
            (
                item.guid, item.feed_id, item.title, item.link, item.author,
                int(item.published.timestamp()) if item.published else None,
                int(item.updated.timestamp()) if item.updated else None,
                item.content, item.summary,
                json.dumps(item.categories, ensure_ascii=False),
                int(item.is_read),
                int(time.time()),
            ),
        )
        conn.commit()
        return cur.lastrowid if cur.rowcount > 0 else None
    finally:
        conn.close()


def get_items(unread_only: bool = False, limit: int = 100, feed_id: Optional[int] = None,
              tag: Optional[str] = None) -> list[Item]:
    limit = max(1, min(int(limit), MAX_ITEM_LIMIT))
    where, params = [], []
    if feed_id:
        where.append("i.feed_id = ?")
        params.append(feed_id)
    if unread_only:
        where.append("i.is_read = 0")
    if tag:
        where.append(_tag_match_sql("f.tags"))
        params.append(tag)
    sql = "SELECT i.* FROM rss_items i JOIN rss_feeds f ON f.id = i.feed_id"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY i.published DESC LIMIT ?"
    params.append(limit)
    conn = get_conn()
    try:
        return [_item_from_row(r) for r in conn.execute(sql, params).fetchall()]
    finally:
        conn.close()


def get_unread_count() -> int:
    conn = get_conn()
    try:
        row = conn.execute("SELECT COUNT(*) AS c FROM rss_items WHERE is_read = 0").fetchone()
        return row["c"]
    finally:
        conn.close()


def mark_as_read(item_id: int):
    conn = get_conn()
    try:
        conn.execute("UPDATE rss_items SET is_read = 1 WHERE id = ?", (item_id,))
        conn.commit()
    finally:
        conn.close()


def mark_all_read() -> int:
    conn = get_conn()
    try:
        cur = conn.execute("UPDATE rss_items SET is_read = 1 WHERE is_read = 0")
        conn.commit()
        return cur.rowcount
    finally:
        conn.close()


def mark_read_filtered(tag: Optional[str] = None, older_than_hours: Optional[int] = None,
                       before_ts: Optional[int] = None) -> int:
    """Batch-mark unread items as read, restricted to feeds with tag, items
    older than N hours, and/or items before a timestamp (all optional)."""
    where, params = ["i.is_read = 0"], []
    joins = ""
    if tag:
        joins = " JOIN rss_feeds f ON f.id = i.feed_id"
        where.append(_tag_match_sql("f.tags"))
        params.append(tag)
    if older_than_hours is not None:
        where.append("COALESCE(i.published, i.created_at) < ?")
        params.append(int(time.time()) - older_than_hours * 3600)
    if before_ts is not None:
        where.append("COALESCE(i.published, i.created_at) < ?")
        params.append(before_ts)
    sql = (f"UPDATE rss_items SET is_read = 1 WHERE id IN "
           f"(SELECT i.id FROM rss_items i{joins} WHERE {' AND '.join(where)})")
    conn = get_conn()
    try:
        cur = conn.execute(sql, params)
        conn.commit()
        return cur.rowcount
    finally:
        conn.close()


def get_unread_count(tag: Optional[str] = None, feed_id: Optional[int] = None) -> int:
    conn = get_conn()
    try:
        where, params = ["i.is_read = 0"], []
        if tag:
            where.append(_tag_match_sql("f.tags"))
            params.append(tag)
        if feed_id:
            where.append("i.feed_id = ?")
            params.append(feed_id)
        sql = ("SELECT COUNT(*) AS c FROM rss_items i "
               "JOIN rss_feeds f ON f.id = i.feed_id WHERE " + " AND ".join(where))
        row = conn.execute(sql, params).fetchone()
        return row["c"]
    finally:
        conn.close()


def search_items(query: str, limit: int = 50, tag: Optional[str] = None,
                 feed_id: Optional[int] = None) -> list[Item]:
    query = (query or "").strip()[:MAX_SEARCH_QUERY]
    if not query:
        return []
    limit = max(1, min(int(limit), MAX_ITEM_LIMIT))
    sql = """SELECT i.* FROM rss_items i
             JOIN rss_items_fts ON i.id = rss_items_fts.rowid
             JOIN rss_feeds f ON f.id = i.feed_id
             WHERE rss_items_fts MATCH ?"""
    args: list = [None, limit]
    if tag:
        sql += " AND " + _tag_match_sql("f.tags")
        args.insert(1, tag)
    if feed_id:
        sql += " AND i.feed_id = ?"
        args.insert(1, feed_id)
    sql += " ORDER BY i.published DESC LIMIT ?"
    conn = get_conn()
    try:
        for candidate in (query, '"' + query.replace('"', '""') + '"'):
            args[0] = candidate
            try:
                return [_item_from_row(r) for r in conn.execute(sql, args).fetchall()]
            except sqlite3.OperationalError:
                continue
        return []
    finally:
        conn.close()


# ---- stats ----

def feed_stats() -> list[dict]:
    """Per-feed aggregates: item counts, unread, last item/fetch time, fetch error."""
    conn = get_conn()
    try:
        rows = conn.execute(
            """SELECT f.id, f.url, f.title, f.tags, f.enabled, f.last_fetch_time, f.fetch_error,
                      COUNT(i.id) AS total_items,
                      SUM(CASE WHEN i.is_read = 0 THEN 1 ELSE 0 END) AS unread_items,
                      MAX(i.published) AS last_item_time
               FROM rss_feeds f LEFT JOIN rss_items i ON i.feed_id = f.id
               GROUP BY f.id ORDER BY f.id"""
        ).fetchall()
    finally:
        conn.close()
    return [dict(r) for r in rows]


def _fmt_ts(ts) -> str:
    if not ts:
        return "-"
    try:
        return datetime.fromtimestamp(int(ts)).strftime("%Y-%m-%d %H:%M")
    except Exception:
        return "-"


# ---------------------------------------------------------------------------
# Fetch layer (stdlib urllib, serial)
# ---------------------------------------------------------------------------

_OPENER = urllib.request.build_opener()


class FeedURLError(Exception):
    """Fetch-level error with a user-readable message."""


def _fetch(url: str, headers: dict[str, str]) -> tuple[bytes, dict[str, str]]:
    """Fetch content (redirects followed by urllib); returns (body, headers) or
    raises FeedURLError with a user-readable message."""
    req = urllib.request.Request(url, headers=headers)
    try:
        resp = _OPENER.open(req, timeout=FETCH_TIMEOUT)
    except urllib.error.HTTPError as e:
        if e.code == 304:
            return b"", {"status": "304"}
        raise FeedURLError(f"HTTP {e.code}")
    except urllib.error.URLError as e:
        raise FeedURLError(f"Request failed: {e.reason}")

    body = resp.read(MAX_FEED_SIZE + 1)
    # Normalize header keys to lowercase: upstream servers (e.g. Node-based
    # ones) may send lowercase names, and lookups must not be case-sensitive.
    resp_headers = {k.lower(): v for k, v in resp.headers.items()}
    resp.close()
    if len(body) > MAX_FEED_SIZE:
        raise FeedURLError(f"Feed exceeds size limit: {MAX_FEED_SIZE} bytes")
    if resp_headers.get("content-encoding", "").lower() == "gzip":
        try:
            body = gzip.decompress(body)
        except OSError as e:
            raise FeedURLError(f"gzip decompression failed: {e}")
    resp_headers["status"] = str(getattr(resp, "status", 200))
    return body, resp_headers


def _parse_feed_safe(content: bytes) -> feedparser.FeedParserDict:
    """XXE sanitization + feedparser parse"""
    sanitized = re.sub(rb"<!DOCTYPE[^>]*>", b"", content, flags=re.IGNORECASE | re.DOTALL)
    sanitized = re.sub(rb"<!ENTITY[^>]*>", b"", sanitized, flags=re.IGNORECASE | re.DOTALL)
    try:
        return feedparser.parse(sanitized)
    except Exception as e:
        raise ValueError(f"Feed parsing failed: {e}")


def _parse_date(struct_time) -> Optional[datetime]:
    if not struct_time:
        return None
    try:
        return datetime.fromtimestamp(time.mktime(struct_time))
    except Exception:
        return None


def _generate_guid(entry: dict) -> str:
    raw = f"{entry.get('title', '')}{entry.get('link', '')}{entry.get('published', '')}"
    return hashlib.md5(raw.encode()).hexdigest()


def _parse_entry(feed_id: int, entry: dict) -> Item:
    guid = entry.get("id") or entry.get("guid") or entry.get("link") or _generate_guid(entry)
    title = entry.get("title", "No title")
    link = entry.get("link")
    author = (entry.get("author") or entry.get("author_detail", {}).get("name") or entry.get("dc_creator"))
    published = _parse_date(entry.get("published_parsed") or entry.get("updated_parsed") or entry.get("created_parsed"))
    updated = _parse_date(entry.get("updated_parsed"))
    content = None
    if "content" in entry:
        content = entry["content"][0].get("value")
    elif "description" in entry:
        content = entry["description"]
    summary = entry.get("summary", "")
    categories = [tag.get("term", tag.get("label", "")) for tag in entry.get("tags", [])]
    return Item(
        id=None, guid=guid, feed_id=feed_id, title=title, link=link, author=author,
        published=published or datetime.now(), updated=updated,
        content=content, summary=summary, categories=categories,
    )


def fetch_feed(feed: Feed) -> int:
    """Fetch a single feed; returns the number of new items"""
    if feed.id is None:
        return 0
    headers = {"User-Agent": "pi-agent-rss/0.4.0", "Accept-Encoding": "gzip"}
    if feed.last_modified:
        headers["If-Modified-Since"] = feed.last_modified
    if feed.etag:
        headers["If-None-Match"] = feed.etag

    try:
        body, resp_headers = _fetch(feed.url, headers)
        status = resp_headers.get("status", "200")

        if status == "304":
            update_fetch_status(feed.id, error=None)
            return 0
        if status != "200":
            update_fetch_status(feed.id, error=f"HTTP {status}")
            return 0

        parsed = _parse_feed_safe(body)

        # Update feed metadata
        feed_info = parsed.get("feed", {})
        updates = {}
        if title := feed_info.get("title"):
            updates["title"] = title
        if link := feed_info.get("link"):
            updates["link"] = link
        if desc := (feed_info.get("subtitle") or feed_info.get("description")):
            updates["description"] = desc
        if updates:
            update_feed_metadata(feed.id, **updates)

        # Store items (guid dedup)
        new_count = 0
        for entry in parsed.get("entries", []):
            try:
                if add_item(_parse_entry(feed.id, entry)):
                    new_count += 1
            except Exception:
                continue

        update_fetch_status(
            feed.id, error=None,
            last_modified=resp_headers.get("last-modified"),
            etag=resp_headers.get("etag"),
        )
        return new_count

    except FeedURLError as e:
        update_fetch_status(feed.id, error=str(e))
        return 0
    except Exception as e:
        update_fetch_status(feed.id, error=str(e))
        return 0


def fetch_all_feeds(workers: Optional[int] = None) -> dict:
    """Fetch all enabled feeds concurrently; returns per-feed structured stats."""
    feeds = get_all_feeds(enabled_only=True)
    n_workers = max(1, int(workers or os.getenv("RSS_FETCH_WORKERS", "6") or 6))
    results: dict[int, int] = {}
    if feeds:
        with ThreadPoolExecutor(max_workers=n_workers) as pool:
            futures = {pool.submit(fetch_feed, f): f for f in feeds}
            for fut in as_completed(futures):
                feed = futures[fut]
                try:
                    results[feed.id] = int(fut.result() or 0)
                except Exception:
                    results[feed.id] = 0

    details: list[dict] = []
    new_total = 0
    failed = 0
    for feed in feeds:
        n = results.get(feed.id, 0)
        new_total += n
        conn = get_conn()
        try:
            row = conn.execute(
                "SELECT title, fetch_error FROM rss_feeds WHERE id = ?", (feed.id,)
            ).fetchone()
        finally:
            conn.close()
        error = row["fetch_error"] if row else None
        if error:
            failed += 1
        details.append({
            "id": feed.id,
            "title": (row["title"] if row else None) or feed.url,
            "url": feed.url,
            "new": n,
            "error": error,
        })
    return {
        "total_feeds": len(feeds),
        "success": len(feeds) - failed,
        "failed": failed,
        "total_new_items": new_total,
        "feeds": details,
    }


@contextmanager
def _fetch_lock():
    """Cross-process lock so two sessions (or schedules) never fetch at once."""
    if fcntl is None:
        yield
        return
    lock_path = DB_PATH.with_name(DB_PATH.name + ".fetch.lock")
    handle = open(lock_path, "w")
    try:
        fcntl.flock(handle, fcntl.LOCK_EX)
        yield
    finally:
        fcntl.flock(handle, fcntl.LOCK_UN)
        handle.close()


def fetch_all_feeds_guarded(workers: Optional[int] = None) -> dict:
    with _fetch_lock():
        return fetch_all_feeds(workers=workers)


# ---------------------------------------------------------------------------
# Output formatting
# ---------------------------------------------------------------------------

def _to_text(content: Optional[str], limit: int = 300) -> str:
    """HTML content -> plain text, truncated (minimal html2text replacement)"""
    if not content:
        return ""
    text = re.sub(r"<[^>]+>", "", content)
    text = html.unescape(text)
    text = re.sub(r"\s+", " ", text).strip()
    return text[:limit]


def _pretty_items(items: list[Item]) -> list[str]:
    lines: list[str] = []
    for idx, item in enumerate(items, 1):
        feed = get_feed(item.feed_id)
        feed_title = feed.title if feed else "Unknown source"
        lines.append(f"{idx}. [{feed_title}] {item.title or 'No title'}")
        content = _to_text(item.content or item.summary)
        lines.append(content if content else "(no content)")
        meta = []
        if item.author:
            meta.append(f"Author: {item.author}")
        if item.published:
            meta.append(f"Time: {item.published.strftime('%Y-%m-%d %H:%M')}")
        if item.link:
            meta.append(f"Link: {item.link}")
        if meta:
            lines.append(" | ".join(meta))
        lines.append("---")
    return lines


# ---------------------------------------------------------------------------
# CLI commands
# ---------------------------------------------------------------------------

def cmd_add(url: str, tags: str = "") -> str:
    tags = _norm_tags(tags)
    feed_id = add_feed(url, tags)
    lines = [f"✅ Feed added", f"Feed ID: {feed_id}", f"URL: {url}"]
    if tags:
        lines.append(f"Tags: {tags}")
    else:
        lines.append("💡 Tip: tag it later with: tag <feed_id> -t tag1,tag2")
    return "\n".join(lines)


def cmd_fetch() -> tuple[str, dict]:
    stats = fetch_all_feeds_guarded()
    lines = [
        "✅ Fetch complete",
        f"Total feeds: {stats['total_feeds']}",
        f"Succeeded: {stats['success']}",
        f"Failed: {stats['failed']}",
        f"New items: {stats['total_new_items']}",
    ]
    failed = [f for f in stats["feeds"] if f["error"]]
    if failed:
        lines.append("\nFailed feeds:")
        for feed in failed[:5]:
            lines.append(f"  - ID {feed['id']}: {feed['error']}")
    return "\n".join(lines), stats


def cmd_unread(limit: int, tag: Optional[str] = None, feed_id: Optional[int] = None) -> str:
    items = get_items(unread_only=True, limit=limit, tag=tag, feed_id=feed_id)
    total = get_unread_count(tag=tag, feed_id=feed_id)
    if not items:
        return f"No unread items{' with tag ' + tag if tag else ''}{' from feed ' + str(feed_id) if feed_id else ''}"
    header = f"📰 Unread items ({len(items)} shown / {total} total)"
    if tag:
        header += f" tagged '{tag}'"
    if feed_id:
        header += f" from feed {feed_id}"
    return "\n".join([header, ""] + _pretty_items(items)
                     + ["💡 Tip: use markread after reading"])


def cmd_search(query: str, limit: int, tag: Optional[str] = None, feed_id: Optional[int] = None) -> str:
    items = search_items(query, limit=limit, tag=tag, feed_id=feed_id)
    if not items:
        return f"No items found for '{query}'"
    header = f"🔍 Search results: '{query}' ({len(items)})"
    if tag:
        header += f" tagged '{tag}'"
    if feed_id:
        header += f" from feed {feed_id}"
    return "\n".join([header, ""] + _pretty_items(items))


def cmd_recent(feed_id: Optional[int], limit: int) -> str:
    """Recent items from one feed (or all), regardless of read state."""
    if feed_id is not None and not get_feed(feed_id):
        return f"❌ No feed with ID {feed_id}"
    items = get_items(unread_only=False, limit=limit, feed_id=feed_id)
    if not items:
        return f"No items{' for feed ' + str(feed_id) if feed_id else ''}"
    header = f"🕒 Recent items ({len(items)})" + (f" from feed {feed_id}" if feed_id else "")
    return "\n".join([header, ""] + _pretty_items(items))


def cmd_markread(item_id: Optional[int], tag: Optional[str] = None,
                 older_than: Optional[int] = None, before: Optional[str] = None) -> str:
    if item_id is not None:
        conn = get_conn()
        try:
            row = conn.execute("SELECT id FROM rss_items WHERE id = ?", (item_id,)).fetchone()
        finally:
            conn.close()
        if not row:
            return f"❌ No item with ID {item_id}"
        mark_as_read(item_id)
        return f"✅ Marked item {item_id} as read"

    filters: list[str] = []
    before_ts: Optional[int] = None
    if tag:
        filters.append(f"tag '{tag}'")
    if older_than:
        filters.append(f"older than {older_than}h")
    if before:
        before_ts = _parse_ts(before)
        if before_ts is None:
            return (f"❌ Cannot parse time: '{before}'. Use a unix timestamp or ISO date, "
                    f"e.g. 1757700000 or 2026-09-10 or 2026-09-10T12:00")
        filters.append(f"before {_fmt_ts(before_ts)}")

    if not filters:
        count = mark_all_read()
        return f"✅ Marked {count} item(s) as read" if count else "No unread items to mark"

    count = mark_read_filtered(tag=tag, older_than_hours=older_than, before_ts=before_ts)
    return f"✅ Marked {count} unread item(s) as read ({', '.join(filters)})"


def cmd_stats() -> str:
    rows = feed_stats()
    if not rows:
        return "No subscriptions"
    total_unread = sum(int(r["unread_items"] or 0) for r in rows)
    failed = sum(1 for r in rows if r["fetch_error"])
    lines = [
        f"📊 RSS stats",
        f"Feeds: {len(rows)} | Unread total: {total_unread} | With errors: {failed}",
        "",
    ]
    for r in rows:
        title = r["title"] or "(not fetched yet)"
        tags = f"  🏷️ {r['tags']}" if r["tags"] else ""
        status = f"⚠️ {r['fetch_error']}" if r["fetch_error"] else "✅"
        lines.append(f"ID {r['id']} {title}{tags}")
        lines.append(
            f"  items: {r['total_items'] or 0} | unread: {r['unread_items'] or 0} | "
            f"last item: {_fmt_ts(r['last_item_time'])} | "
            f"last fetch: {_fmt_ts(r['last_fetch_time'])} | {status}"
        )
    tag_counts: dict[str, dict] = {}
    for r in rows:
        for t in (r["tags"] or "").split(","):
            t = t.strip()
            if not t:
                continue
            agg = tag_counts.setdefault(t, {"feeds": 0, "items": 0, "unread": 0})
            agg["feeds"] += 1
            agg["items"] += int(r["total_items"] or 0)
            agg["unread"] += int(r["unread_items"] or 0)
    if tag_counts:
        lines += ["", "🏷️ By tag"]
        for t, agg in sorted(tag_counts.items(), key=lambda kv: (-kv[1]["unread"], kv[0])):
            lines.append(f"  {t}: {agg['items']} items, {agg['unread']} unread ({agg['feeds']} feed(s))")
    return "\n".join(lines)


def cmd_list(tag: Optional[str] = None) -> str:
    feeds = get_all_feeds(tag=tag)
    if not feeds:
        return f"No subscriptions{' with tag ' + tag if tag else ''}"
    lines = [f"📡 Subscriptions ({len(feeds)})" + (f" tagged '{tag}'" if tag else ""), ""]
    for feed in feeds:
        status = "✅" if feed.enabled else "⏸"
        title = feed.title or "(not fetched yet)"
        err = f"  ⚠️ {feed.fetch_error}" if feed.fetch_error else ""
        tags = f"  🏷️ {feed.tags}" if feed.tags else ""
        lines.append(f"{status} ID {feed.id} {title}{tags}{err}\n    {feed.url}")
    return "\n".join(lines)


def cmd_tag(feed_id: int, tags: str = "") -> str:
    """Set/replace tags on a feed; without -t, show current tags."""
    feed = get_feed(feed_id)
    if not feed:
        return f"❌ No feed with ID {feed_id}"
    tags = _norm_tags(tags)
    if not tags:
        return f"📌 Tags for ID {feed_id} '{feed.title or feed.url}': {feed.tags or '(none)'}"
    update_feed_metadata(feed_id, tags=tags)
    return f"✅ Tags updated for ID {feed_id}: {tags}"


def cmd_tags() -> str:
    """List all tags in use with feed counts."""
    conn = get_conn()
    try:
        rows = conn.execute("SELECT tags FROM rss_feeds WHERE tags != ''").fetchall()
    finally:
        conn.close()
    counts: dict[str, int] = {}
    for row in rows:
        for t in row["tags"].split(","):
            t = t.strip()
            if t:
                counts[t] = counts.get(t, 0) + 1
    if not counts:
        return "No tags yet (use: rss tag <feed_id> -t tag1,tag2)"
    lines = [f"🏷️ Tags ({len(counts)})", ""]
    for t, c in sorted(counts.items(), key=lambda kv: (-kv[1], kv[0])):
        lines.append(f"  {t} ({c} feed(s))")
    return "\n".join(lines)


def cmd_remove(feed_id: int) -> str:
    feed = get_feed(feed_id)
    if not feed:
        return f"❌ No feed with ID {feed_id}"
    delete_feed(feed_id)
    return f"✅ Removed subscription: {feed.url}"


# ---------------------------------------------------------------------------
# Report pipeline (v0.5): brief / payload / annotate / check / commit / ledger
# ---------------------------------------------------------------------------

DEFAULT_SKIP_KEYWORDS = [
    "世界杯", "欧洲杯", "欧冠", "NBA", "中超", "英超", "西甲", "意甲", "德甲",
    "奥运", "夺冠", "比分", "转会", "季后赛", "决赛", "球星",
    "票房", "综艺", "演唱会", "颁奖", "电影节", "电视剧", "明星", "八卦",
    "娱乐", "绯闻", "选秀", "真人秀", "剧集", "游戏攻略",
]

OPINION_MARKERS = ["认为", "预计", "料将", "建议", "称将", "研报显示", "分析师表示",
                   "评论", "指出", "判断", "看好", "看空", "警惕", "呼吁"]
FACT_OPINION_MARKERS = ["认为", "预计", "建议", "称将", "研报显示", "分析师表示"]
FORBIDDEN_PHRASES = ["惨烈", "崩了", "利好", "值得警惕", "大概率", "显然",
                     "直白说", "三点如下", "这意味着"]

_SECTION_FACTS_RE = re.compile(r"^#{1,6}\s*一、\s*资讯速览", re.M)
_SECTION_OPINIONS_RE = re.compile(r"^#{1,6}\s*二、\s*观点与趋势", re.M)
_FACT_LINE_RE = re.compile(r"^\s*(?:[-*·]|\d+[.、])\s+(.*)$")
_OPINION_FIELDS = ["观点：", "依据：", "检验：", "结论：", "趋势：", "与上期相比："]
_NUM_UNIT_RE = re.compile(
    r"[+\-]?\d[\d,]*(?:\.\d+)?\s*(?:万亿|亿|万|%|％|美元|元|点|bp|个基点|倍)")
_NUM_BARE_RE = re.compile(r"[+\-]?\d[\d,]{1,}(?:\.\d+)?")


def _count_hanzi(text: str) -> int:
    return sum(1 for ch in text if "\u4e00" <= ch <= "\u9fff")


def _bj_now() -> datetime:
    try:
        from zoneinfo import ZoneInfo
        return datetime.now(ZoneInfo(os.getenv("RSS_TZ", "Asia/Shanghai")))
    except Exception:
        return datetime.now(timezone(timedelta(hours=8)))


def _fmt_bj(ts: Optional[int]) -> str:
    if not ts:
        return ""
    try:
        return datetime.fromtimestamp(int(ts), _bj_now().tzinfo).strftime("%m-%d %H:%M")
    except Exception:
        return ""


def _report_dir() -> Path:
    path = Path(os.getenv("RSS_REPORT_DIR", "~/Chat/rss")).expanduser()
    path.mkdir(parents=True, exist_ok=True)
    return path


def _skip_keywords() -> list[str]:
    env = os.getenv("RSS_SKIP_KEYWORDS", "").strip()
    if env:
        return [k.strip() for k in env.split(",") if k.strip()]
    return list(DEFAULT_SKIP_KEYWORDS)


def _extract_numbers(text: str, limit: int = 4) -> list[str]:
    out: list[str] = []
    text = text or ""
    for match in _NUM_UNIT_RE.finditer(text):
        value = match.group(0).strip()
        if value and value not in out:
            out.append(value)
        if len(out) >= limit:
            return out
    for match in _NUM_BARE_RE.finditer(text):
        value = match.group(0).strip()
        if value and value not in out and not any(value in item for item in out):
            out.append(value)
        if len(out) >= limit:
            break
    return out


def _canonical_key(link: Optional[str], title: Optional[str]) -> str:
    if link:
        key = link.split("#")[0].split("?")[0].lower()
        key = re.sub(r"^https?://(www\.)?", "", key).rstrip("/")
        if key:
            return key
    return re.sub(r"[\s\W_]+", "", (title or "").lower())[:120]


def _norm_for_match(text: str) -> str:
    return re.sub(r"[\s，。、；：！？,.;:!?\-—–·（）()【】\[\]“”\"'‘’]+", "", text or "").lower()


def _source_from_feed(feed_title: Optional[str]) -> str:
    text = (feed_title or "").strip()
    text = re.sub(r"^(Twitter|X|推特|微博|微博热搜)[\s:：/]*", "", text, flags=re.I)
    return text or "(unknown)"


def _heuristic_triage(title: Optional[str], summary: Optional[str],
                      feed_title: Optional[str], feed_tags: Optional[str]) -> dict:
    text = f"{title or ''} {summary or ''}"
    feed_title = feed_title or ""
    is_opinion = any(marker in text for marker in OPINION_MARKERS) or "@" in feed_title
    tags = [t.strip() for t in (feed_tags or "").split(",") if t.strip()]
    topic = "其他"
    if "AI科技" in tags or "技术" in tags:
        topic = "科技"
    has_number = bool(_extract_numbers(text, 1))
    value = 6 if has_number else 4
    if is_opinion:
        value = max(value, 6)
    return {
        "kind": "opinion" if is_opinion else "fact",
        "topic": topic,
        "has_number": has_number,
        "value": value,
    }


# ---- runs ----

def _create_run(slot: str, report_name: str, prev_report: Optional[str]) -> int:
    conn = get_conn()
    try:
        cur = conn.execute(
            """INSERT INTO rss_runs
               (slot, slot_date, report_name, prev_report, fetched_new, briefed,
                facts_used, opinions_used, status, websearch_used, started_at)
               VALUES (?, ?, ?, ?, 0, 0, 0, 0, 'briefed', 0, ?)""",
            (slot, _bj_now().strftime("%Y-%m-%d"), report_name, prev_report, int(time.time())),
        )
        conn.commit()
        return int(cur.lastrowid)
    finally:
        conn.close()


def _update_run(run_id: int, **fields: Any):
    if not fields:
        return
    conn = get_conn()
    try:
        sets = ", ".join(f"{k} = ?" for k in fields)
        conn.execute(f"UPDATE rss_runs SET {sets} WHERE id = ?", (*fields.values(), run_id))
        conn.commit()
    finally:
        conn.close()


def _get_run(run_id: Optional[int]) -> Optional[sqlite3.Row]:
    conn = get_conn()
    try:
        if run_id is None:
            return conn.execute("SELECT * FROM rss_runs ORDER BY id DESC LIMIT 1").fetchone()
        return conn.execute("SELECT * FROM rss_runs WHERE id = ?", (run_id,)).fetchone()
    finally:
        conn.close()


# ---- reports on disk ----

def _list_reports() -> list[Path]:
    return sorted(
        [p for p in _report_dir().glob("*.md") if re.match(r"^\d{2}-\d{2}_\d{2}\.md$", p.name)],
        key=lambda p: p.stat().st_mtime,
    )


def _prev_report(current_name: str) -> Optional[Path]:
    for path in reversed(_list_reports()):
        if path.name != current_name:
            return path
    return None


def _read_report(path: Optional[Path], limit: int = 12000) -> tuple[str, str]:
    if not path or not path.exists():
        return "", ""
    try:
        return path.name, path.read_text(encoding="utf-8")[:limit]
    except Exception:
        return path.name, ""


# ---- ledger ----

def _ledger_snapshot(limit_facts: int = 80, limit_opinions: int = 30) -> dict:
    conn = get_conn()
    try:
        facts = conn.execute(
            """SELECT id, text, numbers, status FROM rss_ledger_facts
               WHERE status IN ('confirmed', 'unconfirmed')
               ORDER BY updated_at DESC LIMIT ?""", (limit_facts,)).fetchall()
        opinions = conn.execute(
            """SELECT id, source, claim, evidence, checks, conclusion, trend, signal,
                      time_window, change FROM rss_ledger_opinions
               WHERE status = 'active' ORDER BY updated_at DESC LIMIT ?""",
            (limit_opinions,)).fetchall()
    finally:
        conn.close()
    return {
        "prev_facts": [{"id": r["id"], "text": r["text"], "status": r["status"]}
                       for r in facts if r["status"] == "confirmed"],
        "unconfirmed": [{"id": r["id"], "text": r["text"], "status": r["status"]}
                        for r in facts if r["status"] == "unconfirmed"],
        "active_opinions": [dict(r) for r in opinions],
    }


def _ledger_upsert_facts(parsed: dict, run_id: int) -> dict:
    added = updated = 0
    conn = get_conn()
    try:
        existing = conn.execute("SELECT id, text FROM rss_ledger_facts").fetchall()
        by_key = {_norm_for_match(r["text"]): r["id"] for r in existing}
        for fact in parsed["facts"]:
            if fact["continuation"]:
                text = re.sub(r"^延续[：:]\s*", "", fact["text"])
                match_id = by_key.get(_norm_for_match(text))
                if match_id:
                    conn.execute(
                        "UPDATE rss_ledger_facts SET last_seen_run = ?, updated_at = ? WHERE id = ?",
                        (run_id, int(time.time()), match_id))
                    updated += 1
                continue
            text = fact["text"]
            key = _norm_for_match(text)
            if not key:
                continue
            status = "unconfirmed" if "待确认" in text else "confirmed"
            match_id = by_key.get(key)
            if match_id:
                conn.execute(
                    "UPDATE rss_ledger_facts SET status = ?, last_seen_run = ?, updated_at = ? WHERE id = ?",
                    (status, run_id, int(time.time()), match_id))
                updated += 1
            else:
                conn.execute(
                    """INSERT INTO rss_ledger_facts
                       (text, numbers, status, first_seen_run, last_seen_run, created_at, updated_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    (text[:160], json.dumps(_extract_numbers(text), ensure_ascii=False),
                     status, run_id, run_id, int(time.time()), int(time.time())))
                by_key[key] = int(conn.execute("SELECT last_insert_rowid() AS i").fetchone()["i"])
                added += 1
        conn.commit()
    finally:
        conn.close()
    return {"facts_added": added, "facts_updated": updated}


_CHANGE_MAP = [("反转", "reversal"), ("强化", "reinforce"), ("削弱", "weaken"),
               ("新出现", "new"), ("新增", "new")]


def _map_change(text: str) -> str:
    for marker, value in _CHANGE_MAP:
        if marker in (text or ""):
            return value
    return "new"


def _extract_source(claim: str) -> str:
    match = re.search(r"@[A-Za-z0-9_\-\.\u4e00-\u9fff]+", claim or "")
    if match:
        return match.group(0)
    match = re.search(r"财新《([^》]+)》", claim or "")
    if match:
        return f"财新《{match.group(1)}》"
    match = re.search(r"^([A-Za-z0-9_\-\.\u4e00-\u9fff]{2,20})\s*(?:认为|表示|指出|称)", claim or "")
    if match:
        return match.group(1)
    return "(未署名)"


def _ledger_upsert_opinions(parsed: dict, run_id: int) -> dict:
    added = updated = 0
    conn = get_conn()
    try:
        for op in parsed["opinions"]:
            fields = op["fields"]
            claim = (fields.get("观点：") or "").strip()
            if not claim:
                continue
            source = _extract_source(claim)
            change = _map_change(fields.get("与上期相比：", ""))
            trend = (fields.get("趋势：") or "")[:300]
            rows = conn.execute(
                """SELECT id, claim FROM rss_ledger_opinions
                   WHERE source = ? AND status = 'active'""", (source,)).fetchall()
            claim_key = _norm_for_match(claim)
            match_id, best = None, 0.0
            for row in rows:
                ratio = difflib.SequenceMatcher(
                    None, claim_key, _norm_for_match(row["claim"])).ratio()
                if ratio >= 0.6 and ratio > best:
                    best, match_id = ratio, row["id"]
            values = (claim[:240], (fields.get("依据：") or "")[:240],
                      (fields.get("检验：") or "")[:600], (fields.get("结论：") or "")[:240],
                      trend, trend, trend, change)
            if match_id:
                conn.execute(
                    """UPDATE rss_ledger_opinions SET claim = ?, evidence = ?, checks = ?,
                       conclusion = ?, trend = ?, signal = ?, time_window = ?, change = ?,
                       last_seen_run = ?, updated_at = ? WHERE id = ?""",
                    (*values, run_id, int(time.time()), match_id))
                updated += 1
            else:
                conn.execute(
                    """INSERT INTO rss_ledger_opinions
                       (source, claim, evidence, checks, conclusion, trend, signal, time_window,
                        change, status, first_seen_run, last_seen_run, created_at, updated_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'active', ?, ?, ?, ?)""",
                    (source, *values, run_id, run_id, int(time.time()), int(time.time())))
                added += 1
        conn.commit()
    finally:
        conn.close()
    return {"opinions_added": added, "opinions_updated": updated}


def _retire_stale_opinions(run_id: int, after_runs: Optional[int] = None) -> int:
    """Mark active opinions retired when they were not touched for N runs."""
    threshold = int(after_runs if after_runs is not None
                    else (os.getenv("RSS_OPINION_RETIRE_RUNS", "8") or 8))
    conn = get_conn()
    try:
        cur = conn.execute(
            """UPDATE rss_ledger_opinions SET status = 'retired', updated_at = ?
               WHERE status = 'active' AND last_seen_run IS NOT NULL AND last_seen_run < ?""",
            (int(time.time()), run_id - threshold))
        conn.commit()
        return cur.rowcount
    finally:
        conn.close()


def _group_facts(facts: list[dict], threshold: float = 0.62) -> list[dict]:
    """Merge same-event facts from different sources into one candidate
    (representative keeps the best value, dup_count/dup_sources expose the rest)."""
    groups: list[list[dict]] = []
    keys: list[str] = []
    for fact in facts:
        key = _norm_for_match(fact.get("title") or "")
        placed = False
        for idx, existing in enumerate(keys):
            if not existing or not key:
                continue
            if difflib.SequenceMatcher(None, key, existing).ratio() >= threshold:
                groups[idx].append(fact)
                placed = True
                break
        if not placed:
            keys.append(key)
            groups.append([fact])
    out: list[dict] = []
    for group in groups:
        rep = max(group, key=lambda x: (int(x.get("value") or 0), len(x.get("numbers") or [])))
        others = [x for x in group if x.get("id") != rep.get("id")]
        if others:
            rep = dict(rep)
            rep["dup_count"] = len(others)
            rep["dup_sources"] = sorted({str(x.get("feed") or "") for x in others})
            merged = list(rep.get("numbers") or [])
            for other in others:
                for number in other.get("numbers") or []:
                    if number not in merged:
                        merged.append(number)
            rep["numbers"] = merged[:6]
        out.append(rep)
    return out


# ---- report parsing / validation ----

def _parse_report(body: str) -> dict:
    out: dict = {"facts": [], "opinions": [], "section": None}
    current: Optional[dict] = None
    for idx, raw in enumerate(body.splitlines(), 1):
        line = raw.rstrip()
        stripped = line.strip()
        if _SECTION_FACTS_RE.match(stripped):
            out["section"], current = "facts", None
            continue
        if _SECTION_OPINIONS_RE.match(stripped):
            out["section"], current = "opinions", None
            continue
        if out["section"] == "facts":
            match = _FACT_LINE_RE.match(line)
            text = (match.group(1) if match else line).strip()
            if not text:
                continue
            out["facts"].append({"line": idx, "text": text,
                                 "continuation": text.startswith("延续")})
        elif out["section"] == "opinions":
            if stripped.startswith("观点：") or stripped.startswith("- 观点：") \
                    or stripped.startswith("* 观点："):
                current = {"line": idx, "blocks": [stripped], "fields": {}}
                out["opinions"].append(current)
            elif current is not None and stripped:
                current["blocks"].append(stripped)
    for op in out["opinions"]:
        joined = "\n".join(op["blocks"])
        for field in _OPINION_FIELDS:
            match = re.search(rf"{re.escape(field)}\s*(.*)", joined)
            if match:
                op["fields"][field] = match.group(1).strip()
    return out


def _validate_report(body: str) -> tuple[dict, list[dict]]:
    parsed = _parse_report(body)
    violations: list[dict] = []
    total = _count_hanzi(body)
    if total > 2500:
        violations.append({"line": 0, "kind": "budget", "why": f"全文 {total} 汉字 > 2500"})
    if not _SECTION_FACTS_RE.search(body):
        violations.append({"line": 0, "kind": "structure", "why": "缺少「一、资讯速览」标题"})
    if not _SECTION_OPINIONS_RE.search(body):
        violations.append({"line": 0, "kind": "structure", "why": "缺少「二、观点与趋势」标题"})
    skip_kw = _skip_keywords()
    for fact in parsed["facts"]:
        count = _count_hanzi(fact["text"])
        limit = 20 if fact["continuation"] else 40
        if count > limit:
            violations.append({"line": fact["line"], "kind": "length",
                               "why": f"速览行 {count} 字 > {limit}"})
        if fact["continuation"]:
            continue
        for marker in FACT_OPINION_MARKERS:
            if marker in fact["text"]:
                violations.append({"line": fact["line"], "kind": "mixed",
                                   "why": f"速览行含观点标记「{marker}」"})
        for keyword in skip_kw:
            if keyword in fact["text"]:
                violations.append({"line": fact["line"], "kind": "skip",
                                   "why": f"速览行含体育娱乐词「{keyword}」"})
    for op in parsed["opinions"]:
        content = "\n".join(op["blocks"])
        for field in _OPINION_FIELDS:
            content = content.replace(field, "")
        content = re.sub(r"^[-*·]\s*", "", content, flags=re.M)
        count = _count_hanzi(content)
        if count > 200:
            violations.append({"line": op["line"], "kind": "length",
                               "why": f"观点块 {count} 字 > 200"})
        missing = [f for f in _OPINION_FIELDS if f not in op["fields"]]
        if missing:
            violations.append({"line": op["line"], "kind": "format",
                               "why": "缺字段：" + "、".join(missing)})
    if len(parsed["opinions"]) > 3:
        violations.append({"line": 0, "kind": "count",
                           "why": f"观点 {len(parsed['opinions'])} 条 > 3"})
    for idx, raw in enumerate(body.splitlines(), 1):
        if re.search(r"https?://", raw):
            violations.append({"line": idx, "kind": "link", "why": "含链接"})
        for word in FORBIDDEN_PHRASES:
            if word in raw:
                violations.append({"line": idx, "kind": "tone", "why": f"禁用词「{word}」"})
    return parsed, violations


def _read_stdin_json(default: Any = None) -> Any:
    try:
        raw = sys.stdin.read()
        if not raw.strip():
            return default
        return json.loads(raw)
    except Exception:
        return default


# ---- commands ----

def cmd_brief(max_items: Optional[int] = None, do_fetch: bool = True) -> tuple[str, dict]:
    max_items = max(1, min(int(max_items or os.getenv("RSS_BRIEF_MAX", "250") or 250),
                           MAX_ITEM_LIMIT))
    now = _bj_now()
    slot = "morning" if now.hour < 12 else "evening"
    report_name = now.strftime("%m-%d_%H") + ".md"
    prev_name, prev_text = _read_report(_prev_report(report_name))
    run_id = _create_run(slot, report_name, prev_name or None)

    fetch_stats: Optional[dict] = None
    if do_fetch:
        fetch_stats = fetch_all_feeds_guarded()
        _update_run(run_id, fetched_new=int(fetch_stats["total_new_items"]))

    conn = get_conn()
    try:
        rows = conn.execute(
            """SELECT i.*, f.title AS _feed_title, f.tags AS _feed_tags
               FROM rss_items i JOIN rss_feeds f ON f.id = i.feed_id
               WHERE i.is_read = 0
               ORDER BY COALESCE(i.published, i.created_at) DESC
               LIMIT ?""", (max_items,)).fetchall()
    finally:
        conn.close()

    skip_kw = _skip_keywords()
    seen: set[str] = set()
    items: list[dict] = []
    dropped_ids: list[int] = []
    skipped = deduped = 0
    for row in rows:
        title = row["title"] or ""
        summary = _to_text(row["summary"] or row["content"], 600)
        key = _canonical_key(row["link"], title)
        if key in seen:
            deduped += 1
            dropped_ids.append(int(row["id"]))
            continue
        seen.add(key)
        text = f"{title} {summary}"
        is_skipped = any(keyword in text for keyword in skip_kw)
        if is_skipped:
            skipped += 1
        triage = _heuristic_triage(title, summary, row["_feed_title"], row["_feed_tags"])
        numbers = _extract_numbers(text)
        items.append({
            "id": row["id"],
            "title": title,
            "link": row["link"],
            "feed": row["_feed_title"] or "",
            "tags": row["_feed_tags"] or "",
            "published_bj": _fmt_bj(row["published"] or row["created_at"]),
            "summary": summary,
            "kind": triage["kind"],
            "topic": triage["topic"],
            "has_number": triage["has_number"],
            "value": triage["value"],
            "numbers": numbers,
            "is_skipped": is_skipped,
        })

    conn = get_conn()
    try:
        if dropped_ids:
            marks = ",".join("?" for _ in dropped_ids)
            conn.execute(f"UPDATE rss_items SET is_read = 1 WHERE id IN ({marks})", dropped_ids)
        for item in items:
            conn.execute(
                """UPDATE rss_items SET brief_run_id = ?, kind = ?, topic = ?, has_number = ?,
                   verify_needed = 0, is_skipped = ?, triage_json = ?, triaged_at = ?
                   WHERE id = ?""",
                (run_id, item["kind"], item["topic"], int(item["has_number"]),
                 int(item["is_skipped"]),
                 json.dumps({"value": item["value"], "numbers": item["numbers"],
                             "source": "heuristic"}, ensure_ascii=False),
                 int(time.time()), item["id"]))
        conn.commit()
    finally:
        conn.close()
    _update_run(run_id, briefed=len(items))

    unread_total = get_unread_count()
    data = {
        "run_id": run_id,
        "session": {"slot": slot, "beijing": now.strftime("%m-%d %H:%M"),
                    "report_name": report_name, "prev_report": prev_name or "无"},
        "counts": {"fetched_new": int((fetch_stats or {}).get("total_new_items", 0)),
                   "unread_total": unread_total, "selected": len(items),
                   "skipped": skipped, "deduped": deduped},
        "feeds_failed": [f for f in (fetch_stats or {}).get("feeds", []) if f.get("error")],
        "items": items,
        "ledger": _ledger_snapshot(),
        "last_report": {"name": prev_name or "无", "text": prev_text},
    }
    text = (f"✅ Brief ready (run {run_id})\n"
            f"Beijing: {data['session']['beijing']} | slot: {slot}\n"
            f"Fetched new: {data['counts']['fetched_new']} | unread total: {unread_total} | "
            f"selected: {len(items)} | skipped: {skipped} | deduped: {deduped}\n"
            f"Prev report: {prev_name or '(none)'}\n"
            f"Next: rss action=payload run={run_id}")
    return text, data


def cmd_payload(run_id: Optional[int] = None) -> tuple[str, dict]:
    run = _get_run(run_id)
    if run is None:
        return "❌ No run found", {}
    rid = int(run["id"])
    conn = get_conn()
    try:
        rows = conn.execute(
            """SELECT i.*, f.title AS _feed_title, f.tags AS _feed_tags
               FROM rss_items i JOIN rss_feeds f ON f.id = i.feed_id
               WHERE i.brief_run_id = ? AND i.is_read = 0
               ORDER BY COALESCE(i.published, i.created_at) DESC""", (rid,)).fetchall()
    finally:
        conn.close()

    facts_max = int(os.getenv("RSS_BRIEF_FACTS_MAX", "60") or 60)
    op_top = int(os.getenv("RSS_BRIEF_OPINION_TOP", "10") or 10)
    op_chars = int(os.getenv("RSS_BRIEF_OPINION_CHARS", "1200") or 1200)
    facts: list[dict] = []
    opinions: list[dict] = []
    dropped_none = 0
    for row in rows:
        if row["is_skipped"]:
            continue
        try:
            triage = json.loads(row["triage_json"]) if row["triage_json"] else {}
        except Exception:
            triage = {}
        kind = row["kind"] or triage.get("kind") or "fact"
        base = {
            "id": row["id"], "title": row["title"],
            "feed": row["_feed_title"] or "", "tags": row["_feed_tags"] or "",
            "published_bj": _fmt_bj(row["published"] or row["created_at"]),
            "topic": row["topic"] or "其他", "has_number": bool(row["has_number"]),
            "numbers": triage.get("numbers", []), "value": triage.get("value", 5),
            "flags": triage.get("flags", []),
        }
        if triage.get("repeat_of"):
            base["repeat_of"] = triage["repeat_of"]
        if "new_number" in triage:
            base["new_number"] = triage["new_number"]
        if kind in ("fact", "mixed"):
            facts.append(dict(base, summary=_to_text(row["summary"] or row["content"], 300)))
        if kind in ("opinion", "mixed"):
            relation = triage.get("relation") or "new"
            relation_conf = float(triage.get("relation_confidence") or 0.0)
            # “没变化就不重复”：高置信的 none 直接不进观点池；
            # 低置信的 none 不采信，改为 new + 标记，交给主模型判断。
            if relation == "none":
                if relation_conf >= 0.7:
                    dropped_none += 1
                    continue
                relation = "new"
                triage_flags = list(base.get("flags") or [])
                if "relation_uncertain" not in triage_flags:
                    triage_flags.append("relation_uncertain")
                base["flags"] = triage_flags
            entry = dict(
                base, source=_source_from_feed(row["_feed_title"]),
                text=_to_text(row["content"] or row["summary"], op_chars),
                relation=relation,
                change_hint=triage.get("change_hint", ""),
                verify_needed=bool(row["verify_needed"]))
            if relation_conf:
                entry["relation_confidence"] = round(relation_conf, 3)
            opinions.append(entry)

    facts_total, opinions_total = len(facts), len(opinions)
    facts = _group_facts(facts)
    facts = sorted(facts, key=lambda x: (-int(x["value"] or 0), x["published_bj"]))[:facts_max]
    opinions = sorted(opinions, key=lambda x: -int(x["value"] or 0))[:op_top]
    prev_name = run["prev_report"] or ""
    prev_text = ""
    if prev_name:
        prev_text = _read_report(_report_dir() / prev_name)[1]
    if not prev_name:
        prev_name, prev_text = _read_report(_prev_report(run["report_name"] or ""))
    data = {
        "run_id": rid,
        "session": {"slot": run["slot"], "report_name": run["report_name"],
                    "prev_report": run["prev_report"] or "无"},
        "counts": {"fetched_new": run["fetched_new"], "briefed": run["briefed"], 
                   "facts_total": facts_total, "opinions_total": opinions_total,
                   "opinions_omitted": max(0, opinions_total - dropped_none - len(opinions)),
                   "opinions_dropped_no_change": dropped_none},
        "facts": facts,
        "opinions": opinions,
        "ledger": _ledger_snapshot(),
        "last_report": {"name": prev_name or "无", "text": prev_text},
        "next_steps": [
            "只用本 payload 写草稿：速览行 ≤40 字、观点六字段、观点 ≤3 条、全文 ≤2500 汉字",
            "写完调用 rss {action: qa, draft: <草稿>}，按 violations 修正",
            "调用 rss {action: commit, body: <修订稿>}，最终只输出返回的 reportText",
        ],
    }
    text = (f"✅ Payload ready (run {rid}): facts {len(facts)}/{facts_total}, "
            f"opinions {len(opinions)}/{opinions_total}")
    return text, data


def cmd_annotate(rows: Any) -> tuple[str, dict]:
    if not isinstance(rows, list):
        return "❌ annotate expects a JSON array on stdin", {"updated": 0}
    updated = 0
    conn = get_conn()
    try:
        for row in rows:
            if not isinstance(row, dict) or not row.get("id"):
                continue
            item_id = int(row["id"])
            sets = ["triaged_at = ?"]
            args: list = [int(time.time())]
            if row.get("kind"):
                sets.append("kind = ?")
                args.append(row["kind"])
            if row.get("topic"):
                sets.append("topic = ?")
                args.append(row["topic"])
            if "has_number" in row:
                sets.append("has_number = ?")
                args.append(int(bool(row["has_number"])))
            if "verify_needed" in row:
                sets.append("verify_needed = ?")
                args.append(int(bool(row["verify_needed"])))
            if "is_skipped" in row:
                sets.append("is_skipped = ?")
                args.append(int(bool(row["is_skipped"])))
            triage = {k: row[k] for k in ("value", "numbers", "confidence", "relation",
                                          "relation_confidence", "change_hint", "source",
                                          "flags", "repeat_of", "new_number",
                                          "kind_source", "note") if k in row}
            existing = conn.execute("SELECT triage_json FROM rss_items WHERE id = ?",
                                    (item_id,)).fetchone()
            merged: dict = {}
            if existing and existing["triage_json"]:
                try:
                    merged = json.loads(existing["triage_json"]) or {}
                except Exception:
                    merged = {}
            merged.update(triage)
            sets.append("triage_json = ?")
            args.append(json.dumps(merged, ensure_ascii=False) if merged else None)
            args.append(item_id)
            conn.execute(f"UPDATE rss_items SET {', '.join(sets)} WHERE id = ?", args)
            updated += 1
        conn.commit()
    finally:
        conn.close()
    return f"✅ Annotated {updated} item(s)", {"updated": updated}


def cmd_check(body: str) -> tuple[str, dict]:
    parsed, violations = _validate_report(body or "")
    facts = len([f for f in parsed["facts"] if not f["continuation"]])
    continuations = len(parsed["facts"]) - facts
    data = {
        "violations": violations,
        "stats": {"hanzi": _count_hanzi(body or ""), "facts": facts,
                  "continuations": continuations, "opinions": len(parsed["opinions"])},
    }
    if violations:
        text = "❌ " + f"{len(violations)} violation(s):\n" + "\n".join(
            f"  line {v['line']}: [{v['kind']}] {v['why']}" for v in violations)
    else:
        text = (f"✅ No violations (hanzi {data['stats']['hanzi']}/2500, "
                f"facts {facts}, opinions {len(parsed['opinions'])})")
    return text, data


def _commit_kwargs() -> dict:
    payload = _read_stdin_json(default={}) or {}
    return {
        "body": payload.get("body", ""),
        "run_id": payload.get("run_id"),
        "strict": bool(payload.get("strict", True)),
        "websearch_used": int(payload.get("websearch_used", 0) or 0),
    }


def cmd_commit(body: str, run_id: Optional[int] = None, strict: bool = True,
               websearch_used: int = 0) -> tuple[str, dict]:
    body = (body or "").strip()
    if not body:
        return "❌ Empty body", {"ok": False, "violations": [
            {"line": 0, "kind": "empty", "why": "正文为空"}]}
    parsed, violations = _validate_report(body)
    if violations and strict:
        text = "❌ Commit blocked by violations:\n" + "\n".join(
            f"  line {v['line']}: [{v['kind']}] {v['why']}" for v in violations)
        return text, {"ok": False, "violations": violations}
    run = _get_run(run_id)
    if run is None:
        return "❌ No brief run found", {"ok": False,
                                         "violations": [{"line": 0, "kind": "state",
                                                         "why": "没有可提交的 brief run"}]}
    rid = int(run["id"])
    body_clean = re.sub(r"^\s*#\s*\d{2}-\d{2}[ _]\d{2}\s*[｜|][^\n]*\n+", "", body, count=1)
    facts_used = len([f for f in parsed["facts"] if not f["continuation"]])
    opinions_used = len(parsed["opinions"])
    name = run["report_name"] or (_bj_now().strftime("%m-%d_%H") + ".md")
    prev = run["prev_report"] or "无"
    stamp = name[:-3].replace("_", " ")
    header = (f"# {stamp}｜本期使用 {facts_used + opinions_used} 条"
              f"（速览 {facts_used} 条 + 观点 {opinions_used} 条）"
              f"｜本次新抓取 {int(run['fetched_new'] or 0)} 条｜上期：{prev}")
    final = f"{header}\n\n{body_clean}\n"
    path = _report_dir() / name
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(final, encoding="utf-8")
    tmp.replace(path)

    ledger: dict = {}
    ledger.update(_ledger_upsert_facts(parsed, rid))
    ledger.update(_ledger_upsert_opinions(parsed, rid))
    ledger["opinions_retired"] = _retire_stale_opinions(rid)

    conn = get_conn()
    try:
        cur = conn.execute("UPDATE rss_items SET is_read = 1 WHERE brief_run_id = ?", (rid,))
        marked = cur.rowcount
        conn.commit()
    finally:
        conn.close()
    _update_run(rid, status="committed", facts_used=facts_used, opinions_used=opinions_used,
                report_path=str(path), websearch_used=int(websearch_used or 0),
                violations=json.dumps(violations, ensure_ascii=False) if violations else None,
                finished_at=int(time.time()))
    data = {
        "ok": True, "report_path": str(path), "reportText": final, "header": header,
        "counts": {"facts": facts_used, "opinions": opinions_used, "marked_read": marked},
        "ledger": ledger, "violations": violations,
    }
    return final, data


def cmd_sample(n: int = 50, unread_only: bool = False) -> tuple[str, dict]:
    """Random item sample for jev calibration (no DB writes)."""
    n = max(1, min(int(n), 500))
    conn = get_conn()
    try:
        rows = conn.execute(
            """SELECT i.id, i.title, i.summary, i.content, i.is_read,
                      f.title AS _feed_title, f.tags AS _feed_tags
               FROM rss_items i JOIN rss_feeds f ON f.id = i.feed_id
               WHERE (? = 0 OR i.is_read = 0)
               ORDER BY RANDOM() LIMIT ?""",
            (1 if unread_only else 0, n)).fetchall()
    finally:
        conn.close()
    items = [{
        "id": r["id"], "title": r["title"] or "",
        "feed": r["_feed_title"] or "", "tags": r["_feed_tags"] or "",
        "summary": _to_text(r["summary"] or r["content"], 400),
        "is_read": bool(r["is_read"]),
    } for r in rows]
    return f"✅ Sampled {len(items)} item(s)", {"items": items}


def cmd_ledger() -> tuple[str, dict]:
    snapshot = _ledger_snapshot(limit_facts=1000, limit_opinions=500)
    text = (f"📒 Ledger: {len(snapshot['prev_facts'])} confirmed facts | "
            f"{len(snapshot['unconfirmed'])} unconfirmed | "
            f"{len(snapshot['active_opinions'])} active opinions")
    return text, snapshot


def mark_read_ids(ids: list[int]) -> int:
    ids = [int(i) for i in ids if i]
    if not ids:
        return 0
    conn = get_conn()
    try:
        placeholders = ",".join("?" for _ in ids)
        cur = conn.execute(
            f"UPDATE rss_items SET is_read = 1 WHERE id IN ({placeholders}) AND is_read = 0", ids)
        conn.commit()
        return cur.rowcount
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def _parse_ts(s: str) -> Optional[int]:
    """Parse a unix timestamp or ISO date/datetime into a unix timestamp."""
    s = s.strip()
    try:
        return int(s)
    except ValueError:
        pass
    try:
        dt = datetime.fromisoformat(s.replace("T", " "))
        return int(dt.timestamp())
    except ValueError:
        return None


def _cmd_markread_dispatch(a) -> Any:
    ids = [int(x) for x in (getattr(a, "ids", None) or "").replace(" ", "").split(",") if x.strip()]
    if ids:
        count = mark_read_ids(ids)
        return f"✅ Marked {count} item(s) as read", {"marked": count}
    return cmd_markread(a.item_id, a.tag, a.older_than, a.before)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="rss.py", description="pi-agent-rss aggregator")
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--json", action="store_true", help="Machine-readable JSON output")
    sub = parser.add_subparsers(dest="action", required=True)

    p = sub.add_parser("add", parents=[common], help="Add a subscription (optional -t tags)")
    p.add_argument("url")
    p.add_argument("-t", "--tags", default="", help="Comma-separated tags, e.g. -t tech,news")
    p.set_defaults(func=lambda a: cmd_add(a.url, a.tags))

    p = sub.add_parser("fetch", parents=[common], help="Fetch all enabled subscriptions")
    p.set_defaults(func=lambda a: cmd_fetch())

    p = sub.add_parser("tag", parents=[common], help="Set/replace tags on a feed (no -t shows current tags)")
    p.add_argument("feed_id", type=int)
    p.add_argument("-t", "--tags", default="", help="Comma-separated tags, e.g. -t tech,news")
    p.set_defaults(func=lambda a: cmd_tag(a.feed_id, a.tags))

    p = sub.add_parser("tags", parents=[common], help="List all tags with feed counts")
    p.set_defaults(func=lambda a: cmd_tags())

    p = sub.add_parser("unread", parents=[common], help="List unread items (optional -t tag / -f feed filters)")
    p.add_argument("-l", "--limit", type=int, default=20)
    p.add_argument("-t", "--tag", default=None, help="Only items from feeds with this tag")
    p.add_argument("-f", "--feed-id", type=int, default=None, help="Only items from this feed")
    p.set_defaults(func=lambda a: cmd_unread(a.limit, a.tag, a.feed_id))

    p = sub.add_parser("search", parents=[common], help="Full-text search (optional -t tag / -f feed filters)")
    p.add_argument("query")
    p.add_argument("-l", "--limit", type=int, default=50)
    p.add_argument("-t", "--tag", default=None, help="Only results from feeds with this tag")
    p.add_argument("-f", "--feed-id", type=int, default=None, help="Only results from this feed")
    p.set_defaults(func=lambda a: cmd_search(a.query, a.limit, a.tag, a.feed_id))

    p = sub.add_parser("recent", parents=[common], help="Recent items from a feed (or all), regardless of read state")
    p.add_argument("-f", "--feed-id", type=int, default=None)
    p.add_argument("-l", "--limit", type=int, default=20)
    p.set_defaults(func=lambda a: cmd_recent(a.feed_id, a.limit))

    p = sub.add_parser("markread", parents=[common], help="Mark as read: all, one item, ids, or filtered by tag/time")
    p.add_argument("item_id", nargs="?", type=int, default=None)
    p.add_argument("-t", "--tag", default=None, help="Only items from feeds with this tag")
    p.add_argument("--ids", default=None, help="Comma-separated item IDs to mark read")
    p.add_argument("--older-than", type=int, default=None, metavar="HOURS",
                   help="Only unread items published more than HOURS hours ago")
    p.add_argument("--before", default=None, metavar="TS",
                   help="Only unread items before a unix timestamp or ISO date")
    p.set_defaults(func=_cmd_markread_dispatch)

    p = sub.add_parser("stats", parents=[common], help="Per-feed and per-tag stats (item/unread counts, fetch health)")
    p.set_defaults(func=lambda a: cmd_stats())

    p = sub.add_parser("list", parents=[common], help="List subscriptions (optional -t tag filter)")
    p.add_argument("-t", "--tag", default=None, help="Only feeds with this tag")
    p.set_defaults(func=lambda a: cmd_list(a.tag))

    p = sub.add_parser("remove", parents=[common], help="Remove a subscription (cascades to its items)")
    p.add_argument("feed_id", type=int)
    p.set_defaults(func=lambda a: cmd_remove(a.feed_id))

    # ---- report pipeline (v0.5) ----
    p = sub.add_parser("brief", parents=[common],
                       help="Begin a report run: fetch + select unread + heuristic triage")
    p.add_argument("--max", type=int, default=None, help="Max items selected (default RSS_BRIEF_MAX=250)")
    p.add_argument("--no-fetch", action="store_true", help="Skip fetching feeds")
    p.set_defaults(func=lambda a: cmd_brief(a.max, not a.no_fetch))

    p = sub.add_parser("payload", parents=[common],
                       help="Assemble the report payload for a brief run")
    p.add_argument("--run", type=int, default=None, help="Run ID (default: latest)")
    p.set_defaults(func=lambda a: cmd_payload(a.run))

    p = sub.add_parser("annotate", parents=[common],
                       help="Write triage results back (JSON array on stdin)")
    p.set_defaults(func=lambda a: cmd_annotate(_read_stdin_json(default=[]) or []))

    p = sub.add_parser("check", parents=[common],
                       help="Validate a draft report (JSON {body} on stdin)")
    p.set_defaults(func=lambda a: cmd_check((_read_stdin_json(default={}) or {}).get("body", "")))

    p = sub.add_parser("commit", parents=[common],
                       help="Finalize the report: validate, write, ledger, mark read")
    p.set_defaults(func=lambda a: cmd_commit(**_commit_kwargs()))

    p = sub.add_parser("ledger", parents=[common], help="Show the fact/opinion ledger")
    p.set_defaults(func=lambda a: cmd_ledger())

    p = sub.add_parser("sample", parents=[common],
                       help="Random item sample for jev calibration (no DB writes)")
    p.add_argument("--n", type=int, default=50, help="Sample size (default 50)")
    p.add_argument("--unread", action="store_true", help="Sample unread items only")
    p.set_defaults(func=lambda a: cmd_sample(a.n, a.unread))

    return parser


def main(argv: list[str] | None = None) -> int:
    init_db()
    args = build_parser().parse_args(argv)
    json_mode = bool(getattr(args, "json", False))
    try:
        result = args.func(args)
    except Exception as exc:
        if json_mode:
            print(json.dumps({"ok": False, "action": args.action, "data": None,
                              "error": f"{type(exc).__name__}: {exc}"}, ensure_ascii=False))
        else:
            print(f"❌ {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1
    if isinstance(result, tuple):
        text, data = result
    else:
        text, data = result, None
    if json_mode:
        print(json.dumps({"ok": True, "action": args.action, "data": data, "error": None},
                         ensure_ascii=False))
    else:
        print(text)
    return 0


if __name__ == "__main__":
    sys.exit(main())
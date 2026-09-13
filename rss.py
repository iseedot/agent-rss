#!/usr/bin/env python3
"""pi-agent-rss - single-file RSS aggregator (backend for the pi agent extension)

Design:
- Depends only on the Python standard library + feedparser (pure Python, no compilation)
- Fixed database schema: rss_feeds / rss_items / rss_items_fts
- Database defaults to <pi config dir>/rss-data/rss.db; override with RSS_DB_PATH

Commands: add / fetch / unread / search / markread / list / remove / tag / tags
Feeds carry comma-separated tags (rss_feeds.tags) for category filtering
(unread/search/list accept -t to filter by tag).
"""
from __future__ import annotations

import argparse
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
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

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


def search_items(query: str, limit: int = 50, tag: Optional[str] = None) -> list[Item]:
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
    headers = {"User-Agent": "pi-agent-rss/0.3.0", "Accept-Encoding": "gzip"}
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


def fetch_all_feeds() -> dict:
    feeds = get_all_feeds(enabled_only=True)
    new_total = 0
    failed = 0
    for feed in feeds:
        n = fetch_feed(feed)
        if n == 0 and feed.id is not None:
            conn = get_conn()
            try:
                row = conn.execute("SELECT fetch_error FROM rss_feeds WHERE id = ?", (feed.id,)).fetchone()
            finally:
                conn.close()
            if row and row["fetch_error"]:
                failed += 1
        new_total += n
    return {
        "total_feeds": len(feeds),
        "success": len(feeds) - failed,
        "failed": failed,
        "total_new_items": new_total,
    }


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


def cmd_fetch() -> str:
    stats = fetch_all_feeds()
    lines = [
        "✅ Fetch complete",
        f"Total feeds: {stats['total_feeds']}",
        f"Succeeded: {stats['success']}",
        f"Failed: {stats['failed']}",
        f"New items: {stats['total_new_items']}",
    ]
    if stats["failed"] > 0:
        conn = get_conn()
        try:
            failed = conn.execute(
                "SELECT id, url, fetch_error FROM rss_feeds WHERE fetch_error IS NOT NULL LIMIT 5"
            ).fetchall()
        finally:
            conn.close()
        lines.append("\nFailed feeds:")
        for feed in failed:
            lines.append(f"  - ID {feed['id']}: {feed['fetch_error']}")
    return "\n".join(lines)


def cmd_unread(limit: int, tag: Optional[str] = None) -> str:
    items = get_items(unread_only=True, limit=limit, tag=tag)
    if not items:
        return f"No unread items{' with tag ' + tag if tag else ''}"
    header = f"📰 Unread items ({len(items)})" + (f" tagged '{tag}'" if tag else "")
    return "\n".join([header, ""] + _pretty_items(items)
                     + ["💡 Tip: use markread after reading"])


def cmd_search(query: str, limit: int, tag: Optional[str] = None) -> str:
    items = search_items(query, limit=limit, tag=tag)
    if not items:
        return f"No items found for '{query}'"
    header = f"🔍 Search results: '{query}' ({len(items)})" + (f" tagged '{tag}'" if tag else "")
    return "\n".join([header, ""] + _pretty_items(items))


def cmd_markread(item_id: Optional[int]) -> str:
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
    count = mark_all_read()
    return f"✅ Marked {count} item(s) as read" if count else "No unread items to mark"


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
# Entry point
# ---------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="rss.py", description="pi-agent-rss aggregator")
    sub = parser.add_subparsers(dest="action", required=True)

    p = sub.add_parser("add", help="Add a subscription (optional -t tags)")
    p.add_argument("url")
    p.add_argument("-t", "--tags", default="", help="Comma-separated tags, e.g. -t tech,news")
    p.set_defaults(func=lambda a: cmd_add(a.url, a.tags))

    p = sub.add_parser("fetch", help="Fetch all enabled subscriptions")
    p.set_defaults(func=lambda a: cmd_fetch())

    p = sub.add_parser("tag", help="Set/replace tags on a feed (no -t shows current tags)")
    p.add_argument("feed_id", type=int)
    p.add_argument("-t", "--tags", default="", help="Comma-separated tags, e.g. -t tech,news")
    p.set_defaults(func=lambda a: cmd_tag(a.feed_id, a.tags))

    p = sub.add_parser("tags", help="List all tags with feed counts")
    p.set_defaults(func=lambda a: cmd_tags())

    p = sub.add_parser("unread", help="List unread items (optional -t tag filter)")
    p.add_argument("-l", "--limit", type=int, default=20)
    p.add_argument("-t", "--tag", default=None, help="Only items from feeds with this tag")
    p.set_defaults(func=lambda a: cmd_unread(a.limit, a.tag))

    p = sub.add_parser("search", help="Full-text search (optional -t tag filter)")
    p.add_argument("query")
    p.add_argument("-l", "--limit", type=int, default=50)
    p.add_argument("-t", "--tag", default=None, help="Only results from feeds with this tag")
    p.set_defaults(func=lambda a: cmd_search(a.query, a.limit, a.tag))

    p = sub.add_parser("markread", help="Mark as read (all unread by default)")
    p.add_argument("item_id", nargs="?", type=int, default=None)
    p.set_defaults(func=lambda a: cmd_markread(a.item_id))

    p = sub.add_parser("list", help="List subscriptions (optional -t tag filter)")
    p.add_argument("-t", "--tag", default=None, help="Only feeds with this tag")
    p.set_defaults(func=lambda a: cmd_list(a.tag))

    p = sub.add_parser("remove", help="Remove a subscription (cascades to its items)")
    p.add_argument("feed_id", type=int)
    p.set_defaults(func=lambda a: cmd_remove(a.feed_id))

    return parser


def main(argv: list[str] | None = None) -> int:
    init_db()
    args = build_parser().parse_args(argv)
    print(args.func(args))
    return 0


if __name__ == "__main__":
    sys.exit(main())
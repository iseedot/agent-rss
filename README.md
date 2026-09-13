# pi-agent-rss

An RSS aggregation plugin for pi agent: manage RSS subscriptions through conversation —
add feeds, fetch updates, read unread items, full-text search, and mark items as read.

```
rss/                (repository root = pi package root)
├── index.ts        Extension entry: tools, environment gate, optional scheduled fetch
├── rss.py          Single-file Python backend (stdlib + feedparser only)
├── INSTALL.md      AI-readable setup guide
└── data/           Legacy data location (auto-migrated on first run)
```

## Install

```bash
# from a local directory
pi install ./pi-agent-rss

# from git after pushing
pi install git:github.com/<user>/pi-agent-rss
```

On first use, have the AI read `INSTALL.md` to prepare the environment
(python3 + feedparser, one command each).

## Tools

| action | description | parameters |
|---|---|---|
| `rss add` | Add a subscription | feed_url |
| `rss fetch` | Fetch all enabled feeds | — |
| `rss unread` | List unread items | limit |
| `rss search` | Full-text search (FTS5) | query, limit |
| `rss markread` | Mark as read | item_id (optional; all unread if omitted) |
| `rss list` | List subscriptions | — |
| `rss remove` | Remove a subscription | feed_id |

Optional scheduling: set `RSS_AUTO_FETCH_MINUTES=60` to fetch hourly and push
new items to the agent automatically.

## Design

- **Minimal dependencies**: Python side uses only `feedparser` (pure Python), everything else is stdlib; pi side has no npm dependencies.
- **Fixed schema**: `rss_feeds` / `rss_items` / `rss_items_fts` (FTS5 full-text search), stable and documented.
- **Portable data**: the database defaults to `<pi config dir>/rss-data/rss.db`
  (outside the plugin directory, so package updates never wipe it); set `RSS_DB_PATH`
  to point at any SQLite file to reuse an existing database.
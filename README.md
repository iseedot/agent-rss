# pi-agent-rss

An RSS aggregation plugin for pi agent: manage RSS subscriptions through conversation —
add feeds, tag them by category, fetch updates, read unread items, full-text search,
and mark items as read.

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
pi install git:github.com/iseedot/agent-rss
```

On first use, have the AI read `INSTALL.md` to prepare the environment
(python3 + feedparser, one command each).

## Tools

| action | description | parameters |
|---|---|---|
| `rss add` | Add a subscription | feed_url, tags (optional) |
| `rss fetch` | Fetch all enabled feeds | — |
| `rss unread` | List unread items | limit, tag (optional filter) |
| `rss search` | Full-text search (FTS5) | query, limit, tag (optional filter) |
| `rss markread` | Mark as read | item_id (optional; all unread if omitted) |
| `rss list` | List subscriptions | tag (optional filter) |
| `rss remove` | Remove a subscription | feed_id |
| `rss tag` | Set/replace tags on a feed | feed_id, tags |
| `rss tags` | List all tags with feed counts | — |

Optional scheduling: set `RSS_AUTO_FETCH_MINUTES=60` to fetch hourly and push
new items to the agent automatically.

## Tagging feeds by category

Feeds carry comma-separated tags (`rss_feeds.tags`) so you can group
subscriptions and query one category at a time:

```
Add with tags:   rss add https://hnrss.org/frontpage -t tech,news
Set tags later:  rss tag 1 -t tech
Show all tags:   rss tags
List by tag:     rss list -t tech
Unread by tag:   rss unread -t tech
Search by tag:   rss search "LLM" -t tech
```

Tag matching is exact and case-insensitive; a feed may carry multiple tags.
Existing databases are migrated automatically on first run.

## Design

- **Minimal dependencies**: Python side uses only `feedparser` (pure Python), everything else is stdlib; pi side has no npm dependencies.
- **Fixed schema**: `rss_feeds` / `rss_items` / `rss_items_fts` (FTS5 full-text search), stable and documented.
- **Portable data**: the database defaults to `<pi config dir>/rss-data/rss.db`
  (outside the plugin directory, so package updates never wipe it); set `RSS_DB_PATH`
  to point at any SQLite file to reuse an existing database.

## License

[MIT](LICENSE) — use, modify, and redistribute freely with attribution.
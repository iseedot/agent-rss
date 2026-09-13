# pi-agent-rss Setup Guide (for the AI to read and execute)

This plugin consists of:

- `index.ts` — pi extension entry (auto-loaded by pi, nothing to do)
- `rss.py`   — Python backend (needs python3 + feedparser)
- `data/`    — runtime data directory (auto-created, nothing to do)

## Requirements

- Any modern Linux or macOS, or Windows with Python installed (WSL works)
- python3 (>= 3.8)
- Network access (needed when fetching feeds)

## Setup steps (usually steps 1 and 2 only)

### 1. Install python3

Choose one for your system:

```bash
# Ubuntu / Debian
sudo apt install -y python3 python3-pip

# macOS (if not already present)
brew install python3
# or download from https://www.python.org

# Windows (WSL)
wsl --install -d Ubuntu
```

Verify: `python3 --version` should print 3.8 or higher.

### 2. Install feedparser (the only third-party dependency, pure Python)

```bash
# Ubuntu / Debian (recommended: one command, avoids PEP 668 issues)
sudo apt install -y python3-feedparser
```

Other systems (or when sudo is unavailable):

```bash
python3 -m pip install --user feedparser
```

If pip is reported missing, run `python3 -m ensurepip --user` first, then retry.

If you see an `externally-managed-environment` error (PEP 668 protection on
Ubuntu 24.04+), choose one of:

```bash
# Option A: add --break-system-packages (installs into the user directory only)
python3 -m pip install --user --break-system-packages feedparser

# Option B: use a virtual environment
python3 -m venv ~/.venvs/rss && ~/.venvs/rss/bin/pip install feedparser
# then set RSS_PY_PYTHON to the venv interpreter path (see config below)
```

If the network is slow or fails, you can use a mirror:

```bash
python3 -m pip install --user feedparser -i https://pypi.tuna.tsinghua.edu.cn/simple
```

Verify: `python3 -c "import feedparser; print(feedparser.__version__)"` should
print a version number.

### 3. Done

Once installed, just tell pi: "add subscription <feed URL>" and the plugin will work.

## Usage examples

```
Add a feed:       rss add https://example.com/feed.xml
Fetch updates:    rss fetch
Read unread:      rss unread (default 20 items, adjust with limit)
Search history:   rss search <keyword> (FTS5 full-text search)
Mark as read:     rss markread (all unread); rss markread <item_id> (single item)
Manage feeds:     rss list / rss remove <feed_id>
```

## Optional configuration (environment variables)

| Variable | Purpose | Default |
|---|---|---|
| `RSS_DB_PATH` | Database file path (for migrating an old database or long-term storage) | plugin dir `data/rss.db` |
| `RSS_TRUSTED_PRIVATE_ORIGINS` | Comma-separated private origins allowed to be fetched (e.g. `http://nas.local:8000`) | empty (private IPs blocked by default; SSRF protection) |
| `RSS_AUTO_FETCH_MINUTES` | Scheduled fetch interval in minutes; new items are pushed to the agent | empty (disabled) |
| `RSS_PY_PYTHON` | Python interpreter path (only needed when using a venv or a non-default python3) | `python3` |

## Data notes

- The database is SQLite (tables: `rss_feeds` / `rss_items` / `rss_items_fts`).
- To migrate machines: copy the whole plugin directory; to keep read state,
  copy the database file along with it, or point `RSS_DB_PATH` at the copy.
- ⚠️ If installed via `pi install git:...`: `pi update` resets the plugin
  directory, so **set `RSS_DB_PATH`** (e.g. `~/.pi/rss-data/rss.db`) to keep
  your data, otherwise read state and subscriptions are lost on update.
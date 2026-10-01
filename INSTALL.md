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

Report pipeline (the scheduled task; use these three calls, nothing else):

```
1) Prepare:  rss {action:"brief"}                 # fetch + payload in one call
2) Validate: rss {action:"qa", draft:"..."}      # violations + verification
3) Finalize: rss {action:"commit", body:"..."}   # save + ledger + mark read
```

Basic and admin:

```
Fetch now:        rss {action:"fetch"}
List unread:      rss {action:"unread", limit:20, tag?}
Mark read:        rss {action:"markread", ids:"1,2,3"}
                  rss {action:"markread", tag:"技术", older_than:48}
Manage feeds:     rss {action:"manage", op:"add", feed_url:"...", tags:"技术"}
                  rss {action:"manage", op:"list"} / op:"remove" / op:"tag" / op:"tags" / op:"stats"
```

On the command line the same backend works directly: `python3 rss.py brief --json`,
`payload --json`, `check --json` (JSON `{"body":...}` on stdin), `commit --json`,
`annotate --json`, `ledger --json`, `sample --json`.

### jev triage and Chinese calibration

With a jev key configured, `brief` classifies items in batches (kind/topic/value, opinion
change vs the ledger, fact repeats), and `qa` runs semantic line lint plus web verification
(hard cap `RSS_VERIFY_BUDGET`, default 4). Discard decisions only fire at high confidence.

Jev's Chinese accuracy is weaker than English, so calibrate once before trusting it:

```
rss {action:"calibrate", op:"sample", max:50}   # writes calibration.json (labels empty)
# fill label = fact | opinion | mixed | skip for every row
rss {action:"calibrate", op:"score"}            # accuracy + confidence calibration
```

## Optional configuration (environment variables)

| Variable | Purpose | Default |
|---|---|---|
| `RSS_DB_PATH` | Database file path (explicit override) | `<pi config dir>/rss-data/rss.db` — e.g. `~/.pi/agent/rss-data/rss.db` |
| `RSS_REPORT_DIR` | Where `MM-DD_hh.md` reports are written | `~/Chat/rss` |
| `RSS_TZ` | Timezone for report filenames/headers (not for filtering) | `Asia/Shanghai` |
| `RSS_BRIEF_MAX` | Max unread items selected per brief | `250` |
| `RSS_BRIEF_FACTS_MAX` | Facts included in the brief payload | `60` |
| `RSS_BRIEF_OPINION_TOP` | Opinion candidates with full text | `10` |
| `RSS_BRIEF_OPINION_CHARS` | Per-opinion text truncation | `1200` |
| `RSS_FETCH_WORKERS` | Concurrent feed fetches | `6` |
| `RSS_SKIP_KEYWORDS` | Comma-separated sports/entertainment skip list | built-in list |
| `RSS_PY_PYTHON` | Python interpreter path (venv or non-default python3) | `python3` |
| `TYPESAFE_API_KEY` | jev classifier key (or plugin store / shared file, see README) | empty |
| `RSS_JE_V_MODEL` | Classifier model | `typesafe/jev-latest` |
| `RSS_JE_V_CONCURRENCY` | Classifier concurrency | `4` |
| `RSS_JE_V_SKIP_CONF` | Min confidence to drop a `skip` item | `0.7` |
| `RSS_JE_V_NONE_CONF` | Min confidence to drop a `no-change` opinion | `0.7` |
| `RSS_VERIFY_BUDGET` | Hard web-verification cap per run | `4` |
| `RSS_OPINION_RETIRE_RUNS` | Retire opinions not seen for N runs | `8` |
| `RSS_CALIBRATION_FILE` | Calibration file path | `<agent-dir>/rss-plugin/calibration.json` |

## Data notes

- The database is SQLite. Tables: `rss_feeds` / `rss_items` / `rss_items_fts` plus the
  report pipeline tables `rss_ledger_facts` / `rss_ledger_opinions` / `rss_runs` / `rss_state`
  (existing databases migrate automatically on first run).
- `rss_feeds.tags` (comma-separated) enables category filtering via `rss unread -t <tag>`,
  `rss search -t <tag>`, and `rss list -t <tag>`; existing databases are migrated automatically.
- The database lives **outside the plugin directory** (default: `<pi config dir>/rss-data/rss.db`),
  so package updates (`pi update`) never wipe subscriptions or read state.
- Old plugin-dir databases (`data/rss.db`) are migrated automatically on first run.
- To migrate machines: copy the plugin directory and the database file, or point
  `RSS_DB_PATH` at your copy.
# pi-agent-rss

An RSS **report engine** for pi, built for the scheduled short-report task:
every run turns all unread items into a facts + opinions digest with a persistent
cross-period ledger, without flooding the model with raw feed text.

```
pi-agent-rss/            (repository root = pi package root)
├── index.ts             Extension: rss tool (brief/qa/commit pipeline, credentials)
├── rss.py               Single-file Python backend (stdlib + feedparser only)
├── INSTALL.md           AI-readable setup guide
└── docs/                Design plan (PLAN-rss-report.md)
```

## Install

```bash
pi install ./pi-agent-rss
# or from git: pi install git:github.com/iseedot/agent-rss
```

On first use have the AI read `INSTALL.md` (python3 + feedparser).

## The report pipeline (3 calls per run)

| stage | call | what it does |
|---|---|---|
| 1 | `rss {action:"brief"}` | fetch all feeds → all unread → drop sports/entertainment → dedupe → triage (heuristic now, jev next) → previous-period ledger diff → last report. Returns the structured payload: `facts`, `opinions`, `ledger`, `last_report`, `session`. |
| 2 | `rss {action:"qa", draft:"…"}` | validates the draft: ≤2500 hanzi, per-line limits, ≤3 opinions, six opinion fields, tone words, links; (P2) jev line lint + web verification under a hard ≤4 budget. |
| 3 | `rss {action:"commit", body:"…"}` | final gate: validates, builds the first-line header, saves `MM-DD_hh.md` (Beijing time), updates the fact/opinion ledger, marks this batch read, returns the final text. Reply with that text verbatim. |

`brief` returns `next_steps`; the scheduled prompt should tell the model to use only
these three calls (see `prompts/rss-report.md`).

Facts-section format: only `- ` list lines are counted as facts; `**小标题**` / `### 小标题`
headings are grouping-only and never enter the header counts or the ledger. Prose lines
without a bullet are reported as structure violations by `qa`/`commit`.

### Why the pipeline exists

- The model never reads raw feed dumps: only the curated payload enters context.
- Mechanical work (fetch window, dedupe, ledger, counters, filename, header, mark-read)
  is deterministic and lives in the plugin.
- "No change, no repeat" and "待确认 stays unconfirmed" are enforced by a SQLite ledger,
  not by re-reading the last Markdown reports.

## Basic / admin actions

| action | description |
|---|---|
| `fetch` | fetch all subscriptions now |
| `unread` | list unread items (tag/feed/limit filters) |
| `markread` | mark read: `ids`, `item_id`, `tag`, `older_than`, `before` |
| `manage` | `op` = add / remove / list / tag / tags / stats |
| `calibrate` | jev Chinese quality gate: `op:"sample"` writes labeled-sample file, edit labels, `op:"score"` computes accuracy + confidence calibration |

## jev triage & calibration

With `TYPESAFE_API_KEY` configured, `brief` runs batch classifier passes:
kind/topic/value (all items), opinion change vs the active ledger, and fact repeats vs
previous facts. Discard decisions (`skip`, `change=none`) only apply at high confidence;
anything uncertain passes through to the model. `qa` additionally runs per-line semantic
lint and web verification via `source_check` under a hard budget (`RSS_VERIFY_BUDGET`, default 4).

Because Jev's CJK accuracy is weaker than English, calibrate before trusting discard
decisions:

```
rss {action:"calibrate", op:"sample", max:50}   # triage 50 random items -> calibration.json
# edit each row: label = fact | opinion | mixed | skip
rss {action:"calibrate", op:"score"}            # accuracy, per-class, confidence buckets
```

If the score reports `NOT trustworthy`, raise `RSS_JE_V_SKIP_CONF` / `RSS_JE_V_NONE_CONF`
(e.g. 0.85) or keep triage advisory-only.

## Data & state

- DB: `<pi config dir>/rss-data/rss.db` (override `RSS_DB_PATH`).
- Tables: `rss_feeds` / `rss_items` / `rss_items_fts` plus `rss_ledger_facts`,
  `rss_ledger_opinions`, `rss_runs` (auto-migrated on first run).
- Reports: `RSS_REPORT_DIR` (default `~/Chat/rss/`), filenames `MM-DD_hh.md` in Beijing time.
- Tags live in `rss_feeds.tags` and are used as topical hints (真机: 技术/财经/资讯/AI科技).

## Credentials (jev / typesafe, P2)

Resolution order — first hit wins:

1. `TYPESAFE_API_KEY` environment variable
2. `<agent-dir>/rss-plugin/auth.json` (mode 0600; group/other-readable files are refused)
3. `TYPESAFE_KEY_FILE` or `~/.config/typesafe/key`

The resolved key is injected into `process.env.TYPESAFE_API_KEY` so pi's typesafe
provider uses it for `modelRegistry.classify()`. Keys never go into argv, URLs, or logs.

## Environment variables

| variable | default | purpose |
|---|---|---|
| `RSS_DB_PATH` | `<pi config>/rss-data/rss.db` | database location |
| `RSS_REPORT_DIR` | `~/Chat/rss` | where reports are written |
| `RSS_TZ` | `Asia/Shanghai` | timezone for filenames/headers only |
| `RSS_BRIEF_MAX` | 250 | max items selected per brief |
| `RSS_BRIEF_FACTS_MAX` | 60 | facts included in the payload |
| `RSS_BRIEF_OPINION_TOP` | 10 | opinion candidates with full text |
| `RSS_BRIEF_OPINION_CHARS` | 1200 | per-opinion text truncation |
| `RSS_FETCH_WORKERS` | 6 | concurrent feed fetches |
| `RSS_SKIP_KEYWORDS` | built-in sports/entertainment list | skip filter |
| `RSS_PY_PYTHON` | `python3` | interpreter path (venv) |
| `RSS_JE_V_MODEL` | `typesafe/jev-latest` | classifier model |
| `RSS_JE_V_CONCURRENCY` | 4 | classifier concurrency |
| `RSS_JE_V_BATCH` | 10 | items per classifier batch |
| `RSS_JE_V_SKIP_CONF` | 0.7 | min confidence to drop a `skip` item |
| `RSS_JE_V_NONE_CONF` | 0.7 | min confidence to drop a `no-change` opinion |
| `RSS_BRIEF_DEADLINE_MS` | 150000 | triage time budget per brief |
| `RSS_QA_DEADLINE_MS` | 120000 | lint/verification time budget per qa |
| `RSS_VERIFY_BUDGET` | 4 | hard web-verification cap per run |
| `RSS_OPINION_RETIRE_RUNS` | 8 | retire opinions not seen for N periods (one period = one schedule slot) |
| `RSS_CALIBRATION_FILE` | `<agent-dir>/rss-plugin/calibration.json` | calibrate file

## License

[MIT](LICENSE)

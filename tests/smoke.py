#!/usr/bin/env python3
"""End-to-end smoke test for the pi-agent-rss report pipeline.

Runs entirely against a temporary database and report directory; no network.
Usage: python3 tests/smoke.py
"""
from __future__ import annotations

import json
import os
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
RSS = ROOT / "rss.py"

PASS = 0
FAIL = 0


def check(label: str, condition: bool, detail: str = ""):
    global PASS, FAIL
    if condition:
        PASS += 1
        print(f"  ok   {label}")
    else:
        FAIL += 1
        print(f"  FAIL {label} {detail}")


def run(args, stdin=None, env=None):
    ret = subprocess.run(
        [sys.executable, str(RSS), *args],
        input=stdin, capture_output=True, text=True, env=env,
    )
    return ret


def run_json(args, stdin=None, env=None):
    ret = run([*args, "--json"], stdin=stdin, env=env)
    try:
        return json.loads(ret.stdout)
    except Exception:
        raise AssertionError(f"invalid JSON from {args}: {ret.stdout[:300]} {ret.stderr[:300]}")


def seed(db: Path):
    conn = sqlite3.connect(db)
    now = int(time.time())
    feeds = [
        (1, "https://example.com/tech.xml", "技术日报", "技术"),
        (2, "https://x.com/alex", "X @Alex_感知", "财经"),
        (3, "https://example.com/sports.xml", "体育世界", "资讯"),
    ]
    for fid, url, title, tags in feeds:
        conn.execute(
            "INSERT OR REPLACE INTO rss_feeds (id,url,title,tags,created_at,updated_at)"
            " VALUES (?,?,?,?,?,?)",
            (fid, url, title, tags, now, now),
        )
    items = [
        ("g1", 1, "中国1-8月规上工业利润同比+15.7%", "https://ex.com/a1",
         "1-8月规上工业利润同比+15.7%，8月单月+4.2%"),
        ("g2", 2, "美股回调仍将持续", "https://x.com/alex/1",
         "分析师表示，美债利率维持高位，市场认为回调会延续"),
        ("g3", 3, "世界杯决赛阿根廷夺冠 比分3:2", "https://ex.com/s1", "世界杯决赛，比分3:2，夺冠"),
        ("g4", 1, "中国1-8月规上工业利润同比+15.7%", "https://ex.com/a1?utm=1", "重复链接"),
        ("g5", 2, "英伟达发布新一代AI芯片", "https://x.com/alex/2", "英伟达发布新一代AI芯片，算力提升2倍"),
        ("g6", 1, "英伟达发布新一代 AI 芯片（转载）", "https://ex.com/a2", "英伟达新AI芯片算力提升2倍"),
    ]
    for guid, fid, title, link, content in items:
        conn.execute(
            "INSERT OR REPLACE INTO rss_items (guid,feed_id,title,link,published,content,summary,"
            "categories,is_read,is_starred,created_at) VALUES (?,?,?,?,?,?,?,?,0,0,?)",
            (guid, fid, title, link, now - len(guid) * 30, content, content[:200], "[]", now),
        )
    conn.commit()
    conn.close()


def main() -> int:
    tmp = Path(tempfile.mkdtemp(prefix="rss-smoke-"))
    env = dict(os.environ)
    env["RSS_DB_PATH"] = str(tmp / "rss.db")
    env["RSS_REPORT_DIR"] = str(tmp / "reports")
    (tmp / "reports").mkdir(parents=True, exist_ok=True)
    try:
        run(["list"], env=env)  # init schema
        seed(tmp / "rss.db")

        print("brief:")
        brief = run_json(["brief", "--no-fetch"], env=env)["data"]
        check("brief ok", bool(brief.get("run_id")))
        check("deduped duplicate marks it read", brief["counts"]["deduped"] == 1
              and brief["counts"]["unread_total"] == 5, str(brief["counts"]))
        check("deduped duplicate", brief["counts"]["deduped"] == 1)
        check("sports skipped", brief["counts"]["skipped"] == 1)
        check("prev report none on first run", brief["session"]["prev_report"] == "无")

        def item_id(prefix: str) -> int:
            return next(i["id"] for i in brief["items"] if i["title"].startswith(prefix))

        check("numbers extracted", "+15.7%" in next(
            i for i in brief["items"] if "规上工业" in i["title"])["numbers"])
        run_id = brief["run_id"]

        # simulate jev annotate: fact x3 (two are near dups), opinion x1 (no-change)
        ann = [
            {"id": item_id("中国1-8月"), "kind": "fact", "topic": "国内经济", "value": 9,
             "has_number": True, "numbers": ["+15.7%"]},
            {"id": item_id("英伟达发布新一代AI"), "kind": "fact", "topic": "科技", "value": 8,
             "has_number": True, "numbers": ["2倍"]},
            {"id": item_id("英伟达发布新一代 AI"), "kind": "fact", "topic": "科技", "value": 5,
             "has_number": True, "numbers": ["2倍"]},
            {"id": item_id("美股回调"), "kind": "opinion", "topic": "国际经济", "value": 7,
             "relation": "none", "relation_confidence": 0.92, "change_hint": "无变化"},
        ]
        run_json(["annotate"], stdin=json.dumps(ann), env=env)

        print("payload:")
        payload = run_json(["payload", "--run", str(run_id)], env=env)["data"]
        check("skip item excluded", all("世界杯" not in f["title"] for f in payload["facts"]))
        check("no-change opinion dropped", payload["counts"]["opinions_dropped_no_change"] == 1)
        nvidia = next(f for f in payload["facts"] if "英伟达" in f["title"])
        check("cross-source facts grouped", nvidia.get("dup_count") == 1 and nvidia.get("dup_sources"))
        check("next_steps point to a real action",
              "manage op=ledger" in " ".join(payload["next_steps"]), str(payload["next_steps"]))
        ledger_env = run_json(["ledger"], env=env)
        check("ledger command available",
              ledger_env["ok"] is True and "prev_facts" in (ledger_env["data"] or {}),
              str(ledger_env)[:120])

        print("check / commit:")
        body = (
            "## 一、资讯速览\n"
            "**国际经济与市场**\n"
            "- 中国1-8月规上工业利润同比+15.7%\n"
            "### 科技\n"
            "- 英伟达发布新一代AI芯片，算力提升2倍\n"
            "- 延续：霍尔木兹封锁未解\n\n"
            "## 二、观点与趋势\n"
            "- 观点：@Alex_感知 认为美股回调仍未结束\n"
            "  依据：美债利率维持高位\n"
            "  检验：标普500已回落3%，未证实企业盈利下滑\n"
            "  结论：部分成立\n"
            "  趋势：若两周内10年期美债收益率升破4.8%，回调延续；验证信号为美债收益率，时间窗两周\n"
            "  与上期相比：新出现"
        )
        chk = run_json(["check"], stdin=json.dumps({"body": body}), env=env)["data"]
        check("valid draft has no violations", chk["violations"] == [], str(chk["violations"]))
        check("headings are not facts", chk["stats"]["facts"] == 2 and chk["stats"]["continuations"] == 1,
              str(chk["stats"]))
        bad = run_json(["check"], stdin=json.dumps(
            {"body": "## 一、资讯速览\n- 市场认为黄金会涨\n- 参见 https://x.com/a\n\n## 二、观点与趋势\n- 观点：某人认为崩了\n  依据：无"}),
            env=env)["data"]
        kinds = {v["kind"] for v in bad["violations"]}
        check("invalid draft flagged", {"mixed", "link", "tone", "format"} <= kinds, str(kinds))

        commit = run_json(["commit"], stdin=json.dumps({"body": body, "run_id": run_id}), env=env)
        check("commit ok", commit["data"]["ok"] is True)
        report = Path(commit["data"]["report_path"])
        check("report written", report.exists() and report.parent == tmp / "reports")
        text = report.read_text(encoding="utf-8")
        check("header generated", text.startswith("# ") and "本期使用 3 条" in text
              and "速览 2 条 + 观点 1 条" in text and "上期：无" in text, text.splitlines()[0])
        check("model header replaced", "99 条" not in text)

        conn = sqlite3.connect(tmp / "rss.db")
        conn.row_factory = sqlite3.Row
        check("batch marked read", conn.execute(
            "SELECT COUNT(*) c FROM rss_items WHERE is_read = 0").fetchone()["c"] == 0)
        check("run committed", conn.execute(
            "SELECT status FROM rss_runs WHERE id = ?", (run_id,)).fetchone()["status"] == "committed")
        check("fact ledger written", conn.execute(
            "SELECT COUNT(*) c FROM rss_ledger_facts WHERE status = 'confirmed'").fetchone()["c"] == 2)
        check("opinion ledger written", conn.execute(
            "SELECT COUNT(*) c FROM rss_ledger_opinions").fetchone()["c"] == 1)
        check("no heading rows in ledger", conn.execute(
            "SELECT COUNT(*) c FROM rss_ledger_facts WHERE text LIKE '**%' OR text LIKE '#%'").fetchone()["c"] == 0)
        baseline_fact_id = conn.execute(
            "SELECT id FROM rss_ledger_facts WHERE text LIKE '%规上工业%'").fetchone()["id"]
        check("baseline fact recorded", baseline_fact_id is not None)

        run_json(["runstats"],
                 stdin=json.dumps({"run_id": run_id, "jev_stats": {"triage": {"calls": 3}}}),
                 env=env)
        run_json(["runstats"],
                 stdin=json.dumps({"run_id": run_id, "jev_stats": {"qa": {"verification": {"used": 2}}}}),
                 env=env)
        runs = run_json(["runs", "--limit", "1"], env=env)["data"]["runs"][0]
        check("runstats merged into run",
              runs["jev_stats"]["triage"]["calls"] == 3
              and runs["jev_stats"]["qa"]["verification"]["used"] == 2,
              str(runs.get("jev_stats")))

        print("second run (ledger reuse):")
        # simulate the next slot (same-hour runs would reuse the filename)
        (tmp / "reports" / "09-30_21.md").write_text(text, encoding="utf-8")
        report.unlink()
        now = int(time.time())
        conn.execute(
            "INSERT INTO rss_items (guid,feed_id,title,link,published,content,summary,categories,"
            "is_read,is_starred,created_at) VALUES ('g7',2,'美债利率上行，美股回调仍未结束',"
            "'https://x.com/alex/3',?,?,?,'[]',0,0,?)",
            (now, "美债利率上行，市场认为美股回调仍未结束", "美债利率上行", now),
        )
        conn.execute(
            "INSERT INTO rss_items (guid,feed_id,title,link,published,content,summary,categories,"
            "is_read,is_starred,created_at) VALUES ('g8',1,'某传闻待确认','https://ex.com/r1',?,?,?,'[]',0,0,?)",
            (now, "某传闻待确认", "某传闻待确认", now),
        )
        conn.commit()
        conn.close()

        brief2 = run_json(["brief", "--no-fetch"], env=env)["data"]
        ann2 = [
            {"id": i["id"], "kind": "opinion", "value": 7, "relation": "reinforce",
             "relation_confidence": 0.81, "change_hint": "被强化", "verify_needed": True}
            for i in brief2["items"] if "美债" in i["title"]
        ]
        for i in brief2["items"]:
            if "传闻" in i["title"]:
                ann2.append({"id": i["id"], "kind": "fact", "value": 6,
                             "repeat_of": baseline_fact_id, "flags": ["repeat_no_new_number"]})
        run_json(["annotate"], stdin=json.dumps(ann2), env=env)
        payload2 = run_json(["payload", "--run", str(brief2["run_id"])], env=env)["data"]
        check("prev report linked", payload2["session"]["prev_report"] == "09-30_21.md")
        check("opinion carries change", any(o["relation"] == "reinforce" for o in payload2["opinions"]))
        check("payload ledger trimmed to referenced",
              [f["id"] for f in payload2["ledger"]["prev_facts"]] == [baseline_fact_id],
              str(payload2["ledger"]["prev_facts"]))
        check("last report omitted when ledger is warm",
              payload2["counts"]["last_report_included"] is False
              and payload2["last_report"]["text"] == "",
              str(payload2["counts"]))
        rumor = next((f for f in payload2["facts"] if "传闻" in f["title"]), None)
        check("repeat fact keeps title only", rumor is not None and "summary" not in rumor,
              str(rumor))

        body2 = (
            "## 一、资讯速览\n"
            "- 某传闻待确认\n\n"
            "## 二、观点与趋势\n"
            "- 观点：@Alex_感知 认为美股回调仍未结束\n"
            "  依据：美债利率上行\n"
            "  检验：美债10Y升至4.6%，但企业盈利未下滑\n"
            "  结论：部分成立\n"
            "  趋势：若两周内美债10Y升破4.8%，回调延续；验证信号为美债收益率，时间窗两周\n"
            "  与上期相比：被强化"
        )
        commit2 = run_json(["commit"], stdin=json.dumps({"body": body2, "run_id": brief2["run_id"]}),
                           env=env)["data"]
        check("second commit ok", commit2["ok"] is True)
        check("opinion updated not duplicated", commit2["ledger"]["opinions_added"] == 0
              and commit2["ledger"]["opinions_updated"] == 1, str(commit2["ledger"]))

        conn = sqlite3.connect(tmp / "rss.db")
        conn.row_factory = sqlite3.Row
        check("unconfirmed stays unconfirmed", conn.execute(
            "SELECT status FROM rss_ledger_facts WHERE text LIKE '%传闻%'").fetchone()["status"] == "unconfirmed")
        check("single opinion row", conn.execute(
            "SELECT COUNT(*) c FROM rss_ledger_opinions").fetchone()["c"] == 1)
        conn.execute(
            "INSERT INTO rss_items (guid,feed_id,title,link,published,content,summary,categories,"
            "is_read,is_starred,created_at) VALUES ('g9',1,'待标已读条目','https://ex.com/z1',?,?,?,'[]',0,0,?)",
            (now, "待标已读条目", "待标已读条目", now),
        )
        conn.commit()
        mr = run_json(["markread"], env=env)
        check("markread json reports count", mr["data"]["marked"] == 1, str(mr.get("data")))
        stats_env = run_json(["stats"], env=env)
        check("json envelope carries text",
              isinstance(stats_env.get("text"), str) and bool(stats_env["text"]), str(stats_env)[:120])
        stale = run_json(["commit"], stdin=json.dumps({"body": body}), env=env)
        check("commit without fresh brief rejected", stale["data"]["ok"] is False,
              str(stale.get("data"))[:200])
        conn.close()

        print("backfill:")
        legacy = tmp / "reports" / "08-30_21.md"
        legacy.write_text(
            "# 08-30 21｜本期使用 2 条（速览 1 条 + 观点 1 条）｜本次新抓取 0 条｜上期：无\n\n"
            "## 一、资讯速览\n"
            "- 回填测试事实一条\n\n"
            "## 二、观点与趋势\n"
            "- 观点：@回填 认为测试观点成立\n"
            "  依据：回填依据\n"
            "  检验：未证实\n"
            "  结论：存疑\n"
            "  趋势：若两周内指标变化则成立；验证信号为指标\n"
            "  与上期相比：新出现",
            encoding="utf-8",
        )
        dry = run_json(["backfill", "--file", str(legacy), "--dry-run"], env=env)["data"]
        check("backfill dry-run counts",
              dry["details"][0]["facts"] == 1 and dry["details"][0]["opinions"] == 1,
              str(dry))
        bf = run_json(["backfill", "--file", str(legacy)], env=env)["data"]
        check("backfill added rows", bf["facts_added"] >= 1 and bf["opinions_added"] >= 1, str(bf))
        conn2 = sqlite3.connect(tmp / "rss.db")
        check("backfill run recorded", conn2.execute(
            "SELECT COUNT(*) c FROM rss_runs WHERE status = 'backfilled'").fetchone()[0] == 1)
        conn2.close()
        blocked = run_json(["commit"], stdin=json.dumps({"body": body}), env=env)
        check("commit rejects non-briefed latest run",
              blocked["data"]["ok"] is False
              and any("briefed" in v.get("why", "") for v in blocked["data"]["violations"]),
              str(blocked.get("data"))[:200])

        print("sample:")
        sample = run_json(["sample", "--n", "3"], env=env)["data"]
        check("sample returns items", len(sample["items"]) == 3)

        print(f"\n{PASS} passed, {FAIL} failed")
        return 1 if FAIL else 0
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())

# 方案：RSS 短报告定时任务的 pi + jev + codemode 流水线（v4）

> 针对你那条定时任务提示词（08:00 / 21:00 两期、资讯速览 + 观点与趋势、≤2500 汉字、
> websearch ≤4、存 `/home/ericfu/Chat/rss/MM-DD_hh.md`、标记已读）。
> 实施在**真机**（已有大量订阅、每期约 200 条、早晚各一次），本目录是开发平台。
> 插件 `pi-agent-rss` 专供此任务，接口直接按任务重新设计。
>
> **版本脉络**
> - v2：200 条/期估算；jev 批量分类（官方验证批处理不改变答案）；置信度三档门控；
>   typesafe 价格/限流；key 分层读取。
> - v3（你的三条基线）：① 资讯不按发布时间过滤，未读即本期窗口；② 类别标签复用
>   sqlite `rss_feeds.tags`；③ 标已读 = 本批全部，未出报告不标。
> - **v4（本轮）**：接口重新设计——把整个报告流程压缩成 **3 次工具调用**
>   （`brief` → `qa` → `commit`），所有可确定性完成的判断/记账/组装/落盘都下沉到插件，
>   LLM 只负责两次生成（初稿 + 按质检修订）。并给出定时任务 prompt 的接口约定改写稿。

---

## 1. 任务解构与三条基线

| # | 约束 | 谁来做 |
|---|---|---|
| C1 | 叙事与观点严格分离，不混句 | jev 质检 + 主模型 |
| C2 | 中性、无情绪词、观点署名、结论可检验 | jev 质检 + 主模型 |
| C3 | 观点只在「新增/反转/强化/削弱」时写；事实无新数字写 `延续：` | **插件台账**（jev 判 change） |
| C4 | ≤2500 汉字；速览每条 ≤40 字；观点每条 ≤200 字；观点 ≤3 条 | **插件硬校验** |
| C5 | 抓取 → 读上期 → 写速览 → 筛观点 → 标已读 → 存盘 | **接口编排**（3 次调用） |
| C6 | 速览=主体+动作+数字；机构判断不入速览；体育娱乐不报；`待确认` 不升级 | 规则过滤 + jev + **台账状态机** |
| C7 | 观点六字段（观点/依据/检验含反例/结论/趋势含信号+时间窗/与上期相比） | 主模型写 + **插件解析入库** |
| C8 | websearch ≤4，只用于核验；核不出标「未证实」 | **插件计数强制** + jev 判定 |
| C9 | 08:00 / 21:00 定时执行；两期事实不重复 | **未读即窗口** + 台账 |
| C10 | 只输出最终报告；首行统计固定；无链接 | **插件拼表头** + 主模型照抄 |

**你的三条基线（v3 起生效）**
1. 资讯**不按发布时间过滤**：程序到点抓"全部未读"即可——上一期出报告后全标已读，
   未读即本期新资讯；失败漏跑则累积，语义自洽（没出报告 = 没处理）。
2. **标签体系现成、但只分主题**：真机 4 个 tag——`技术`(9) / `财经`(9) / `资讯`(8) / `AI科技`(5)，
   共 31 个 feed。**没有** opinion/skip 类标签，所以：观点源识别靠 feed title（`@账号`/`财新`）
   + jev J1 逐条判定；体育娱乐过滤靠关键词表 + jev `skip`；tags 只作为主题提示传给 jev。
3. **标已读口径**：抓一批 → 出报告 → **整批全部标已读**；报告未产出不标，下期重来。

**核心判断**：C3/C4/C5/C6/C8/C9/C10 全部可以在插件内确定性完成（含用 jev 做判定），
不需要 LLM 参与；LLM 只保留 C1/C2/C7 的语言生成。这是 v4 压缩调用的依据。

---

## 2. 现状流程与失效点

现状每期（~200 条）：`rss fetch` → `rss unread -l 大` → `read` 上期 1~2 篇 →
上下文里人肉分事实/观点 → `web_search` → `write` 文件 → `rss markread`（大概率全标）→ 输出。
**10+ 次工具调用，60k 字原文进上下文，全部机械工作压在主模型上。**

| # | 失效点 | 根因 |
|---|---|---|
| ~~F1~~ | （v3 撤回）无需时间窗：`unread` 就是本期窗口 | 用户基线 1 |
| F2 | 200 条 × ~300 字 ≈ 60k 字全文进上下文，超长被截断，只能抽样 | 用"列清单"接口干"准备素材"的活 |
| F3 | 文件名/表头要北京时间，机器是 NY/UTC | 让 LLM 自己算时间 |
| F4 | `_to_text(limit=300)` 截断，观点常在正文后段看不到 | 输出格式固定 300 字 |
| F5 | 无跨期台账：「待确认」超两期就丢，「与上期相比」靠肉眼 | 状态存在 md 里而不是 DB 里 |
| F6 | markread 不可精确（只能单条或按 tag/时间批） | 缺少 `--ids` 与"本批"概念 |
| F7 | 2500 字 / ≤3 观点 / 禁用词 / websearch≤4 全靠 LLM 自觉 | 没有硬闸门 |
| F8 | 标已读后失败 = 丢失；重跑无幂等 | 流程步骤与状态更新不同步 |
| F9 | 观点署名（@账号/财新《栏目》）靠猜 | 未利用 `rss_feeds.tags` 与 feed 元数据 |

---

## 3. v4 目标流程：3 次工具调用 + 2 次生成

```
paseo cron 08:00 / 21:00 Asia/Shanghai（两条 schedule，同一提示词）
  │ 新建 pi agent（flash 级即可）
  ▼
[1] rss {action:"brief"}                 ← 一次调用干完所有准备工作
      fetch（并发/ETag/flock）→ 全部未读 → 规则预过滤（skip tags/关键词/去重）
      → jev 批量分类（kind/topic/数字/value/重复/观点变更/需核验）
      → 台账对账（上期事实、活跃观点、待确认）
      → 读上期报告全文
      → 返回 payload（速览候选 + 观点候选 + 台账 + 上期报告 + 文件名/表头数据）
      【本批 item ids 与 fetched_new 存入 run 记录，供 commit 用】
  ▼
[2] 主模型写草稿（唯一的大块生成）
  ▼
[3] rss {action:"qa", draft:"<草稿>"}    ← 质检 + 核验（一次调用）
      确定性检查（字数/条数/格式/禁用词/链接）
      + jev 逐行语义质检（叙事夹观点/情绪词/缺署名/缺检验行）
      + 核验循环 ≤4 次（source_check 取引文 → jev 判 supports/refutes/mixed）
      → 返回 violations[] + verification[]（含反例引文）
      【websearch 计数写 run，硬性 ≤4】
  ▼
[4] 主模型按 violations 改一版（第二次也是最后一次生成）
  ▼
[5] rss {action:"commit", body:"<修订稿>"}
      硬校验终审 → 解析正文（速览行/观点六字段/待确认）→ 自动拼首行表头
      → 原子写 /home/ericfu/Chat/rss/MM-DD_hh.md（北京时区）
      → 更新台账（事实状态机 / 观点 insert-update / change）
      → 本批全部标已读 → 写 run 记录
      → 返回 reportText（含表头）
  ▼
[6] 主模型原样输出 reportText（文件与对话输出一字不差）
```

对比：**工具调用 10+ → 3**；进上下文的原文 60k 字 → 结构化 payload（约 10~15k tokens，
含上期报告全文）；LLM 的机械工作全部消失。

---

## 4. 接口重新设计

### 4.1 接口总表

| 类别 | action | 一句话 | 定时任务 |
|---|---|---|---|
| **流程** | `brief` | 抓取 + 准备报告素材（含 jev 分类与台账对账） | ✅ 第 1 次 |
| **流程** | `qa` | 草稿质检 + 关键事实核验（≤4 次） | ✅ 第 2 次 |
| **流程** | `commit` | 终审 + 落盘 + 台账 + 标已读，返回最终全文 | ✅ 第 3 次 |
| 基础 | `fetch` | 单独抓取（brief 已内置；调试/人工用） | — |
| 基础 | `unread` | 人工查看未读（tag/feed/limit 过滤） | — |
| 基础 | `markread` | 人工标记（item_id / tag / older_than / `--ids`） | — |
| 管理 | `manage` | `op` ∈ {add, remove, list, tag, tags, stats} | — |

旧的 `search`/`recent` 删除（需要时用 `unread` + tag 过滤，或 sqlite 直接查）；
`tags`/`list`/`tag`/`add`/`remove`/`stats` 合并进 `manage`，action 枚举从 11 个降到 7 个。
工具描述会把三个流程接口写详细（何时用、返回什么、下一步做什么），管理接口一句带过。

### 4.2 `brief` — 把所有准备工作一次做完

**参数**：`fetch?: boolean`（默认 true）、`max?: number`（默认 250）、`topOpinions?: number`（默认 10）。

**内部流水线**（全部在插件里，LLM 不可见）：

1. **抓取**：并发（默认 6）、单 feed 30s、整轮预算 300s、`flock` 防撞；记录 `fetched_new`。
2. **取候选**：全部未读（不按时间过滤）；join feed 的 `title/tags`。
3. **规则预过滤**：
   - 标题/摘要命中 `RSS_SKIP_KEYWORDS`（体育/娱乐/广告）→ 标 `skip`（真机无 skip 类 tag，纯关键词）；
   - canonical key（去参 URL / 归一标题）去同源重复；同事件跨源先按标题相似聚合。
   （feed tags 只作主题提示，不参与排除）
4. **jev 批量分类**（§6）：J1 全量分类；J2 只跑 opinion/mixed；J3 只跑 fact/mixed。
   批量 10 条/调用，问题带 `i0_`/`i1_` 前缀，一次问完（Speculative Fan-Out）。
5. **台账对账**：读 `rss_ledger_facts`（含 `unconfirmed`）、`rss_ledger_opinions`（active）、
   最近报告文件名。
6. **读上期报告**：读最近 1 篇 md 全文（可配 1~2 篇）放进 payload，省掉 LLM 的 `read` 调用。
7. **排序与裁剪**：每主题取 top；观点候选按 value×relation 排序取前 `topOpinions` 条给全文，
   其余只给摘要；事实候选上限 `RSS_BRIEF_FACTS_MAX`（默认 60）。
8. **记录 run**：本批 item ids、`fetched_new`、triage 统计写入 `rss_runs`（status=`briefed`）。
9. **返回** payload。

**payload 结构**（数字为示例）：

```json
{
  "session": {
    "slot": "morning", "beijing": "10-01 08:00",
    "report_name": "10-01_08.md", "prev_report": "09-30_21.md",
    "header": "# 10-01 08｜本期使用 N 条（速览 x 条 + 观点 y 条）｜本次新抓取 M 条｜上期：09-30_21"
  },
  "counts": { "fetched_new": 187, "unread_total": 203, "skipped": 41,
              "deduped": 18, "triaged": 144, "uncertain": 7 },
  "facts": [
    { "id": 10231, "title": "中国1-8月规上工业利润同比+15.7%", "summary": "…",
      "feed": "统计局", "tags": "news,cn", "published_bj": "10-01 06:20",
      "topic": "国内经济", "has_number": true, "numbers": ["15.7%", "8月单月+4.2%"],
      "value": 9, "confidence": 0.86, "dup_sources": ["财新", "新华社"] }
  ],
  "opinions": [
    { "id": 10244, "source": "@宏观边际MacroMargin", "feed": "X/宏观边际", "tags": "opinion,x",
      "title": "…", "text": "<正文，上限 1200 字>", "published_bj": "10-01 05:11",
      "relation": "reinforce", "target_opinion_id": 17, "change_hint": "被强化",
      "verify_needed": true, "confidence": 0.71 }
  ],
  "opinions_omitted": 23,
  "ledger": {
    "prev_facts": [ { "id": 88, "text": "现货黄金跌破4200美元", "status": "confirmed" } ],
    "active_opinions": [ { "id": 17, "source": "@Alex_感知", "claim": "…", "conclusion": "部分成立",
                           "signal": "美债10Y跌破4%", "time_window": "2周内" } ],
    "unconfirmed": [ { "id": 91, "text": "霍尔木兹封锁未解", "first_seen": "09-28" } ]
  },
  "last_report": { "name": "09-30_21.md", "text": "<上期报告全文>" },
  "next_steps": [
    "只用本 payload 写草稿：速览行 ≤40 字、观点六字段、观点 ≤3 条、全文 ≤2500 汉字",
    "草稿完成后调用 rss {action:\"qa\", draft:\"…\"}",
    "按 qa 返回修订后调用 rss {action:\"commit\", body:\"…\"}，最终只输出返回的 reportText"
  ]
}
```

要点：`facts[].numbers` 是插件用正则从标题/摘要提取的现成数字（帮 LLM 写"关键数字"）；
`opinions[].change_hint` 是 jev relation 的中文映射；`unconfirmed` 保证「待确认」跨期不丢；
`last_report` 让"读上期 1~2 篇"这一步内化。

### 4.3 `qa` — 质检 + 核验（一次调用，替掉 codemode 与零散 websearch）

**参数**：`draft: string`（草稿正文）。

**内部**：

1. **确定性检查**（不花 jev）：
   - 汉字总数 ≤2500；速览行 ≤40；观点块 ≤200；观点 ≤3；无 `http` 链接；
   - 两节标题存在（`一、资讯速览` / `二、观点与趋势`）；
   - 观点六字段齐全（`观点：/依据：/检验：/结论：/趋势：/与上期相比：`）；
   - 禁用词：`惨烈|崩了|利好|值得警惕|大概率|显然|直白说|三点如下|这意味着`；
   - 速览行不含 `认为|预计|建议|称将|研报显示|分析师表示`；
   - 体育娱乐关键词。
2. **jev 逐行语义质检**（批量 10 行/调用，noul 问题）：叙事夹带观点、情绪化措辞。
3. **核验循环**（web 工具存在时）：
   - 从草稿中提取需核验对象：观点块的 `检验` 行含「未证实」，或 `verify_needed=true` 的候选；
   - 逐条 `source_check({claim, fetchContent:true, numResults:5})` → jev 判 `verdict` + `counter`；
   - **硬性预算 ≤4 次**，计数写 run；工具不存在则返回 `verification_unavailable:true`。
4. **返回**：

```json
{
  "ok": true,
  "violations": [ { "line": 12, "kind": "length", "why": "速览行 52 字 > 40" },
                  { "line": 31, "kind": "mixed", "why": "叙事夹带观点" } ],
  "verification": [ { "claim": "…", "verdict": "mixed",
                      "counter": "…>引文片段<…", "source": "reuters.com" } ],
  "budget_used": 2,
  "stats": { "hanzi": 2180, "facts": 14, "opinions": 3 }
}
```

### 4.4 `commit` — 终审 + 落盘 + 状态更新（唯一的写入口）

**参数**：`body: string`（正文，含或不含表头都兼容）、`strict?: boolean`（默认 true）。

**内部**：
1. 重复 qa 的确定性检查作为**终审闸门**；不过 → `{ok:false, violations}`，不落盘。
   `strict:false` 时落盘并在 run 里记违规（逃生舱，避免整期无产出）。
2. **解析正文**（格式是固定的，解析可靠）：
   - 速览行 `- …` → 事实：`text`、`unconfirmed = /待确认/`、`numbers`（正则）；
   - 观点块六行 → 观点：`source`（从 `观点：@账号/财新《…》` 提取）、`claim/evidence/checks/
     conclusion/trend`、`change`（映射"新出现/结论反转/被强化/被削弱"）；
   - `延续：…` 行 → 事实的延续标记。
3. **拼首行表头**：`# MM-DD hh｜本期使用 N 条（速览 x 条 + 观点 y 条）｜本次新抓取 M 条｜上期：…`
   - x、y 来自解析计数；`M` 来自 brief 的 run 记录；上期名来自最近报告文件；北京时间由插件算。
4. **落盘**：原子写 `RSS_REPORT_DIR/MM-DD_hh.md`（tmp + rename）。
5. **台账状态机**：
   - 事实：已存在且本期有数字 → 更新/保持 `confirmed`；只写 `延续` → 不动状态；
     `unconfirmed` 且本期仍无证据 → 保持 `unconfirmed`；本期带证据出现 → 升 `confirmed`；
     新事实 → insert。
   - 观点：`change=new` → insert；`结论反转/被强化/被削弱` → 更新对应条目并刷新六字段；
     未在本期出现的 active 观点保持（到期策略见 §8）。
6. **标已读**：把 brief 记录的**本批 item ids 全部**标已读（用户基线 3）。
7. **写 run**（status=`committed`，记录 x/y/M/路径/违规/核验次数）。
8. **返回** `{ok, reportPath, reportText, header, ledger:{facts_added, opinions_added, unconfirmed_open}}`。

### 4.5 与旧接口的对照

| 旧 | 新 | 说明 |
|---|---|---|
| `fetch` + `unread` + `read`×2 | `brief` | 3~5 次调用 → 1 次，且素材已分类去重 |
| LLM 在上下文里分事实/观点 | jev 批量分类 | 机械判断下沉 |
| `web_search`×N + 自查格式 | `qa` | 预算硬控 + 逐行质检 + 核验判定 |
| `write` + `markread` | `commit` | 原子收尾，文件与状态一致 |
| `search`/`recent`/`tags` | 删除/合并进 `manage` | 任务用不到 |

---

## 5. 各组件分工（v4 调整）

| 组件 | 职责 | 说明 |
|---|---|---|
| **插件（pi 扩展）** | 全部确定性工作 + jev 调度 + 状态机 | brief/qa/commit 三个接口就是完整的报告引擎 |
| **jev（typesafe）** | 分类、对比、质检、核验判定 | 批量调用；置信度门控；中文校准前置 |
| **主模型** | 只做两次生成：初稿 + 按质检修订 | 不再调 unread/read/websearch/write/markread |
| **codemode** | **逃生舱**：调试 triage、临时分析、特殊日子的自定义编排 | 不再是主路径。主路径零脚本，因为插件已把编排固化 |

> codemode 仍值得保留的能力：`tools.rss({action:"brief"})` 本身就是 codemode 可调用的，
> 遇到"今天需要核验 8 条而不是 4 条"或"想看看某条为什么被降权"这类临时需求时，
> 写一段脚本组合 `rss` + `source_check` + `models.classify` 即可，不影响主路径。

---

## 6. jev 任务设计

### 6.1 机制

- **批量一次调用**：一个 state = 一批 ≤10 条 item；questions 用 `i0_/i1_` 前缀，一次问完。
  官方 cookbook 验证：批处理与单条调用答案一致，成本省 ~12x、快 ~10x（Speculative Fan-Out）。
- **state 上限** 32k tokens、总 context 64k：每批 ~3k tokens，安全。
- **置信度三档门控**（Choice/Score 有 `confidence`；Noul 在 pi 里映射为 `type:"bool"` 取 `probability`）：

| 档 | 规则 | 动作 |
|---|---|---|
| 高（≥0.7） | 采信 | 可执行"丢弃"类决定（skip / 重复 / change=none） |
| 中（0.5~0.7） | 存疑 | 标 `uncertain` 放行给主模型 |
| 低（<0.5） | 不采信 | 放行 + 不据此丢条目 |

> **中文修正**：jev 支持中文但 CJK 准确率低于英文，且有社区实测的「低准确率 + 高置信度」现象。
> 因此：① 校准（§12 P2）前，**丢弃类判定一律要求高置信**；② criteria 用中英双语
> （instructions 英文、state 中文）；③ 校准要同时测置信度-准确率一致性，不一致则上调阈值。

### 6.2 任务清单

| ID | 作用域 | questions |
|---|---|---|
| J1 初筛（全部候选） | 每批 10 条 | `ik_kind`:{fact,opinion,mixed,skip}；`ik_topic`:{国际经济,国内经济,国际形势,国内形势,政策,科技,民生,趣闻,其他}；`ik_has_number`:noul；`ik_value`:score 0-10 |
| J2 观点变更（opinion/mixed） | 批内附活跃观点台账 | `ik_relation`:{new,reversal,reinforce,weaken,none,na}；`ik_target`:{台账id,none}；`ik_verify`:noul |
| J3 事实重复（fact/mixed） | 批内附上期事实 | `ik_repeat`:noul；`ik_repeat_of`:{id,none}；`ik_new_number`:noul |
| J4 跨源聚类 | 近似标题对 | `same_event`:noul（只在确定性相似对上跑） |
| J5 核验判定（qa） | claim + source_check 引文 | `verdict`:{supports,refutes,mixed,insufficient}；`counter`:{片段id,none} |
| J6 草稿质检（qa） | 逐行 | `l_k_mixed`:noul；`l_k_emotion`:noul |

### 6.3 中文校准（上线前置）

真机抽 3 期/预跑 3 期，每类人工标注 50 条：
- 达标线：J1 `skip` 误杀 <2%（宁多勿漏）；J2 与人工一致 ≥85%；J3 ≥85%；置信度-准确率一致；
- 不达标：先双语 criteria 重试；仍不行 → J1 降级为只做 J2/J3 + 主模型多看一点，
  或本地 llama.cpp 分类器兜底（pi 原生支持 llama-cpp classifier）。

---

## 7. 定时任务 prompt 改造（替换原「2. 执行步骤」）

原提示词 1~6 步里，"抓取/读上期/标记已读/保存"现在都由接口完成。把执行步骤替换为：

```markdown
## 2. 执行步骤（接口固定，禁止使用 unread/search/recent 等查询接口）

1. **准备**：调用 rss 工具一次：`{"action":"brief"}`。
   它已完成抓取、去重、分类、上期台账对账，并返回：
   `facts`（速览候选，含主题/数字/来源）、`opinions`（观点候选，含 relation 与正文）、
   `ledger`（上期事实、活跃观点、待确认清单）、`last_report`（上期报告全文）、
   `session`（本期文件名与表头数据）。**只用这些材料**，不要再抓取或翻文件。
2. **写草稿**：按第 0、1 节的规则写「资讯速览」与「观点与趋势」；
   速览行的数字优先用候选里的 `numbers`；观点只写 relation 为
   new/reversal/reinforce/weaken 的候选（这是硬约束）；待确认事实按 `ledger.unconfirmed` 继续标「待确认」。
3. **质检与核验**：调用 `{"action":"qa","draft":"<草稿全文>"}`。
   按返回的 `violations` 逐条修改；用 `verification` 里的引文写「检验」行
   （该接口已强制 websearch ≤4 次，你不要自己再搜）。
4. **提交**：调用 `{"action":"commit","body":"<修订后的正文>"}`。
   它做终审、拼首行表头、存 `/home/ericfu/Chat/rss/`、更新台账并标记已读。
   若返回 `ok:false`，按其 `violations` 修正后重试（最多 2 次）。
5. **输出**：最终回复只输出 `commit` 返回的 `reportText` 原文，不加任何额外内容。
```

效果：LLM 每期固定 3 次调用；首行统计、文件名、标已读、台账全部自动，
"读完自己数一遍"由插件用正则精确完成。

> 如果你不想动现有提示词，也可以在插件里用 `pi.on("input")` 检测到报告类提示词时，
> 自动在末尾追加同样的「接口约定」。两种都行，改提示词更直接。

---

## 8. 数据与状态

### 8.1 Schema 增量

```sql
-- rss_items 追加
brief_run_id INTEGER, kind TEXT, topic TEXT, has_number INTEGER,
novel INTEGER, change TEXT, verify_needed INTEGER,
conf_json TEXT, triage_json TEXT, triaged_at INTEGER

-- 新表
rss_ledger_facts(id, text, entities, numbers, status,      -- confirmed|unconfirmed|closed
                 supersedes_id, source_item_ids, first_seen_run, last_seen_run,
                 created_at, updated_at)
rss_ledger_opinions(id, source, claim, evidence, checks, conclusion, trend,
                    signal, time_window, change, status,     -- active|retired
                    source_item_ids, first_seen_run, last_seen_run,
                    created_at, updated_at)
rss_runs(id, slot, slot_date, fetched_new, briefed, facts_used, opinions_used,
         report_path, status, jev_stats, websearch_used, violations,
         started_at, finished_at)
rss_state(key TEXT PK, value TEXT, updated_at INT)
```

标签继续用现有 `rss_feeds.tags`（真机：技术/财经/资讯/AI科技），作为 jev J1 的主题提示，
不承担过滤职责；观点来源由 feed title 提供（`source` 字段），skip 由关键词 + jev 判定。

### 8.2 台账与状态机

| 对象 | 规则 |
|---|---|
| 事实 `confirmed` | 本期带数字出现 → 更新 `last_seen_run`；缺失久了 → 不动（历史事实不为报告服务，只在 `prev_facts` 里给去重用） |

> **表头计数口径（P1 实现）**：`x` = 普通速览行数，**不含 `延续：` 行**；`N = x + y`。
> 若希望 `延续` 也计入 x，改 `cmd_commit` 里 `facts_used` 的一行即可。
| 事实 `unconfirmed` | 每期都进 `ledger.unconfirmed`；本期有证据 → 升 `confirmed`；**不因跨期自动升级**（C6） |
| 观点 `active` | 本期 `reinforce/weaken/reversal` → 更新六字段 + `change`；本期 `none` → 不返回主模型（C3） |
| 观点 `retired` | 连续 N 期（默认 8）未被引用且未被推翻 → 标记 retired，从 `active_opinions` 移出 |
| run | `briefed` → `committed`；失败留痕，下期未读自然重来 |

### 8.3 时间与幂等

| 项 | 设计 |
|---|---|
| 时区 | `RSS_TZ=Asia/Shanghai`：**仅用于文件名/表头**（机器是 NY/UTC 无妨）；调度 cron 用 `--timezone Asia/Shanghai`；资讯时间不过滤 |
| 窗口 | 未读即窗口（标已读闭环） |
| 防重 | `is_read` 是唯一状态；未出报告不标已读 → 下期重来；fetch（guid）与 commit（tmp+rename）幂等 |
| 崩溃 | commit 前失败 = 未标已读 = 下期重来；commit 后失败 = 文件与已读已落地 |

---

## 9. 凭据读取（key 分层）

```ts
const AGENT_DIR = (process.env.PI_CODING_AGENT_DIR ?? join(homedir(), ".pi", "agent"))
  .replace(/^~(?=$|\/)/, homedir());
const STORE  = join(AGENT_DIR, "rss-plugin", "auth.json");      // 插件私有，0600
const SHARED = process.env.TYPESAFE_KEY_FILE ?? join(homedir(), ".config", "typesafe", "key");

// 优先级 1 env → 2 插件私有 → 3 共享文件（兼容你现有自建扩展）
```

安全规矩（照抄 pi-typesafe `credentials.js` 的既有做法）：
1. 写盘 `mode: 0o600`；读前拒绝他人可读：`if (statSync(p).mode & 0o077) throw`；
2. key 只进 `Authorization: Bearer` 头，不进 argv/URL/日志，永不回显；
3. headless 逃生口：env 优先；可选 `/rss login` 交互写入 STORE；
4. 不硬编码唯一路径，换 key 只动一处。

插件加载时把找到的 key 写入 `process.env.TYPESAFE_API_KEY`，pi 的 typesafe provider
请求时自动带认证（`ctx.modelRegistry.classify` 无需其他改动）；已 `/login` 过则 env 已存在。

---

## 10. 调度与配置

```bash
paseo schedule create --cron "0 8 * * *"  --timezone Asia/Shanghai \
  --name rss-report-morning --provider pi/opencode-go/deepseek-v4.1-flash \
  --cwd /home/ericfu --max-runs 720 --expires-in 60d "$(cat ~/rss-report-prompt.md)"

paseo schedule create --cron "0 21 * * *" --timezone Asia/Shanghai \
  --name rss-report-evening --provider pi/opencode-go/deepseek-v4.1-flash \
  --cwd /home/ericfu --max-runs 720 --expires-in 60d "$(cat ~/rss-report-prompt.md)"
```

| 变量 | 默认 | 用途 |
|---|---|---|
| `TYPESAFE_API_KEY` | — | 优先级 1；无则走插件私有库/共享文件 |
| `TYPESAFE_KEY_FILE` | `~/.config/typesafe/key` | 共享文件（兼容现有扩展） |
| `RSS_JE_V_MODEL` | `typesafe/jev-latest` | 分类器；可 pin `typesafe/jev-1.13`；兜底 `opencode/jev-1.13-free` |
| `RSS_JE_V_CONCURRENCY` | 4 | 分类并发（限流 1200 rpm 用不满） |
| `RSS_BRIEF_MAX` | 250 | 单期候选上限（真机约 200） |
| `RSS_BRIEF_FACTS_MAX` | 60 | 事实候选进 payload 的上限 |
| `RSS_BRIEF_OPINION_TOP` | 10 | 观点候选给全文的条数 |
| `RSS_BRIEF_OPINION_CHARS` | 1200 | 观点候选正文截断 |
| `RSS_BRIEF_DEADLINE_MS` | 150000 | triage 预算，超时返回部分 + pending |
| `RSS_REPORT_DIR` | `/home/ericfu/Chat/rss` | 报告目录 |
| `RSS_TZ` | `Asia/Shanghai` | 文件名/表头时区 |
| `RSS_SKIP_KEYWORDS` | 内置体育娱乐词 | 标题/摘要过滤（真机无 skip 类 tag） |
| `RSS_OPINION_HINTS` | `@` / `财新` 等 | 观点源 title 匹配提示（可选，jev 仍会独立判定 kind） |
| `RSS_SKIP_KEYWORDS` | 内置体育娱乐词 | 标题/摘要过滤 |
| ~~`RSS_AUTO_FETCH_MINUTES`~~ | — | **已移除**：插件内定时抓取删除，paseo 是唯一时钟 |

一次性准备：`rss manage {op:"tags"}` 查现有 tag → 配映射；建报告目录；
`paseo schedule run-once <id>` 全链路验证。

---

## 11. 成本与延迟（真机 200 条/期）

| 项 | 现状 | v4 |
|---|---|---|
| 工具调用 | 10+ 次 | **3 次** |
| 进主模型的内容 | ~60k 字原文 | 结构化 payload ~10~15k tokens（含上期报告） |
| 主模型生成 | 多轮（边查边写） | **2 次**（初稿 + 修订） |
| jev | 0 | 全量分类 + 观点/事实对比 + 质检 ≈ 30 次批量调用 ≈ 90k input tokens |
| jev 成本 | — | $0.042/M input → 约 **$0.004/期、$4/年** |
| web 核验 | LLM 自觉，可能超 4 | **硬性 ≤4**，计数入 run |
| 延迟 | 串行抓取 + 多轮 | 抓取 10~30s + triage 5~10s + 2 次生成，预计 <5 分钟 |

jev 便宜到不值得为省钱牺牲质量：**所有"丢弃"判定必须高置信**，省下的永远是主模型 token。

---

## 12. 分阶段落地

> **实现状态（P1~P3 已完成，待真机校准）**：`rss.py`：`--json`、并发 fetch + flock、`brief`/`payload`/
> `annotate`/`check`/`commit`/`ledger`/`sample` 命令、`markread --ids`、四张新表、跨源事实聚类、
> 观点退役（`RSS_OPINION_RETIRE_RUNS`）。`index.ts`：新接口表（brief/qa/commit/fetch/unread/
> markread/manage/calibrate）+ `outputSchema`/`structuredContent` + key 分层读取 +
> **jev 批量分类（J1/J2/J3 + 置信度门控 + 429 退避 + 降级）** + **qa 语义质检（J6）与核验（J5，硬预算）**。
> 合成数据、真 pi 会话（含降级路径与 source_check 核验）均已验通。
> **唯一未做**：用真机 key 跑 `calibrate` 中文校准（需在真机执行，决定 jev 丢弃类判定能否启用）。

| 阶段 | 内容 | 验收 |
|---|---|---|
| P0 准备（真机） | 查 tag、配 key、建目录、建两条 schedule | `run-once` 出文件 |
| P1 接口骨架 | rss.py：`--json`、并发 fetch、`candidates`、`ledger`、`commit`（解析+表头+落盘+标已读）、`markread --ids`、`manage`；index.ts：新接口表 + `brief`（先启发式，无 jev）+ `qa`（仅确定性）+ `commit`；prompt 换成 §7 | 全流程 3 次调用跑通；文件/对话一致 |
| P2 jevy + 中文校准 | J1~J3 入 brief，J5/J6 入 qa；真机抽 3 期人工标注 50 条/类 | §6.3 达标线；payload ≤15k tokens |
| P3 调优 | J4 聚类、topic 分流、feed 权重、延续/待确认回归、retired 策略 | 连续 10 期无重复事实、无待确认升格 |
| P4 逃生舱 | codemode 调试脚本（explain/手动重跑）、`/rss-report` 补跑命令 | 运维顺手 |

---

## 13. 风险

| 风险 | 对策 |
|---|---|
| **jev 中文不准（最大风险）** | §6.3 校准前置；丢弃类判定高置信；criteria 双语；不达标降级 J1 或本地 llama.cpp |
| 低准确率高置信 | 校准测一致性；阈值上调；关键判定抽检 |
| jev 挂/限流(429) | `classify` 不抛异常；429 指数退避；连续失败 → `triaged:false` 全量放行，绝不静默丢条目 |
| triage 超时 | 返回部分 + pending，下期续做（结果已缓存） |
| 报告解析失败 | commit 返回 violations 让模型修格式；最多 2 次；`strict:false` 逃生舱 |
| 台账写坏 | 只在 commit 成功路径更新；`rss manage {op:"stats"}` 可导出；run 可回放 |
| 双 schedule 撞车 | flock + run 互斥；未读即窗口，天然不重叠 |
| key 泄露 | §9 安全规矩 |

---

## 14. 已确认 / 待确认

已确认（来自你）：
1. 真机订阅已长期工作，每期约 200 条，早晚各一次；
2. 标签只有主题四类：技术(9)/财经(9)/资讯(8)/AI科技(5)；无 opinion/skip 类标签，
   方案改为「feed title + jev」做观点源识别、关键词做 skip，tags 只作主题提示；
3. `RSS_DB_PATH` 未设置 → 库在 `/home/ericfu/.pi/agent/rss-data/rss.db`，报告目录 `/home/ericfu/Chat/rss/`；
4. 机器时区 NY/UTC，文件名按北京时间转换；
3. 资讯不按发布时间过滤（未读即窗口）；标已读 = 本批全部（未出报告不标）；
4. key 采用 env → 插件私有 auth.json → 共享文件 三层读取；
5. 接口可以按任务重新设计，定时任务 prompt 里可以直接写明用哪些接口。

待确认：无。可以开工 P1。

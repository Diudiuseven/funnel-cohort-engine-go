# Funnel & Cohort Engine

面向**持续追加、可能乱序/重复/迟到**的用户行为事件流，实时维护两类可查询结果：

1. **漏斗转化（Funnel Conversion）**：按配置的有序步骤序列与转化时间窗，统计每个旅程开始日触达各步骤的**去重旅程数**。
2. **分群留存（Cohort Retention）**：按用户**首次出现**所在周期（时/日/周/月）划分群组，统计之后每个周期的去重活跃留存。

核心正确性目标：**任何时刻的查询结果，都等于把当前已接受事件按事件时间 `(timestamp, event_id)` 完整有序重放后得到的结果**（受"允许迟到上限"与有界内存策略的明确约束，见下文）。

---

## 快速开始

```go
loc, _ := time.LoadLocation("Asia/Shanghai")
cfg := funnel.Config{
    Timezone: loc,
    Funnel: funnel.FunnelConfig{
        Steps:  []string{"app_open", "add_cart", "pay"},
        Window: 24 * time.Hour,
    },
    Cohort:          funnel.CohortConfig{Period: funnel.PeriodDay},
    AllowedLateness: 2 * time.Hour,
    MaxUsers:        100_000,
}
e, _ := funnel.NewEngine(cfg, nil)

e.AddEvent(funnel.Event{ID: "1", UserID: "alice", Name: "app_open", Timestamp: t0})
rows := e.FunnelResult()  // []FunnelRow
cohorts := e.CohortResult() // []CohortRow
```

---

## 窗口与转化语义

- 每个**步骤 0 事件**都会开启一条新旅程（同一用户同一天可以有多条独立旅程）。
- 对于一条在 `start` 时刻开启的旅程，事件 `e` 满足
  `e.timestamp - start <= Window` 且名称等于"下一步"时，旅程推进到该步骤；
  **窗口边界为闭区间**（恰好等于 `Window` 仍算转化）。
- 一个后续事件会同时推进所有仍在窗口内、等待该步骤的活跃旅程
  （例如同一次"支付"不会跨旅程串行匹配，每条旅程独立判定）。
- 步骤只允许**严格按顺序**推进；没有先加购的支付不计转化。
- 旅程归属到其**步骤 0 事件在配置时区下的当地自然日**（按日分桶聚合计数）。
- 结果行：`Started`/`StepReached[0]` 为去重旅程数，`StepReached[k]` 为到达第 k 步的去重旅程数，因此有 `StepReached[k] >= StepReached[k+1]`。

### 提交时间线（漏斗）

某旅程开始日 D 的桶在以下时刻才转为**已提交（committed）**：

```
commitDeadline(D) = D 日 24:00（当地） + Window + AllowedLateness
```

理由：D 日开启的旅程最晚在 `D 日 24:00 + Window` 转化，而任何属于它的事件
最晚在再经过 `AllowedLateness` 后到达。水印到达该时刻后，该桶不可能再变化。

---

## 分群留存语义

- 群组 = 用户**最早事件**所在周期；周期支持 `hour / day / week / month`。
  - `week` 使用 **ISO 周**（周一开始），键格式 `2026-W02`。
- `Retained[0]` = 群组大小；`Retained[k]` = 群组中在"首见周期之后第 k 个周期"
  至少活跃过一次的去重用户数。
- **迟到的首次事件会纠正群组归属**：如果一个用户已经被临时归入较晚群组，
  随后一条更早的首见事件到达，引擎会把该用户从旧群组移除并加入正确群组，
  旧群组对应格子的临时计数随之回退。只要纠正发生在相关格子提交之前
  （水印越过格子截止时间），最终结果与有序重放完全一致。

### 提交时间线（分群）

格子 `(群组周期 C, 留存偏移 k)` 在以下时刻提交：

```
commitDeadline(C, k) = (C + k) 周期的下一个周期起点（当地） + AllowedLateness
```

---

## 时区处理

- `Config.Timezone` 决定**所有**日历切分：漏斗旅程归属日、群组周期、周期起止、
  跨日/跨周边界。所有"日/周/月"加减都使用 Go 的日历运算（`time.Date`/`AddDate`），
  因此夏令时导致的长短日会被正确处理（"下一天 00:00"始终是当地墙上时间）。
- 转化窗 `Window` 是固定物理时长（`time.Duration`），按时间戳差比较。
- 周期键均为当地墙上时间渲染：小时 `2006-01-02T15`、日 `2006-01-02`、
  周 `2006-W02`、月 `2006-01`。
- 事件的 `Timestamp` 建议携带明确位置信息；内部统一 `In(Timezone)` 处理。
- `Timezone == nil` 时默认 `UTC`。

---

## 乱序、重复与迟到

| 情形 | 处理 |
|---|---|
| 乱序到达 | 每用户事件按 `(timestamp, id)` 插入有序；新事件到达时以"删除该用户旧贡献 → 重放该用户 → 加回贡献"的方式做增量修正，结果始终等价于有序重放。 |
| 重复投递 | 以 `Event.ID` 在有界 LRU 集合中去重，同一 ID 只计一次；`ID` 为空的事件不做去重。 |
| 迟到但未超限 | 正常参与修正（包括在提交前移动漏斗旅程与群组归属）。 |
| 迟到超过上限 | 当 `event.timestamp + AllowedLateness < watermark` 时丢弃，`AddEvent` 返回 `ErrLateDropped`；该事件已不可能影响任何未关闭窗口。 |

---

## 水印（Watermark）

- 水印自动推进为 `max(已见最大事件时间) - AllowedLateness`；
  也可用 `AdvanceWatermark(t)` 显式推进（例如源空闲时声明"不会再有更早数据"），
  显式值作为水印下限。
- 水印**只能单调前进**：`AdvanceWatermark` 传入更早时间返回 `ErrWatermarkBack`；
  更早的事件也绝不让水印回退。
- 水印越过窗口/格子截止时间后，临时结果（`Committed=false`）转为已提交
  （`Committed=true`）。**已提交结果保证不再被迟到数据改动。**
- 查询结果同时包含临时与已提交行，行级 `Committed` 标志可区分二者。

---

## 有界内存与精度影响（重要）

引擎**不会无上限增长**，采用以下确定性策略：

1. **水印驱动的状态清理（精确，无精度损失）**
   - 漏斗：某旅程日提交后，严格早于"第一个仍开放日 00:00"的用户事件被裁剪；
     裁剪后无保留事件的用户状态被删除。
   - 分群：已完全关闭周期之前的活跃记录被裁剪；`firstSeen` 永不在此阶段删除，
     以保证迟到首见事件仍能纠正归属。
2. **LRU 硬上限（`MaxUsers`、`MaxDedupEntries`，有明确精度影响）**
   - 每类每用户状态表最多保留 `MaxUsers` 个用户，超限按最近最少使用淘汰；
     去重 ID 集合最多保留 `MaxDedupEntries` 个。
   - **精度影响**：被淘汰用户若再次出现，会被当作**新用户/新旅程**处理；
     其未提交期间的旅程可能漏计，分群可能被重新归入新群组；被淘汰的事件 ID
     若被重投，可能被再次计数。该误差只影响**尚未提交**的临时结果，
     **已提交结果永不因淘汰而改变**。
   - **配置建议**：把上限设为"允许迟到时间窗 + 漏斗窗口内的预期活跃用户数"，
     在水印能稳定前进的流上，绝大多数状态由机制 1 精确清理，LRU 仅作为
     高基数/水印停滞场景下的内存安全阀。
3. 不依赖全局后台 goroutine：提交、裁剪、淘汰都在事件摄入/推进水印的路径上同步发生，
   因此结果可复现、无定时不确定性。

---

## 并发模型

- 所有方法（`AddEvent` / `AdvanceWatermark` / `FunnelResult` / `CohortResult` /
  `Snapshot` 等）都在同一把互斥锁下串行执行。
- 因而**写入与查询可并发**；任一查询看到的都是某次摄入原子完成之后的单一自洽快照，
  不会出现半更新或字段级混合。

---

## 快照与恢复

- `SaveSnapshot(path)` 原子写入（临时文件 + `rename`）版本化 JSON 全量状态：
  水印、已提交漏斗/分群计数、未提交的每用户事件与首见/活跃记录、去重 ID 集合、
  已应用序号 `AppliedSeq` 与清理水位。
- `LoadSnapshot(path, cfg)` 校验快照内嵌配置（步骤、窗口、周期、允许迟到、时区
  必须一致，否则 `ErrConfigMismatch`），随后从**最近已提交位置**继续：
  - 已提交计数直接沿用，未提交结果由保留状态重放重建；
  - 去重集合恢复，重启后重复投递不会被重复计数；
  - 同一输入与配置下，"中途快照恢复后继续"与"不中断运行"的输出逐项一致
    （见 `TestSnapshotRecoveryReproducibility`）。

建议的持久化节奏：每处理 N 条事件或每推进一段水印后保存一次。崩溃恢复后，
从快照点之后重新投递事件即可（重复投递由事件 ID 安全抑制）。

---

## 配置项

| 字段 | 含义 | 默认/约束 |
|---|---|---|
| `Timezone *time.Location` | 全部日历切分所依据的时区 | nil → UTC |
| `Funnel.Steps []string` | 有序步骤事件名 | 至少 2 个 |
| `Funnel.Window time.Duration` | 从步骤 0 起的最大转化时长（闭区间） | 必须 > 0 |
| `Cohort.Period` | `hour`/`day`/`week`/`month` | 必填且合法 |
| `AllowedLateness time.Duration` | 事件可落后于水印仍被接受的上限；同时决定自动水印与提交时间 | 通常 ≥ 实际迟到分布 |
| `MaxUsers int` | 每类每用户状态 LRU 上限 | 0 → 1,000,000 |
| `MaxDedupEntries int` | 去重 ID 集合 LRU 上限 | 0 → 1,000,000 |

---

## 测试覆盖

`go test ./... -race` 覆盖：

- 有序基础转化、同用户多条旅程
- 乱序到达与有序重放逐项一致（`TestOutOfOrderConvergesToOrderedReplay`）
- 重复投递不重复计数
- 水印自动/显式推进、单调约束、临时→已提交翻转、超迟到丢弃
- 分群留存、迟到首见事件纠正群组
- 时区分桶（America/Los_Angeles 本地日/小时）、跨午夜窗口的提交时间线
- 周（ISO）/月周期
- LRU 状态有界
- 快照保存/恢复/配置不匹配拒绝、恢复后可复现且重复投递安全
- 并发写入 + 并发查询下的快照自洽（race detector）

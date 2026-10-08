# funnel-cohort-engine-go

面向用户行为事件流的**漏斗转化**与**分群留存**计算引擎。输入是持续追加、
可能乱序/迟到/重复的事件，输出是可并发查询的转化与留存结果。
任何计算结果都收敛到与"按事件时间完整有序重放"一致的状态。

## 快速开始

```go
cfg := engine.Config{
    Timezone: "Asia/Shanghai",
    Funnel: engine.FunnelConfig{
        Steps:  []string{"view", "cart", "pay"},
        Window: time.Hour,
    },
    Cohort:          engine.CohortConfig{Period: engine.DayPeriod, MaxOffsets: 7},
    AllowedLateness: 10 * time.Minute,
    MaxActiveUsers:  100000,
    SnapshotPath:    "/var/lib/engine/snap.gob",
}
e, _ := engine.New(cfg)        // 或 engine.Load(cfg) 从快照恢复
e.Ingest(events...)            // 可乱序、可重复、可迟到
funnel := e.QueryFunnel()      // 自洽快照，与写入并发安全
retention := e.QueryRetention()
_ = e.SaveSnapshot()           // 原子持久化，重启后 Load 恢复
```

## 事件模型

```go
type Event struct {
    ID        string    // 全局唯一，用于去重（重复投递不重复计数）
    UserID    string
    Name      string    // 事件名，与漏斗步骤匹配
    Timestamp time.Time // 事件时间（非到达时间）
}
```

## 漏斗语义

- 用户在 `Window` 时长内**按顺序**完成 `Steps` 全部步骤计为一次转化。
- 匹配采用**贪心、不重叠尝试**：空闲时遇到第 1 步事件开启一次尝试；
  尝试中按序消费后续步骤事件；事件时间超出 `attemptStart + Window`
  尝试即失败。尝试之间不重叠（进行中的尝试忽略新的第 1 步事件）。
- 每次尝试按**第 1 步事件所在日**（配置时区）归入日桶，统计
  `Entered`（进入）与 `Converted`（转化）。
- 乱序/迟到事件触发该用户**全量重算 + 增量 diff**，因此任意到达顺序
  的最终结果与有序重放完全一致（有性质测试保证）。

## 分群留存语义

- 群组 = 用户**最早事件时间**所属的周期（日或周，周一起始），
  周期边界按 `Timezone` 对齐。
- `Retained[n]` = 群组中在群组周期之后第 n 个周期仍有事件的用户数。
- **迟到的更早首次事件**会把用户改归正确的群组：旧的错误归属与
  留存计数被增量冲销，不会长期保留。

## 时区处理

所有"按日/按周"的边界（漏斗日桶、群组周期、提交判定）都基于
`Config.Timezone`（IANA 名称，如 `Asia/Shanghai`）。周期索引使用
civil-date 算法计算，不受 DST 影响；同一事件在不同 `Timezone`
配置下会落入不同的桶（有测试覆盖跨日场景）。

## 水印与提交语义

- 水印 `W = max(已见事件时间) - AllowedLateness`，**只单调前进**。
- 事件时间早于 W 的事件视为超出允许迟到上限，**被丢弃**并计入
  `IngestStats.TooLate`。
- 查询结果中每个桶/群组带 `Committed` 标记：
  - 漏斗日桶 D：当 `W >= D 末日(配置时区) + Funnel.Window` 时提交；
  - 群组周期 P 的规模：当 `W >= P 期末` 时提交。
- 提交前的结果是**临时的**，可能被后续乱序/迟到事件修正；
  提交后的结果不再变化。

## 一致性保证

| 问题 | 机制 |
|---|---|
| 重复投递 | 按事件 ID 去重，重复件直接丢弃 |
| 乱序/迟到 | 每用户事件列表为唯一事实来源，到达即重算并 diff 聚合 |
| 并发读写 | 写入串行化；每次写入后发布不可变快照（`atomic.Pointer`），查询无锁读快照，不会看到半更新 |
| 崩溃恢复 | `SaveSnapshot` 原子落盘（tmp+rename+fsync），`Load` 恢复水印/去重表/聚合/用户状态，重放已处理事件会被去重 |
| 可复现 | 相同输入+配置输出完全一致（聚合为可交换的加法，查询按键排序） |

## 内存上限与淘汰策略（确定性）

内存**禁止无上限增长**，淘汰层级如下：

1. **去重表**：事件时间早于水印的条目即删（其重复件必然被判迟到），
   规模上界 ≈ 允许迟到窗口内的事件量。
2. **冷用户休眠化**：`lastEventTime < W - horizon` 的用户转为休眠态：
   丢弃事件列表与贡献缓存，仅保留群组归属（防止回归用户被重复计入
   群组规模）。其中
   `horizon = AllowedLateness + Funnel.Window + (MaxOffsets+1)*周期 + 25h`，
   这些用户的所有贡献已提交且不可能再被合法迟到事件修改，
   **对精度无影响**。
3. **硬上限降级**：用户条目（活跃+休眠）超过 `MaxActiveUsers` 时，
   先删除最冷的休眠条目，再按 `lastEventTime` 淘汰最冷的活跃用户。
   **精度影响**：被删除的休眠条目若回归，会按新用户重新入群
   （群组规模可能重复计数）；被强制淘汰的活跃用户，其未提交贡献
   被冻结在聚合中，后续迟到事件无法修正。已提交结果均不受影响。
   上限触发表明容量配置不足，应调大 `MaxActiveUsers` 或缩短
   `AllowedLateness`/`MaxOffsets`。

## 配置项

| 字段 | 含义 | 备注 |
|---|---|---|
| `Timezone` | 所有周期边界的时区 | IANA 名，必填 |
| `Funnel.Steps` | 有序步骤（事件名） | 至少 1 步 |
| `Funnel.Window` | 首步到末步的最大时长 | 也影响提交判定与 horizon |
| `Cohort.Period` | `DayPeriod` / `WeekPeriod` | 周一起始 |
| `Cohort.MaxOffsets` | 统计的最大后续周期数 | 影响 horizon |
| `AllowedLateness` | 允许迟到上限（水印滞后量） | 越大越容错，内存越久 |
| `MaxActiveUsers` | 内存用户数硬上限 | 0 = 不设硬上限（不推荐） |
| `SnapshotPath` | 快照文件路径 | 空 = 不启用持久化 |

## 测试

```sh
go test -race ./...
```

覆盖：乱序收敛、重复去重、迟到修正与超迟到丢弃、窗口过期、
跨日时区、群组改归、水印提交标记、快照恢复等价、并发读写快照
自洽（-race）、内存淘汰上限、输出可复现。

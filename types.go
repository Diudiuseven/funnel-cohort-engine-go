// Package engine 提供面向用户行为事件流的漏斗转化与分群留存计算能力。
package engine

import "time"

// Event 是一条用户行为事件。ID 用于去重，Timestamp 为事件时间（允许乱序/迟到）。
type Event struct {
	ID        string
	UserID    string
	Name      string
	Timestamp time.Time
}

// PeriodKind 分群周期类型。
type PeriodKind int

const (
	DayPeriod PeriodKind = iota
	WeekPeriod
)

// FunnelConfig 漏斗配置：步骤序列与转化窗口。
type FunnelConfig struct {
	Steps  []string      // 有序步骤，按事件名匹配
	Window time.Duration // 从第 1 步到最后一步允许的最大时长
}

// CohortConfig 分群留存配置。
type CohortConfig struct {
	Period     PeriodKind // 群组周期（日/周，按时区对齐）
	MaxOffsets int        // 统计的最大后续周期数
}

// Config 引擎配置。
type Config struct {
	Timezone        string // 窗口边界与跨日切分时区，如 "Asia/Shanghai"
	Funnel          FunnelConfig
	Cohort          CohortConfig
	AllowedLateness time.Duration // 允许迟到的上限（水印滞后量）
	MaxActiveUsers  int           // 内存中保留的最大用户数（超出触发淘汰/降级）
	SnapshotPath    string        // 快照文件路径，空表示不启用持久化
}

// FunnelBucket 一个按日（配置时区）切分的漏斗统计桶。
type FunnelBucket struct {
	Bucket    string // 桶起始日，格式 2006-01-02
	Entered   int    // 进入第 1 步的尝试数
	Converted int    // 在窗口内按序完成全部步骤的尝试数
	Committed bool   // 水印已越过该桶（含转化窗口）则为 true
}

// FunnelResult 漏斗查询结果。
type FunnelResult struct {
	Buckets []FunnelBucket
}

// CohortRow 一个群组的留存行。
type CohortRow struct {
	Cohort    string // 群组周期键，如 "2026-10-05"（周为首日）
	Size      int    // 群组规模（首次出现于该周期的用户数）
	Retained  []int  // Retained[n] 为第 n 个后续周期仍活跃的用户数
	Committed bool   // 群组规模是否已提交（留存单元格的提交状态见 RetentionResult）
}

// RetentionResult 留存查询结果。
type RetentionResult struct {
	Offsets       int // 最大偏移数（含 0）
	Rows          []CohortRow
	CommittedUpTo string // 水印对应的周期键，早于它的周期已提交
}

// IngestStats 一次写入的统计。
type IngestStats struct {
	Accepted   int // 接受的事件数
	Duplicates int // 重复投递被去重的数量
	TooLate    int // 超过允许迟到上限被丢弃的数量
}

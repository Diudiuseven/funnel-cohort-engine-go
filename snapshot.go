package engine

import (
	"encoding/gob"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// Snapshot 是某一时刻状态的不可变视图，查询只读它，因此查询之间、
// 查询与写入之间不会互相看到半更新状态。
type Snapshot struct {
	watermark time.Time
	funnel    map[string]funnelCell
	cohort    map[string]cohortCell
}

// funnelResult 由快照构造漏斗查询结果。
// 桶的提交判定：水印越过 桶末日(配置时区) + 漏斗窗口 后，该桶结果不再变化。
func (s *Snapshot) funnelResult(cfg Config) FunnelResult {
	keys := make([]string, 0, len(s.funnel))
	for k, c := range s.funnel {
		if c.Entered != 0 || c.Converted != 0 {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	res := FunnelResult{Buckets: make([]FunnelBucket, 0, len(keys))}
	for _, k := range keys {
		c := s.funnel[k]
		dayEnd := c.BucketStart.AddDate(0, 0, 1)
		committed := !s.watermark.IsZero() && !dayEnd.Add(cfg.Funnel.Window).After(s.watermark)
		res.Buckets = append(res.Buckets, FunnelBucket{
			Bucket:    k,
			Entered:   c.Entered,
			Converted: c.Converted,
			Committed: committed,
		})
	}
	return res
}

// retentionResult 由快照构造留存查询结果。
// 群组规模提交判定：水印越过群组周期末；留存单元格[offset]提交判定：
// 水印越过 群组起始 + (offset+1) 个周期。
func (s *Snapshot) retentionResult(cfg Config) RetentionResult {
	keys := make([]string, 0, len(s.cohort))
	for k, c := range s.cohort {
		if !cohortCellEmpty(c) {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	res := RetentionResult{Offsets: cfg.Cohort.MaxOffsets}
	if !s.watermark.IsZero() {
		res.CommittedUpTo = periodKey(s.watermark, cfg.Cohort.Period, s.locOf(cfg))
	}
	for _, k := range keys {
		c := s.cohort[k]
		row := CohortRow{
			Cohort:   k,
			Size:     c.Size,
			Retained: make([]int, cfg.Cohort.MaxOffsets+1),
		}
		for off, n := range c.Retained {
			if off >= 0 && off <= cfg.Cohort.MaxOffsets {
				row.Retained[off] = n
			}
		}
		// 群组规模是否提交：水印越过该群组周期末。
		start := c.CohortStart
		if start.IsZero() {
			start = periodStart(periodIndexOfKey(k, cfg.Cohort.Period, s.locOf(cfg)), cfg.Cohort.Period, s.locOf(cfg))
		}
		end := start.AddDate(0, 0, 1)
		if cfg.Cohort.Period == WeekPeriod {
			end = start.AddDate(0, 0, 7)
		}
		row.Committed = !s.watermark.IsZero() && !end.After(s.watermark)
		res.Rows = append(res.Rows, row)
	}
	return res
}

// cohortCellEmpty 判断群组单元是否全零（瞬时归属被纠正后的残留），
// 全零单元不进入查询结果，保证输出与事件到达顺序无关。
func cohortCellEmpty(c cohortCell) bool {
	if c.Size != 0 {
		return false
	}
	for _, n := range c.Retained {
		if n != 0 {
			return false
		}
	}
	return true
}

// locOf 从配置解析时区；快照本身不持有时区（时区属于配置）。
func (s *Snapshot) locOf(cfg Config) *time.Location {
	loc, err := time.LoadLocation(cfg.Timezone)
	if err != nil {
		return time.UTC
	}
	return loc
}

// diskState 持久化到磁盘的完整状态。
type diskState struct {
	Watermark time.Time
	MaxSeen   time.Time
	Users     map[string]*userState
	Dedup     map[string]time.Time
	Funnel    map[string]*funnelCell
	Cohort    map[string]*cohortCell
}

// SaveSnapshot 将当前状态持久化到配置的快照路径（写临时文件后原子改名）。
func (e *Engine) SaveSnapshot() error {
	if e.cfg.SnapshotPath == "" {
		return fmt.Errorf("snapshot path not configured")
	}
	e.mu.Lock()
	ds := diskState{
		Watermark: e.state.watermark,
		MaxSeen:   e.state.maxSeen,
		Users:     e.state.users,
		Dedup:     e.state.dedup,
		Funnel:    e.state.funnel,
		Cohort:    e.state.cohort,
	}
	e.mu.Unlock()

	tmp := e.cfg.SnapshotPath + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	encErr := gob.NewEncoder(f).Encode(&ds)
	syncErr := f.Sync()
	closeErr := f.Close()
	if encErr != nil {
		return encErr
	}
	if syncErr != nil {
		return syncErr
	}
	if closeErr != nil {
		return closeErr
	}
	if err := os.Rename(tmp, e.cfg.SnapshotPath); err != nil {
		return err
	}
	// 尽量保证目录项落盘。
	if d, err := os.Open(filepath.Dir(e.cfg.SnapshotPath)); err == nil {
		_ = d.Sync()
		_ = d.Close()
	}
	return nil
}

// Load 从快照恢复引擎；快照不存在时等价于 New。
// 恢复后水印、去重表与聚合全部还原，重复投递已被处理的事件会被去重，
// 新事件从已提交的位置继续，不会重复计数或漏计。
func Load(cfg Config) (*Engine, error) {
	e, err := New(cfg)
	if err != nil {
		return nil, err
	}
	if cfg.SnapshotPath == "" {
		return e, nil
	}
	f, err := os.Open(cfg.SnapshotPath)
	if os.IsNotExist(err) {
		return e, nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var ds diskState
	if err := gob.NewDecoder(f).Decode(&ds); err != nil {
		return nil, fmt.Errorf("decode snapshot: %w", err)
	}
	e.state.watermark = ds.Watermark
	e.state.maxSeen = ds.MaxSeen
	if ds.Users != nil {
		e.state.users = ds.Users
	}
	if ds.Dedup != nil {
		e.state.dedup = ds.Dedup
	}
	if ds.Funnel != nil {
		e.state.funnel = ds.Funnel
	}
	if ds.Cohort != nil {
		e.state.cohort = ds.Cohort
	}
	e.publish()
	return e, nil
}

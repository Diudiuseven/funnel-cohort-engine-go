package engine

import (
	"sort"
	"time"
)

// userState 单用户状态：事件列表是唯一事实来源，派生贡献可重算。
type userState struct {
	Events []Event // 按 (Timestamp, ID) 排序、去重后的该用户事件

	// 以下为派生贡献缓存，用于在重算后做增量 diff。
	FunnelContrib map[string]funnelCounts // 日桶 -> 计数
	CohortKey     string                  // 所属群组周期键
	ActivePeriods map[string]bool         // 活跃周期键集合
	LastEventTime time.Time
	// Dormant 为 true 表示该用户已被冷淘汰：事件列表与已提交贡献已丢弃，
	// 仅保留群组归属，防止回归用户被重复计入群组规模。
	Dormant bool
}

type funnelCounts struct {
	Entered   int
	Converted int
}

// funnelCell 漏斗日桶聚合单元。
type funnelCell struct {
	BucketStart time.Time
	Entered     int
	Converted   int
}

// cohortCell 群组聚合单元。
type cohortCell struct {
	CohortStart time.Time
	Size        int
	Retained    map[int]int // offset -> 留存人数
}

// mutableState 全部可变状态，仅由写入方在锁内访问。
type mutableState struct {
	cfg       Config
	loc       *time.Location
	watermark time.Time
	maxSeen   time.Time

	users  map[string]*userState
	dedup  map[string]time.Time // 事件 ID -> 事件时间
	funnel map[string]*funnelCell
	cohort map[string]*cohortCell
}

func newMutableState(cfg Config, loc *time.Location) *mutableState {
	return &mutableState{
		cfg:    cfg,
		loc:    loc,
		users:  map[string]*userState{},
		dedup:  map[string]time.Time{},
		funnel: map[string]*funnelCell{},
		cohort: map[string]*cohortCell{},
	}
}

// horizon 返回保守的内存保留时长：任何早于 watermark-horizon 的数据，
// 其影响到的所有桶都已提交且不可能再被合法迟到事件修改。
func (s *mutableState) horizon() time.Duration {
	h := s.cfg.AllowedLateness + s.cfg.Funnel.Window +
		time.Duration(s.cfg.Cohort.MaxOffsets+1)*periodDuration(s.cfg.Cohort.Period) +
		25*time.Hour // DST 与安全余量
	if h < 0 {
		return 0
	}
	return h
}

// ingestOne 处理单条事件：去重、迟到判定、更新用户状态并重算贡献。
func (s *mutableState) ingestOne(ev Event, st *IngestStats) {
	if ev.ID == "" || ev.UserID == "" {
		return
	}
	// 去重：重复投递直接丢弃。
	if _, ok := s.dedup[ev.ID]; ok {
		st.Duplicates++
		return
	}
	// 迟到上限：事件时间早于水印即超出允许迟到范围。
	if !s.watermark.IsZero() && ev.Timestamp.Before(s.watermark) {
		st.TooLate++
		return
	}
	s.dedup[ev.ID] = ev.Timestamp
	st.Accepted++

	if ev.Timestamp.After(s.maxSeen) {
		s.maxSeen = ev.Timestamp
	}
	// 水印单调前进：watermark = maxSeen - allowedLateness。
	if w := s.maxSeen.Add(-s.cfg.AllowedLateness); w.After(s.watermark) {
		s.watermark = w
	}

	u := s.users[ev.UserID]
	if u == nil {
		u = &userState{}
		s.users[ev.UserID] = u
	}
	// 按 (Timestamp, ID) 有序插入，保证与有序重放一致。
	pos := sort.Search(len(u.Events), func(i int) bool {
		a, b := u.Events[i], ev
		if !a.Timestamp.Equal(b.Timestamp) {
			return a.Timestamp.After(b.Timestamp)
		}
		return a.ID > b.ID
	})
	u.Events = append(u.Events, Event{})
	copy(u.Events[pos+1:], u.Events[pos:])
	u.Events[pos] = ev
	if ev.Timestamp.After(u.LastEventTime) {
		u.LastEventTime = ev.Timestamp
	}

	s.recomputeUser(u)
	s.evict()
}

// recomputeUser 重算单个用户的派生贡献，并把增量 diff 应用到聚合。
// 这是收敛到有序重放结果的关键：任何乱序/迟到事件只触发该用户重算。
func (s *mutableState) recomputeUser(u *userState) {
	newFunnel := computeFunnelContrib(u.Events, s.cfg.Funnel, s.loc)
	newCohort, newActive := computeCohortContrib(u.Events, s.cfg.Cohort.Period, s.loc)
	if u.Dormant && u.CohortKey != "" {
		// 休眠用户的历史事件已丢弃，其真实群组以保留的归属为准，
		// 不得按现存事件重新归属（否则会重复计入群组规模）。
		newCohort = u.CohortKey
	}
	u.Dormant = false

	// diff 漏斗贡献。
	for k, old := range u.FunnelContrib {
		nw := newFunnel[k]
		if nw == old {
			continue
		}
		s.addFunnel(k, nw.Entered-old.Entered, nw.Converted-old.Converted)
	}
	for k, nw := range newFunnel {
		if _, ok := u.FunnelContrib[k]; !ok {
			s.addFunnel(k, nw.Entered, nw.Converted)
		}
	}
	u.FunnelContrib = newFunnel

	// diff 群组归属：迟到的更早首次事件会把用户移入正确群组。
	if u.CohortKey != "" && u.CohortKey != newCohort {
		s.addCohort(u.CohortKey, -1)
	}
	if newCohort != "" && u.CohortKey != newCohort {
		s.addCohort(newCohort, 1)
	}
	// diff 活跃周期 -> 留存计数。
	cohortIdx := periodIndexOfKey(newCohort, s.cfg.Cohort.Period, s.loc)
	oldCohortIdx := periodIndexOfKey(u.CohortKey, s.cfg.Cohort.Period, s.loc)
	for p := range u.ActivePeriods {
		if newActive[p] && u.CohortKey == newCohort {
			continue
		}
		if off := periodIndexOfKey(p, s.cfg.Cohort.Period, s.loc) - oldCohortIdx; off >= 0 && off <= s.cfg.Cohort.MaxOffsets {
			s.addRetained(u.CohortKey, off, -1)
		}
	}
	for p := range newActive {
		if u.ActivePeriods[p] && u.CohortKey == newCohort {
			continue
		}
		if off := periodIndexOfKey(p, s.cfg.Cohort.Period, s.loc) - cohortIdx; off >= 0 && off <= s.cfg.Cohort.MaxOffsets {
			s.addRetained(newCohort, off, 1)
		}
	}
	u.CohortKey = newCohort
	u.ActivePeriods = newActive
}

func (s *mutableState) addFunnel(bucket string, entered, converted int) {
	c := s.funnel[bucket]
	if c == nil {
		start := periodStartOfDayKey(bucket, s.loc)
		c = &funnelCell{BucketStart: start}
		s.funnel[bucket] = c
	}
	c.Entered += entered
	c.Converted += converted
}

func (s *mutableState) addCohort(key string, sizeDelta int) {
	if key == "" {
		return
	}
	c := s.cohort[key]
	if c == nil {
		c = &cohortCell{Retained: map[int]int{}}
		s.cohort[key] = c
	}
	c.Size += sizeDelta
}

func (s *mutableState) addRetained(cohortKey string, offset, delta int) {
	if cohortKey == "" {
		return
	}
	s.addCohort(cohortKey, 0)
	c := s.cohort[cohortKey]
	c.Retained[offset] += delta
}

// evict 执行确定性的内存淘汰：
//  1. 去重表：事件时间早于水印的条目可安全删除（其重复件必然被判迟到）；
//  2. 冷用户休眠化：lastEventTime 早于 watermark-horizon 的用户，其所有
//     贡献已提交且不可能再被合法迟到事件修改，丢弃事件列表与贡献缓存，
//     仅保留群组归属（防止回归用户重复入群），对精度无影响；
//  3. 硬上限：超出 MaxActiveUsers 时先删最冷的休眠条目，再淘汰最冷的
//     活跃用户（降级：其未提交贡献被冻结，见文档）。
func (s *mutableState) evict() {
	cutoff := s.watermark
	for id, ts := range s.dedup {
		if ts.Before(cutoff) {
			delete(s.dedup, id)
		}
	}
	cold := s.watermark.Add(-s.horizon())
	for _, u := range s.users {
		if !u.Dormant && u.LastEventTime.Before(cold) {
			u.Events = nil
			u.FunnelContrib = nil
			u.ActivePeriods = nil
			u.Dormant = true
		}
	}
	if max := s.cfg.MaxActiveUsers; max > 0 && len(s.users) > max {
		type kv struct {
			id      string
			t       time.Time
			dormant bool
		}
		all := make([]kv, 0, len(s.users))
		for id, u := range s.users {
			all = append(all, kv{id, u.LastEventTime, u.Dormant})
		}
		// 休眠条目优先、同状态按 lastEventTime 从最冷开始淘汰。
		sort.Slice(all, func(i, j int) bool {
			if all[i].dormant != all[j].dormant {
				return all[i].dormant
			}
			return all[i].t.Before(all[j].t)
		})
		for i := 0; len(s.users) > max && i < len(all); i++ {
			delete(s.users, all[i].id)
		}
	}
}

// snapshot 生成不可变快照（深拷贝聚合数据）。
func (s *mutableState) snapshot() *Snapshot {
	snap := &Snapshot{
		watermark: s.watermark,
		funnel:    make(map[string]funnelCell, len(s.funnel)),
		cohort:    make(map[string]cohortCell, len(s.cohort)),
	}
	for k, c := range s.funnel {
		snap.funnel[k] = *c
	}
	for k, c := range s.cohort {
		cc := cohortCell{CohortStart: c.CohortStart, Size: c.Size, Retained: make(map[int]int, len(c.Retained))}
		for off, n := range c.Retained {
			cc.Retained[off] = n
		}
		snap.cohort[k] = cc
	}
	return snap
}

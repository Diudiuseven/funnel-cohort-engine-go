package funnel

import (
	"container/list"
	"errors"
	"sort"
	"sync"
	"time"
)

// Errors returned by the engine.
var (
	ErrNoSteps        = errors.New("funnel: at least two steps required")
	ErrBadPeriod      = errors.New("funnel: invalid cohort period")
	ErrBadWindow      = errors.New("funnel: window must be positive")
	ErrDuplicateStep  = errors.New("funnel: funnel step names must be unique")
	ErrLateDropped    = errors.New("funnel: event arrived beyond allowed lateness and was dropped")
	ErrWatermarkBack  = errors.New("funnel: watermark can only advance forward")
	ErrConfigMismatch = errors.New("funnel: snapshot config does not match engine config")
)

// Default limits applied when configuration leaves them at zero.
const (
	DefaultMaxUsers        = 1_000_000
	DefaultMaxDedupEntries = 1_000_000
)

// Engine processes an out-of-order event stream and maintains queryable,
// snapshot-consistent funnel conversion and cohort retention results.
//
// Concurrency: all methods are safe for concurrent use. Mutations are
// serialized; each query executes under the same lock and therefore observes a
// single self-consistent state snapshot, never a field-level mix.
type Engine struct {
	cfg Config

	mu        sync.Mutex
	watermark time.Time // current (monotonic) watermark
	maxSeen   time.Time // largest accepted event timestamp
	manualWM  time.Time // externally advanced watermark floor

	dedup *dedupSet

	funnelUsers map[string]*funnelUserState
	cohortUsers map[string]*cohortUserState
	funnelLRU   *lruIndex
	cohortLRU   *lruIndex

	funnel *funnelAggregate
	cohort *cohortAggregate

	// funnelCutoff is the exclusive lower bound for retained funnel events:
	// the start of the day after the latest committed journey-start day.
	funnelCutoff time.Time
	// cohortCutoff is the exclusive period-start lower bound for retained
	// cohort activity records.
	cohortCutoff time.Time

	appliedSeq uint64
}

// lruIndex tracks insertion/access order of user states for bounded eviction.
type lruIndex struct {
	ll    *list.List
	pos   map[string]*list.Element
	limit int
}

func newLRU(limit int) *lruIndex {
	return &lruIndex{ll: list.New(), pos: make(map[string]*list.Element), limit: limit}
}

// use marks id as most-recently-used, adding it if new. When the index was at
// capacity it evicts the least-recently-used key and returns evicted, true.
func (l *lruIndex) use(id string) (evicted string, didEvict bool) {
	if el, found := l.pos[id]; found {
		l.ll.MoveToBack(el)
		return "", false
	}
	if l.ll.Len() >= l.limit {
		front := l.ll.Front()
		evicted = front.Value.(string)
		l.ll.Remove(front)
		delete(l.pos, evicted)
		l.pos[id] = l.ll.PushBack(id)
		return evicted, true
	}
	l.pos[id] = l.ll.PushBack(id)
	return "", false
}

func (l *lruIndex) remove(id string) {
	if el, found := l.pos[id]; found {
		l.ll.Remove(el)
		delete(l.pos, id)
	}
}

// NewEngine constructs an engine and applies a restored snapshot when not nil.
// The configuration must be identical to the one that produced the snapshot.
func NewEngine(cfg Config, snap *Snapshot) (*Engine, error) {
	if err := validateConfig(cfg); err != nil {
		return nil, err
	}
	if cfg.Timezone == nil {
		cfg.Timezone = time.UTC
	}
	if cfg.MaxUsers <= 0 {
		cfg.MaxUsers = DefaultMaxUsers
	}
	if cfg.MaxDedupEntries <= 0 {
		cfg.MaxDedupEntries = DefaultMaxDedupEntries
	}

	e := &Engine{
		cfg:         cfg,
		dedup:       newDedupSet(cfg.MaxDedupEntries),
		funnelUsers: map[string]*funnelUserState{},
		cohortUsers: map[string]*cohortUserState{},
		funnelLRU:   newLRU(cfg.MaxUsers),
		cohortLRU:   newLRU(cfg.MaxUsers),
		funnel:      newFunnelAggregate(len(cfg.Funnel.Steps)),
		cohort:      newCohortAggregate(),
	}

	if snap != nil {
		if err := e.restore(snap); err != nil {
			return nil, err
		}
	}
	return e, nil
}

func validateConfig(cfg Config) error {
	if len(cfg.Funnel.Steps) < 2 {
		return ErrNoSteps
	}
	if cfg.Funnel.Window <= 0 {
		return ErrBadWindow
	}
	seen := map[string]struct{}{}
	for _, s := range cfg.Funnel.Steps {
		if _, dup := seen[s]; dup {
			return ErrDuplicateStep
		}
		seen[s] = struct{}{}
	}
	switch cfg.Cohort.Period {
	case PeriodHour, PeriodDay, PeriodWeek, PeriodMonth:
	default:
		return ErrBadPeriod
	}
	return nil
}

// AddEvent ingests one event. Events may arrive in any order, duplicated or
// late. A duplicate (same Event.ID) is ignored. An event later than the
// watermark minus AllowedLateness cannot affect any still-open window and is
// dropped with ErrLateDropped.
func (e *Engine) AddEvent(ev Event) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	// Duplicate suppression first: a re-delivery of an already-accepted event
	// must be a harmless no-op even after the watermark has advanced past it
	// (e.g. source replay after snapshot recovery). Empty ids are not deduped.
	if ev.ID != "" && e.dedup.contains(ev.ID) {
		e.dedup.add(ev.ID) // refresh LRU position
		return nil
	}

	// Watermark gate: strictly older than watermark-AllowedLateness is lost.
	if !e.watermark.IsZero() && ev.Timestamp.Add(e.cfg.AllowedLateness).Before(e.watermark) {
		return ErrLateDropped
	}

	if ev.ID != "" {
		e.dedup.add(ev.ID)
	}

	e.appliedSeq++

	if e.maxSeen.IsZero() || ev.Timestamp.After(e.maxSeen) {
		e.maxSeen = ev.Timestamp
	}
	e.refreshWatermark()

	uid := ev.UserID

	// --- Funnel: remove this user's old contribution, mutate, re-add. ---
	st := e.funnelUsers[uid]
	var before map[string]*funnelCounters
	if st != nil {
		before = computeFunnelCounters(map[string]*funnelUserState{uid: st}, e.cfg.Funnel, e.cfg.Timezone)
	}
	if st == nil {
		st = newFunnelUserState()
		e.funnelUsers[uid] = st
		if evicted, didEvict := e.funnelLRU.use(uid); didEvict {
			e.evictFunnelUser(evicted)
		}
	} else {
		e.funnelLRU.use(uid)
	}
	st.insertEvent(ev)
	after := computeFunnelCounters(map[string]*funnelUserState{uid: st}, e.cfg.Funnel, e.cfg.Timezone)
	e.applyFunnelDelta(before, after)

	// --- Cohort: same delta procedure (handles late first-seen moves). ---
	cs := e.cohortUsers[uid]
	var cBefore map[string]map[int]int64
	if cs != nil {
		cBefore = computeCohortCounters(map[string]*cohortUserState{uid: cs}, e.cfg.Cohort.Period, e.cfg.Timezone)
	}
	if cs == nil {
		cs = newCohortUserState()
		e.cohortUsers[uid] = cs
		if evicted, didEvict := e.cohortLRU.use(uid); didEvict {
			e.evictCohortUser(evicted)
		}
	} else {
		e.cohortLRU.use(uid)
	}
	cs.observe(ev.Timestamp, e.cfg.Cohort.Period, e.cfg.Timezone)
	cAfter := computeCohortCounters(map[string]*cohortUserState{uid: cs}, e.cfg.Cohort.Period, e.cfg.Timezone)
	e.applyCohortDelta(cBefore, cAfter)

	e.commitPass()
	return nil
}

// applyFunnelDelta adjusts tentative counters by (after - before) for one user.
func (e *Engine) applyFunnelDelta(before, after map[string]*funnelCounters) {
	for date, c := range before {
		t := e.funnel.tentative[date]
		if t == nil {
			t = newFunnelCounters(e.funnel.steps)
			e.funnel.tentative[date] = t
		}
		for k, v := range c.reached {
			t.reached[k] -= v
		}
	}
	for date, c := range after {
		t := e.funnel.tentative[date]
		if t == nil {
			t = newFunnelCounters(e.funnel.steps)
			e.funnel.tentative[date] = t
		}
		for k, v := range c.reached {
			t.reached[k] += v
		}
	}
	// Drop all-zero rows.
	for date, t := range e.funnel.tentative {
		allZero := true
		for _, v := range t.reached {
			if v != 0 {
				allZero = false
				break
			}
		}
		if allZero {
			delete(e.funnel.tentative, date)
		}
	}
}

// applyCohortDelta adjusts tentative cohort cells by (after - before).
func (e *Engine) applyCohortDelta(cBefore, cAfter map[string]map[int]int64) {
	for key, row := range cBefore {
		t := e.cohort.tentative[key]
		if t == nil {
			t = map[int]int64{}
			e.cohort.tentative[key] = t
		}
		for off, v := range row {
			t[off] -= v
			if t[off] == 0 {
				delete(t, off)
			}
		}
	}
	for key, row := range cAfter {
		t := e.cohort.tentative[key]
		if t == nil {
			t = map[int]int64{}
			e.cohort.tentative[key] = t
		}
		for off, v := range row {
			t[off] += v
		}
	}
	for key, row := range e.cohort.tentative {
		if len(row) == 0 {
			delete(e.cohort.tentative, key)
		}
	}
}

func (e *Engine) evictFunnelUser(uid string) {
	// Remove the evicted user's tentative contribution, then its state.
	if st := e.funnelUsers[uid]; st != nil {
		before := computeFunnelCounters(map[string]*funnelUserState{uid: st}, e.cfg.Funnel, e.cfg.Timezone)
		e.applyFunnelDelta(before, nil)
		delete(e.funnelUsers, uid)
	}
}

func (e *Engine) evictCohortUser(uid string) {
	if cs := e.cohortUsers[uid]; cs != nil {
		before := computeCohortCounters(map[string]*cohortUserState{uid: cs}, e.cfg.Cohort.Period, e.cfg.Timezone)
		e.applyCohortDelta(before, nil)
		delete(e.cohortUsers, uid)
	}
}

// AdvanceWatermark externally advances the watermark (e.g. on source idleness).
// It never moves backwards; older values are ignored.
func (e *Engine) AdvanceWatermark(t time.Time) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if t.Before(e.watermark) {
		return ErrWatermarkBack
	}
	if e.manualWM.Before(t) {
		e.manualWM = t
	}
	e.refreshWatermark()
	e.commitPass()
	return nil
}

// refreshWatermark sets watermark to the larger of the externally advanced
// value and maxSeen-AllowedLateness. Monotonic by construction.
func (e *Engine) refreshWatermark() {
	w := e.manualWM
	if !e.maxSeen.IsZero() {
		auto := e.maxSeen.Add(-e.cfg.AllowedLateness)
		if auto.After(w) {
			w = auto
		}
	}
	if w.After(e.watermark) {
		e.watermark = w
	}
}

// commitPass finalizes every funnel day and cohort cell whose deadline the
// watermark has reached and prunes state that can no longer change anything.
func (e *Engine) commitPass() {
	if e.watermark.IsZero() {
		return
	}
	e.commitFunnel()
	e.commitCohort()
}

func (e *Engine) commitFunnel() {
	loc := e.cfg.Timezone
	window := e.cfg.Funnel.Window
	latency := e.cfg.AllowedLateness

	// Walk tentative day buckets in chronological order; a day commits when the
	// watermark reaches its deadline.
	for _, date := range sortedCounterKeys(e.funnel.tentative) {
		dayStart := parsePeriodKey(date, PeriodDay, loc)
		if dayStart.IsZero() {
			continue
		}
		deadline := funnelDeadline(dayStart, window, latency, loc)
		if e.watermark.Before(deadline) {
			break // later dates have later deadlines; keys are sorted
		}
		// Tentative becomes committed verbatim: no retained event can change it.
		e.funnel.committed[date] = e.funnel.tentative[date]
		delete(e.funnel.tentative, date)
	}

	// The cutoff is determined purely by the watermark, not by which buckets
	// currently have tentative rows: every day whose full
	// window+lateness horizon lies behind the watermark is final, regardless
	// of whether journeys existed that day.
	cut := latestFullyClosedDayStart(e.watermark, window, latency, loc)
	if cut.After(e.funnelCutoff) {
		e.funnelCutoff = cut
	}

	if !e.funnelCutoff.IsZero() {
		for uid, st := range e.funnelUsers {
			st.pruneBefore(e.funnelCutoff)
			if st.empty() {
				delete(e.funnelUsers, uid)
				e.funnelLRU.remove(uid)
			}
		}
	}
}

// latestFullyClosedDayStart returns the exclusive prune cutoff: midnight that
// starts the day after the latest day whose full window+lateness horizon lies
// behind wm. Events strictly before it can no longer be part of any open
// journey and are safe to discard.
func latestFullyClosedDayStart(wm time.Time, window, latency time.Duration, loc *time.Location) time.Time {
	// A day D is closed when end(D)+window+lateness <= wm, i.e.
	// end(D) <= wm-window-lateness. The day containing that boundary is the
	// first still-open day, so pruning is safe up to that day's start (which
	// equals the end of the last closed day).
	boundary := wm.Add(-window).Add(-latency)
	firstOpen := periodStart(boundary, PeriodDay, loc)
	_ = firstOpen
	// firstOpen is a day start; events strictly before firstOpen belong to
	// closed days. (firstOpen itself is retained.)
	return firstOpen
}

func (e *Engine) commitCohort() {
	loc := e.cfg.Timezone
	p := e.cfg.Cohort.Period
	latency := e.cfg.AllowedLateness

	for _, key := range sortedCounterKeys(e.cohort.tentative) {
		cStart := parsePeriodKey(key, p, loc)
		if cStart.IsZero() {
			continue
		}
		row := e.cohort.tentative[key]
		committed := e.cohort.committed[key]
		if committed == nil {
			committed = map[int]int64{}
			e.cohort.committed[key] = committed
		}
		// Commit offsets in order while their deadline is reached. Offsets are
		// contiguous in time, so a not-yet-reached deadline blocks all later
		// offsets. We stop beyond the largest offset ever observed.
		maxOff := 0
		for off := range committed {
			if off > maxOff {
				maxOff = off
			}
		}
		for off := range row {
			if off > maxOff {
				maxOff = off
			}
		}
		for off := 0; off <= maxOff; off++ {
			deadline := cohortDeadline(cStart, p, off, latency, loc)
			if e.watermark.Before(deadline) {
				break
			}
			if v, exists := row[off]; exists {
				committed[off] += v
				delete(row, off)
			}
			// Missing offsets commit as zero (user simply retained nothing).
		}
		if len(row) == 0 {
			delete(e.cohort.tentative, key)
		}
	}

	// Global boundary of periods that are fully closed for every cohort.
	boundary := e.watermark.Add(-latency)
	b := periodStart(boundary, p, loc) // greatest period start <= boundary
	if !b.IsZero() {
		if b.After(e.cohortCutoff) {
			e.cohortCutoff = b
		}
	}
	if !e.cohortCutoff.IsZero() {
		cutKey := periodKey(e.cohortCutoff, p, loc)
		for _, cs := range e.cohortUsers {
			for k := range cs.active {
				if k < cutKey {
					delete(cs.active, k)
				}
			}
		}
	}
}

// Watermark returns the current watermark.
func (e *Engine) Watermark() time.Time {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.watermark
}

// FunnelResult returns a self-consistent snapshot of funnel results sorted by
// start date. Rows backed by committed buckets report Committed=true; rows that
// can still be revised by late/reordered events report Committed=false.
func (e *Engine) FunnelResult() []FunnelRow {
	e.mu.Lock()
	defer e.mu.Unlock()

	dates := map[string]struct{}{}
	for k := range e.funnel.committed {
		dates[k] = struct{}{}
	}
	for k := range e.funnel.tentative {
		dates[k] = struct{}{}
	}
	keys := make([]string, 0, len(dates))
	for k := range dates {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	rows := make([]FunnelRow, 0, len(keys))
	for _, date := range keys {
		var c *funnelCounters
		committed := false
		if cc, ok := e.funnel.committed[date]; ok {
			c = cc
			committed = true
		} else {
			c = e.funnel.tentative[date]
		}
		reached := make([]int64, len(c.reached))
		copy(reached, c.reached)
		rows = append(rows, FunnelRow{
			Date:        date,
			Started:     reached[0],
			StepReached: reached,
			Committed:   committed,
		})
	}
	return rows
}

// CohortResult returns a self-consistent snapshot of cohort retention rows,
// sorted by cohort period.
func (e *Engine) CohortResult() []CohortRow {
	e.mu.Lock()
	defer e.mu.Unlock()

	keys := map[string]struct{}{}
	for k := range e.cohort.committed {
		keys[k] = struct{}{}
	}
	for k := range e.cohort.tentative {
		keys[k] = struct{}{}
	}
	ordered := make([]string, 0, len(keys))
	for k := range keys {
		ordered = append(ordered, k)
	}
	sort.Strings(ordered)

	rows := make([]CohortRow, 0, len(ordered))
	for _, key := range ordered {
		committedCells := e.cohort.committed[key]
		tentativeCells := e.cohort.tentative[key]
		maxOff := 0
		for off := range committedCells {
			if off > maxOff {
				maxOff = off
			}
		}
		for off := range tentativeCells {
			if off > maxOff {
				maxOff = off
			}
		}
		retained := make([]int64, maxOff+1)
		for off, v := range committedCells {
			retained[off] += v
		}
		for off, v := range tentativeCells {
			retained[off] += v
		}
		if retained[0] == 0 {
			continue // cohort lost all members to a corrected earlier cohort
		}
		rows = append(rows, CohortRow{
			Period:    key,
			Size:      retained[0],
			Retained:  retained,
			Committed: tentativeCells == nil || len(tentativeCells) == 0,
		})
	}
	return rows
}

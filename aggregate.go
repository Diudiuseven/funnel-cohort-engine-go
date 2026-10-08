package funnel

import (
	"sort"
	"time"
)

// FunnelRow is the result for one journey-start date: unique journeys started
// (step 0 reached) and unique journeys reaching each subsequent step.
type FunnelRow struct {
	Date        string // journey start date key in the configured timezone
	Started     int64
	StepReached []int64 // index 0 == Started; StepReached[k] reached step k
	Committed   bool
}

// CohortRow is the result for one first-seen cohort period.
type CohortRow struct {
	Period    string
	Size      int64
	Retained  []int64 // Retained[k]: unique users active k periods later
	Committed bool
}

// funnelAggregate stores tentative and committed per-date counters.
type funnelAggregate struct {
	tentative map[string]*funnelCounters
	committed map[string]*funnelCounters
	steps     int
}

// cohortAggregate stores tentative and committed per-cohort counters.
type cohortAggregate struct {
	// tentative[periodKey][offset] = unique users
	tentative map[string]map[int]int64
	committed map[string]map[int]int64
}

type funnelCounters struct {
	reached []int64
}

func newFunnelCounters(steps int) *funnelCounters {
	return &funnelCounters{reached: make([]int64, steps)}
}

func newFunnelAggregate(steps int) *funnelAggregate {
	return &funnelAggregate{
		tentative: map[string]*funnelCounters{},
		committed: map[string]*funnelCounters{},
		steps:     steps,
	}
}

func newCohortAggregate() *cohortAggregate {
	return &cohortAggregate{
		tentative: map[string]map[int]int64{},
		committed: map[string]map[int]int64{},
	}
}

// journey is one attempt of one user: started by a step-0 event.
type journey struct {
	start   time.Time
	reached int // highest step index reached (0 == only the first step)
}

// computeFunnelCounters replays every retained user's events in deterministic
// order and tallies unique journeys by their start date. This is exactly what
// an ordered replay of the same retained event set would produce.
func computeFunnelCounters(
	users map[string]*funnelUserState,
	cfg FunnelConfig,
	loc *time.Location,
) map[string]*funnelCounters {

	out := map[string]*funnelCounters{}
	get := func(date string) *funnelCounters {
		c := out[date]
		if c == nil {
			c = newFunnelCounters(len(cfg.Steps))
			out[date] = c
		}
		return c
	}

	for _, st := range users {
		// Active journeys not yet failed by window expiry, in start order.
		var js []journey
		for _, e := range st.events {
			if e.Name == cfg.Steps[0] {
				// Every step-0 event starts a new journey.
				js = append(js, journey{start: e.Timestamp, reached: 0})
				continue
			}
			// Advance every active journey still waiting on this step.
			for i := range js {
				j := &js[i]
				next := j.reached + 1
				if next >= len(cfg.Steps) {
					continue
				}
				if cfg.Steps[next] != e.Name {
					continue
				}
				if e.Timestamp.Sub(j.start) > cfg.Window {
					continue // outside the conversion window (inclusive edge)
				}
				js[i].reached = next
			}
		}
		for _, j := range js {
			c := get(periodKey(j.start, PeriodDay, loc))
			for k := 0; k <= j.reached; k++ {
				c.reached[k]++
			}
		}
	}
	return out
}

// computeCohortCounters replays cohort states: users are grouped by the period
// of their earliest event and counted in cohort size and in each later period
// in which they were active.
func computeCohortCounters(
	users map[string]*cohortUserState,
	p Period,
	loc *time.Location,
) map[string]map[int]int64 {

	out := map[string]map[int]int64{}
	get := func(key string) map[int]int64 {
		m := out[key]
		if m == nil {
			m = map[int]int64{}
			out[key] = m
		}
		return m
	}

	for _, c := range users {
		if c.firstSeen.IsZero() {
			continue
		}
		cStart := periodStart(c.firstSeen, p, loc)
		cKey := periodKey(c.firstSeen, p, loc)
		row := get(cKey)
		row[0]++ // size: unique users grouped by first-seen period
		for actKey := range c.active {
			// Parse active period back to a start instant. All active keys were
			// produced in loc; round-trip through the representative instant.
			at := parsePeriodKey(actKey, p, loc)
			if at.Before(cStart) {
				continue // cannot be before first seen (defensive)
			}
			if at.Equal(cStart) {
				continue
			}
			off, ok := offsetBetween(cStart, at, p, loc)
			if !ok {
				continue
			}
			row[off]++
		}
	}
	return out
}

// parsePeriodKey builds a period-start time from a periodKey.
func parsePeriodKey(key string, p Period, loc *time.Location) time.Time {
	switch p {
	case PeriodHour:
		if t, err := time.ParseInLocation("2006-01-02T15", key, loc); err == nil {
			return t
		}
	case PeriodDay:
		if t, err := time.ParseInLocation("2006-01-02", key, loc); err == nil {
			return t
		}
	case PeriodMonth:
		if t, err := time.ParseInLocation("2006-01", key, loc); err == nil {
			return t
		}
	case PeriodWeek:
		var y, w int
		if _, err := parseWeekKey(key, &y, &w); err == nil {
			// ISO week: Jan 4 is always in week 1; find Monday of week w.
			jan4 := time.Date(y, time.January, 4, 0, 0, 0, 0, loc)
			jan4WeekStart := periodStart(jan4, PeriodWeek, loc)
			return jan4WeekStart.AddDate(0, 0, 7*(w-1))
		}
	}
	return time.Time{}
}

// funnelDeadline returns the watermark at/before which the journey bucket for
// a journey started at `start` can be committed:
//
//	end of start day (local) + Window + AllowedLateness.
//
// Any journey starting that day converts no later than day-end + Window, and
// every event for it arrives by then + AllowedLateness.
func funnelDeadline(start time.Time, window, allowedLateness time.Duration, loc *time.Location) time.Time {
	dayStart := periodStart(start, PeriodDay, loc)
	dayEnd := nextPeriodStart(dayStart, PeriodDay, loc) // exclusive
	return dayEnd.Add(window).Add(allowedLateness)
}

// cohortDeadline returns the watermark at which cell (cohort starting at
// periodStart, retention offset `offset`) becomes final:
//
//	end of the offset period + AllowedLateness.
func cohortDeadline(start time.Time, p Period, offset int, allowedLateness time.Duration, loc *time.Location) time.Time {
	actStart := addPeriods(start, p, offset, loc)
	actEnd := nextPeriodStart(actStart, p, loc)
	return actEnd.Add(allowedLateness)
}

// sortedCounterKeys returns date/period keys sorted ascending.
func sortedCounterKeys[T any](m map[string]T) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

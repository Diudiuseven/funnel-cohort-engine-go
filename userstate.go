package funnel

import "time"

// funnelUserState holds per-user funnel bookkeeping for events that have not
// yet been safely committed and pruned.
type funnelUserState struct {
	// events are the user's retained events sorted by (timestamp, id).
	events []Event
}

// cohortUserState holds a user's first-seen timestamp and the set of periods in
// which activity has been observed.
type cohortUserState struct {
	firstSeen time.Time
	active    map[string]struct{}
}

func newFunnelUserState() *funnelUserState {
	return &funnelUserState{events: make([]Event, 0, 4)}
}

func newCohortUserState() *cohortUserState {
	return &cohortUserState{active: make(map[string]struct{}, 1)}
}

// insertEvent inserts e maintaining (timestamp, id) order. It returns
// inserted=false (without changing state) if an event with the same id is
// already retained.
func (s *funnelUserState) insertEvent(e Event) (inserted bool) {
	if e.ID != "" {
		for _, ex := range s.events {
			if ex.ID == e.ID {
				return false
			}
		}
	}
	i := len(s.events)
	for i > 0 && lessEvent(e, s.events[i-1]) {
		i--
	}
	s.events = append(s.events, Event{})
	copy(s.events[i+1:], s.events[i:])
	s.events[i] = e
	return true
}

// pruneBefore removes events with timestamp strictly before cutoff. Events at
// or after cutoff are kept: journeys may still be open for them.
func (s *funnelUserState) pruneBefore(cutoff time.Time) {
	keep := s.events[:0]
	for _, e := range s.events {
		if !e.Timestamp.Before(cutoff) {
			keep = append(keep, e)
		}
	}
	// Clear tail beyond new length so dropped Events can be collected.
	for i := len(keep); i < len(s.events); i++ {
		s.events[i] = Event{}
	}
	s.events = keep
}

func (s *funnelUserState) empty() bool { return len(s.events) == 0 }

// lessEvent defines the deterministic replay order: timestamp then id.
func lessEvent(a, b Event) bool {
	if a.Timestamp.Equal(b.Timestamp) {
		return a.ID < b.ID
	}
	return a.Timestamp.Before(b.Timestamp)
}

// observe records an event at timestamp t for a user: it initializes firstSeen
// to the earliest timestamp seen and marks the containing period active.
func (c *cohortUserState) observe(t time.Time, p Period, loc *time.Location) {
	if c.active == nil {
		c.active = make(map[string]struct{}, 1)
	}
	if c.firstSeen.IsZero() || t.Before(c.firstSeen) {
		c.firstSeen = t
	}
	c.active[periodKey(t, p, loc)] = struct{}{}
}

// hasPeriod reports activity in the given period key.
func (c *cohortUserState) hasPeriod(key string) bool {
	_, ok := c.active[key]
	return ok
}

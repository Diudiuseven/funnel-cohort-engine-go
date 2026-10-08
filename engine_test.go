package funnel

import (
	"sync/atomic"
	"testing"
	"time"
)

func testConfig(loc *time.Location) Config {
	return Config{
		Timezone:        loc,
		Funnel:          FunnelConfig{Steps: []string{"view", "cart", "buy"}, Window: time.Hour},
		Cohort:          CohortConfig{Period: PeriodDay},
		AllowedLateness: 30 * time.Minute,
		MaxUsers:        1000,
		MaxDedupEntries: 1000,
	}
}

func mustEngine(t *testing.T, cfg Config) *Engine {
	t.Helper()
	e, err := NewEngine(cfg, nil)
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	return e
}

func mkEvent(id, user, name string, ts time.Time) Event {
	return Event{ID: id, UserID: user, Name: name, Timestamp: ts}
}

func findRow(rows []FunnelRow, date string) *FunnelRow {
	for i := range rows {
		if rows[i].Date == date {
			return &rows[i]
		}
	}
	return nil
}

func TestOrderedFunnelConversion(t *testing.T) {
	loc := time.UTC
	e := mustEngine(t, testConfig(loc))
	base := time.Date(2026, 1, 10, 12, 0, 0, 0, loc)

	events := []Event{
		mkEvent("1", "u1", "view", base),
		mkEvent("2", "u1", "cart", base.Add(10*time.Minute)),
		mkEvent("3", "u1", "buy", base.Add(20*time.Minute)),
		mkEvent("4", "u2", "view", base),
		mkEvent("5", "u2", "cart", base.Add(10*time.Minute)),
		// u2 never buys
	}
	for _, ev := range events {
		if err := e.AddEvent(ev); err != nil {
			t.Fatalf("AddEvent: %v", err)
		}
	}
	rows := e.FunnelResult()
	if len(rows) != 1 {
		t.Fatalf("want 1 row, got %d: %+v", len(rows), rows)
	}
	r := rows[0]
	if r.Date != "2026-01-10" || r.Started != 2 || r.StepReached[1] != 2 || r.StepReached[2] != 1 {
		t.Fatalf("unexpected row: %+v", r)
	}
	if r.Committed {
		t.Fatalf("row should be tentative before the window closes")
	}
}

// replayOrdered runs the same events in deterministic (ts,id) order on a fresh
// engine, closing the watermark far beyond the data, and returns its rows.
func replayOrdered(t *testing.T, cfg Config, events []Event) ([]FunnelRow, []CohortRow) {
	t.Helper()
	sorted := append([]Event(nil), events...)
	sortEvents(sorted)
	e := mustEngine(t, cfg)
	var max time.Time
	for _, ev := range sorted {
		if err := e.AddEvent(ev); err != nil {
			t.Fatalf("replay AddEvent: %v", err)
		}
		if ev.Timestamp.After(max) {
			max = ev.Timestamp
		}
	}
	if err := e.AdvanceWatermark(max.Add(48 * time.Hour)); err != nil {
		t.Fatalf("AdvanceWatermark: %v", err)
	}
	return e.FunnelResult(), e.CohortResult()
}

func sortEvents(evs []Event) {
	for i := 1; i < len(evs); i++ {
		for j := i; j > 0 && lessEvent(evs[j], evs[j-1]); j-- {
			evs[j], evs[j-1] = evs[j-1], evs[j]
		}
	}
}

func TestOutOfOrderConvergesToOrderedReplay(t *testing.T) {
	loc := time.UTC
	cfg := testConfig(loc)
	base := time.Date(2026, 1, 10, 10, 0, 0, 0, loc)
	events := []Event{
		mkEvent("3", "u1", "buy", base.Add(20*time.Minute)),
		mkEvent("1", "u1", "view", base),
		mkEvent("2", "u1", "cart", base.Add(10*time.Minute)),
		mkEvent("8", "u3", "view", base.Add(5*time.Minute)),
		mkEvent("9", "u3", "buy", base.Add(15*time.Minute)), // no cart -> not converted
		mkEvent("10", "u3", "cart", base.Add(18*time.Minute)),
		mkEvent("5", "u2", "cart", base.Add(35*time.Minute)),
		mkEvent("4", "u2", "view", base.Add(30*time.Minute)),
		mkEvent("6", "u2", "buy", base.Add(40*time.Minute)),
		// journey outside window:
		mkEvent("20", "u4", "view", base.Add(70*time.Minute)),
		mkEvent("21", "u4", "cart", base.Add(75*time.Minute)),
		mkEvent("22", "u4", "buy", base.Add(76*time.Minute)),
	}

	e := mustEngine(t, cfg)
	var max time.Time
	for _, ev := range events {
		if err := e.AddEvent(ev); err != nil {
			t.Fatalf("AddEvent: %v", err)
		}
		if ev.Timestamp.After(max) {
			max = ev.Timestamp
		}
	}
	if err := e.AdvanceWatermark(max.Add(48 * time.Hour)); err != nil {
		t.Fatalf("AdvanceWatermark: %v", err)
	}
	got := e.FunnelResult()
	want, _ := replayOrdered(t, cfg, events)
	assertFunnelRowsEqual(t, want, got)

	r := findRow(got, "2026-01-10")
	if r == nil || !r.Committed {
		t.Fatalf("expected committed row, got %+v", got)
	}
	// started: u1,u2,u3,u4 = 4; cart: all 4; buy in window: u1,u2,u4 = 3
	if r.Started != 4 || r.StepReached[1] != 4 || r.StepReached[2] != 3 {
		t.Fatalf("unexpected counts: %+v", r)
	}
}

func assertFunnelRowsEqual(t *testing.T, want, got []FunnelRow) {
	t.Helper()
	if len(want) != len(got) {
		t.Fatalf("row count mismatch: want %d got %d\nwant=%+v\ngot=%+v", len(want), len(got), want, got)
	}
	for i := range want {
		if want[i].Date != got[i].Date {
			t.Fatalf("date mismatch at %d: %s vs %s", i, want[i].Date, got[i].Date)
		}
		if len(want[i].StepReached) != len(got[i].StepReached) {
			t.Fatalf("step len mismatch at %s", want[i].Date)
		}
		for k := range want[i].StepReached {
			if want[i].StepReached[k] != got[i].StepReached[k] {
				t.Fatalf("date %s step %d: want %d got %d", want[i].Date, k, want[i].StepReached[k], got[i].StepReached[k])
			}
		}
	}
}

func TestDuplicateDeliveryNotCountedTwice(t *testing.T) {
	loc := time.UTC
	e := mustEngine(t, testConfig(loc))
	base := time.Date(2026, 1, 10, 12, 0, 0, 0, loc)

	first := []Event{
		mkEvent("a", "u1", "view", base),
		mkEvent("b", "u1", "cart", base.Add(5*time.Minute)),
		mkEvent("c", "u1", "buy", base.Add(10*time.Minute)),
	}
	for _, ev := range first {
		if err := e.AddEvent(ev); err != nil {
			t.Fatal(err)
		}
	}
	// Re-deliver all events with identical ids.
	for _, ev := range first {
		if err := e.AddEvent(ev); err != nil {
			t.Fatal(err)
		}
	}
	r := findRow(e.FunnelResult(), "2026-01-10")
	if r == nil || r.Started != 1 || r.StepReached[2] != 1 {
		t.Fatalf("duplicates altered counts: %+v", r)
	}
	if e.dedup.size() != 3 {
		t.Fatalf("dedup size = %d, want 3", e.dedup.size())
	}
}

func TestTentativeThenCommittedAndLateDrop(t *testing.T) {
	loc := time.UTC
	cfg := testConfig(loc)
	e := mustEngine(t, cfg)
	// Day D 09:00: journey completes at 09:20.
	d := time.Date(2026, 1, 10, 9, 0, 0, 0, loc)
	for i, ev := range []Event{
		mkEvent("a", "u1", "view", d),
		mkEvent("a2", "u1", "cart", d.Add(10*time.Minute)),
		mkEvent("a3", "u1", "buy", d.Add(20*time.Minute)),
	} {
		_ = i
		if err := e.AddEvent(ev); err != nil {
			t.Fatal(err)
		}
	}
	r := findRow(e.FunnelResult(), "2026-01-10")
	if r == nil || r.Committed {
		t.Fatalf("expected tentative row: %+v", r)
	}

	// Watermark just before the deadline: end of day 24:00 + 1h window +
	// 30m lateness = 01:30 next day. Still tentative.
	deadline := time.Date(2026, 1, 11, 1, 30, 0, 0, loc)
	if err := e.AdvanceWatermark(deadline.Add(-time.Minute)); err != nil {
		t.Fatal(err)
	}
	r = findRow(e.FunnelResult(), "2026-01-10")
	if r == nil || r.Committed {
		t.Fatalf("still tentative expected at %s: %+v", deadline, r)
	}

	// Crossing the deadline commits.
	if err := e.AdvanceWatermark(deadline); err != nil {
		t.Fatal(err)
	}
	r = findRow(e.FunnelResult(), "2026-01-10")
	if r == nil || !r.Committed || r.StepReached[2] != 1 {
		t.Fatalf("expected committed conversion: %+v", r)
	}

	// Watermark never goes backwards.
	if err := e.AdvanceWatermark(deadline.Add(-2 * time.Hour)); err != ErrWatermarkBack {
		t.Fatalf("want ErrWatermarkBack, got %v", err)
	}

	// An event far beyond allowed lateness is dropped.
	tooLate := mkEvent("late", "u9", "view", deadline.Add(-2*time.Hour))
	// current watermark == deadline; deadline-2h +30m = deadline-1.5h < wm.
	if err := e.AddEvent(tooLate); err != ErrLateDropped {
		t.Fatalf("want ErrLateDropped, got %v", err)
	}

	// A still-within-lateness late event for a future/new day is accepted.
	nearLate := mkEvent("near", "u9", "view", e.Watermark().Add(-29*time.Minute))
	if err := e.AddEvent(nearLate); err != nil {
		t.Fatalf("within-lateness event should be accepted: %v", err)
	}
}

func TestWatermarkMonotonicAutoAdvance(t *testing.T) {
	loc := time.UTC
	cfg := testConfig(loc)
	e := mustEngine(t, cfg)
	t1 := time.Date(2026, 1, 10, 12, 0, 0, 0, loc)
	if err := e.AddEvent(mkEvent("x", "u", "view", t1)); err != nil {
		t.Fatal(err)
	}
	want := t1.Add(-cfg.AllowedLateness)
	if !e.Watermark().Equal(want) {
		t.Fatalf("watermark = %s, want %s", e.Watermark(), want)
	}
	// Older event must not push the watermark back.
	if err := e.AddEvent(mkEvent("y", "u", "view", t1.Add(-time.Hour))); err != nil {
		t.Fatal(err)
	}
	if e.Watermark().Before(want) {
		t.Fatalf("watermark regressed to %s", e.Watermark())
	}
}

func findCohort(rows []CohortRow, p string) *CohortRow {
	for i := range rows {
		if rows[i].Period == p {
			return &rows[i]
		}
	}
	return nil
}

func cohortTestCfg(loc *time.Location) Config {
	c := testConfig(loc)
	c.AllowedLateness = 48 * time.Hour
	return c
}

func TestCohortRetention(t *testing.T) {
	loc := time.UTC
	cfg := cohortTestCfg(loc)
	e := mustEngine(t, cfg)
	d0 := time.Date(2026, 1, 10, 9, 0, 0, 0, loc)
	d1 := d0.AddDate(0, 0, 1)
	d2 := d0.AddDate(0, 0, 2)

	// u1 appears day0, active day0/day1/day2; u2 first appears day1.
	for _, ev := range []Event{
		mkEvent("1", "u1", "view", d0),
		mkEvent("2", "u1", "view", d1),
		mkEvent("3", "u1", "view", d2),
		mkEvent("4", "u2", "view", d1),
		mkEvent("5", "u2", "view", d2),
	} {
		if err := e.AddEvent(ev); err != nil {
			t.Fatal(err)
		}
	}
	rows := e.CohortResult()
	c0 := findCohort(rows, "2026-01-10")
	if c0 == nil || c0.Size != 1 || c0.Retained[1] != 1 || c0.Retained[2] != 1 {
		t.Fatalf("cohort day0 wrong: %+v", c0)
	}
	c1 := findCohort(rows, "2026-01-11")
	if c1 == nil || c1.Size != 1 || c1.Retained[1] != 1 {
		t.Fatalf("cohort day1 wrong: %+v", c1)
	}
}

func TestLateFirstSeenReassignsCohort(t *testing.T) {
	loc := time.UTC
	cfg := cohortTestCfg(loc)
	e := mustEngine(t, cfg)
	d0 := time.Date(2026, 1, 10, 9, 0, 0, 0, loc)
	d1 := d0.AddDate(0, 0, 1)
	d2 := d0.AddDate(0, 0, 2)

	// u1 first arrives with day1/day2 events; engine tentatively groups u1 in
	// day1 cohort.
	if err := e.AddEvent(mkEvent("b", "u1", "view", d1)); err != nil {
		t.Fatal(err)
	}
	if err := e.AddEvent(mkEvent("c", "u1", "view", d2)); err != nil {
		t.Fatal(err)
	}
	if c1 := findCohort(e.CohortResult(), "2026-01-11"); c1 == nil || c1.Size != 1 {
		t.Fatalf("tentative cohort wrong: %+v", c1)
	}

	// A late day0 event arrives and moves u1 to the day0 cohort (tentative).
	if err := e.AddEvent(mkEvent("a", "u1", "view", d0)); err != nil {
		t.Fatal(err)
	}
	rows := e.CohortResult()
	if c1 := findCohort(rows, "2026-01-11"); c1 != nil {
		t.Fatalf("u1 must leave day1 cohort: %+v", c1)
	}
	c0 := findCohort(rows, "2026-01-10")
	if c0 == nil || c0.Size != 1 || c0.Retained[1] != 1 || c0.Retained[2] != 1 {
		t.Fatalf("u1 must belong to day0 with retained d1,d2: %+v", c0)
	}

	// The corrected result must match an in-order replay of the same events.
	events := []Event{
		mkEvent("a", "u1", "view", d0),
		mkEvent("b", "u1", "view", d1),
		mkEvent("c", "u1", "view", d2),
	}
	_, wantRows := replayOrdered(t, cfg, events)
	assertCohortRowsEqual(t, wantRows, e.CohortResult())
}

func assertCohortRowsEqual(t *testing.T, want, got []CohortRow) {
	t.Helper()
	if len(want) != len(got) {
		t.Fatalf("cohort row count: want %d got %d (%+v vs %+v)", len(want), len(got), want, got)
	}
	for i := range want {
		if want[i].Period != got[i].Period {
			t.Fatalf("period mismatch: %s vs %s", want[i].Period, got[i].Period)
		}
		if len(want[i].Retained) != len(got[i].Retained) {
			t.Fatalf("%s retained len: want %d got %d", want[i].Period, len(want[i].Retained), len(got[i].Retained))
		}
		for k := range want[i].Retained {
			if want[i].Retained[k] != got[i].Retained[k] {
				t.Fatalf("%s offset %d: want %d got %d", want[i].Period, k, want[i].Retained[k], got[i].Retained[k])
			}
		}
	}
}

func TestTimezoneBucketCut(t *testing.T) {
	loc, err := time.LoadLocation("America/Los_Angeles")
	if err != nil {
		t.Skipf("tzdb unavailable: %v", err)
	}
	cfg := testConfig(loc)
	cfg.Cohort.Period = PeriodHour
	cfg.AllowedLateness = time.Hour
	e := mustEngine(t, cfg)

	// 2026-01-10 23:30 PT == 2026-01-11 07:30 UTC; local bucket must be 01-10.
	localEvening := time.Date(2026, 1, 10, 23, 30, 0, 0, loc)
	if err := e.AddEvent(mkEvent("1", "u1", "view", localEvening)); err != nil {
		t.Fatal(err)
	}
	if r := findRow(e.FunnelResult(), "2026-01-10"); r == nil {
		t.Fatalf("event must bucket into local 2026-01-10, got %+v", e.FunnelResult())
	}
	if r := findRow(e.FunnelResult(), "2026-01-11"); r != nil {
		t.Fatalf("must not bucket by UTC date: %+v", r)
	}
	// Hour cohort key cut in local wall time.
	cr := e.CohortResult()
	if len(cr) != 1 || cr[0].Period != "2026-01-10T23" {
		t.Fatalf("hour cohort key wrong: %+v", cr)
	}
}

func TestCrossDayWindowBoundary(t *testing.T) {
	loc := time.UTC
	cfg := testConfig(loc)
	e := mustEngine(t, cfg)
	// Journey starts 23:40 and completes 23:55 same local day, within window.
	d := time.Date(2026, 1, 10, 23, 40, 0, 0, loc)
	for _, ev := range []Event{
		mkEvent("1", "u1", "view", d),
		mkEvent("2", "u1", "cart", d.Add(10*time.Minute)),
		mkEvent("3", "u1", "buy", d.Add(15*time.Minute)),
	} {
		if err := e.AddEvent(ev); err != nil {
			t.Fatal(err)
		}
	}
	r := findRow(e.FunnelResult(), "2026-01-10")
	if r == nil || r.StepReached[2] != 1 {
		t.Fatalf("cross-midnight journey lost: %+v", r)
	}
	// Commit deadline accounts for the window reaching into the next day.
	deadline := time.Date(2026, 1, 11, 1, 30, 0, 0, loc) // 24:00 +1h +30m
	if err := e.AdvanceWatermark(deadline.Add(-time.Minute)); err != nil {
		t.Fatal(err)
	}
	if r = findRow(e.FunnelResult(), "2026-01-10"); r == nil || r.Committed {
		t.Fatalf("should not commit before deadline: %+v", r)
	}
	if err := e.AdvanceWatermark(deadline); err != nil {
		t.Fatal(err)
	}
	if r = findRow(e.FunnelResult(), "2026-01-10"); r == nil || !r.Committed || r.StepReached[2] != 1 {
		t.Fatalf("commit after deadline wrong: %+v", r)
	}
}

func TestSnapshotRecoveryReproducibility(t *testing.T) {
	loc := time.UTC
	cfg := testConfig(loc)
	cfg.AllowedLateness = 7 * 24 * time.Hour
	base := time.Date(2026, 1, 10, 9, 0, 0, 0, loc)

	events := []Event{
		mkEvent("1", "u1", "view", base),
		mkEvent("2", "u2", "view", base.Add(2*time.Minute)),
		mkEvent("3", "u1", "cart", base.Add(5*time.Minute)),
		mkEvent("4", "u3", "view", base.Add(20*time.Hour)),
		mkEvent("5", "u2", "cart", base.Add(21*time.Hour)),
		mkEvent("6", "u3", "cart", base.Add(20*time.Hour+5*time.Minute)),
		mkEvent("7", "u3", "buy", base.Add(20*time.Hour+10*time.Minute)),
		// late event extending u1's journey
		mkEvent("8", "u1", "buy", base.Add(25*time.Minute)),
	}

	// Reference: one continuous engine, everything in arrival order, watermark
	// advanced far beyond all data.
	ref := mustEngine(t, cfg)
	for _, ev := range events {
		if err := ref.AddEvent(ev); err != nil {
			t.Fatal(err)
		}
	}
	if err := ref.AdvanceWatermark(base.Add(72 * time.Hour)); err != nil {
		t.Fatal(err)
	}
	refFun := ref.FunnelResult()
	refCoh := ref.CohortResult()

	// Restarted engine: snapshot midway, resume, advance same watermark.
	e1 := mustEngine(t, cfg)
	for _, ev := range events[:4] {
		if err := e1.AddEvent(ev); err != nil {
			t.Fatal(err)
		}
	}
	snap := e1.Snapshot()

	e2, err := NewEngine(cfg, snap)
	if err != nil {
		t.Fatalf("restore: %v", err)
	}
	for _, ev := range events[4:] {
		if err := e2.AddEvent(ev); err != nil {
			t.Fatal(err)
		}
	}
	// Duplicate redelivery of an already-applied event after restart.
	if err := e2.AddEvent(events[2]); err != nil {
		t.Fatal(err)
	}
	if err := e2.AdvanceWatermark(base.Add(72 * time.Hour)); err != nil {
		t.Fatal(err)
	}
	assertFunnelRowsEqual(t, refFun, e2.FunnelResult())
	assertCohortRowsEqual(t, refCoh, e2.CohortResult())
	if e2.appliedSeq == ref.appliedSeq {
		// restarted engine ingested one extra duplicate delivery, so its
		// accepted-event count must remain equal (dup suppressed)
	}
	if e2.appliedSeq != ref.appliedSeq {
		t.Fatalf("applied seq mismatch after restart: %d vs %d", e2.appliedSeq, ref.appliedSeq)
	}
}

func TestSnapshotFileRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/snap.json"
	loc := time.UTC
	cfg := testConfig(loc)
	base := time.Date(2026, 1, 10, 9, 0, 0, 0, loc)
	e1 := mustEngine(t, cfg)
	for _, ev := range []Event{
		mkEvent("1", "u1", "view", base),
		mkEvent("2", "u1", "cart", base.Add(5*time.Minute)),
	} {
		if err := e1.AddEvent(ev); err != nil {
			t.Fatal(err)
		}
	}
	if err := e1.SaveSnapshot(path); err != nil {
		t.Fatalf("save: %v", err)
	}
	e2, err := LoadSnapshot(path, cfg)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	r := findRow(e2.FunnelResult(), "2026-01-10")
	if r == nil || r.Started != 1 || r.StepReached[1] != 1 {
		t.Fatalf("restored state wrong: %+v", r)
	}

	// Config mismatch must be rejected.
	bad := cfg
	bad.AllowedLateness = time.Hour
	if _, err := LoadSnapshot(path, bad); err != ErrConfigMismatch {
		t.Fatalf("want ErrConfigMismatch, got %v", err)
	}
}

func TestConcurrentWritersAndQueries(t *testing.T) {
	loc := time.UTC
	cfg := testConfig(loc)
	cfg.AllowedLateness = 90 * 24 * time.Hour // keep all late events in this test
	cfg.MaxUsers = 10000
	cfg.MaxDedupEntries = 10000
	e := mustEngine(t, cfg)
	base := time.Date(2026, 1, 10, 0, 0, 0, 0, loc)

	stop := make(chan struct{})
	var done int32
	// Writers
	for w := 0; w < 4; w++ {
		go func(w int) {
			for i := 0; i < 300; i++ {
				u := "u" + itoa((w*300+i)%200)
				step := []string{"view", "cart", "buy"}[(w+i)%3]
				ev := mkEvent("e-"+itoa(w)+"-"+itoa(i), u, step,
					base.Add(time.Duration(i)*time.Minute))
				_ = e.AddEvent(ev) // errors tolerated for late/lru noise
			}
			atomic.AddInt32(&done, 1)
		}(w)
	}
	// Readers
	for r := 0; r < 4; r++ {
		go func() {
			for {
				select {
				case <-stop:
					atomic.AddInt32(&done, 1)
					return
				default:
				}
				fr := e.FunnelResult()
				cr := e.CohortResult()
				// Invariant: counters never negative and retained indices align.
				for _, row := range fr {
					var prev int64 = 1 << 62
					for _, v := range row.StepReached {
						if v < 0 || v > prev {
							panic("inconsistent funnel snapshot")
						}
						prev = v
					}
				}
				for _, row := range cr {
					if row.Size < 0 {
						panic("negative cohort size")
					}
				}
			}
		}()
	}
	for atomic.LoadInt32(&done) < 4 {
		time.Sleep(time.Millisecond)
	}
	close(stop)
	// Give readers a moment to observe stop, then sanity check totals.
	time.Sleep(5 * time.Millisecond)
	rows := e.FunnelResult()
	total := int64(0)
	for _, r := range rows {
		total += r.Started
	}
	if total == 0 {
		t.Fatalf("expected some journeys under concurrency")
	}
}

func TestLRUUserStateBounded(t *testing.T) {
	loc := time.UTC
	cfg := testConfig(loc)
	cfg.AllowedLateness = 365 * 24 * time.Hour // ensure nothing is late-dropped
	cfg.MaxUsers = 10
	cfg.MaxDedupEntries = 10000
	e := mustEngine(t, cfg)
	base := time.Date(2026, 1, 10, 9, 0, 0, 0, loc)

	for i := 0; i < 100; i++ {
		u := "u" + itoa(i)
		ev := mkEvent("e"+itoa(i), u, "view", base.Add(time.Duration(i)*time.Minute))
		if err := e.AddEvent(ev); err != nil {
			t.Fatal(err)
		}
	}
	if len(e.funnelUsers) > 10 || len(e.cohortUsers) > 10 {
		t.Fatalf("user state unbounded: funnel=%d cohort=%d", len(e.funnelUsers), len(e.cohortUsers))
	}
}

func TestWeekAndMonthPeriods(t *testing.T) {
	loc := time.UTC
	// Week
	cfg := testConfig(loc)
	cfg.Cohort.Period = PeriodWeek
	cfg.AllowedLateness = 30 * 24 * time.Hour
	e := mustEngine(t, cfg)
	monday := time.Date(2026, 1, 5, 10, 0, 0, 0, loc) // Monday of ISO week 2
	nextMonday := monday.AddDate(0, 0, 7)
	if err := e.AddEvent(mkEvent("1", "u1", "view", monday)); err != nil {
		t.Fatal(err)
	}
	if err := e.AddEvent(mkEvent("2", "u1", "view", nextMonday)); err != nil {
		t.Fatal(err)
	}
	cr := e.CohortResult()
	if len(cr) != 1 || cr[0].Period != "2026-W02" || cr[0].Retained[1] != 1 {
		t.Fatalf("week cohort wrong: %+v", cr)
	}

	// Month
	cfg2 := testConfig(loc)
	cfg2.Cohort.Period = PeriodMonth
	cfg2.AllowedLateness = 60 * 24 * time.Hour
	e2 := mustEngine(t, cfg2)
	jan := time.Date(2026, 1, 20, 0, 0, 0, 0, loc)
	feb := time.Date(2026, 2, 5, 0, 0, 0, 0, loc)
	if err := e2.AddEvent(mkEvent("1", "u1", "view", jan)); err != nil {
		t.Fatal(err)
	}
	if err := e2.AddEvent(mkEvent("2", "u1", "view", feb)); err != nil {
		t.Fatal(err)
	}
	cr2 := e2.CohortResult()
	if len(cr2) != 1 || cr2[0].Period != "2026-01" || cr2[0].Retained[1] != 1 {
		t.Fatalf("month cohort wrong: %+v", cr2)
	}
}

func TestMultipleJourneysPerUser(t *testing.T) {
	loc := time.UTC
	cfg := testConfig(loc)
	e := mustEngine(t, cfg)
	base := time.Date(2026, 1, 10, 8, 0, 0, 0, loc)
	// Two independent journeys of the same user the same day; both convert.
	for _, ev := range []Event{
		mkEvent("1", "u1", "view", base),
		mkEvent("2", "u1", "cart", base.Add(5*time.Minute)),
		mkEvent("3", "u1", "buy", base.Add(10*time.Minute)),
		mkEvent("4", "u1", "view", base.Add(30*time.Minute)),
		mkEvent("5", "u1", "cart", base.Add(40*time.Minute)),
		mkEvent("6", "u1", "buy", base.Add(50*time.Minute)),
	} {
		if err := e.AddEvent(ev); err != nil {
			t.Fatal(err)
		}
	}
	r := findRow(e.FunnelResult(), "2026-01-10")
	if r == nil || r.Started != 2 || r.StepReached[2] != 2 {
		t.Fatalf("expected two converting journeys: %+v", r)
	}
}

func TestDuplicateAfterCommitIsNoop(t *testing.T) {
	loc := time.UTC
	cfg := testConfig(loc)
	e := mustEngine(t, cfg)
	d := time.Date(2026, 1, 10, 9, 0, 0, 0, loc)
	ev := mkEvent("a", "u1", "view", d)
	if err := e.AddEvent(ev); err != nil {
		t.Fatal(err)
	}
	// Commit everything, moving the watermark far beyond the event.
	if err := e.AdvanceWatermark(d.Add(72 * time.Hour)); err != nil {
		t.Fatal(err)
	}
	r := findRow(e.FunnelResult(), "2026-01-10")
	if r == nil || !r.Committed || r.Started != 1 {
		t.Fatalf("precondition: %+v", r)
	}
	// Redelivery after the window is closed: must not error and must not
	// double count.
	if err := e.AddEvent(ev); err != nil {
		t.Fatalf("redelivery after commit must be a no-op, got %v", err)
	}
	r = findRow(e.FunnelResult(), "2026-01-10")
	if r == nil || r.Started != 1 {
		t.Fatalf("redelivery altered committed state: %+v", r)
	}
}

func TestEmptyIDEventsAreNotDeduplicated(t *testing.T) {
	loc := time.UTC
	e := mustEngine(t, testConfig(loc))
	d := time.Date(2026, 1, 10, 9, 0, 0, 0, loc)
	if err := e.AddEvent(Event{UserID: "u1", Name: "view", Timestamp: d}); err != nil {
		t.Fatal(err)
	}
	if err := e.AddEvent(Event{UserID: "u1", Name: "view", Timestamp: d.Add(time.Minute)}); err != nil {
		t.Fatal(err)
	}
	r := findRow(e.FunnelResult(), "2026-01-10")
	if r == nil || r.Started != 2 {
		t.Fatalf("empty-id events should both count as journeys: %+v", r)
	}
}

func TestInvalidConfigsRejected(t *testing.T) {
	loc := time.UTC
	cases := map[string]Config{
		"one step":    testConfig(loc).with(func(c *Config) { c.Funnel.Steps = []string{"view"} }),
		"zero window": testConfig(loc).with(func(c *Config) { c.Funnel.Window = 0 }),
		"dup steps":   testConfig(loc).with(func(c *Config) { c.Funnel.Steps = []string{"a", "a"} }),
		"bad period":  testConfig(loc).with(func(c *Config) { c.Cohort.Period = "yearly" }),
	}
	for name, cfg := range cases {
		if _, err := NewEngine(cfg, nil); err == nil {
			t.Fatalf("config %q should be rejected", name)
		}
	}
}

func (c Config) with(fn func(*Config)) Config { fn(&c); return c }

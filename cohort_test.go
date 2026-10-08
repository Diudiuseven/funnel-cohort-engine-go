package engine

import (
	"math/rand"
	"testing"
	"time"
)

func rowByCohort(t *testing.T, r RetentionResult, key string) CohortRow {
	t.Helper()
	for _, row := range r.Rows {
		if row.Cohort == key {
			return row
		}
	}
	t.Fatalf("cohort %s not found in %+v", key, r.Rows)
	return CohortRow{}
}

func TestCohortBasicRetention(t *testing.T) {
	e := mustNew(t, testConfig(func(c *Config) { c.AllowedLateness = 72 * time.Hour }))
	day0 := time.Date(2026, 10, 1, 10, 0, 0, 0, time.UTC) // 周四，CST 18:00 -> 10-01
	// u1: 10-01 首次出现，10-02、10-03 仍活跃。
	e.Ingest(
		Event{ID: "1", UserID: "u1", Name: "view", Timestamp: day0},
		Event{ID: "2", UserID: "u1", Name: "view", Timestamp: day0.Add(24 * time.Hour)},
		Event{ID: "3", UserID: "u1", Name: "view", Timestamp: day0.Add(48 * time.Hour)},
		// u2: 10-01 首次出现，仅当日活跃。
		Event{ID: "4", UserID: "u2", Name: "view", Timestamp: day0.Add(time.Hour)},
	)
	row := rowByCohort(t, e.QueryRetention(), "2026-10-01")
	if row.Size != 2 {
		t.Fatalf("size=%d", row.Size)
	}
	if row.Retained[0] != 2 || row.Retained[1] != 1 || row.Retained[2] != 1 || row.Retained[3] != 0 {
		t.Fatalf("retained=%v", row.Retained)
	}
}

func TestCohortLateFirstEventReassigns(t *testing.T) {
	e := mustNew(t, testConfig(func(c *Config) { c.AllowedLateness = 72 * time.Hour }))
	day2 := time.Date(2026, 10, 2, 10, 0, 0, 0, time.UTC)
	day1 := time.Date(2026, 10, 1, 10, 0, 0, 0, time.UTC)
	// 先到的事件把 u1 归入 10-02。
	e.Ingest(Event{ID: "1", UserID: "u1", Name: "view", Timestamp: day2})
	r := e.QueryRetention()
	if rowByCohort(t, r, "2026-10-02").Size != 1 {
		t.Fatalf("before late event: %+v", r.Rows)
	}
	// 迟到的更早首次事件必须把 u1 改归 10-01，且 10-02 的错误归属被纠正。
	e.Ingest(Event{ID: "2", UserID: "u1", Name: "view", Timestamp: day1})
	r = e.QueryRetention()
	if rowByCohort(t, r, "2026-10-01").Size != 1 {
		t.Fatalf("after late event: %+v", r.Rows)
	}
	for _, row := range r.Rows {
		if row.Cohort == "2026-10-02" && row.Size != 0 {
			t.Fatalf("stale cohort assignment kept: %+v", row)
		}
	}
	// 10-02 的活跃应计为 10-01 群组的 offset=1 留存。
	row := rowByCohort(t, r, "2026-10-01")
	if row.Retained[0] != 1 || row.Retained[1] != 1 {
		t.Fatalf("retained=%v", row.Retained)
	}
}

func TestCohortWeeklyPeriod(t *testing.T) {
	e := mustNew(t, testConfig(func(c *Config) {
		c.Cohort.Period = WeekPeriod
		c.AllowedLateness = 200 * time.Hour
	}))
	// 2026-10-05 是周一（CST）。周内任意一天首次出现都归入同一周群组。
	mon := time.Date(2026, 10, 5, 1, 0, 0, 0, time.UTC)   // CST 09:00 周一
	wed := time.Date(2026, 10, 7, 1, 0, 0, 0, time.UTC)   // CST 周三
	next := time.Date(2026, 10, 12, 1, 0, 0, 0, time.UTC) // CST 下周一
	e.Ingest(
		Event{ID: "1", UserID: "u1", Name: "view", Timestamp: wed},
		Event{ID: "2", UserID: "u1", Name: "view", Timestamp: next},
		Event{ID: "3", UserID: "u2", Name: "view", Timestamp: mon},
	)
	row := rowByCohort(t, e.QueryRetention(), "2026-10-05")
	if row.Size != 2 {
		t.Fatalf("size=%d", row.Size)
	}
	if row.Retained[0] != 2 || row.Retained[1] != 1 {
		t.Fatalf("retained=%v", row.Retained)
	}
}

func TestCohortCommittedFlag(t *testing.T) {
	e := mustNew(t, testConfig(nil))
	day1 := time.Date(2026, 10, 1, 10, 0, 0, 0, time.UTC)
	e.Ingest(Event{ID: "1", UserID: "u1", Name: "view", Timestamp: day1})
	if rowByCohort(t, e.QueryRetention(), "2026-10-01").Committed {
		t.Fatal("should be provisional")
	}
	// 水印越过 10-01 周期末。
	e.Ingest(Event{ID: "2", UserID: "u2", Name: "view", Timestamp: day1.Add(48 * time.Hour)})
	if !rowByCohort(t, e.QueryRetention(), "2026-10-01").Committed {
		t.Fatal("should be committed")
	}
}

// TestCohortShuffledEqualsOrdered 性质测试：乱序+重复后留存结果与有序重放一致。
func TestCohortShuffledEqualsOrdered(t *testing.T) {
	cfg := testConfig(func(c *Config) { c.AllowedLateness = 100 * time.Hour })
	rng := rand.New(rand.NewSource(7))
	base := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	var events []Event
	id := 0
	for u := 0; u < 30; u++ {
		uid := string(rune('A' + u))
		for i := 0; i < 20; i++ {
			id++
			events = append(events, Event{
				ID:        string(rune(id)),
				UserID:    uid,
				Name:      "view",
				Timestamp: base.Add(time.Duration(rng.Intn(4*24*60)) * time.Minute),
			})
		}
	}
	ref := mustNew(t, cfg)
	sorted := append([]Event(nil), events...)
	for i := 0; i < len(sorted); i++ {
		for j := i + 1; j < len(sorted); j++ {
			if sorted[j].Timestamp.Before(sorted[i].Timestamp) ||
				(sorted[j].Timestamp.Equal(sorted[i].Timestamp) && sorted[j].ID < sorted[i].ID) {
				sorted[i], sorted[j] = sorted[j], sorted[i]
			}
		}
	}
	ref.Ingest(sorted...)
	want := ref.QueryRetention()

	e := mustNew(t, cfg)
	shuffled := append([]Event(nil), events...)
	rng.Shuffle(len(shuffled), func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })
	withDups := append(shuffled, shuffled[:50]...)
	rng.Shuffle(len(withDups), func(i, j int) { withDups[i], withDups[j] = withDups[j], withDups[i] })
	e.Ingest(withDups...)
	got := e.QueryRetention()

	if len(got.Rows) != len(want.Rows) {
		t.Fatalf("rows got %d want %d", len(got.Rows), len(want.Rows))
	}
	for i := range want.Rows {
		gr, wr := got.Rows[i], want.Rows[i]
		if gr.Cohort != wr.Cohort || gr.Size != wr.Size || gr.Committed != wr.Committed {
			t.Fatalf("row %d got %+v want %+v", i, gr, wr)
		}
		if len(gr.Retained) != len(wr.Retained) {
			t.Fatalf("row %d retained len", i)
		}
		for j := range wr.Retained {
			if gr.Retained[j] != wr.Retained[j] {
				t.Fatalf("row %d offset %d got %d want %d", i, j, gr.Retained[j], wr.Retained[j])
			}
		}
	}
}

package engine

import (
	"math/rand"
	"testing"
	"time"
)

func testConfig(mut func(*Config)) Config {
	cfg := Config{
		Timezone:        "Asia/Shanghai",
		Funnel:          FunnelConfig{Steps: []string{"view", "cart", "pay"}, Window: time.Hour},
		Cohort:          CohortConfig{Period: DayPeriod, MaxOffsets: 3},
		AllowedLateness: 10 * time.Minute,
	}
	if mut != nil {
		mut(&cfg)
	}
	return cfg
}

func mustNew(t *testing.T, cfg Config) *Engine {
	t.Helper()
	e, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return e
}

func bucketByKey(t *testing.T, r FunnelResult, key string) FunnelBucket {
	t.Helper()
	for _, b := range r.Buckets {
		if b.Bucket == key {
			return b
		}
	}
	t.Fatalf("bucket %s not found in %+v", key, r.Buckets)
	return FunnelBucket{}
}

func TestFunnelOrderedConversion(t *testing.T) {
	e := mustNew(t, testConfig(nil))
	base := time.Date(2026, 10, 1, 10, 0, 0, 0, time.UTC)
	e.Ingest(
		Event{ID: "1", UserID: "u1", Name: "view", Timestamp: base},
		Event{ID: "2", UserID: "u1", Name: "cart", Timestamp: base.Add(time.Minute)},
		Event{ID: "3", UserID: "u1", Name: "pay", Timestamp: base.Add(2 * time.Minute)},
	)
	b := bucketByKey(t, e.QueryFunnel(), "2026-10-01")
	if b.Entered != 1 || b.Converted != 1 {
		t.Fatalf("got %+v", b)
	}
}

func TestFunnelOutOfOrderConverges(t *testing.T) {
	e := mustNew(t, testConfig(nil))
	base := time.Date(2026, 10, 1, 10, 0, 0, 0, time.UTC)
	// 逆序到达：先 pay 后 view，最终结果必须与有序一致。
	e.Ingest(
		Event{ID: "3", UserID: "u1", Name: "pay", Timestamp: base.Add(2 * time.Minute)},
		Event{ID: "2", UserID: "u1", Name: "cart", Timestamp: base.Add(time.Minute)},
		Event{ID: "1", UserID: "u1", Name: "view", Timestamp: base},
	)
	b := bucketByKey(t, e.QueryFunnel(), "2026-10-01")
	if b.Entered != 1 || b.Converted != 1 {
		t.Fatalf("got %+v", b)
	}
}

func TestFunnelDuplicateNotCounted(t *testing.T) {
	e := mustNew(t, testConfig(nil))
	base := time.Date(2026, 10, 1, 10, 0, 0, 0, time.UTC)
	ev := Event{ID: "1", UserID: "u1", Name: "view", Timestamp: base}
	st := e.Ingest(ev, ev, ev)
	if st.Accepted != 1 || st.Duplicates != 2 {
		t.Fatalf("stats %+v", st)
	}
	b := bucketByKey(t, e.QueryFunnel(), "2026-10-01")
	if b.Entered != 1 {
		t.Fatalf("got %+v", b)
	}
}

func TestFunnelWindowExpiry(t *testing.T) {
	e := mustNew(t, testConfig(nil))
	base := time.Date(2026, 10, 1, 10, 0, 0, 0, time.UTC)
	// pay 在 1 小时窗口之外，不应计为转化。
	e.Ingest(
		Event{ID: "1", UserID: "u1", Name: "view", Timestamp: base},
		Event{ID: "2", UserID: "u1", Name: "cart", Timestamp: base.Add(30 * time.Minute)},
		Event{ID: "3", UserID: "u1", Name: "pay", Timestamp: base.Add(61 * time.Minute)},
	)
	b := bucketByKey(t, e.QueryFunnel(), "2026-10-01")
	if b.Entered != 1 || b.Converted != 0 {
		t.Fatalf("got %+v", b)
	}
}

func TestFunnelLateEventCompletesConversion(t *testing.T) {
	e := mustNew(t, testConfig(nil))
	base := time.Date(2026, 10, 1, 10, 0, 0, 0, time.UTC)
	e.Ingest(
		Event{ID: "1", UserID: "u1", Name: "view", Timestamp: base},
		Event{ID: "2", UserID: "u1", Name: "cart", Timestamp: base.Add(time.Minute)},
	)
	if b := bucketByKey(t, e.QueryFunnel(), "2026-10-01"); b.Converted != 0 {
		t.Fatalf("premature conversion %+v", b)
	}
	// 迟到的 pay 到达后，临时结果应收敛为已转化。
	e.Ingest(Event{ID: "3", UserID: "u1", Name: "pay", Timestamp: base.Add(2 * time.Minute)})
	if b := bucketByKey(t, e.QueryFunnel(), "2026-10-01"); b.Converted != 1 {
		t.Fatalf("got %+v", b)
	}
}

func TestFunnelTooLateDropped(t *testing.T) {
	e := mustNew(t, testConfig(nil))
	day1 := time.Date(2026, 10, 1, 10, 0, 0, 0, time.UTC)
	day2 := time.Date(2026, 10, 2, 10, 0, 0, 0, time.UTC)
	// 推进水印到 day2+50min（maxSeen=day2+1h, lateness=10min）。
	e.Ingest(Event{ID: "w", UserID: "u9", Name: "view", Timestamp: day2.Add(time.Hour)})
	// day1 的事件远早于水印，超出允许迟到上限，必须被丢弃。
	st := e.Ingest(Event{ID: "1", UserID: "u1", Name: "view", Timestamp: day1})
	if st.TooLate != 1 || st.Accepted != 0 {
		t.Fatalf("stats %+v", st)
	}
	for _, b := range e.QueryFunnel().Buckets {
		if b.Bucket == "2026-10-01" {
			t.Fatalf("too-late event counted: %+v", b)
		}
	}
}

func TestFunnelCommittedAfterWatermark(t *testing.T) {
	e := mustNew(t, testConfig(nil))
	base := time.Date(2026, 10, 1, 10, 0, 0, 0, time.UTC)
	e.Ingest(Event{ID: "1", UserID: "u1", Name: "view", Timestamp: base})
	if b := bucketByKey(t, e.QueryFunnel(), "2026-10-01"); b.Committed {
		t.Fatal("should be provisional before watermark passes")
	}
	// 水印需要越过 10-01 末日 + 1h 窗口：maxSeen >= 10-03 08:00(UTC+8) + lateness。
	e.Ingest(Event{ID: "2", UserID: "u2", Name: "view", Timestamp: time.Date(2026, 10, 3, 1, 0, 0, 0, time.UTC)})
	if b := bucketByKey(t, e.QueryFunnel(), "2026-10-01"); !b.Committed {
		t.Fatalf("should be committed, watermark=%v", e.Watermark())
	}
}

func TestFunnelCrossDayTimezone(t *testing.T) {
	// UTC 2026-10-01 20:30 在 Asia/Shanghai 是 10-02 04:30，桶应归入 10-02。
	e := mustNew(t, testConfig(nil))
	ts := time.Date(2026, 10, 1, 20, 30, 0, 0, time.UTC)
	e.Ingest(Event{ID: "1", UserID: "u1", Name: "view", Timestamp: ts})
	r := e.QueryFunnel()
	if len(r.Buckets) != 1 || r.Buckets[0].Bucket != "2026-10-02" {
		t.Fatalf("got %+v", r.Buckets)
	}
	// 换用 UTC 时区则归入 10-01。
	e2 := mustNew(t, testConfig(func(c *Config) { c.Timezone = "UTC" }))
	e2.Ingest(Event{ID: "1", UserID: "u1", Name: "view", Timestamp: ts})
	r2 := e2.QueryFunnel()
	if len(r2.Buckets) != 1 || r2.Buckets[0].Bucket != "2026-10-01" {
		t.Fatalf("got %+v", r2.Buckets)
	}
}

// TestFunnelShuffledEqualsOrdered 性质测试：同一事件集任意乱序+重复投递，
// 最终结果必须与有序重放一致。
func TestFunnelShuffledEqualsOrdered(t *testing.T) {
	// 允许迟到必须覆盖乱序幅度，否则超出部分按语义被丢弃。
	cfg := testConfig(func(c *Config) { c.AllowedLateness = 100 * time.Hour })
	rng := rand.New(rand.NewSource(42))
	base := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	names := []string{"view", "cart", "pay", "other"}
	var events []Event
	id := 0
	for u := 0; u < 20; u++ {
		uid := string(rune('a' + u))
		for i := 0; i < 30; i++ {
			id++
			events = append(events, Event{
				ID:        string(rune(id)),
				UserID:    uid,
				Name:      names[rng.Intn(len(names))],
				Timestamp: base.Add(time.Duration(rng.Intn(3*24*60)) * time.Minute),
			})
		}
	}
	// 有序重放参照。
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
	want := ref.QueryFunnel()

	// 随机乱序 + 随机重复，分多批写入。
	e := mustNew(t, cfg)
	shuffled := append([]Event(nil), events...)
	rng.Shuffle(len(shuffled), func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })
	withDups := append([]Event(nil), shuffled...)
	for _, ev := range shuffled[:100] {
		withDups = append(withDups, ev) // 重复投递
	}
	rng.Shuffle(len(withDups), func(i, j int) { withDups[i], withDups[j] = withDups[j], withDups[i] })
	for i := 0; i < len(withDups); i += 50 {
		end := i + 50
		if end > len(withDups) {
			end = len(withDups)
		}
		e.Ingest(withDups[i:end]...)
	}
	got := e.QueryFunnel()
	if len(got.Buckets) != len(want.Buckets) {
		t.Fatalf("bucket count got %d want %d", len(got.Buckets), len(want.Buckets))
	}
	for i := range want.Buckets {
		if got.Buckets[i] != want.Buckets[i] {
			t.Fatalf("bucket %d got %+v want %+v", i, got.Buckets[i], want.Buckets[i])
		}
	}
}

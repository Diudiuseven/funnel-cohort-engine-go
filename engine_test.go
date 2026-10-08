package engine

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"
)

// TestRecoveryEqualsNoRestart 快照恢复后继续写入，结果必须与未重启一致。
func TestRecoveryEqualsNoRestart(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(func(c *Config) {
		c.AllowedLateness = 100 * time.Hour
		c.SnapshotPath = dir + "/snap.gob"
	})
	base := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	var batch1, batch2 []Event
	id := 0
	for u := 0; u < 10; u++ {
		uid := fmt.Sprintf("u%d", u)
		for i := 0; i < 10; i++ {
			id++
			ev := Event{
				ID:        fmt.Sprintf("e%d", id),
				UserID:    uid,
				Name:      []string{"view", "cart", "pay"}[id%3],
				Timestamp: base.Add(time.Duration(id*7) * time.Minute),
			}
			if i < 5 {
				batch1 = append(batch1, ev)
			} else {
				batch2 = append(batch2, ev)
			}
		}
	}

	// 参照：不重启，连续写入两批。
	ref := mustNew(t, cfg)
	ref.Ingest(batch1...)
	ref.Ingest(batch2...)
	wantF := ref.QueryFunnel()
	wantR := ref.QueryRetention()

	// 被测：第一批后快照并“重启”，再写第二批（并重复投递第一批）。
	e := mustNew(t, cfg)
	e.Ingest(batch1...)
	if err := e.SaveSnapshot(); err != nil {
		t.Fatal(err)
	}
	e2, err := Load(cfg)
	if err != nil {
		t.Fatal(err)
	}
	dup := e2.Ingest(batch1...)
	if dup.Accepted != 0 || dup.Duplicates != len(batch1) {
		t.Fatalf("replayed batch after restore: %+v", dup)
	}
	e2.Ingest(batch2...)
	gotF := e2.QueryFunnel()
	gotR := e2.QueryRetention()

	if fmt.Sprintf("%+v", gotF.Buckets) != fmt.Sprintf("%+v", wantF.Buckets) {
		t.Fatalf("funnel mismatch\ngot  %+v\nwant %+v", gotF.Buckets, wantF.Buckets)
	}
	if fmt.Sprintf("%+v", gotR.Rows) != fmt.Sprintf("%+v", wantR.Rows) {
		t.Fatalf("retention mismatch\ngot  %+v\nwant %+v", gotR.Rows, wantR.Rows)
	}
}

// TestConcurrentQuerySnapshot 写入与查询并发：查询必须始终看到自洽快照
// （留存人数不得超过群组规模，计数不得为负）。配合 -race 运行。
func TestConcurrentQuerySnapshot(t *testing.T) {
	e := mustNew(t, testConfig(func(c *Config) { c.AllowedLateness = 100 * time.Hour }))
	base := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	var wg sync.WaitGroup
	stop := make(chan struct{})

	for w := 0; w < 4; w++ {
		startWriter(w, &wg, stop, e, base)
	}
	// 查询 goroutine：校验快照自洽性。
	for r := 0; r < 2; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				fr := e.QueryFunnel()
				for _, b := range fr.Buckets {
					if b.Entered < 0 || b.Converted < 0 || b.Converted > b.Entered {
						t.Errorf("inconsistent funnel bucket: %+v", b)
						return
					}
				}
				rr := e.QueryRetention()
				for _, row := range rr.Rows {
					if row.Size < 0 {
						t.Errorf("negative cohort size: %+v", row)
						return
					}
					for _, n := range row.Retained {
						if n < 0 || n > row.Size {
							t.Errorf("inconsistent retention: %+v", row)
							return
						}
					}
				}
			}
		}()
	}
	// 写一段时间后停止。
	time.Sleep(100 * time.Millisecond)
	close(stop)
	wg.Wait()
}

func startWriter(w int, wg *sync.WaitGroup, stop chan struct{}, e *Engine, base time.Time) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		rng := rand.New(rand.NewSource(int64(w)))
		i := 0
		for {
			select {
			case <-stop:
				return
			default:
			}
			batch := make([]Event, 0, 20)
			for j := 0; j < 20; j++ {
				i++
				batch = append(batch, Event{
					ID:        fmt.Sprintf("w%d-e%d", w, i),
					UserID:    fmt.Sprintf("u%d", rng.Intn(50)),
					Name:      []string{"view", "cart", "pay"}[rng.Intn(3)],
					Timestamp: base.Add(time.Duration(rng.Intn(48*60)) * time.Minute),
				})
			}
			e.Ingest(batch...)
		}
	}()
}

// TestMemoryBoundEviction 超出 MaxActiveUsers 后内存中的用户数必须受限。
func TestMemoryBoundEviction(t *testing.T) {
	e := mustNew(t, testConfig(func(c *Config) {
		c.AllowedLateness = 100 * time.Hour
		c.MaxActiveUsers = 100
	}))
	base := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	for u := 0; u < 1000; u++ {
		e.Ingest(Event{
			ID:        fmt.Sprintf("e%d", u),
			UserID:    fmt.Sprintf("u%d", u),
			Name:      "view",
			Timestamp: base.Add(time.Duration(u) * time.Minute),
		})
	}
	e.mu.Lock()
	n := len(e.state.users)
	e.mu.Unlock()
	if n > 100 {
		t.Fatalf("users in memory = %d, want <= 100", n)
	}
	// 聚合结果不受淘汰影响：1000 个用户各进入一次漏斗。
	var total int
	for _, b := range e.QueryFunnel().Buckets {
		total += b.Entered
	}
	if total != 1000 {
		t.Fatalf("total entered = %d, want 1000", total)
	}
}

// TestDedupBounded 去重表必须随水印推进而收缩，不得无限增长。
func TestDedupBounded(t *testing.T) {
	e := mustNew(t, testConfig(nil)) // lateness=10min
	base := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	for i := 0; i < 5000; i++ {
		e.Ingest(Event{
			ID:        fmt.Sprintf("e%d", i),
			UserID:    fmt.Sprintf("u%d", i),
			Name:      "view",
			Timestamp: base.Add(time.Duration(i) * time.Minute),
		})
	}
	e.mu.Lock()
	n := len(e.state.dedup)
	e.mu.Unlock()
	// 水印 = maxSeen - 10min，去重表只应保留水位之后的条目（约 10 个）。
	if n > 100 {
		t.Fatalf("dedup size = %d, want bounded", n)
	}
}

// TestReproducible 相同输入与配置，两次独立运行的输出必须一致。
func TestReproducible(t *testing.T) {
	cfg := testConfig(func(c *Config) { c.AllowedLateness = 100 * time.Hour })
	base := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	rng := rand.New(rand.NewSource(1))
	var events []Event
	for i := 0; i < 500; i++ {
		events = append(events, Event{
			ID:        fmt.Sprintf("e%d", i),
			UserID:    fmt.Sprintf("u%d", rng.Intn(30)),
			Name:      []string{"view", "cart", "pay"}[rng.Intn(3)],
			Timestamp: base.Add(time.Duration(rng.Intn(72*60)) * time.Minute),
		})
	}
	run := func() (FunnelResult, RetentionResult) {
		e := mustNew(t, cfg)
		e.Ingest(events...)
		return e.QueryFunnel(), e.QueryRetention()
	}
	f1, r1 := run()
	f2, r2 := run()
	if fmt.Sprintf("%+v", f1) != fmt.Sprintf("%+v", f2) ||
		fmt.Sprintf("%+v", r1) != fmt.Sprintf("%+v", r2) {
		t.Fatal("outputs not reproducible")
	}
}

// TestWatermarkMonotonic 水印只能单调前进。
func TestWatermarkMonotonic(t *testing.T) {
	e := mustNew(t, testConfig(nil))
	base := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	e.Ingest(Event{ID: "1", UserID: "u1", Name: "view", Timestamp: base.Add(time.Hour)})
	w1 := e.Watermark()
	// 写入更早（但在允许迟到内）的事件，水印不得回退。
	e.Ingest(Event{ID: "2", UserID: "u1", Name: "view", Timestamp: base.Add(50 * time.Minute)})
	if e.Watermark().Before(w1) {
		t.Fatalf("watermark regressed: %v -> %v", w1, e.Watermark())
	}
	e.Ingest(Event{ID: "3", UserID: "u1", Name: "view", Timestamp: base.Add(2 * time.Hour)})
	if !e.Watermark().After(w1) {
		t.Fatal("watermark should advance")
	}
}

// TestDormantUserReturn 冷用户休眠化后回归：不得被重复计入群组规模。
func TestDormantUserReturn(t *testing.T) {
	e := mustNew(t, testConfig(func(c *Config) {
		c.AllowedLateness = time.Hour
		c.MaxActiveUsers = 10000
	}))
	day0 := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	day8 := day0.Add(8 * 24 * time.Hour)
	e.Ingest(Event{ID: "1", UserID: "u1", Name: "view", Timestamp: day0})
	// 用其他用户的事件推进水印，使 u1 休眠化（horizon 约 5.1 天）。
	e.Ingest(Event{ID: "2", UserID: "u2", Name: "view", Timestamp: day8})
	e.mu.Lock()
	dormant := e.state.users["u1"].Dormant
	e.mu.Unlock()
	if !dormant {
		t.Fatal("u1 should be dormant after watermark advanced beyond horizon")
	}
	// u1 回归：群组规模不得重复计数。
	e.Ingest(Event{ID: "3", UserID: "u1", Name: "view", Timestamp: day8})
	r := e.QueryRetention()
	var total int
	for _, row := range r.Rows {
		total += row.Size
		if row.Size > 1 {
			t.Fatalf("cohort %s size=%d, user double counted", row.Cohort, row.Size)
		}
	}
	if total != 2 { // u1(day0群组) + u2(day8群组)
		t.Fatalf("total cohort size = %d, want 2", total)
	}
	if rowByCohort(t, r, "2026-10-01").Size != 1 {
		t.Fatalf("u1 must stay in original cohort: %+v", r.Rows)
	}
}

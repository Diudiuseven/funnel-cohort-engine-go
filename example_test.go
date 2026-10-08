package funnel_test

import (
	"fmt"
	"time"

	funnel "github.com/Diudiuseven/funnel-cohort-engine-go"
)

func Example() {
	loc, _ := time.LoadLocation("Asia/Shanghai")
	cfg := funnel.Config{
		Timezone: loc,
		Funnel: funnel.FunnelConfig{
			Steps:  []string{"app_open", "add_cart", "pay"},
			Window: 24 * time.Hour,
		},
		Cohort:          funnel.CohortConfig{Period: funnel.PeriodDay},
		AllowedLateness: 2 * time.Hour,
		MaxUsers:        100_000,
	}

	e, err := funnel.NewEngine(cfg, nil)
	if err != nil {
		panic(err)
	}

	day := time.Date(2026, 5, 1, 10, 0, 0, 0, loc)
	events := []funnel.Event{
		{ID: "1", UserID: "alice", Name: "app_open", Timestamp: day},
		{ID: "2", UserID: "alice", Name: "add_cart", Timestamp: day.Add(time.Hour)},
		{ID: "3", UserID: "alice", Name: "pay", Timestamp: day.Add(3 * time.Hour)},
		{ID: "4", UserID: "bob", Name: "app_open", Timestamp: day},
	}
	for _, ev := range events {
		if err := e.AddEvent(ev); err != nil {
			panic(err)
		}
	}

	for _, r := range e.FunnelResult() {
		fmt.Printf("%s started=%d pay=%d committed=%v\n",
			r.Date, r.Started, r.StepReached[2], r.Committed)
	}
	// Output: 2026-05-01 started=2 pay=1 committed=false
}

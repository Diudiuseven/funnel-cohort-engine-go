package funnel

import "time"

// Event is a single user behavior event in the input stream.
type Event struct {
	ID        string    // unique delivery id, used for duplicate suppression
	UserID    string    // high-cardinality user identifier
	Name      string    // event/step name
	Timestamp time.Time // logical event time (any monotonic clock reading is ignored)
}

// Period is the calendar granularity used for cohort grouping and retention.
type Period string

const (
	PeriodHour  Period = "hour"
	PeriodDay   Period = "day"
	PeriodWeek  Period = "week"
	PeriodMonth Period = "month"
)

// FunnelConfig configures an ordered step funnel.
type FunnelConfig struct {
	// Steps is the ordered list of event names that form the funnel.
	// A user converts by reaching every step in order within Window measured
	// from the timestamp of the step-0 event.
	Steps []string
	// Window is the maximum elapsed time allowed between the first step and
	// any later step. Boundary is inclusive: t - start <= Window.
	Window time.Duration
}

// CohortConfig configures first-seen cohort retention.
type CohortConfig struct {
	Period Period
}

// Config bundles all engine configuration. Timezone determines how funnel
// journey start dates and cohort/retention period boundaries are cut.
type Config struct {
	Timezone *time.Location // required; defaults to UTC when nil

	Funnel FunnelConfig
	Cohort CohortConfig

	// AllowedLateness is the maximum amount an event may lag behind the
	// watermark and still be accepted. The auto watermark is
	// maxObservedEventTime - AllowedLateness.
	AllowedLateness time.Duration

	// MaxUsers bounds the number of per-user states kept in memory (LRU).
	// Zero means DefaultMaxUsers. See README for the accuracy impact.
	MaxUsers int

	// MaxDedupEntries bounds the duplicate-suppression set. Zero means
	// DefaultMaxDedupEntries.
	MaxDedupEntries int
}

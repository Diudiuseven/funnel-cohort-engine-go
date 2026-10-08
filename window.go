package funnel

import (
	"fmt"
	"time"
)

// maxPeriodOffset guards against absurd period-distance computations.
const maxPeriodOffset = 100_000

// periodStart truncates t to the beginning of its period in location loc.
// All calendar cuts (hour/day/week/month) are performed in loc, so daylight
// saving and timezone offsets are handled by the time package calendar math.
func periodStart(t time.Time, p Period, loc *time.Location) time.Time {
	t = t.In(loc)
	y, m, d := t.Date()
	switch p {
	case PeriodHour:
		return time.Date(y, m, d, t.Hour(), 0, 0, 0, loc)
	case PeriodDay:
		return time.Date(y, m, d, 0, 0, 0, 0, loc)
	case PeriodWeek:
		// ISO weeks start on Monday.
		dow := int(t.Weekday())
		if dow == 0 {
			dow = 7 // Sunday -> 7
		}
		monday := time.Date(y, m, d, 0, 0, 0, 0, loc).AddDate(0, 0, -(dow - 1))
		return monday
	case PeriodMonth:
		return time.Date(y, m, 1, 0, 0, 0, 0, loc)
	default:
		return time.Date(y, m, d, 0, 0, 0, 0, loc)
	}
}

// periodKey renders the stable calendar key of the period containing t.
func periodKey(t time.Time, p Period, loc *time.Location) string {
	t = t.In(loc)
	switch p {
	case PeriodHour:
		return t.Format("2006-01-02T15")
	case PeriodDay:
		return t.Format("2006-01-02")
	case PeriodWeek:
		y, w := t.ISOWeek()
		return fmt.Sprintf("%04d-W%02d", y, w)
	case PeriodMonth:
		return t.Format("2006-01")
	default:
		return t.Format("2006-01-02")
	}
}

// addPeriods returns the start of the period n periods after the period that
// contains t (n may be negative). Calendar math is used so short/long days
// caused by DST are respected.
func addPeriods(t time.Time, p Period, n int, loc *time.Location) time.Time {
	start := periodStart(t, p, loc)
	switch p {
	case PeriodHour:
		// Add per-hour across the local calendar to stay on local hour walls.
		y, m, d := start.Date()
		h := start.Hour() + n
		total := h
		var nh int
		if total >= 0 {
			nh = total % 24
			d += total / 24
		} else {
			nh = (total%24 + 24) % 24
			d += total/24 - 1
		}
		return time.Date(y, m, d, nh, 0, 0, 0, loc)
	case PeriodDay:
		return start.AddDate(0, 0, n)
	case PeriodWeek:
		return start.AddDate(0, 0, 7*n)
	case PeriodMonth:
		return start.AddDate(0, n, 0)
	default:
		return start.AddDate(0, 0, n)
	}
}

// nextPeriodStart returns the start of the period immediately following the
// period containing t.
func nextPeriodStart(t time.Time, p Period, loc *time.Location) time.Time {
	return addPeriods(t, p, 1, loc)
}

// offsetBetween returns the period distance from the period start `from` to
// the period start `t`. Both arguments must be period starts produced by
// periodStart. ok is false if t lies before from or the distance is
// implausibly large.
func offsetBetween(from, t time.Time, p Period, loc *time.Location) (int, bool) {
	if t.Before(from) {
		return 0, false
	}
	switch p {
	case PeriodHour:
		// Both are local hour boundaries; exact calendar stepping.
		n := 0
		cur := from
		for cur.Before(t) && n < maxPeriodOffset {
			cur = addPeriods(cur, p, 1, loc)
			n++
		}
		if cur.Equal(t) {
			return n, true
		}
	case PeriodDay, PeriodWeek, PeriodMonth:
		n := 0
		cur := from
		for cur.Before(t) && n < maxPeriodOffset {
			cur = addPeriods(cur, p, 1, loc)
			n++
		}
		if cur.Equal(t) {
			return n, true
		}
	}
	return 0, false
}

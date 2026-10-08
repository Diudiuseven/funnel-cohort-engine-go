package engine

import "time"

// daysFromCivil 计算日期距 1970-01-01 的天数（Howard Hinnant 算法），
// 与 DST 无关，保证周期索引的确定性。
func daysFromCivil(y int, m time.Month, d int) int {
	if m <= time.February {
		y--
	}
	era := y / 400
	if y < 0 && y%400 != 0 {
		era--
	}
	yoe := y - era*400
	mp := (int(m) + 9) % 12
	doy := (153*mp+2)/5 + d - 1
	doe := yoe*365 + yoe/4 - yoe/100 + doy
	return era*146097 + doe - 719468
}

// periodIndex 返回 t 在配置时区下所属的周期序号（日或周，周一起始）。
func periodIndex(t time.Time, kind PeriodKind, loc *time.Location) int {
	lt := t.In(loc)
	days := daysFromCivil(lt.Date())
	if kind == WeekPeriod {
		// 1970-01-05 是周一，以其为周起点对齐。
		const mondayEpoch = 4 // daysFromCivil(1970,1,5)
		d := days - mondayEpoch
		w := d / 7
		if d < 0 && d%7 != 0 {
			w--
		}
		return w
	}
	return days
}

// periodStart 返回周期序号 idx 在配置时区下的起始时刻。
func periodStart(idx int, kind PeriodKind, loc *time.Location) time.Time {
	dayIdx := idx
	if kind == WeekPeriod {
		dayIdx = idx*7 + 4
	}
	// 由天数反推 civil 日期：从 1970-01-01 起在 loc 中定位。
	base := time.Date(1970, 1, 1, 0, 0, 0, 0, loc)
	return base.AddDate(0, 0, dayIdx)
}

// periodKey 返回事件时间所属周期（按配置时区对齐）的键。
func periodKey(t time.Time, kind PeriodKind, loc *time.Location) string {
	return periodStart(periodIndex(t, kind, loc), kind, loc).In(loc).Format("2006-01-02")
}

// periodDuration 返回一个周期的标称时长（用于内存淘汰的保守估计）。
func periodDuration(kind PeriodKind) time.Duration {
	if kind == WeekPeriod {
		return 7 * 24 * time.Hour
	}
	return 24 * time.Hour
}

// computeCohortContrib 由用户有序事件列表重算群组归属与活跃周期集合。
// 群组 = 最早事件时间所在周期；迟到更早的事件会改变归属，由上层 diff 修正。
func computeCohortContrib(events []Event, kind PeriodKind, loc *time.Location) (string, map[string]bool) {
	if len(events) == 0 {
		return "", nil
	}
	cohort := periodKey(events[0].Timestamp, kind, loc)
	active := make(map[string]bool, len(events))
	for _, ev := range events {
		active[periodKey(ev.Timestamp, kind, loc)] = true
	}
	return cohort, active
}

// periodIndexOfKey 由周期键（"2006-01-02"，周期首日的本地日期）反解周期序号。
// 空键返回 0，调用方需自行判空。
func periodIndexOfKey(key string, kind PeriodKind, loc *time.Location) int {
	if key == "" {
		return 0
	}
	t, err := time.ParseInLocation("2006-01-02", key, loc)
	if err != nil {
		return 0
	}
	return periodIndex(t, kind, loc)
}

// periodStartOfDayKey 由日桶键反解该日 00:00（配置时区）的时刻。
func periodStartOfDayKey(key string, loc *time.Location) time.Time {
	t, err := time.ParseInLocation("2006-01-02", key, loc)
	if err != nil {
		return time.Time{}
	}
	return t
}

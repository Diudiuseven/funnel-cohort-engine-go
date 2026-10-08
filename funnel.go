package engine

import "time"

// computeFunnelContrib 由用户有序事件列表重算其漏斗贡献。
//
// 语义（确定性，与按时间有序重放一致）：
//   - 贪心、不重叠的尝试：空闲时遇到第 1 步事件开启一次尝试；
//   - 尝试中按序匹配后续步骤事件（同名事件按时间先后消费）；
//   - 事件时间超出 attemptStart+Window 时尝试失败，回到空闲；
//   - 尝试归属的日桶 = 第 1 步事件在配置时区下的日期。
func computeFunnelContrib(events []Event, cfg FunnelConfig, loc *time.Location) map[string]funnelCounts {
	out := map[string]funnelCounts{}
	if len(cfg.Steps) == 0 || len(events) == 0 {
		return out
	}
	stepIdx := -1 // -1 表示空闲
	var attemptStart time.Time
	var bucket string
	record := func(b string, converted bool) {
		c := out[b]
		c.Entered++
		if converted {
			c.Converted++
		}
		out[b] = c
	}
	for _, ev := range events {
		if stepIdx >= 0 && ev.Timestamp.Sub(attemptStart) > cfg.Window {
			// 当前尝试已过期：结算为未转化，回到空闲重新评估本事件。
			record(bucket, false)
			stepIdx = -1
		}
		if stepIdx < 0 {
			if ev.Name == cfg.Steps[0] {
				stepIdx = 0
				attemptStart = ev.Timestamp
				bucket = dayKey(ev.Timestamp, loc)
				if len(cfg.Steps) == 1 {
					record(bucket, true)
					stepIdx = -1
				}
			}
			continue
		}
		if ev.Name == cfg.Steps[stepIdx+1] {
			stepIdx++
			if stepIdx == len(cfg.Steps)-1 {
				record(bucket, true)
				stepIdx = -1
			}
		}
	}
	if stepIdx >= 0 {
		// 末尾未完成的尝试：是否已确定失败取决于水印，这里先计为已进入未转化；
		// 后续事件到达会触发重算并 diff 修正。
		record(bucket, false)
	}
	return out
}

// dayKey 返回 t 在配置时区下的日桶键。
func dayKey(t time.Time, loc *time.Location) string {
	return t.In(loc).Format("2006-01-02")
}

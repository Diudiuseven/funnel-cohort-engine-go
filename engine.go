package engine

import (
	"sync"
	"sync/atomic"
	"time"
)

// Engine 是漏斗/留存计算引擎。写入与查询可并发：
// 查询读取的是原子发布的不可变快照，写操作串行化。
type Engine struct {
	cfg Config
	loc *time.Location

	mu    sync.Mutex // 串行化写入
	state *mutableState

	snap atomic.Pointer[Snapshot] // 查询侧快照
}

// New 创建引擎。
func New(cfg Config) (*Engine, error) {
	loc, err := time.LoadLocation(cfg.Timezone)
	if err != nil {
		return nil, err
	}
	e := &Engine{cfg: cfg, loc: loc, state: newMutableState(cfg, loc)}
	e.publish()
	return e, nil
}

// Ingest 写入一批事件（可乱序、可重复、可迟到）。
func (e *Engine) Ingest(events ...Event) IngestStats {
	e.mu.Lock()
	defer e.mu.Unlock()
	var st IngestStats
	for _, ev := range events {
		e.state.ingestOne(ev, &st)
	}
	e.publish()
	return st
}

// Watermark 当前水印（读自已发布快照，无锁且单调）。
func (e *Engine) Watermark() time.Time { return e.snap.Load().watermark }

// publish 将当前状态发布为不可变快照供查询使用。
func (e *Engine) publish() {
	e.snap.Store(e.state.snapshot())
}

// QueryFunnel 查询漏斗转化结果（自洽快照）。
func (e *Engine) QueryFunnel() FunnelResult {
	return e.snap.Load().funnelResult(e.cfg)
}

// QueryRetention 查询分群留存结果（自洽快照）。
func (e *Engine) QueryRetention() RetentionResult {
	return e.snap.Load().retentionResult(e.cfg)
}

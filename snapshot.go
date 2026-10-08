package funnel

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

// Snapshot is an in-memory copy of all engine state needed to resume from the
// last committed position without double counting or losing events.
type Snapshot struct {
	Watermark    time.Time
	MaxSeen      time.Time
	ManualWM     time.Time
	FunnelCutoff time.Time
	CohortCutoff time.Time
	DedupIDs     []string
	FunnelStates map[string]*funnelUserState
	CohortStates map[string]*cohortUserState
	CommittedFun map[string]*funnelCounters
	CommittedCoh map[string]map[int]int64
	AppliedSeq   uint64
}

// ---- Wire format (versioned JSON) -------------------------------------

const snapshotVersion = 1

type wireSnapshot struct {
	Version         int                       `json:"version"`
	Config          wireConfig                `json:"config"`
	Watermark       time.Time                 `json:"watermark"`
	MaxSeen         time.Time                 `json:"max_seen"`
	ManualWM        time.Time                 `json:"manual_wm"`
	FunnelCutoff    time.Time                 `json:"funnel_cutoff"`
	CohortCutoff    time.Time                 `json:"cohort_cutoff"`
	AppliedSeq      uint64                    `json:"applied_seq"`
	DedupIDs        []string                  `json:"dedup_ids"`
	FunnelUsers     map[string][]Event        `json:"funnel_users"`
	CohortUsers     map[string]wireCohortUser `json:"cohort_users"`
	CommittedFun    map[string][]int64        `json:"committed_funnel"`
	CommittedCohort map[string]map[int]int64  `json:"committed_cohort"`
}

type wireConfig struct {
	Steps         []string `json:"steps"`
	WindowNS      int64    `json:"window_ns"`
	Period        string   `json:"period"`
	AllowedLateNS int64    `json:"allowed_lateness_ns"`
	Timezone      string   `json:"timezone"`
}

type wireCohortUser struct {
	FirstSeen time.Time `json:"first_seen"`
	Active    []string  `json:"active"`
}

// FileStore atomically persists snapshots as JSON at Path.
type FileStore struct {
	Path string
}

func (f *FileStore) load() (*Snapshot, wireConfig, error) {
	file, err := os.Open(f.Path)
	if err != nil {
		return nil, wireConfig{}, err
	}
	defer file.Close()
	return readSnapshotWire(file)
}

// writeSnapshot serializes snap to w using cfg for embedded config metadata.
func writeSnapshot(w io.Writer, snap *Snapshot, cfg Config) error {
	wire := toWire(snap, cfg)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(wire)
}

func writeSnapshotFile(path string, snap *Snapshot, cfg Config) error {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".snapshot-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if err := writeSnapshot(tmp, snap, cfg); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpName, path)
}

func readSnapshotWire(r io.Reader) (*Snapshot, wireConfig, error) {
	var wire wireSnapshot
	if err := json.NewDecoder(r).Decode(&wire); err != nil {
		return nil, wireConfig{}, err
	}
	if wire.Version != snapshotVersion {
		return nil, wireConfig{}, fmt.Errorf("funnel: unsupported snapshot version %d", wire.Version)
	}
	snap := &Snapshot{
		Watermark:    wire.Watermark,
		MaxSeen:      wire.MaxSeen,
		ManualWM:     wire.ManualWM,
		FunnelCutoff: wire.FunnelCutoff,
		CohortCutoff: wire.CohortCutoff,
		AppliedSeq:   wire.AppliedSeq,
		DedupIDs:     wire.DedupIDs,
		FunnelStates: map[string]*funnelUserState{},
		CohortStates: map[string]*cohortUserState{},
		CommittedFun: map[string]*funnelCounters{},
		CommittedCoh: wire.CommittedCohort,
	}
	if snap.CommittedCoh == nil {
		snap.CommittedCoh = map[string]map[int]int64{}
	}
	for uid, evs := range wire.FunnelUsers {
		snap.FunnelStates[uid] = &funnelUserState{events: append([]Event(nil), evs...)}
	}
	for uid, wc := range wire.CohortUsers {
		active := make(map[string]struct{}, len(wc.Active))
		for _, k := range wc.Active {
			active[k] = struct{}{}
		}
		snap.CohortStates[uid] = &cohortUserState{firstSeen: wc.FirstSeen, active: active}
	}
	for date, reached := range wire.CommittedFun {
		snap.CommittedFun[date] = &funnelCounters{reached: append([]int64(nil), reached...)}
	}
	return snap, wire.Config, nil
}

func toWire(snap *Snapshot, cfg Config) wireSnapshot {
	funnelUsers := make(map[string][]Event, len(snap.FunnelStates))
	for uid, st := range snap.FunnelStates {
		funnelUsers[uid] = append([]Event(nil), st.events...)
	}
	cohortUsers := make(map[string]wireCohortUser, len(snap.CohortStates))
	for uid, cs := range snap.CohortStates {
		keys := make([]string, 0, len(cs.active))
		for k := range cs.active {
			keys = append(keys, k)
		}
		cohortUsers[uid] = wireCohortUser{FirstSeen: cs.firstSeen, Active: keys}
	}
	committedFun := make(map[string][]int64, len(snap.CommittedFun))
	for date, c := range snap.CommittedFun {
		committedFun[date] = append([]int64(nil), c.reached...)
	}
	return wireSnapshot{
		Version: snapshotVersion,
		Config: wireConfig{
			Steps:         append([]string(nil), cfg.Funnel.Steps...),
			WindowNS:      int64(cfg.Funnel.Window),
			Period:        string(cfg.Cohort.Period),
			AllowedLateNS: int64(cfg.AllowedLateness),
			Timezone:      cfg.Timezone.String(),
		},
		Watermark:       snap.Watermark,
		MaxSeen:         snap.MaxSeen,
		ManualWM:        snap.ManualWM,
		FunnelCutoff:    snap.FunnelCutoff,
		CohortCutoff:    snap.CohortCutoff,
		AppliedSeq:      snap.AppliedSeq,
		DedupIDs:        append([]string(nil), snap.DedupIDs...),
		FunnelUsers:     funnelUsers,
		CohortUsers:     cohortUsers,
		CommittedFun:    committedFun,
		CommittedCohort: snap.CommittedCoh,
	}
}

// Snapshot returns a point-in-time, deep-copied state image.
func (e *Engine) Snapshot() *Snapshot {
	e.mu.Lock()
	defer e.mu.Unlock()

	dedupIDs := e.dedup.snapshotIDs()
	funnelStates := make(map[string]*funnelUserState, len(e.funnelUsers))
	for uid, st := range e.funnelUsers {
		funnelStates[uid] = &funnelUserState{events: append([]Event(nil), st.events...)}
	}
	cohortStates := make(map[string]*cohortUserState, len(e.cohortUsers))
	for uid, cs := range e.cohortUsers {
		active := make(map[string]struct{}, len(cs.active))
		for k := range cs.active {
			active[k] = struct{}{}
		}
		cohortStates[uid] = &cohortUserState{firstSeen: cs.firstSeen, active: active}
	}
	committedFun := make(map[string]*funnelCounters, len(e.funnel.committed))
	for date, c := range e.funnel.committed {
		committedFun[date] = &funnelCounters{reached: append([]int64(nil), c.reached...)}
	}
	committedCoh := make(map[string]map[int]int64, len(e.cohort.committed))
	for key, row := range e.cohort.committed {
		cp := make(map[int]int64, len(row))
		for off, v := range row {
			cp[off] = v
		}
		committedCoh[key] = cp
	}
	return &Snapshot{
		Watermark:    e.watermark,
		MaxSeen:      e.maxSeen,
		ManualWM:     e.manualWM,
		FunnelCutoff: e.funnelCutoff,
		CohortCutoff: e.cohortCutoff,
		DedupIDs:     dedupIDs,
		FunnelStates: funnelStates,
		CohortStates: cohortStates,
		CommittedFun: committedFun,
		CommittedCoh: committedCoh,
		AppliedSeq:   e.appliedSeq,
	}
}

// SaveSnapshot atomically writes the current state to path.
func (e *Engine) SaveSnapshot(path string) error {
	snap := e.Snapshot()
	return writeSnapshotFile(path, snap, e.cfg)
}

// LoadSnapshot reads and applies a snapshot file, verifying that the supplied
// configuration matches the one embedded in the snapshot.
func LoadSnapshot(path string, cfg Config) (*Engine, error) {
	store := &FileStore{Path: path}
	snap, wireCfg, err := store.load()
	if err != nil {
		return nil, err
	}
	if err := verifyConfig(cfg, wireCfg); err != nil {
		return nil, err
	}
	return NewEngine(cfg, snap)
}

func (e *Engine) restore(snap *Snapshot) error {
	e.watermark = snap.Watermark
	e.maxSeen = snap.MaxSeen
	e.manualWM = snap.ManualWM
	e.funnelCutoff = snap.FunnelCutoff
	e.cohortCutoff = snap.CohortCutoff
	e.appliedSeq = snap.AppliedSeq

	e.dedup = newDedupSet(e.cfg.MaxDedupEntries)
	for _, id := range snap.DedupIDs {
		e.dedup.add(id)
	}

	e.funnelUsers = map[string]*funnelUserState{}
	e.cohortUsers = map[string]*cohortUserState{}
	e.funnelLRU = newLRU(e.cfg.MaxUsers)
	e.cohortLRU = newLRU(e.cfg.MaxUsers)
	for uid, st := range snap.FunnelStates {
		e.funnelUsers[uid] = st
		e.funnelLRU.use(uid)
	}
	for uid, cs := range snap.CohortStates {
		e.cohortUsers[uid] = cs
		e.cohortLRU.use(uid)
	}

	e.funnel = newFunnelAggregate(len(e.cfg.Funnel.Steps))
	e.funnel.committed = snap.CommittedFun
	e.cohort = newCohortAggregate()
	e.cohort.committed = snap.CommittedCoh

	// Rebuild tentative results as "full replay over retained state" minus the
	// already committed counters, so queries resume exactly where they stopped.
	allFun := computeFunnelCounters(e.funnelUsers, e.cfg.Funnel, e.cfg.Timezone)
	for date, c := range allFun {
		if cc, ok := e.funnel.committed[date]; ok {
			for k := range c.reached {
				c.reached[k] -= cc.reached[k]
			}
		}
		if !allZero(c.reached) {
			e.funnel.tentative[date] = c
		}
	}
	allCoh := computeCohortCounters(e.cohortUsers, e.cfg.Cohort.Period, e.cfg.Timezone)
	for key, row := range allCoh {
		if cc, ok := e.cohort.committed[key]; ok {
			for off, v := range cc {
				row[off] -= v
			}
		}
		cleaned := map[int]int64{}
		for off, v := range row {
			if v != 0 {
				cleaned[off] = v
			}
		}
		if len(cleaned) > 0 {
			e.cohort.tentative[key] = cleaned
		}
	}
	return nil
}

func allZero(xs []int64) bool {
	for _, v := range xs {
		if v != 0 {
			return false
		}
	}
	return true
}

func verifyConfig(cfg Config, w wireConfig) error {
	if cfg.Timezone == nil {
		cfg.Timezone = time.UTC
	}
	if cfg.Funnel.Window != time.Duration(w.WindowNS) {
		return ErrConfigMismatch
	}
	if string(cfg.Cohort.Period) != w.Period {
		return ErrConfigMismatch
	}
	if cfg.AllowedLateness != time.Duration(w.AllowedLateNS) {
		return ErrConfigMismatch
	}
	if len(cfg.Funnel.Steps) != len(w.Steps) {
		return ErrConfigMismatch
	}
	for i := range cfg.Funnel.Steps {
		if cfg.Funnel.Steps[i] != w.Steps[i] {
			return ErrConfigMismatch
		}
	}
	if cfg.Timezone.String() != w.Timezone {
		return ErrConfigMismatch
	}
	return nil
}

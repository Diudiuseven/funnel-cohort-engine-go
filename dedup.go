package funnel

// dedupSet is a bounded LRU set of event ids used for duplicate suppression.
// It never grows beyond maxSize: when full, the oldest-accessed id is evicted.
// A re-delivery after eviction can be counted again; choose MaxDedupEntries
// large enough to cover the expected replay/late-delivery horizon.
type dedupSet struct {
	entries map[string]int // id -> index in order
	order   []string       // LRU order, oldest at 0
	maxSize int
}

func newDedupSet(maxSize int) *dedupSet {
	if maxSize <= 0 {
		maxSize = DefaultMaxDedupEntries
	}
	return &dedupSet{
		entries: make(map[string]int, min(maxSize, 1024)),
		order:   make([]string, 0, min(maxSize, 1024)),
		maxSize: maxSize,
	}
}

// add reports whether id was newly inserted. An already-known id returns false
// and is refreshed to most-recently-used.
func (d *dedupSet) add(id string) bool {
	if idx, ok := d.entries[id]; ok {
		d.touch(idx)
		return false
	}
	if len(d.order) >= d.maxSize {
		d.evictOldest()
	}
	d.entries[id] = len(d.order)
	d.order = append(d.order, id)
	return true
}

// contains reports whether id is currently retained (no LRU refresh).
func (d *dedupSet) contains(id string) bool {
	_, ok := d.entries[id]
	return ok
}

// snapshotIDs returns the retained ids in LRU order (oldest first).
func (d *dedupSet) snapshotIDs() []string {
	out := make([]string, len(d.order))
	copy(out, d.order)
	return out
}

func (d *dedupSet) size() int { return len(d.order) }

// touch moves the entry at idx to the MRU end.
func (d *dedupSet) touch(idx int) {
	id := d.order[idx]
	copy(d.order[idx:], d.order[idx+1:])
	d.order[len(d.order)-1] = id
	for i := idx; i < len(d.order); i++ {
		d.entries[d.order[i]] = i
	}
}

func (d *dedupSet) evictOldest() {
	if len(d.order) == 0 {
		return
	}
	old := d.order[0]
	delete(d.entries, old)
	d.order = d.order[1:]
	for i := 0; i < len(d.order); i++ {
		d.entries[d.order[i]] = i
	}
}

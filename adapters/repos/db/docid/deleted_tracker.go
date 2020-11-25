package docid

import "sync"

type InMemDeletedTracker struct {
	sync.RWMutex
	ids map[uint32]struct{}
}

func NewInMemDeletedTracker() *InMemDeletedTracker {
	return &InMemDeletedTracker{
		ids: map[uint32]struct{}{},
	}
}

// Add is a thread-safe way to add a single deleted DocIDs
func (t *InMemDeletedTracker) Add(id uint32) {
	t.Lock()
	defer t.Unlock()

	t.ids[id] = struct{}{}
}

// BulkAdd is a thread safe way to add multiple DocIDs, it looks only once for
// the entire duration of the import
func (t *InMemDeletedTracker) BulkAdd(ids []uint32) {
	t.Lock()
	defer t.Unlock()

	for _, id := range ids {
		t.ids[id] = struct{}{}
	}
}

// Contains is a thread-safe way to check if an ID is contained in the deleted
// tracker, it uses "only" a ReadLock, so concurrent reads are possible.
func (t *InMemDeletedTracker) Contains(id uint32) bool {
	t.RLock()
	defer t.RUnlock()

	_, ok := t.ids[id]
	return ok
}

// Remove is a thread-safe way to remove a single deleted DocIDs (e.g. because
// it has been ultimately cleaned up)
func (t *InMemDeletedTracker) Remove(id uint32) {
	t.Lock()
	defer t.Unlock()

	delete(t.ids, id)
}

// TODO add GetAll() // for cleanup

// TODO add BulkRemove() // for when cleanup finished cleanup

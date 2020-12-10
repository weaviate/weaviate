//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package docid

import "sync"

type InMemDeletedTracker struct {
	sync.RWMutex
	ids map[uint64]struct{}
}

func NewInMemDeletedTracker() *InMemDeletedTracker {
	return &InMemDeletedTracker{
		ids: map[uint64]struct{}{},
	}
}

// Add is a thread-safe way to add a single deleted DocIDs
func (t *InMemDeletedTracker) Add(id uint64) {
	t.Lock()
	defer t.Unlock()

	t.ids[id] = struct{}{}
}

// BulkAdd is a thread safe way to add multiple DocIDs, it looks only once for
// the entire duration of the import
func (t *InMemDeletedTracker) BulkAdd(ids []uint64) {
	t.Lock()
	defer t.Unlock()

	for _, id := range ids {
		t.ids[id] = struct{}{}
	}
}

// Contains is a thread-safe way to check if an ID is contained in the deleted
// tracker, it uses "only" a ReadLock, so concurrent reads are possible.
func (t *InMemDeletedTracker) Contains(id uint64) bool {
	t.RLock()
	defer t.RUnlock()

	_, ok := t.ids[id]
	return ok
}

// Remove is a thread-safe way to remove a single deleted DocIDs (e.g. because
// it has been ultimately cleaned up)
func (t *InMemDeletedTracker) Remove(id uint64) {
	t.Lock()
	defer t.Unlock()

	delete(t.ids, id)
}

// GetAll is a thread-safe way to retrieve all entries, it uses a ReadLock for
// concurrent reading
func (t *InMemDeletedTracker) GetAll() []uint64 {
	t.RLock()
	defer t.RUnlock()

	out := make([]uint64, len(t.ids))
	i := 0
	for id := range t.ids {
		out[i] = id
		i++
	}

	return out
}

// BulkRemove is a thread-safe way to remove multiple ids, it locks only once,
// for the entire duration of the deletion
func (t *InMemDeletedTracker) BulkRemove(ids []uint64) {
	t.Lock()
	defer t.Unlock()

	for _, id := range ids {
		delete(t.ids, id)
	}
}

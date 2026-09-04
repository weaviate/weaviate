//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package db

import (
	"errors"
	"sync"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/entities/modelsext"
)

var errVectorIndexSlotExists = errors.New("vector index slot already exists")

// vectorIndexSlots owns a shard's vector indexes and their queues, one slot
// per logical vector. The legacy vector is the slot named "". The zero value
// is ready to use, like the maps it replaces.
type vectorIndexSlots struct {
	mu    sync.RWMutex
	slots map[string]*vectorIndexSlot
}

// vectorIndexSlot is one logical vector's index and queue. They are
// published and removed together. users counts the leases held on the
// pair; closing stops new leases once a removal has started.
type vectorIndexSlot struct {
	name    string
	index   VectorIndex
	queue   *VectorIndexQueue
	users   *common.SharedGauge
	closing bool // guarded by vectorIndexSlots.mu
}

// resolve maps the legacy vector's two names onto its slot key: "" always,
// and modelsext.DefaultNamedVectorName only while a legacy slot exists.
// Callers hold mu.
func (v *vectorIndexSlots) resolve(name string) string {
	if name == modelsext.DefaultNamedVectorName {
		if _, ok := v.slots[""]; ok {
			return ""
		}
	}
	return name
}

// Publish installs a slot under name. A taken name is an error, which is
// what keeps two concurrent creates of the same vector from orphaning an
// index and queue.
func (v *vectorIndexSlots) Publish(name string, index VectorIndex, queue *VectorIndexQueue) error {
	v.mu.Lock()
	defer v.mu.Unlock()

	if v.slots == nil {
		v.slots = map[string]*vectorIndexSlot{}
	}
	if _, exists := v.slots[name]; exists {
		return errVectorIndexSlotExists
	}
	v.slots[name] = &vectorIndexSlot{name: name, index: index, queue: queue, users: common.NewSharedGauge()}
	return nil
}

// Acquire hands out a lease on a slot: the caller may use the index and
// queue until it calls release, and a removal waits for it. ok is false
// when no slot has that name or its removal has already started.
func (v *vectorIndexSlots) Acquire(name string) (slot *vectorIndexSlot, release func(), ok bool) {
	v.mu.RLock()
	defer v.mu.RUnlock()

	slot, ok = v.slots[v.resolve(name)]
	if !ok || slot.closing {
		return nil, nil, false
	}
	slot.users.Incr()
	return slot, func() { slot.users.Decr() }, true
}

// get looks a slot up without a lease. Only the shard's own accessors use
// it, for the callers that still hold no lease.
func (v *vectorIndexSlots) get(name string) (*vectorIndexSlot, bool) {
	v.mu.RLock()
	defer v.mu.RUnlock()

	slot, ok := v.slots[v.resolve(name)]
	return slot, ok
}

// ForEach calls f on every slot under the read lock, named slots first and
// the legacy slot last, stopping at the first error. Creation and removal
// wait for the walk, as they did with the maps.
func (v *vectorIndexSlots) ForEach(f func(name string, index VectorIndex, queue *VectorIndexQueue) error) error {
	v.mu.RLock()
	defer v.mu.RUnlock()

	for name, slot := range v.slots {
		if name == "" {
			continue
		}
		err := f(name, slot.index, slot.queue)
		if err != nil {
			return err
		}
	}
	if slot, ok := v.slots[""]; ok {
		return f("", slot.index, slot.queue)
	}
	return nil
}

// Replace swaps the index of a published slot, for the debug reindex
// endpoint. The queue stays; the caller re-points it. Reports whether the
// slot existed.
func (v *vectorIndexSlots) Replace(name string, index VectorIndex) bool {
	v.mu.Lock()
	defer v.mu.Unlock()

	slot, ok := v.slots[v.resolve(name)]
	if !ok {
		return false
	}
	slot.index = index
	return true
}

// remove takes a slot out of the map and hands its index and queue to the
// caller for teardown. No alias: a drop names the vector exactly. The
// draining Remove replaces this once leases exist.
func (v *vectorIndexSlots) remove(name string) (VectorIndex, *VectorIndexQueue, bool) {
	v.mu.Lock()
	defer v.mu.Unlock()

	slot, ok := v.slots[name]
	if !ok {
		return nil, nil, false
	}
	delete(v.slots, name)
	return slot.index, slot.queue, true
}

// Len is the number of published slots.
func (v *vectorIndexSlots) Len() int {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return len(v.slots)
}

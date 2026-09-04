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
// published and removed together.
type vectorIndexSlot struct {
	name  string
	index VectorIndex
	queue *VectorIndexQueue
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
	v.slots[name] = &vectorIndexSlot{name: name, index: index, queue: queue}
	return nil
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

// Len is the number of published slots.
func (v *vectorIndexSlots) Len() int {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return len(v.slots)
}

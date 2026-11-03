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

package schema

import (
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storagestate"
	"github.com/weaviate/weaviate/usecases/sharding"
)

type shardKey struct {
	class string
	shard string
}

// NodeOverlay is this node's shard state outside RAFT: in memory, never
// snapshotted. Reads sit on the write path, so they take no lock and allocate
// nothing. Few shards are flagged, so writes copy the set.
type NodeOverlay struct {
	mu       sync.Mutex
	readOnly atomic.Pointer[map[shardKey]struct{}]
	pressure atomic.Bool
	phases   sync.Map // shardKey to *ShardPhase

	readOnlyChanged func(class, shard string)
}

// OnReadOnlyChange sets the callback SetReadOnly calls after a user's change.
// Set it before the node applies any RAFT entry: SetReadOnly reads it unlocked.
// The callback runs on the RAFT apply path, so it must not block.
func (o *NodeOverlay) OnReadOnlyChange(changed func(class, shard string)) {
	o.readOnlyChanged = changed
}

func (o *NodeOverlay) ReadOnly(class, shard string) bool {
	if o == nil {
		return false
	}
	set := o.readOnly.Load()
	if set == nil {
		return false
	}
	_, ok := (*set)[shardKey{class, shard}]
	return ok
}

func (o *NodeOverlay) SetReadOnly(class, shard string, readOnly bool) {
	o.update(func(set map[shardKey]struct{}) {
		if readOnly {
			set[shardKey{class, shard}] = struct{}{}
		} else {
			delete(set, shardKey{class, shard})
		}
	})
	if o.readOnlyChanged != nil {
		o.readOnlyChanged(class, shard)
	}
}

// ResourcePressure reports whether this node refuses writes for disk or memory.
func (o *NodeOverlay) ResourcePressure() bool {
	return o != nil && o.pressure.Load()
}

func (o *NodeOverlay) SetResourcePressure(pressure bool) {
	o.pressure.Store(pressure)
}

func (o *NodeOverlay) clearShards(class string, shards []string) {
	o.update(func(set map[shardKey]struct{}) {
		for _, shard := range shards {
			delete(set, shardKey{class, shard})
		}
	})
}

func (o *NodeOverlay) clearClass(class string) {
	o.update(func(set map[shardKey]struct{}) {
		maps.DeleteFunc(set, func(key shardKey, _ struct{}) bool { return key.class == class })
	})
}

// reset drops every user read-only flag, as a restart does.
func (o *NodeOverlay) reset() {
	var dropped []shardKey
	o.update(func(set map[shardKey]struct{}) {
		dropped = slices.Collect(maps.Keys(set))
		clear(set)
	})
	if o.readOnlyChanged == nil {
		return
	}
	for _, key := range dropped {
		o.readOnlyChanged(key.class, key.shard)
	}
}

func (o *NodeOverlay) update(change func(map[shardKey]struct{})) {
	o.mu.Lock()
	defer o.mu.Unlock()

	next := make(map[shardKey]struct{})
	if set := o.readOnly.Load(); set != nil {
		maps.Copy(next, *set)
	}
	change(next)
	o.readOnly.Store(&next)
}

// NewShardPhase registers a shard object's phase. Each object gets its own, so
// one that was replaced cannot touch its successor.
func (o *NodeOverlay) NewShardPhase(class, shard string, status storagestate.Status) *ShardPhase {
	phase := NewShardPhase(status)
	if o != nil {
		o.phases.Store(shardKey{class, shard}, phase)
	}
	return phase
}

// DropShardPhase forgets a discarded shard object unless a newer one took over.
func (o *NodeOverlay) DropShardPhase(class, shard string, phase *ShardPhase) {
	if o != nil {
		o.phases.CompareAndDelete(shardKey{class, shard}, phase)
	}
}

// PhaseOf returns the phase of the shard's current object, or nil.
func (o *NodeOverlay) PhaseOf(class, shard string) *ShardPhase {
	if o == nil {
		return nil
	}
	phase, ok := o.phases.Load(shardKey{class, shard})
	if !ok {
		return nil
	}
	return phase.(*ShardPhase)
}

// ShardPhase is a shard object's place in its lifecycle. The shard holds a
// handle, so reading it needs no lookup.
type ShardPhase struct {
	status       atomic.Pointer[storagestate.Status]
	reindexing   atomic.Bool
	transferring atomic.Bool
}

func NewShardPhase(status storagestate.Status) *ShardPhase {
	phase := &ShardPhase{}
	phase.Set(status)
	return phase
}

func (p *ShardPhase) Set(status storagestate.Status) {
	p.status.Store(&status)
}

func (p *ShardPhase) Status() storagestate.Status {
	return *p.status.Load()
}

// Reindexing lives on the object: an unfinished reindex ends with the store it
// held, so the next object starts clear.
func (p *ShardPhase) Reindexing() bool {
	return p.reindexing.Load()
}

func (p *ShardPhase) SetReindexing(reindexing bool) {
	p.reindexing.Store(reindexing)
}

// Transferring holds the store for a freeze's upload, which would leave out any
// later write.
func (p *ShardPhase) Transferring() bool {
	return p.transferring.Load()
}

func (p *ShardPhase) SetTransferring(transferring bool) {
	p.transferring.Store(transferring)
}

func (s *SchemaManager) NodeOverlay() *NodeOverlay {
	return s.overlay
}

// applyShardStatus runs on every node, owner or not.
func (s *SchemaManager) applyShardStatus(class, shard, status string) error {
	target, err := storagestate.ValidateStatus(strings.ToUpper(status))
	if err != nil {
		return fmt.Errorf("%s: %w", status, err)
	}
	switch target {
	case storagestate.StatusReadOnly:
		s.overlay.SetReadOnly(class, shard, true)
	case storagestate.StatusReady:
		s.overlay.SetReadOnly(class, shard, false)
	default:
	}
	return nil
}

func (s *schema) requireShard(class, shard string) error {
	s.mu.RLock()
	meta := s.classes[class]
	s.mu.RUnlock()
	if meta == nil {
		return fmt.Errorf("%w: %s", ErrClassNotFound, class)
	}
	return meta.RLockGuard(func(_ *models.Class, ss *sharding.State) error {
		if _, ok := ss.Physical[shard]; !ok {
			return fmt.Errorf("%w: %s", ErrShardNotFound, shard)
		}
		return nil
	})
}

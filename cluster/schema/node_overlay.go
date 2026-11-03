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

// NodeOverlay is node-local shard state, never snapshotted. Reads are on the write path: no locks, no allocs.
type NodeOverlay struct {
	readOnly sync.Map // shardKey to struct{}
	pressure atomic.Bool
	phases   sync.Map // shardKey to *ShardPhase

	readOnlyChanged func(class, shard string)
}

// OnReadOnlyChange must be set before the first RAFT apply (read unlocked); the callback must not block.
func (o *NodeOverlay) OnReadOnlyChange(changed func(class, shard string)) {
	o.readOnlyChanged = changed
}

func (o *NodeOverlay) ReadOnly(class, shard string) bool {
	if o == nil {
		return false
	}
	_, ok := o.readOnly.Load(shardKey{class, shard})
	return ok
}

func (o *NodeOverlay) SetReadOnly(class, shard string, readOnly bool) {
	if readOnly {
		o.readOnly.Store(shardKey{class, shard}, struct{}{})
	} else {
		o.readOnly.Delete(shardKey{class, shard})
	}
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
	for _, shard := range shards {
		o.readOnly.Delete(shardKey{class, shard})
	}
}

func (o *NodeOverlay) clearClass(class string) {
	o.readOnly.Range(func(key, _ any) bool {
		if key.(shardKey).class == class {
			o.readOnly.Delete(key)
		}
		return true
	})
}

// reset drops every user read-only flag, as a restart does.
func (o *NodeOverlay) reset() {
	var dropped []shardKey
	o.readOnly.Range(func(key, _ any) bool {
		dropped = append(dropped, key.(shardKey))
		o.readOnly.Delete(key)
		return true
	})
	if o.readOnlyChanged == nil {
		return
	}
	for _, key := range dropped {
		o.readOnlyChanged(key.class, key.shard)
	}
}

// NewShardPhase gives each shard object its own phase, so a replaced one cannot touch its successor.
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

// ShardPhase is a shard object's lifecycle phase, held by the shard so reads need no lookup.
type ShardPhase struct {
	status     atomic.Pointer[storagestate.Status]
	reindexing atomic.Bool
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

// Reindexing is per object: an unfinished reindex ends with its store, so the next object starts clear.
func (p *ShardPhase) Reindexing() bool {
	return p.reindexing.Load()
}

func (p *ShardPhase) SetReindexing(reindexing bool) {
	p.reindexing.Store(reindexing)
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

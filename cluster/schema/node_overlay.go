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

	"github.com/weaviate/weaviate/entities/storagestate"
)

type shardKey struct {
	class string
	shard string
}

// NodeOverlay is node-local shard state, never snapshotted. Reads are on the write path: no locks, no allocs.
type NodeOverlay struct {
	readOnly sync.Map // shardKey to struct{}
	pressure atomic.Bool

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
	key := shardKey{class, shard}
	changed := false
	if readOnly {
		_, held := o.readOnly.LoadOrStore(key, struct{}{})
		changed = !held
	} else {
		_, changed = o.readOnly.LoadAndDelete(key)
	}
	if changed && o.readOnlyChanged != nil {
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

// RefuseWrite is why this node refuses any write to a shard, nil when it takes one; phase is nil for an
// unloaded shard. Client writes also check RefuseClientWrite.
func (o *NodeOverlay) RefuseWrite(phase *ShardPhase) error {
	if phase != nil && phase.Has(PhaseMigrationHeld) {
		return storagestate.ErrStatusReadOnlyWithReason(storagestate.ReasonMirrorUnarmed)
	}
	if o.ResourcePressure() {
		return storagestate.ErrStatusReadOnlyWithReason(storagestate.ReasonResourcePressure)
	}
	if phase != nil && phase.Has(PhaseReindexing) {
		return storagestate.ErrStatusReadOnly
	}
	return nil
}

// RefuseClientWrite refuses a client write under a user's read-only. Migrations and repair skip it, so
// replicas cannot diverge over it.
func (o *NodeOverlay) RefuseClientWrite(class, shard string) error {
	if o.ReadOnly(class, shard) {
		return storagestate.ErrStatusReadOnlyWithReason(storagestate.ReasonUserReadOnly)
	}
	return nil
}

// StorePaused reports whether a shard's store pauses its background I/O, and whether a reindex's temp
// buckets pause with it: they stay writable while the reindex alone holds the store.
func (o *NodeOverlay) StorePaused(phase *ShardPhase) (store, temp bool) {
	pressure := o.ResourcePressure()
	return pressure || phase.Has(PhaseReindexing|PhaseMigrationHeld),
		pressure || phase.Has(PhaseReindexSwapping|PhaseMigrationHeld)
}

// Status is the status this node reports for a shard whose own status folds in its phase.
func (o *NodeOverlay) Status(class, shard string, status storagestate.Status) storagestate.Status {
	switch status {
	case storagestate.StatusReady, storagestate.StatusIndexing, storagestate.StatusLazyLoading:
		if o.ReadOnly(class, shard) || o.ResourcePressure() {
			return storagestate.StatusReadOnly
		}
	default:
	}
	return status
}

// PhaseCondition is a node-local reason a shard object refuses writes. Each has its own writer, so it
// is a bit of its own: clearing one never clears another.
type PhaseCondition uint32

const (
	// PhaseReindexing holds the store for a reindex, which still writes its temp buckets.
	PhaseReindexing PhaseCondition = 1 << iota
	// PhaseReindexSwapping holds the temp buckets too, while the reindex swaps them in.
	PhaseReindexSwapping
	// PhaseMigrationHeld holds the shard while a reindex migration's double-write mirror is not armed.
	PhaseMigrationHeld
)

// ShardPhase is a shard object's lifecycle phase, held by the shard so reads need no lookup.
type ShardPhase struct {
	status     atomic.Pointer[storagestate.Status]
	conditions atomic.Uint32
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

// Has is per object: a condition ends with its store, so the next object starts clear.
func (p *ShardPhase) Has(c PhaseCondition) bool {
	return p.conditions.Load()&uint32(c) != 0
}

func (p *ShardPhase) SetCondition(c PhaseCondition, on bool) {
	if on {
		p.conditions.Or(uint32(c))
	} else {
		p.conditions.And(^uint32(c))
	}
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

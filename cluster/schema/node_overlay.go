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
	"runtime"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/sirupsen/logrus"

	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/storagestate"
)

// Condition is a node-local reason a shard refuses writes. Each has one writer, so each is a bit of its
// own: clearing one never clears another.
type Condition uint32

const (
	// ConditionUserReadOnly is a user's READONLY, per (class, shard). It refuses client writes only, so
	// migrations and repair cannot make replicas diverge over it.
	ConditionUserReadOnly Condition = 1 << iota
	// ConditionResourcePressure is the disk and memory scanner's, for the whole node.
	ConditionResourcePressure
	// ConditionReindexing holds a shard object's store for a reindex, which still writes its temp buckets.
	ConditionReindexing
	// ConditionReindexSwapping holds the temp buckets too, while the reindex swaps them in.
	ConditionReindexSwapping
	// ConditionMigrationHeld holds a shard object while a reindex migration's double-write mirror is not armed.
	ConditionMigrationHeld
)

// conditionsReadOnly report a shard READONLY; a swap is part of a reindex.
const conditionsReadOnly = ConditionUserReadOnly | ConditionResourcePressure | ConditionReindexing | ConditionMigrationHeld

// refusal is why a write is refused under c, nil when it is taken.
func (c Condition) refusal() error {
	switch {
	case c&ConditionUserReadOnly != 0:
		return storagestate.ErrStatusReadOnlyWithReason(storagestate.ReasonUserReadOnly)
	case c&ConditionMigrationHeld != 0:
		return storagestate.ErrStatusReadOnlyWithReason(storagestate.ReasonMirrorUnarmed)
	case c&ConditionResourcePressure != 0:
		return storagestate.ErrStatusReadOnlyWithReason(storagestate.ReasonResourcePressure)
	case c&ConditionReindexing != 0:
		return storagestate.ErrStatusReadOnly
	default:
		return nil
	}
}

// storePaused is whether a store pauses its background I/O, and whether a reindex's temp buckets pause
// with it: they stay writable while the reindex alone holds the store.
func (c Condition) storePaused() (store, temp bool) {
	held := c & (ConditionResourcePressure | ConditionMigrationHeld)
	return held|c&ConditionReindexing != 0, held|c&ConditionReindexSwapping != 0
}

// Phase is a shard object's lifecycle, reported by the shard. The zero value is PhaseLoading.
type Phase uint32

const (
	PhaseLoading Phase = iota
	PhaseReady
	PhaseShutdown
)

func (p Phase) Status() storagestate.Status {
	switch p {
	case PhaseReady:
		return storagestate.StatusReady
	case PhaseShutdown:
		return storagestate.StatusShutdown
	default:
		return storagestate.StatusLoading
	}
}

// ShardStore is a loaded shard object's storage, which the overlay drives. Calls for one shard object
// never overlap.
type ShardStore interface {
	// PauseStore pauses or resumes background I/O; temp also covers a reindex's temp buckets.
	PauseStore(store, temp bool) error
	// StatusChanged follows the status this node reports for the shard object, after each pause.
	StatusChanged(status storagestate.Status)
}

type shardKey struct {
	class string
	shard string
}

// NodeOverlay owns the node-local part of shard status and drives storage from it. It holds the
// conditions of each (class, shard), of the whole node, and of each loaded shard object through its
// ShardHandle. Nothing in it is replicated or snapshotted. Writes are refused without a lock or an
// allocation, and a nil overlay has no conditions and drives nothing.
type NodeOverlay struct {
	conditions sync.Map      // shardKey to *atomic.Uint32; outlives shard objects, so a lazy unload keeps it
	attached   sync.Map      // shardKey to the *ShardHandle the overlay drives
	node       atomic.Uint32 // Condition
	log        logrus.FieldLogger
}

func NewNodeOverlay(log logrus.FieldLogger) *NodeOverlay {
	return &NodeOverlay{log: log}
}

// Attach links a new shard object, LOADING and with no conditions of its own. The overlay drives its store
// from PhaseReady until PhaseShutdown, or until a newer object of the shard is ready.
func (o *NodeOverlay) Attach(class, shard string, store ShardStore) *ShardHandle {
	return &ShardHandle{overlay: o, key: shardKey{class, shard}, store: store}
}

// Shard is the conditions of (class, shard), whether a shard object is loaded or not.
func (o *NodeOverlay) Shard(class, shard string) Condition {
	if o == nil {
		return 0
	}
	bits, ok := o.conditions.Load(shardKey{class, shard})
	if !ok {
		return 0
	}
	return Condition(bits.(*atomic.Uint32).Load())
}

// Set sets a condition of (class, shard). It may run in the RAFT apply, so the store follows off it.
func (o *NodeOverlay) Set(class, shard string, c Condition, on bool) {
	key := shardKey{class, shard}
	loaded, ok := o.conditions.Load(key)
	if !ok {
		if !on {
			return
		}
		loaded, _ = o.conditions.LoadOrStore(key, new(atomic.Uint32))
	}
	bits := loaded.(*atomic.Uint32)
	var prev Condition
	if on {
		prev = Condition(bits.Or(uint32(c)))
	} else {
		prev = Condition(bits.And(^uint32(c)))
	}
	changed := prev&c != 0
	if on {
		changed = prev&c != c
	}
	if changed {
		o.driveLater(key)
	}
}

// Node is the conditions of the whole node.
func (o *NodeOverlay) Node() Condition {
	if o == nil {
		return 0
	}
	return Condition(o.node.Load())
}

// SetNode sets a condition of the whole node and drives every ready shard object's store. It returns once
// all are driven.
func (o *NodeOverlay) SetNode(c Condition, on bool) {
	if on {
		o.node.Or(uint32(c))
	} else {
		o.node.And(^uint32(c))
	}
	eg := enterrors.NewErrorGroupWrapper(o.logger())
	eg.SetLimit(runtime.GOMAXPROCS(0))
	o.attached.Range(func(_, h any) bool {
		eg.Go(func() error {
			_ = h.(*ShardHandle).drive() // only a closed store fails, and it takes no writes
			return nil
		})
		return true
	})
	_ = eg.Wait()
}

// RefuseWrite is why a write to a shard with no loaded object is refused, nil when it is taken.
func (o *NodeOverlay) RefuseWrite() error {
	return o.Node().refusal()
}

// RefuseClientWrite is why a client write to (class, shard) is refused before any shard is looked up.
func (o *NodeOverlay) RefuseClientWrite(class, shard string) error {
	return o.Shard(class, shard).refusal()
}

// Status is the status of a shard with no loaded object: READONLY under any condition, else LAZY_LOADING.
func (o *NodeOverlay) Status(class, shard string) storagestate.Status {
	if (o.Shard(class, shard)|o.Node())&conditionsReadOnly != 0 {
		return storagestate.StatusReadOnly
	}
	return storagestate.StatusLazyLoading
}

// driveLater drives the attached store off the caller, which may be the RAFT apply.
func (o *NodeOverlay) driveLater(key shardKey) {
	h, ok := o.attached.Load(key)
	if !ok {
		return
	}
	enterrors.GoWrapper(func() { _ = h.(*ShardHandle).drive() }, o.logger())
}

func (o *NodeOverlay) logger() logrus.FieldLogger {
	if o.log == nil {
		return logrus.StandardLogger()
	}
	return o.log
}

func (o *NodeOverlay) clearShards(class string, shards []string) {
	for _, shard := range shards {
		o.conditions.Delete(shardKey{class, shard})
	}
}

func (o *NodeOverlay) clearClass(class string) {
	o.conditions.Range(func(key, _ any) bool {
		if key.(shardKey).class == class {
			o.conditions.Delete(key)
		}
		return true
	})
}

// reset drops every (class, shard) condition, as a restart does.
func (o *NodeOverlay) reset() {
	o.conditions.Range(func(key, bits any) bool {
		o.conditions.Delete(key)
		if bits.(*atomic.Uint32).Load() != 0 {
			o.driveLater(key.(shardKey))
		}
		return true
	})
}

// ShardHandle is a shard object's link to the overlay: the object reports its phase and its own
// conditions, and the overlay drives its store. A handle shut down or replaced is no longer driven by the
// overlay.
type ShardHandle struct {
	overlay    *NodeOverlay
	key        shardKey
	store      ShardStore
	phase      atomic.Uint32 // Phase
	conditions atomic.Uint32 // Condition of this object

	driving sync.Mutex
}

func (h *ShardHandle) Phase() Phase {
	return Phase(h.phase.Load())
}

// ReportPhase is the shard object's report. PhaseReady drives its store and hands it to the overlay,
// replacing an older object of the shard; PhaseShutdown waits for a drive in progress and ends them.
func (h *ShardHandle) ReportPhase(p Phase) error {
	h.driving.Lock()
	h.phase.Store(uint32(p))
	h.driving.Unlock()
	if h.overlay == nil {
		return h.drive()
	}
	switch p {
	case PhaseReady:
		// attached first: a node condition set after this drive's read then drives the store again
		h.overlay.attached.Store(h.key, h)
		return h.drive()
	case PhaseShutdown:
		h.overlay.attached.CompareAndDelete(h.key, h)
	default:
	}
	return nil
}

// Conditions is this shard object's own conditions.
func (h *ShardHandle) Conditions() Condition {
	return Condition(h.conditions.Load())
}

// Set sets a condition of this shard object and drives its store. A store that is not ready is not driven,
// so Set then returns nil.
func (h *ShardHandle) Set(c Condition, on bool) error {
	if on {
		h.conditions.Or(uint32(c))
	} else {
		h.conditions.And(^uint32(c))
	}
	return h.drive()
}

// Sync drives the store again, for a store that gained buckets, which start unpaused.
func (h *ShardHandle) Sync() error {
	return h.drive()
}

// RefuseWrite is why this shard object refuses any write, nil when it takes one. A client write also
// needs NodeOverlay.RefuseClientWrite.
func (h *ShardHandle) RefuseWrite() error {
	return h.objectConditions().refusal()
}

// RefuseConfigUpdate is why a vector index config update is refused: as RefuseWrite, except that an
// inverted reindex holds no vector index.
func (h *ShardHandle) RefuseConfigUpdate() error {
	return (h.objectConditions() &^ (ConditionReindexing | ConditionReindexSwapping)).refusal()
}

// Status is the status this node reports for the shard object: its phase while it starts or stops,
// READONLY under any condition, else READY. The shard reports INDEXING over READY itself.
func (h *ShardHandle) Status() storagestate.Status {
	if p := h.Phase(); p != PhaseReady {
		return p.Status()
	}
	if (h.overlay.Shard(h.key.class, h.key.shard)|h.objectConditions())&conditionsReadOnly != 0 {
		return storagestate.StatusReadOnly
	}
	return storagestate.StatusReady
}

func (h *ShardHandle) objectConditions() Condition {
	return h.overlay.Node() | h.Conditions()
}

// drive brings the store in line with the conditions it reads now, so the last drive wins.
func (h *ShardHandle) drive() error {
	h.driving.Lock()
	defer h.driving.Unlock()
	if h.Phase() != PhaseReady {
		return nil
	}
	if err := h.store.PauseStore(h.objectConditions().storePaused()); err != nil {
		return err
	}
	h.store.StatusChanged(h.Status())
	return nil
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
		s.overlay.Set(class, shard, ConditionUserReadOnly, true)
	case storagestate.StatusReady:
		s.overlay.Set(class, shard, ConditionUserReadOnly, false)
	default:
	}
	return nil
}

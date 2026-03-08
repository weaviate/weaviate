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

package distributedtask

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	enterrors "github.com/weaviate/weaviate/entities/errors"
)

const ShardNoopProviderNamespace = "shard-noop"

// ShardLister provides local shard names for a collection, allowing the
// [ShardNoopProvider] to determine sub-unit ownership based on real shard
// topology without importing the db package.
type ShardLister interface {
	GetLocalShardNames(collection string) ([]string, error)
}

// ShardNoopProviderPayload is the JSON payload for tasks created with the
// [ShardNoopProvider]. When Collection is set and a [ShardLister] is available,
// the provider uses real shard placement for sub-unit ownership instead of the
// synthetic NodeID-based assignment.
//
// SubUnitToShard maps sub-unit IDs to shard names, allowing multiple sub-units
// per shard (one per replica). SubUnitToNode maps sub-unit IDs to the node that
// should process them, providing deterministic ownership when RF > 1 (where
// multiple nodes have the same shard locally). Both maps are required when
// Collection is set.
type ShardNoopProviderPayload struct {
	FailSubUnitID      string            `json:"failSubUnitId,omitempty"`
	Collection         string            `json:"collection,omitempty"`
	SubUnitToShard     map[string]string `json:"subUnitToShard,omitempty"`
	SubUnitToNode      map[string]string `json:"subUnitToNode,omitempty"`
	SlowSubUnitID      string            `json:"slowSubUnitId,omitempty"`
	SlowSubUnitDelayMs int               `json:"slowSubUnitDelayMs,omitempty"`
	ProcessingDelayMs  int               `json:"processingDelayMs,omitempty"`
}

// ShardNoopProvider is a test-only [SubUnitAwareProvider] used by acceptance tests to exercise
// the sub-unit lifecycle end-to-end. It is registered in configure_api.go behind the
// SHARD_NOOP_PROVIDER_ENABLED env var and exposed via a debug HTTP endpoint
// on port 6060.
//
// On StartTask, it spawns a goroutine that iterates sub-units sequentially, reports 50%
// progress, then completes each one. Set FailSubUnitID in the payload to make one sub-unit
// fail instead, which triggers the task-level fail-fast behavior.
//
// When a Collection is specified in the payload and a [ShardLister] is provided, the provider
// only claims sub-units whose IDs match local shard names. This validates that sub-unit
// ownership aligns with actual shard placement.
type ShardNoopProvider struct {
	mu       sync.Mutex
	recorder TaskCompletionRecorder
	tasks    []TaskDescriptor
	nodeID   string
	logger   logrus.FieldLogger

	shardLister ShardLister

	completedTasks   map[TaskDescriptor]bool
	completedTasksMu sync.Mutex

	finalizedSubUnits   map[TaskDescriptor][]string
	finalizedSubUnitsMu sync.Mutex
}

// NewShardNoopProvider creates a new [ShardNoopProvider]. Pass nil for shardLister
// when real shard topology is not needed (e.g. unit tests with synthetic sub-unit IDs).
func NewShardNoopProvider(nodeID string, logger logrus.FieldLogger, shardLister ShardLister) *ShardNoopProvider {
	return &ShardNoopProvider{
		nodeID:            nodeID,
		logger:            logger,
		shardLister:       shardLister,
		completedTasks:    make(map[TaskDescriptor]bool),
		finalizedSubUnits: make(map[TaskDescriptor][]string),
	}
}

func (p *ShardNoopProvider) SetCompletionRecorder(recorder TaskCompletionRecorder) {
	p.recorder = recorder
}

func (p *ShardNoopProvider) GetLocalTasks() []TaskDescriptor {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]TaskDescriptor{}, p.tasks...)
}

func (p *ShardNoopProvider) CleanupTask(desc TaskDescriptor) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	filtered := p.tasks[:0]
	for _, d := range p.tasks {
		if d != desc {
			filtered = append(filtered, d)
		}
	}
	p.tasks = filtered
	return nil
}

func (p *ShardNoopProvider) StartTask(task *Task) (TaskHandle, error) {
	p.mu.Lock()
	p.tasks = append(p.tasks, task.TaskDescriptor)
	p.mu.Unlock()

	handle := &shardNoopTaskHandle{stopCh: make(chan struct{}), doneCh: make(chan struct{})}

	if task.HasSubUnits() {
		enterrors.GoWrapper(func() {
			defer close(handle.doneCh)
			p.processSubUnits(task, handle)
		}, p.logger)
	} else {
		enterrors.GoWrapper(func() {
			defer close(handle.doneCh)
			p.processLegacyTask(task, handle)
		}, p.logger)
	}

	return handle, nil
}

func (p *ShardNoopProvider) OnSubUnitsCompleted(task *Task, localSubUnitIDs []string) {
	p.finalizedSubUnitsMu.Lock()
	p.finalizedSubUnits[task.TaskDescriptor] = append(
		p.finalizedSubUnits[task.TaskDescriptor], localSubUnitIDs...,
	)
	p.finalizedSubUnitsMu.Unlock()

	// Write marker files for each finalized sub-unit
	dir := filepath.Join("/tmp/dtm-finalize", task.ID)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		p.logger.WithError(err).Error("shard-noop provider: failed to create marker dir")
		return
	}
	for _, suID := range localSubUnitIDs {
		path := filepath.Join(dir, suID)
		if err := os.WriteFile(path, []byte(fmt.Sprintf("finalized by %s", p.nodeID)), 0o644); err != nil {
			p.logger.WithError(err).Error("shard-noop provider: failed to write marker file")
		}
	}

	p.logger.WithField("taskID", task.ID).WithField("localSubUnitIDs", localSubUnitIDs).
		Info("shard-noop provider: OnSubUnitsCompleted fired")
}

func (p *ShardNoopProvider) GetFinalizedSubUnits(desc TaskDescriptor) []string {
	p.finalizedSubUnitsMu.Lock()
	defer p.finalizedSubUnitsMu.Unlock()
	return append([]string{}, p.finalizedSubUnits[desc]...)
}

func (p *ShardNoopProvider) OnTaskCompleted(task *Task) {
	p.completedTasksMu.Lock()
	defer p.completedTasksMu.Unlock()
	p.completedTasks[task.TaskDescriptor] = true

	// Write completion marker file
	dir := filepath.Join("/tmp/dtm-complete", task.ID)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		p.logger.WithError(err).Error("shard-noop provider: failed to create completion marker dir")
	} else {
		path := filepath.Join(dir, fmt.Sprintf("done-%s", p.nodeID))
		if err := os.WriteFile(path, []byte(fmt.Sprintf("completed by %s, status=%s", p.nodeID, task.Status)), 0o644); err != nil {
			p.logger.WithError(err).Error("shard-noop provider: failed to write completion marker")
		}
	}

	p.logger.WithField("taskID", task.ID).WithField("status", task.Status).
		Info("shard-noop provider: OnTaskCompleted fired")
}

func (p *ShardNoopProvider) IsTaskCompleted(desc TaskDescriptor) bool {
	p.completedTasksMu.Lock()
	defer p.completedTasksMu.Unlock()
	return p.completedTasks[desc]
}

func (p *ShardNoopProvider) processSubUnits(task *Task, handle *shardNoopTaskHandle) {
	var payload ShardNoopProviderPayload
	if len(task.Payload) > 0 {
		_ = json.Unmarshal(task.Payload, &payload)
	}

	// Build a local shard set when a collection is specified and a ShardLister is available.
	var localShardSet map[string]bool
	if payload.Collection != "" && p.shardLister != nil {
		// Retry GetLocalShardNames with backoff — shards may not be loaded yet on this node
		// shortly after collection creation.
		var shardNames []string
		for attempt := 0; attempt < 10; attempt++ {
			var err error
			shardNames, err = p.shardLister.GetLocalShardNames(payload.Collection)
			if err == nil {
				break
			}
			p.logger.WithError(err).Warn("shard-noop provider: waiting for local shards")
			select {
			case <-handle.stopCh:
				return
			case <-time.After(time.Duration(attempt+1) * 500 * time.Millisecond):
			}
		}
		if shardNames == nil {
			p.logger.Error("shard-noop provider: failed to list local shards after retries")
			return
		}
		localShardSet = make(map[string]bool, len(shardNames))
		for _, name := range shardNames {
			localShardSet[name] = true
		}
	}

	// Determine processing delay (default 100ms).
	processingDelay := time.Duration(payload.ProcessingDelayMs) * time.Millisecond
	if processingDelay == 0 {
		processingDelay = 100 * time.Millisecond
	}

	p.logger.WithField("nodeID", p.nodeID).WithField("taskID", task.ID).
		WithField("subUnitCount", len(task.SubUnits)).WithField("localShardSet", localShardSet).
		Info("shard-noop provider: starting sub-unit processing")

	for suID, su := range task.SubUnits {
		select {
		case <-handle.stopCh:
			return
		default:
		}

		// Skip sub-units that are already in a terminal state (from a previous run)
		if su.Status == SubUnitStatusCompleted || su.Status == SubUnitStatusFailed {
			continue
		}

		if localShardSet != nil && payload.SubUnitToShard != nil {
			// Per-replica mode: use SubUnitToShard to check shard locality, and
			// SubUnitToNode for deterministic ownership (avoids RAFT contention when
			// multiple nodes have the same shard with RF > 1).
			shardName, ok := payload.SubUnitToShard[suID]
			if !ok || !localShardSet[shardName] {
				p.logger.WithField("suID", suID).WithField("nodeID", p.nodeID).
					Debug("shard-noop provider: skipping non-local sub-unit (per-replica)")
				continue
			}
			if ownerNode, ok := payload.SubUnitToNode[suID]; ok && ownerNode != p.nodeID {
				p.logger.WithField("suID", suID).WithField("nodeID", p.nodeID).
					Debug("shard-noop provider: skipping sub-unit owned by another node")
				continue
			}
		} else if localShardSet != nil {
			// Legacy collection-aware mode: sub-unit ID = shard name.
			if !localShardSet[suID] {
				p.logger.WithField("suID", suID).WithField("nodeID", p.nodeID).
					Debug("shard-noop provider: skipping non-local sub-unit")
				continue
			}
		} else {
			// Synthetic mode: use NodeID-based ownership (original behavior).
			nodeID := su.NodeID
			if nodeID == "" {
				nodeID = p.nodeID
			}
			if nodeID != p.nodeID {
				continue
			}
		}

		p.logger.WithField("suID", suID).WithField("nodeID", p.nodeID).
			Info("shard-noop provider: processing sub-unit")

		ctx := context.Background()

		// Report initial progress with retry
		p.retryRecorderCall(handle, func() error {
			return p.recorder.UpdateDistributedTaskSubUnitProgress(
				ctx, task.Namespace, task.ID, task.Version, p.nodeID, suID, 0.5,
			)
		})

		// Apply slow sub-unit delay if this sub-unit is the designated slow one.
		if payload.SlowSubUnitID == suID && payload.SlowSubUnitDelayMs > 0 {
			select {
			case <-handle.stopCh:
				return
			case <-time.After(time.Duration(payload.SlowSubUnitDelayMs) * time.Millisecond):
			}
		}

		select {
		case <-handle.stopCh:
			return
		case <-time.After(processingDelay):
		}

		if payload.FailSubUnitID == suID {
			p.retryRecorderCall(handle, func() error {
				return p.recorder.RecordDistributedTaskSubUnitFailure(
					ctx, task.Namespace, task.ID, task.Version, p.nodeID, suID, "dummy failure",
				)
			})
			return // stop processing after failure
		}

		p.retryRecorderCall(handle, func() error {
			return p.recorder.RecordDistributedTaskSubUnitCompletion(
				ctx, task.Namespace, task.ID, task.Version, p.nodeID, suID,
			)
		})

		// Write processing marker file
		dir := filepath.Join("/tmp/dtm-process", task.ID)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			p.logger.WithError(err).Error("shard-noop provider: failed to create processing marker dir")
		} else {
			path := filepath.Join(dir, suID)
			if err := os.WriteFile(path, []byte(fmt.Sprintf("processed by %s", p.nodeID)), 0o644); err != nil {
				p.logger.WithError(err).Error("shard-noop provider: failed to write processing marker")
			}
		}
	}
}

func (p *ShardNoopProvider) processLegacyTask(task *Task, handle *shardNoopTaskHandle) {
	select {
	case <-handle.stopCh:
		return
	case <-time.After(100 * time.Millisecond):
	}

	_ = p.recorder.RecordDistributedTaskNodeCompletion(
		context.Background(), task.Namespace, task.ID, task.Version,
	)
}

type shardNoopTaskHandle struct {
	once   sync.Once
	stopCh chan struct{}
	doneCh chan struct{}
}

func (h *shardNoopTaskHandle) Terminate() {
	h.once.Do(func() { close(h.stopCh) })
}

func (h *shardNoopTaskHandle) Done() <-chan struct{} { return h.doneCh }

// retryRecorderCall retries a recorder operation up to 3 times with a 500ms delay between attempts.
// It respects the stop channel and returns early if termination is requested.
func (p *ShardNoopProvider) retryRecorderCall(handle *shardNoopTaskHandle, fn func() error) {
	for attempt := 0; attempt < 3; attempt++ {
		err := fn()
		if err == nil {
			return
		}
		p.logger.WithError(err).WithField("attempt", attempt+1).
			Warn("shard-noop provider: recorder call failed, retrying")
		select {
		case <-handle.stopCh:
			return
		case <-time.After(500 * time.Millisecond):
		}
	}
	p.logger.Error("shard-noop provider: recorder call failed after all retries")
}

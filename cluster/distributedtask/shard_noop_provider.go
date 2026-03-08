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
//
// MaxConcurrency controls how many sub-units are processed in parallel on each
// node. When > 1, processSubUnits fans out with a [ConcurrencyLimiter] instead
// of sequential iteration. Default 0 = sequential (existing behavior).
type ShardNoopProviderPayload struct {
	FailSubUnitID      string            `json:"failSubUnitId,omitempty"`
	Collection         string            `json:"collection,omitempty"`
	SubUnitToShard     map[string]string `json:"subUnitToShard,omitempty"`
	SubUnitToNode      map[string]string `json:"subUnitToNode,omitempty"`
	SlowSubUnitID      string            `json:"slowSubUnitId,omitempty"`
	SlowSubUnitDelayMs int               `json:"slowSubUnitDelayMs,omitempty"`
	ProcessingDelayMs  int               `json:"processingDelayMs,omitempty"`
	MaxConcurrency     int               `json:"maxConcurrency,omitempty"`
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

	finalizedGroups   map[TaskDescriptor]map[string][]string
	finalizedGroupsMu sync.Mutex
}

// NewShardNoopProvider creates a new [ShardNoopProvider]. Pass nil for shardLister
// when real shard topology is not needed (e.g. unit tests with synthetic sub-unit IDs).
func NewShardNoopProvider(nodeID string, logger logrus.FieldLogger, shardLister ShardLister) *ShardNoopProvider {
	return &ShardNoopProvider{
		nodeID:          nodeID,
		logger:          logger,
		shardLister:     shardLister,
		completedTasks:  make(map[TaskDescriptor]bool),
		finalizedGroups: make(map[TaskDescriptor]map[string][]string),
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

func (p *ShardNoopProvider) OnGroupCompleted(task *Task, groupID string, localGroupSubUnitIDs []string) {
	p.finalizedGroupsMu.Lock()
	if p.finalizedGroups[task.TaskDescriptor] == nil {
		p.finalizedGroups[task.TaskDescriptor] = make(map[string][]string)
	}
	p.finalizedGroups[task.TaskDescriptor][groupID] = append(
		p.finalizedGroups[task.TaskDescriptor][groupID], localGroupSubUnitIDs...,
	)
	p.finalizedGroupsMu.Unlock()

	// Write marker files for each finalized sub-unit (grouped by groupID)
	dir := filepath.Join("/tmp/dtm-finalize", task.ID)
	if groupID != "" {
		dir = filepath.Join(dir, groupID)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		p.logger.WithError(err).Error("shard-noop provider: failed to create marker dir")
		return
	}
	for _, suID := range localGroupSubUnitIDs {
		path := filepath.Join(dir, suID)
		if err := os.WriteFile(path, []byte(fmt.Sprintf("finalized by %s", p.nodeID)), 0o644); err != nil {
			p.logger.WithError(err).Error("shard-noop provider: failed to write marker file")
		}
	}

	p.logger.WithField("taskID", task.ID).WithField("groupID", groupID).
		WithField("localGroupSubUnitIDs", localGroupSubUnitIDs).
		Info("shard-noop provider: OnGroupCompleted fired")
}

// GetFinalizedSubUnits returns all finalized sub-unit IDs across all groups for a task.
func (p *ShardNoopProvider) GetFinalizedSubUnits(desc TaskDescriptor) []string {
	p.finalizedGroupsMu.Lock()
	defer p.finalizedGroupsMu.Unlock()

	var all []string
	for _, groupSUs := range p.finalizedGroups[desc] {
		all = append(all, groupSUs...)
	}
	return all
}

// GetFinalizedGroups returns the per-group finalized sub-unit IDs for a task.
func (p *ShardNoopProvider) GetFinalizedGroups(desc TaskDescriptor) map[string][]string {
	p.finalizedGroupsMu.Lock()
	defer p.finalizedGroupsMu.Unlock()

	result := make(map[string][]string, len(p.finalizedGroups[desc]))
	for g, ids := range p.finalizedGroups[desc] {
		result[g] = append([]string{}, ids...)
	}
	return result
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
		WithField("maxConcurrency", payload.MaxConcurrency).
		Info("shard-noop provider: starting sub-unit processing")

	if payload.MaxConcurrency > 1 {
		p.processSubUnitsConcurrent(task, handle, payload, localShardSet, processingDelay)
		return
	}

	ctx := context.Background()
	for suID, su := range task.SubUnits {
		select {
		case <-handle.stopCh:
			return
		default:
		}

		if su.Status == SubUnitStatusCompleted || su.Status == SubUnitStatusFailed {
			continue
		}

		if !p.shouldProcessSubUnit(suID, su, payload, localShardSet) {
			continue
		}

		p.processOneSubUnit(ctx, task, handle, suID, payload, processingDelay)
	}
}

func (p *ShardNoopProvider) processSubUnitsConcurrent(task *Task, handle *shardNoopTaskHandle, payload ShardNoopProviderPayload, localShardSet map[string]bool, processingDelay time.Duration) {
	limiter := NewConcurrencyLimiter(payload.MaxConcurrency)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Cancel context when handle is terminated
	enterrors.GoWrapper(func() {
		select {
		case <-handle.stopCh:
			cancel()
		case <-ctx.Done():
		}
	}, p.logger)

	var wg sync.WaitGroup
	for suID, su := range task.SubUnits {
		if su.Status == SubUnitStatusCompleted || su.Status == SubUnitStatusFailed {
			continue
		}

		if !p.shouldProcessSubUnit(suID, su, payload, localShardSet) {
			continue
		}

		if err := limiter.Acquire(ctx); err != nil {
			return
		}

		wg.Add(1)
		suID := suID // capture loop variable
		enterrors.GoWrapper(func() {
			defer wg.Done()
			defer limiter.Release()

			p.processOneSubUnit(ctx, task, handle, suID, payload, processingDelay)
		}, p.logger)
	}

	wg.Wait()
}

// shouldProcessSubUnit checks whether this node should process the given sub-unit.
func (p *ShardNoopProvider) shouldProcessSubUnit(suID string, su *SubUnit, payload ShardNoopProviderPayload, localShardSet map[string]bool) bool {
	if localShardSet != nil && payload.SubUnitToShard != nil {
		shardName, ok := payload.SubUnitToShard[suID]
		if !ok || !localShardSet[shardName] {
			return false
		}
		if ownerNode, ok := payload.SubUnitToNode[suID]; ok && ownerNode != p.nodeID {
			return false
		}
		return true
	} else if localShardSet != nil {
		return localShardSet[suID]
	}
	nodeID := su.NodeID
	if nodeID == "" {
		nodeID = p.nodeID
	}
	return nodeID == p.nodeID
}

// processOneSubUnit handles a single sub-unit's lifecycle (progress, delay, complete/fail).
func (p *ShardNoopProvider) processOneSubUnit(ctx context.Context, task *Task, handle *shardNoopTaskHandle, suID string, payload ShardNoopProviderPayload, processingDelay time.Duration) {
	p.logger.WithField("suID", suID).WithField("nodeID", p.nodeID).
		Info("shard-noop provider: processing sub-unit")

	p.retryRecorderCall(handle, func() error {
		return p.recorder.UpdateDistributedTaskSubUnitProgress(
			ctx, task.Namespace, task.ID, task.Version, p.nodeID, suID, 0.5,
		)
	})

	if payload.SlowSubUnitID == suID && payload.SlowSubUnitDelayMs > 0 {
		select {
		case <-handle.stopCh:
			return
		case <-ctx.Done():
			return
		case <-time.After(time.Duration(payload.SlowSubUnitDelayMs) * time.Millisecond):
		}
	}

	select {
	case <-handle.stopCh:
		return
	case <-ctx.Done():
		return
	case <-time.After(processingDelay):
	}

	if payload.FailSubUnitID == suID {
		p.retryRecorderCall(handle, func() error {
			return p.recorder.RecordDistributedTaskSubUnitFailure(
				ctx, task.Namespace, task.ID, task.Version, p.nodeID, suID, "dummy failure",
			)
		})
		return
	}

	p.retryRecorderCall(handle, func() error {
		return p.recorder.RecordDistributedTaskSubUnitCompletion(
			ctx, task.Namespace, task.ID, task.Version, p.nodeID, suID,
		)
	})

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

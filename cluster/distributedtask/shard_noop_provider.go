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
	"strings"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	enterrors "github.com/weaviate/weaviate/entities/errors"
)

const ShardNoopProviderNamespace = "shard-noop"

// ShardLister provides local shard names for a collection, allowing the
// [ShardNoopProvider] to determine unit ownership based on real shard
// topology without importing the db package.
type ShardLister interface {
	GetLocalShardNames(collection string) ([]string, error)
}

// ShardNoopProviderPayload is the JSON payload for tasks created with the
// [ShardNoopProvider]. When Collection is set and a [ShardLister] is available,
// the provider uses real shard placement for unit ownership instead of the
// synthetic NodeID-based assignment.
//
// UnitToShard maps unit IDs to shard names, allowing multiple units
// per shard (one per replica). UnitToNode maps unit IDs to the node that
// should process them, providing deterministic ownership when RF > 1 (where
// multiple nodes have the same shard locally). Both maps are required when
// Collection is set.
//
// MaxConcurrency controls how many units are processed in parallel on each
// node. When > 1, processUnits fans out with a [ConcurrencyLimiter] instead
// of sequential iteration. Default 0 = sequential (existing behavior).
type ShardNoopProviderPayload struct {
	FailUnitID        string            `json:"failUnitId,omitempty"`
	Collection        string            `json:"collection,omitempty"`
	UnitToShard       map[string]string `json:"unitToShard,omitempty"`
	UnitToNode        map[string]string `json:"unitToNode,omitempty"`
	SlowUnitID        string            `json:"slowUnitId,omitempty"`
	SlowUnitDelayMs   int               `json:"slowUnitDelayMs,omitempty"`
	ProcessingDelayMs int               `json:"processingDelayMs,omitempty"`
	MaxConcurrency    int               `json:"maxConcurrency,omitempty"`
}

// ShardNoopProvider is a test-only [UnitAwareProvider] used by acceptance tests to exercise
// the unit lifecycle end-to-end. It is registered in configure_api.go behind the
// SHARD_NOOP_PROVIDER_ENABLED env var and exposed via a debug HTTP endpoint
// on port 6060.
//
// On StartTask, it spawns a goroutine that iterates units sequentially, reports 50%
// progress, then completes each one. Set FailUnitID in the payload to make one unit
// fail instead, which triggers the task-level fail-fast behavior.
//
// When a Collection is specified in the payload and a [ShardLister] is provided, the provider
// only claims units whose IDs match local shard names. This validates that unit
// ownership aligns with actual shard placement.
//
// Marker files are written as hidden dot-files inside shard directories (when a collection is
// specified) or under {dataRoot}/.dtm/ (for synthetic units). This avoids writing to /tmp
// and keeps side effects scoped to the Weaviate data directory.
type ShardNoopProvider struct {
	mu       sync.Mutex
	recorder TaskCompletionRecorder
	tasks    []TaskDescriptor
	nodeID   string
	logger   logrus.FieldLogger
	dataRoot string

	shardLister ShardLister

	completedTasks   map[TaskDescriptor]bool
	completedTasksMu sync.Mutex

	finalizedGroups   map[TaskDescriptor]map[string][]string
	finalizedGroupsMu sync.Mutex
}

// NewShardNoopProvider creates a new [ShardNoopProvider]. Pass nil for shardLister
// when real shard topology is not needed (e.g. unit tests with synthetic unit IDs).
// dataRoot is the Weaviate persistence data path; marker files are written as hidden
// dot-files inside shard directories (collection-aware mode) or under {dataRoot}/.dtm/
// (synthetic mode).
func NewShardNoopProvider(nodeID string, logger logrus.FieldLogger, shardLister ShardLister, dataRoot string) *ShardNoopProvider {
	return &ShardNoopProvider{
		nodeID:          nodeID,
		logger:          logger,
		shardLister:     shardLister,
		dataRoot:        dataRoot,
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

func (p *ShardNoopProvider) addTask(desc TaskDescriptor) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.tasks = append(p.tasks, desc)
}

func (p *ShardNoopProvider) StartTask(task *Task) (TaskHandle, error) {
	p.addTask(task.TaskDescriptor)

	handle := &shardNoopTaskHandle{stopCh: make(chan struct{}), doneCh: make(chan struct{})}

	enterrors.GoWrapper(func() {
		defer close(handle.doneCh)
		p.processUnits(task, handle)
	}, p.logger)

	return handle, nil
}

func (p *ShardNoopProvider) recordFinalizedGroup(desc TaskDescriptor, groupID string, localGroupUnitIDs []string) {
	p.finalizedGroupsMu.Lock()
	defer p.finalizedGroupsMu.Unlock()

	if p.finalizedGroups[desc] == nil {
		p.finalizedGroups[desc] = make(map[string][]string)
	}
	p.finalizedGroups[desc][groupID] = append(
		p.finalizedGroups[desc][groupID], localGroupUnitIDs...,
	)
}

func (p *ShardNoopProvider) OnGroupCompleted(task *Task, groupID string, localGroupUnitIDs []string) {
	p.recordFinalizedGroup(task.TaskDescriptor, groupID, localGroupUnitIDs)

	// Write marker files for each finalized unit.
	var payload ShardNoopProviderPayload
	if len(task.Payload) > 0 {
		_ = json.Unmarshal(task.Payload, &payload)
	}
	for _, suID := range localGroupUnitIDs {
		var dir string
		if payload.Collection != "" {
			shardName := suID
			if payload.UnitToShard != nil {
				if sn, ok := payload.UnitToShard[suID]; ok {
					shardName = sn
				}
			}
			dir = p.shardDir(payload.Collection, shardName)
		} else {
			dir = filepath.Join(p.syntheticMarkerDir(), "dtm-finalize", task.ID)
			if groupID != "" {
				dir = filepath.Join(dir, groupID)
			}
		}
		if err := os.MkdirAll(dir, 0o755); err != nil {
			p.logger.WithError(err).Error("shard-noop provider: failed to create marker dir")
			continue
		}
		var markerName string
		if payload.Collection != "" {
			if groupID != "" {
				markerName = fmt.Sprintf(".dtm-finalize--%s--%s--%s", task.ID, groupID, suID)
			} else {
				markerName = fmt.Sprintf(".dtm-finalize--%s--%s", task.ID, suID)
			}
		} else {
			// Synthetic mode: use unit ID as the file name (old layout under dataRoot/.dtm/).
			markerName = suID
		}
		path := filepath.Join(dir, markerName)
		if err := os.WriteFile(path, []byte(fmt.Sprintf("finalized by %s", p.nodeID)), 0o644); err != nil {
			p.logger.WithError(err).Error("shard-noop provider: failed to write marker file")
		}
	}

	p.logger.WithField("taskID", task.ID).WithField("groupID", groupID).
		WithField("localGroupUnitIDs", localGroupUnitIDs).
		Info("shard-noop provider: OnGroupCompleted fired")
}

// GetFinalizedUnits returns all finalized unit IDs across all groups for a task.
func (p *ShardNoopProvider) GetFinalizedUnits(desc TaskDescriptor) []string {
	p.finalizedGroupsMu.Lock()
	defer p.finalizedGroupsMu.Unlock()

	var all []string
	for _, groupSUs := range p.finalizedGroups[desc] {
		all = append(all, groupSUs...)
	}
	return all
}

// GetFinalizedGroups returns the per-group finalized unit IDs for a task.
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

	// Write completion marker file into the collection's index directory (collection-aware)
	// or into {dataRoot}/.dtm/dtm-complete/{taskID}/ (synthetic).
	var payload ShardNoopProviderPayload
	if len(task.Payload) > 0 {
		_ = json.Unmarshal(task.Payload, &payload)
	}
	var dir string
	if payload.Collection != "" {
		dir = p.indexDir(payload.Collection)
	} else {
		dir = filepath.Join(p.syntheticMarkerDir(), "dtm-complete", task.ID)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		p.logger.WithError(err).Error("shard-noop provider: failed to create completion marker dir")
	} else {
		var markerName string
		if payload.Collection != "" {
			markerName = fmt.Sprintf(".dtm-complete--%s--%s", task.ID, p.nodeID)
		} else {
			markerName = fmt.Sprintf("done-%s", p.nodeID)
		}
		path := filepath.Join(dir, markerName)
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

// shardDir returns the filesystem path for a shard given a collection name.
// Follows the Weaviate convention: {dataRoot}/{lowercase(collection)}/{shardName}.
func (p *ShardNoopProvider) shardDir(collection, shardName string) string {
	return filepath.Join(p.dataRoot, strings.ToLower(collection), shardName)
}

// indexDir returns the filesystem path for a collection's index directory.
func (p *ShardNoopProvider) indexDir(collection string) string {
	return filepath.Join(p.dataRoot, strings.ToLower(collection))
}

// syntheticMarkerDir returns the base directory for marker files when no
// collection is specified (synthetic units).
func (p *ShardNoopProvider) syntheticMarkerDir() string {
	return filepath.Join(p.dataRoot, ".dtm")
}

func (p *ShardNoopProvider) processUnits(task *Task, handle *shardNoopTaskHandle) {
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
		WithField("unitCount", len(task.Units)).WithField("localShardSet", localShardSet).
		WithField("maxConcurrency", payload.MaxConcurrency).
		Info("shard-noop provider: starting unit processing")

	if payload.MaxConcurrency > 1 {
		p.processUnitsConcurrent(task, handle, payload, localShardSet, processingDelay)
		return
	}

	ctx := context.Background()
	for suID, su := range task.Units {
		select {
		case <-handle.stopCh:
			return
		default:
		}

		if su.Status == UnitStatusCompleted || su.Status == UnitStatusFailed {
			continue
		}

		if !p.shouldProcessUnit(suID, su, payload, localShardSet) {
			continue
		}

		p.processOneUnit(ctx, task, handle, suID, payload, processingDelay)
	}
}

func (p *ShardNoopProvider) processUnitsConcurrent(task *Task, handle *shardNoopTaskHandle, payload ShardNoopProviderPayload, localShardSet map[string]bool, processingDelay time.Duration) {
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
	for suID, su := range task.Units {
		if su.Status == UnitStatusCompleted || su.Status == UnitStatusFailed {
			continue
		}

		if !p.shouldProcessUnit(suID, su, payload, localShardSet) {
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

			p.processOneUnit(ctx, task, handle, suID, payload, processingDelay)
		}, p.logger)
	}

	wg.Wait()
}

// shouldProcessUnit checks whether this node should process the given unit.
func (p *ShardNoopProvider) shouldProcessUnit(suID string, su *Unit, payload ShardNoopProviderPayload, localShardSet map[string]bool) bool {
	if localShardSet != nil && payload.UnitToShard != nil {
		shardName, ok := payload.UnitToShard[suID]
		if !ok || !localShardSet[shardName] {
			return false
		}
		if ownerNode, ok := payload.UnitToNode[suID]; ok && ownerNode != p.nodeID {
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

// processOneUnit handles a single unit's lifecycle (progress, delay, complete/fail).
func (p *ShardNoopProvider) processOneUnit(ctx context.Context, task *Task, handle *shardNoopTaskHandle, suID string, payload ShardNoopProviderPayload, processingDelay time.Duration) {
	p.logger.WithField("suID", suID).WithField("nodeID", p.nodeID).
		Info("shard-noop provider: processing unit")

	p.retryRecorderCall(handle, func() error {
		return p.recorder.UpdateDistributedTaskUnitProgress(
			ctx, task.Namespace, task.ID, task.Version, p.nodeID, suID, 0.5,
		)
	})

	if payload.SlowUnitID == suID && payload.SlowUnitDelayMs > 0 {
		select {
		case <-handle.stopCh:
			return
		case <-ctx.Done():
			return
		case <-time.After(time.Duration(payload.SlowUnitDelayMs) * time.Millisecond):
		}
	}

	select {
	case <-handle.stopCh:
		return
	case <-ctx.Done():
		return
	case <-time.After(processingDelay):
	}

	if payload.FailUnitID == suID {
		p.retryRecorderCall(handle, func() error {
			return p.recorder.RecordDistributedTaskUnitFailure(
				ctx, task.Namespace, task.ID, task.Version, p.nodeID, suID, "dummy failure",
			)
		})
		return
	}

	p.retryRecorderCall(handle, func() error {
		return p.recorder.RecordDistributedTaskUnitCompletion(
			ctx, task.Namespace, task.ID, task.Version, p.nodeID, suID,
		)
	})

	// Write processing marker into the shard directory (collection-aware) or
	// {dataRoot}/.dtm/dtm-process/{taskID}/ (synthetic).
	var dir string
	if payload.Collection != "" {
		shardName := suID
		if payload.UnitToShard != nil {
			if sn, ok := payload.UnitToShard[suID]; ok {
				shardName = sn
			}
		}
		dir = p.shardDir(payload.Collection, shardName)
	} else {
		dir = filepath.Join(p.syntheticMarkerDir(), "dtm-process", task.ID)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		p.logger.WithError(err).Error("shard-noop provider: failed to create processing marker dir")
	} else {
		var markerName string
		if payload.Collection != "" {
			markerName = fmt.Sprintf(".dtm-process--%s--%s", task.ID, suID)
		} else {
			markerName = suID
		}
		path := filepath.Join(dir, markerName)
		if err := os.WriteFile(path, []byte(fmt.Sprintf("processed by %s", p.nodeID)), 0o644); err != nil {
			p.logger.WithError(err).Error("shard-noop provider: failed to write processing marker")
		}
	}
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

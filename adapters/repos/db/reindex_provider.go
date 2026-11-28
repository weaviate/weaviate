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
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/logrusext"
	"github.com/weaviate/weaviate/usecases/sharding"
)

// ReindexProvider implements distributedtask.UnitAwareProvider for reindex tasks.
type ReindexProvider struct {
	mu       sync.Mutex
	recorder distributedtask.TaskCompletionRecorder

	db          *DB
	logger      logrus.FieldLogger
	localNode   string
	concurrency func() int

	flipper ReindexFlipper // guarded by mu

	serverCtx context.Context

	runningHandles map[distributedtask.TaskDescriptor]*reindexTaskHandle

	// seenTasks holds every task a callback read the header of, so GetLocalTasks hands it to the scheduler's cleanup.
	seenTasks map[distributedtask.TaskDescriptor]struct{}

	unitDoneOnce sync.Once
	unitDone     *logrusext.Sampler

	recorderRetryOnce sync.Once
	recorderRetry     *logrusext.Sampler
}

func NewReindexProvider(
	db *DB,
	logger logrus.FieldLogger,
	localNode string,
	concurrency func() int,
	serverCtx context.Context,
) *ReindexProvider {
	if serverCtx == nil {
		serverCtx = context.Background()
	}
	return &ReindexProvider{
		db:             db,
		logger:         logger,
		localNode:      localNode,
		concurrency:    concurrency,
		serverCtx:      serverCtx,
		runningHandles: make(map[distributedtask.TaskDescriptor]*reindexTaskHandle),
		seenTasks:      make(map[distributedtask.TaskDescriptor]struct{}),
	}
}

func (p *ReindexProvider) SetCompletionRecorder(recorder distributedtask.TaskCompletionRecorder) {
	p.recorder = recorder
}

func (p *ReindexProvider) GetLocalTasks() []distributedtask.TaskDescriptor {
	p.mu.Lock()
	defer p.mu.Unlock()
	return slices.Collect(maps.Keys(p.seenTasks))
}

func (p *ReindexProvider) registerStartingTask(desc distributedtask.TaskDescriptor, handle *reindexTaskHandle) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.runningHandles[desc] = handle
}

func (p *ReindexProvider) deleteRunningHandle(desc distributedtask.TaskDescriptor) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.runningHandles, desc)
}

func (p *ReindexProvider) markSeen(desc distributedtask.TaskDescriptor) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.seenTasks[desc] = struct{}{}
}

func (p *ReindexProvider) forget(desc distributedtask.TaskDescriptor) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.seenTasks, desc)
}

func (p *ReindexProvider) CleanupTask(desc distributedtask.TaskDescriptor) error {
	p.forget(desc)
	return nil
}

// headerOf refuses a run without a generation: boot refuses an unfinished one.
func (p *ReindexProvider) headerOf(task *distributedtask.Task) (ReindexTaskHeader, error) {
	h, ok := ReindexHeaderOf(task)
	if !ok {
		return h, fmt.Errorf("reindex task %s has no readable payload", task.ID)
	}
	p.markSeen(task.TaskDescriptor)
	if !h.SwitchedByView() {
		return h, fmt.Errorf("reindex task %s has no generation", task.ID)
	}
	return h, nil
}

func (p *ReindexProvider) StartTask(task *distributedtask.Task) (distributedtask.TaskHandle, error) {
	h, err := p.headerOf(task)
	if err != nil {
		return nil, err
	}
	return p.startGenerationRun(task, h)
}

// OnGroupCompleted does nothing: each unit reported done only once its build was durable.
func (p *ReindexProvider) OnGroupCompleted(*distributedtask.Task, string, []string) error {
	return nil
}

func (p *ReindexProvider) OnTaskCompleted(task *distributedtask.Task) error {
	defer p.forget(task.TaskDescriptor)
	h, ok := ReindexHeaderOf(task)
	if !ok || !h.SwitchedByView() {
		return nil
	}
	return p.completeGenerationRun(task, h)
}

func (p *ReindexProvider) recorderRetrySampler() *logrusext.Sampler {
	p.recorderRetryOnce.Do(func() {
		p.recorderRetry = logrusext.NewSampler(p.logger, maxReportedErrors, migrationUnitContentionWindow)
	})
	return p.recorderRetry
}

func (p *ReindexProvider) failUnit(
	ctx context.Context,
	task *distributedtask.Task,
	unitID string,
	recorder distributedtask.TaskCompletionRecorder,
	errMsg string,
) {
	logger := p.logger.WithField("taskID", task.ID).WithField("unit", unitID)
	logger.Errorf("reindex provider: unit failed: %s", errMsg)
	p.recordOutcome(ctx, logger, "unit failure", func() error {
		return recorder.RecordDistributedTaskUnitFailure(
			ctx, task.Namespace, task.ID, task.Version, p.localNode, unitID, errMsg)
	})
}

const recorderRetryMaxBackoff = time.Minute

// recordOutcome retries a write until it lands, the FSM rejects it for good, or ctx ends. The worker
// keeps its slot, because a recorder error comes from RAFT and would fail any other unit too.
func (p *ReindexProvider) recordOutcome(ctx context.Context, logger logrus.FieldLogger, what string, write func() error) (recorded bool) {
	for backoff := 200 * time.Millisecond; ; backoff = min(2*backoff, recorderRetryMaxBackoff) {
		err := write()
		if err == nil {
			return true
		}
		if p.isPermanentRecorderRejection(logger, err) {
			p.recorderRetrySampler().WithSampling(func(logrus.FieldLogger) {
				logger.Warnf("reindex provider: FSM rejected %s record as permanent (task already terminal, "+
					"version mismatch, or unit owned by another node); not retrying: %v", what, err)
			})
			return false
		}
		p.recorderRetrySampler().WithSampling(func(logrus.FieldLogger) {
			logger.Warnf("reindex provider: failed to record %s, retrying in %s: %v", what, backoff, err)
		})
		select {
		case <-ctx.Done():
			p.recorderRetrySampler().WithSampling(func(logrus.FieldLogger) {
				logger.Warnf("reindex provider: context cancelled while recording %s; the FSM may not see it", what)
			})
			return false
		case <-time.After(backoff):
		}
	}
}

// isPermanentRecorderRejection reports whether the recorder error
// describes a stable FSM state that retrying cannot fix.
func (p *ReindexProvider) isPermanentRecorderRejection(logger logrus.FieldLogger, err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, distributedtask.ErrPermanentRejection) {
		return true
	}
	msg := err.Error()
	for _, marker := range []string{
		"is no longer running",
		"does not exist",
		"is already terminal",
		"belongs to node",
	} {
		if strings.Contains(msg, marker) {
			p.recorderRetrySampler().WithSampling(func(logrus.FieldLogger) {
				logger.WithField("matchedMarker", marker).WithField("recorderError", msg).
					Warn("reindex provider: permanent-rejection detected via legacy substring fallback; " +
						"a peer is likely running a pre-sentinel build")
			})
			return true
		}
	}
	return false
}

const migrationUnitContentionWindow = time.Minute

// IsLiveReindexTaskStatus reports whether a task in the given DTM status
// still owns the on-disk sidecar buckets of its migration.
//
// A status this build never declared answers true. The other answer
// deletes those dirs while a newer node is still migrating, which is the
// one outcome nothing downstream can undo.
func IsLiveReindexTaskStatus(status distributedtask.TaskStatus) bool {
	switch status {
	case distributedtask.TaskStatusStarted,
		distributedtask.TaskStatusPreparing,
		distributedtask.TaskStatusSwapping:
		return true
	case distributedtask.TaskStatusFinished,
		distributedtask.TaskStatusCancelled,
		distributedtask.TaskStatusFailed:
		return false
	}
	return true
}

func ReindexResubmitCommand(payload *ReindexTaskPayload, propName string) string {
	put := func(indexType, body string) string {
		return fmt.Sprintf("PUT /v1/schema/%s/properties/%s/index/%s -d '%s'", payload.Collection, propName, indexType, body)
	}
	tok := payload.TargetTokenization
	if tok == "" {
		tok = "<tokenization>"
	}
	switch payload.MigrationType {
	case ReindexTypeEnableFilterable:
		return put("filterable", "{}")
	case ReindexTypeEnableRangeable:
		return put("rangeFilters", "{}")
	case ReindexTypeChangeAlgorithm:
		return put("searchable", `{"algorithm":"blockmax"}`)
	case ReindexTypeChangeTokenizationFilterable:
		return put("filterable", fmt.Sprintf(`{"tokenization":%q}`, tok))
	default:
		return put("searchable", fmt.Sprintf(`{"tokenization":%q}`, tok))
	}
}

func IsSemanticMigration(mt ReindexMigrationType) bool {
	return mt == ReindexTypeChangeTokenization ||
		mt == ReindexTypeChangeTokenizationFilterable ||
		mt == ReindexTypeEnableFilterable ||
		mt == ReindexTypeEnableSearchable ||
		mt == ReindexTypeEnableRangeable ||
		mt == ReindexTypeChangeAlgorithm
}

// reindexTaskHandle implements distributedtask.TaskHandle.
type reindexTaskHandle struct {
	cancel context.CancelFunc
	doneCh chan struct{}
}

func (h *reindexTaskHandle) Terminate() {
	h.cancel()
}

func (h *reindexTaskHandle) Done() <-chan struct{} {
	return h.doneCh
}

// ErrTenantsNotHot refuses a reindex that targets tenants which are not HOT.
// It names no tenant: a collection-wide caller may lack permission to read
// tenants. A tenant mid freeze or unfreeze rejects a HOT update until done.
var ErrTenantsNotHot = errors.New("some target tenants are not HOT and cannot be reindexed; " +
	"once any freeze or unfreeze in progress finishes, set them to HOT")

// ShardReplicaOwnership returns a map of node name to shard names, with one
// entry per replica. Unlike ShardOwnership (which assigns each shard to one
// node for export load balancing), this returns ALL replica nodes for each
// shard. This is needed for reindex tasks where every replica must process
// its own local copy of the data.
//
// WARNING: Do NOT use ShardOwnership for reindex — it only returns one node per
// shard and would leave replicas on other nodes un-reindexed.
func (db *DB) ShardReplicaOwnership(ctx context.Context, className string) (map[string][]string, error) {
	result := make(map[string][]string)

	err := db.schemaReader.Read(className, true, func(_ *models.Class, state *sharding.State) error {
		if state == nil {
			return fmt.Errorf("unable to retrieve sharding state for class %s", className)
		}

		for shardName, shard := range state.Physical {
			for _, node := range shard.BelongsToNodes {
				if node != "" {
					result[node] = append(result[node], shardName)
				}
			}
		}

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to read sharding state for class %s: %w", className, err)
	}

	// Sort shard names per node for determinism.
	for _, shards := range result {
		sort.Strings(shards)
	}

	return result, nil
}

func lookupShardByName(idx *Index, shardName string) (ShardLike, error) {
	if idx.closeCause() == nil {
		if s := idx.shards.Load(shardName); s != nil {
			return s, nil
		}
	}
	return nil, fmt.Errorf("shard %q not found", shardName)
}

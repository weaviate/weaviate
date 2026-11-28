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
	"slices"
	"sync"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/cluster/proto/api"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	entschema "github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/logrusext"
)

// ReindexFlipper proposes a full run's flip once every tenant switched.
type ReindexFlipper interface {
	ReindexFlip(ctx context.Context, class string, req api.ReindexFlipRequest) (uint64, error)
}

func (p *ReindexProvider) SetReindexFlipper(f ReindexFlipper) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.flipper = f
}

// startGenerationRun builds this node's unfinished units; each replica's view serves the switch.
func (p *ReindexProvider) startGenerationRun(task *distributedtask.Task, h ReindexTaskHeader,
) (distributedtask.TaskHandle, error) {
	idx := p.db.GetIndex(entschema.ClassName(h.Collection()))
	if idx == nil {
		return nil, fmt.Errorf("collection %q not found", h.Collection())
	}
	var units []*distributedtask.Unit
	for _, u := range task.Units {
		if u.NodeID == p.localNode && u.Status != distributedtask.UnitStatusCompleted && u.Status != distributedtask.UnitStatusFailed {
			units = append(units, u)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	handle := &reindexTaskHandle{cancel: cancel, doneCh: make(chan struct{})}
	p.registerStartingTask(task.TaskDescriptor, handle)
	enterrors.GoWrapper(func() {
		defer func() {
			p.deleteRunningHandle(task.TaskDescriptor)
			close(handle.doneCh)
		}()
		limiter := distributedtask.NewConcurrencyLimiter(p.concurrency())
		var wg sync.WaitGroup
		defer wg.Wait()
		for _, u := range units {
			if err := limiter.Acquire(ctx); err != nil {
				return
			}
			wg.Add(1)
			enterrors.GoWrapper(func() {
				defer wg.Done()
				defer limiter.Release()
				p.buildGenerationUnit(ctx, task, h, idx, u)
			}, p.logger)
		}
	}, p.logger)
	return handle, nil
}

// buildGenerationUnit records the start after staging, so a crash in between never keeps a dead run's folder.
func (p *ReindexProvider) buildGenerationUnit(ctx context.Context, task *distributedtask.Task, h ReindexTaskHeader,
	idx *Index, u *distributedtask.Unit,
) {
	defer func() {
		if r := recover(); r != nil {
			defer panic(r)
			// Unrecorded, the task stays STARTED and relaunches the panicking unit every retryFloor.
			if ctx.Err() == nil && p.serverCtx.Err() == nil {
				p.failUnit(ctx, task, u.ID, p.recorder, fmt.Sprintf("panic: %v", r))
			}
		}
	}()
	err := p.runGenerationUnit(ctx, task, h, idx, u)
	switch {
	case errors.Is(err, context.Canceled): // the task restarts it
	case errors.Is(err, distributedtask.ErrUnitAlreadyTerminal): // a relaunch from a trailing copy; the leader ended it
	case err != nil:
		p.failUnit(ctx, task, u.ID, p.recorder, fmt.Sprintf("build: %v", err))
	default:
		if err := p.recorder.RecordDistributedTaskUnitCompletion(ctx, task.Namespace, task.ID, task.Version,
			p.localNode, u.ID); err != nil && !errors.Is(err, distributedtask.ErrUnitAlreadyTerminal) {
			p.unitDoneSampler().WithSampling(func(l logrus.FieldLogger) {
				l.WithField("taskID", task.ID).WithField("unit", u.ID).
					Errorf("reindex provider: cannot report the unit done: %v", err)
			})
		}
	}
}

func (p *ReindexProvider) unitDoneSampler() *logrusext.Sampler {
	p.unitDoneOnce.Do(func() {
		p.unitDone = logrusext.NewSampler(p.logger, maxReportedErrors, migrationUnitContentionWindow)
	})
	return p.unitDone
}

func (p *ReindexProvider) runGenerationUnit(ctx context.Context, task *distributedtask.Task, h ReindexTaskHeader,
	idx *Index, u *distributedtask.Unit,
) error {
	shardLike, err := lookupShardByName(idx, u.Shard)
	if err != nil {
		return err
	}
	shard, err := unwrapShard(ctx, shardLike)
	if err != nil {
		return err
	}
	var once sync.Once
	var startErr error
	started := func(ctx context.Context) error {
		once.Do(func() { startErr = p.recordUnitStarted(ctx, task, shard, u) })
		return startErr
	}
	for _, prop := range h.Properties {
		if err := shard.ReindexBuild(ctx, task.ID, h.Generation, prop, started); err != nil {
			return err
		}
	}
	return nil
}

// recordUnitStarted proposes the start and waits until this node applied it, so
// the shard's next refresh keeps the folder instead of emptying it.
func (p *ReindexProvider) recordUnitStarted(ctx context.Context, task *distributedtask.Task, shard *Shard,
	u *distributedtask.Unit,
) error {
	if err := p.recorder.UpdateDistributedTaskUnitProgress(ctx, task.Namespace, task.ID, task.Version,
		p.localNode, u.ID, 0); err != nil {
		return fmt.Errorf("record the unit start: %w", err)
	}
	src := p.db.reindexTaskStateSource()
	if src == nil {
		return errors.New("no reindex task state source wired")
	}
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	for {
		states := src.ShardTaskStates(task.Namespace, shard.index.Config.ClassName.String(), shard.name, p.localNode)
		i := slices.IndexFunc(states, func(st distributedtask.ShardTaskState) bool {
			return st.TaskDescriptor == task.TaskDescriptor
		})
		if i < 0 || states[i].Status.IsTerminal() {
			return fmt.Errorf("task %s ended before its unit started", task.ID)
		}
		if states[i].LocalUnitStarted {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
		}
	}
}

// completeGenerationRun flips a full run once every group decided; a
// tenant-scoped run never flips, as no class value can name only some tenants.
func (p *ReindexProvider) completeGenerationRun(task *distributedtask.Task, h ReindexTaskHeader) error {
	if task.Status != distributedtask.TaskStatusSwapping || len(h.Tenants) > 0 {
		return nil
	}
	target, err := reindexBuildTargetOf(h)
	if err != nil {
		return fmt.Errorf("%w: %w", distributedtask.ErrTaskCompletionPermanent, err)
	}
	p.mu.Lock()
	flipper := p.flipper
	p.mu.Unlock()
	if flipper == nil {
		return errors.New("no flip proposer wired")
	}
	for _, prop := range h.Properties {
		if _, err := flipper.ReindexFlip(p.serverCtx, h.Collection(), api.ReindexFlipRequest{
			Run:  api.MigrationRun{Namespace: task.Namespace, ID: task.ID, Version: task.Version},
			Prop: prop, Gen: h.Generation, Tokenization: target.tokenization, IndexTypes: target.indexTypes,
			Enable: target.enable, Blockmax: target.blockmax,
		}); err != nil {
			return fmt.Errorf("flip %q: %w", prop, err)
		}
	}
	return nil
}

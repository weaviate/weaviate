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
	"time"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/models"
	entschema "github.com/weaviate/weaviate/entities/schema"
)

const (
	// searchableBlockmaxRepairReadyAttempts × its 2s backoff bounds how long the
	// repair loop waits for the RAFT read path before its first pass.
	searchableBlockmaxRepairReadyAttempts = 30
	// searchableBlockmaxRepairInterval re-runs the pass to pick up shards that
	// load after startup — lazily-activated tenant shards, most of all — whose
	// blockmax buckets weren't observable during the first pass.
	searchableBlockmaxRepairInterval = 5 * time.Minute
)

// RunSearchableBlockmaxRepair closes the v1.38→v1.39 upgrade residual: in a
// permanently-partial class, a nil-stamp property genuinely on blockmax reads
// back as WAND once its FINISHED task ages out. It seeds the stamp from
// whichever proves blockmax first — a loaded shard's inverted bucket, or a
// still-present FINISHED task — idempotently and RAFT-consistently, so
// concurrent seeds across shard-holders can't reintroduce the bug being fixed.
func (p *ReindexProvider) RunSearchableBlockmaxRepair(ctx context.Context) {
	if p.schemaManager == nil || p.db == nil || p.taskLister == nil {
		return
	}

	// Wait (bounded) for the RAFT read path so the seed write lands on a
	// reachable leader rather than failing against an unelected one.
	for i := 0; i < searchableBlockmaxRepairReadyAttempts; i++ {
		if ctx.Err() != nil {
			return
		}
		if _, err := p.taskLister.ListDistributedTasks(ctx); err == nil {
			break
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(2 * time.Second):
		}
	}

	for {
		// Panic-contained: one bad round (e.g. a class dropped mid-pass) must
		// not kill the loop, which is the sole recovery path for the residual.
		func() {
			defer func() {
				if r := recover(); r != nil {
					p.logger.Errorf("searchable-blockmax repair: round panicked (loop continues): %v", r)
				}
			}()
			p.reconcileSearchableBlockmaxStamps(ctx)
		}()
		select {
		case <-ctx.Done():
			return
		case <-time.After(searchableBlockmaxRepairInterval):
		}
	}
}

// reconcileSearchableBlockmaxStamps runs one read-repair pass over every class.
func (p *ReindexProvider) reconcileSearchableBlockmaxStamps(ctx context.Context) {
	sch := p.schemaManager.GetSchemaSkipAuth()
	if sch.Objects == nil {
		return
	}

	// One task-list snapshot per pass (the FINISHED-task seeding evidence). On
	// failure, degrade to on-disk-only seeding; the next pass retries.
	var reindexTasks []*distributedtask.Task
	if byNamespace, err := p.taskLister.ListDistributedTasks(ctx); err == nil {
		reindexTasks = byNamespace[ReindexNamespace]
	} else {
		p.logger.Warnf("searchable-blockmax repair: task list unavailable, on-disk seeding only: %v", err)
	}

	for _, class := range sch.Objects.Classes {
		if ctx.Err() != nil {
			return
		}
		p.reconcileClassSearchableBlockmax(ctx, class, reindexTasks)
	}
}

func (p *ReindexProvider) reconcileClassSearchableBlockmax(ctx context.Context, class *models.Class, reindexTasks []*distributedtask.Task) {
	if class == nil {
		return
	}
	// Only the partial-class case needs seeding: with the class flag already
	// blockmax, nil stamps resolve to blockmax via the class flag anyway, so
	// there is nothing to repair.
	if class.InvertedIndexConfig != nil && class.InvertedIndexConfig.UsingBlockMaxWAND {
		return
	}

	var candidates []string
	for _, prop := range class.Properties {
		if inverted.HasSearchableIndex(prop) && prop.SearchableBlockmax == nil {
			candidates = append(candidates, prop.Name)
		}
	}
	if len(candidates) == 0 {
		return
	}

	// Observe on-disk truth on loaded shards only (never force-load a lazy
	// shard to probe): StrategyInverted means genuinely blockmax. A shardless
	// node skips this and seeds from FINISHED-task evidence below instead.
	observed := make(map[string]bool, len(candidates))
	if idx := p.db.GetIndex(entschema.ClassName(class.Class)); idx != nil {
		_ = idx.ForEachLoadedShard(func(_ string, shard ShardLike) error {
			for _, propName := range candidates {
				if observed[propName] {
					continue
				}
				b := shard.Store().Bucket(helpers.BucketSearchableFromPropNameLSM(propName))
				if b != nil && b.Strategy() == lsmkv.StrategyInverted {
					observed[propName] = true
				}
			}
			return nil
		})
	}

	for _, propName := range candidates {
		// classFlagBlockmax=false: the class-flag case returned above, so the
		// task-list derivation carries the FINISHED-task evidence on its own.
		blockmax := observed[propName] ||
			SearchablePropertyBlockmaxFromRAFT(false, class.Class, propName, reindexTasks)
		if !blockmax {
			continue
		}
		if err := p.stampSearchableBlockmax(ctx, class.Class, []string{propName}); err != nil {
			p.logger.WithField("collection", class.Class).WithField("property", propName).
				Warnf("searchable-blockmax repair: failed to seed stamp: %v", err)
		}
	}
}

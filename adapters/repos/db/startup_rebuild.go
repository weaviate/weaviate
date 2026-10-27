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
	"os"
	"slices"
	"sync"
	"time"

	"github.com/sirupsen/logrus"

	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
)

// startupRebuild makes REINDEX_VECTOR_DIMENSIONS_AT_STARTUP or
// RECOUNT_PROPERTIES_AT_STARTUP rebuild a shard once per process, not on every
// lazy reload.
type startupRebuild struct {
	enabled bool

	mu sync.Mutex
	// whether the rebuild succeeded
	outcomes map[rebuiltShard]bool
	objects  int64
}

// rebuiltShard is not keyed by shardId, which joins names that may contain its "_"
type rebuiltShard struct {
	index, shard string
}

func (r *startupRebuild) pending(index, shard string) bool {
	if r == nil || !r.enabled {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return !r.outcomes[rebuiltShard{index, shard}]
}

func (r *startupRebuild) record(index, shard string, objects int, ok bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.outcomes == nil {
		r.outcomes = map[rebuiltShard]bool{}
	}
	r.outcomes[rebuiltShard{index, shard}] = ok
	if ok {
		r.objects += int64(objects)
	}
}

func (r *startupRebuild) done(index, shard string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.outcomes[rebuiltShard{index, shard}]
}

// recorded reports whether the shard was rebuilt or failed.
func (r *startupRebuild) recorded(index, shard string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, ok := r.outcomes[rebuiltShard{index, shard}]
	return ok
}

// forget makes dropped shards pending again, as a restore or a replica copy can bring
// them back with other data. No shards forgets the whole index.
func (r *startupRebuild) forget(index string, shards ...string) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for key := range r.outcomes {
		if key.index == index && (len(shards) == 0 || slices.Contains(shards, key.shard)) {
			delete(r.outcomes, key)
		}
	}
}

func (r *startupRebuild) summary() (rebuilt, failed int, objects int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, ok := range r.outcomes {
		if ok {
			rebuilt++
		} else {
			failed++
		}
	}
	return rebuilt, failed, r.objects
}

// rebuildShardsBeforeLoad runs rebuild on each of the shards that has a dir,
// _NUMCPU at a time.
func (i *Index) rebuildShardsBeforeLoad(ctx context.Context, action, finished string, shardNames []string,
	rebuild func(logger logrus.FieldLogger, name string),
) {
	start := time.Now()
	eg := enterrors.NewErrorGroupWrapper(i.logger)
	eg.SetLimit(_NUMCPU)
	for _, name := range shardNames {
		eg.Go(func() error {
			logger := i.logger.WithField("action", action).WithField("shard", name)
			if _, err := os.Stat(shardPath(i.path(), name)); err != nil {
				if !os.IsNotExist(err) {
					logger.Errorf("could not stat shard dir: %v", err)
				}
				return nil
			}
			rebuild(logger, name)
			return nil
		}, name)
	}
	_ = eg.Wait()
	i.logger.WithField("action", action).WithField("shards", len(shardNames)).
		WithField("took", time.Since(start)).Info(finished)
}

// rebuildUnloadedShard runs rebuild on the shard opened by openUnloadedShard.
func (i *Index) rebuildUnloadedShard(ctx context.Context, class *models.Class, name string,
	rebuild func(s *Shard) (objects int, err error),
) (objects int, err error) {
	s, closeShard, err := i.openUnloadedShard(ctx, class, name)
	if err != nil {
		return 0, err
	}
	defer func() {
		if closeErr := closeShard(); closeErr != nil && err == nil {
			err = closeErr
		}
	}()
	return rebuild(s)
}

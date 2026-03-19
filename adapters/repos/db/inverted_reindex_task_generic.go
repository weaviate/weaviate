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
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/additional"
	entcfg "github.com/weaviate/weaviate/entities/config"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/storobj"
)

// ShardReindexTaskGeneric is a strategy-parameterized implementation of
// ShardReindexTaskV3. All lifecycle logic (state machine, merge/swap/tidy,
// object iteration, progress tracking) lives here, with strategy-specific
// behavior delegated to a MigrationStrategy.
type ShardReindexTaskGeneric struct {
	name                 string
	logger               logrus.FieldLogger
	strategy             MigrationStrategy
	newReindexTracker    func(lsmPath string) (reindexTracker, error)
	keyParser            indexKeyParser
	objectsIteratorAsync objectsIteratorAsync
	config               reindexTaskConfig

	// skipSwapOnFinish, when true, causes the reindex iteration to stop after
	// markReindexed() without proceeding to runtimeSwap(). This is used by
	// RunReindexOnlyOnShard for barrier semantics: all shards must finish
	// reindexing before any shard swaps.
	skipSwapOnFinish bool

	// callbackDisableFuncs collects the disable functions returned by
	// registerAddToPropertyValueIndex / registerDeleteFromPropertyValueIndex.
	// They are called after a runtime swap to stop the double-write callbacks.
	callbackDisableFuncsMu sync.Mutex
	callbackDisableFuncs   []func()
}

// NewShardReindexTaskGeneric creates a new generic reindex task.
func NewShardReindexTaskGeneric(name string, logger logrus.FieldLogger,
	strategy MigrationStrategy, config reindexTaskConfig,
	keyParser indexKeyParser, objectsIteratorAsync objectsIteratorAsync,
) *ShardReindexTaskGeneric {
	logger = logger.WithField("task", name)
	newReindexTracker := func(lsmPath string) (reindexTracker, error) {
		rt := NewFileReindexTracker(lsmPath, strategy.MigrationDirName(), keyParser)
		if err := rt.init(); err != nil {
			return nil, err
		}
		return rt, nil
	}

	logger.WithField("config", fmt.Sprintf("%+v", config)).Debug("task created")

	return &ShardReindexTaskGeneric{
		name:                 name,
		logger:               logger,
		strategy:             strategy,
		newReindexTracker:    newReindexTracker,
		keyParser:            keyParser,
		objectsIteratorAsync: objectsIteratorAsync,
		config:               config,
	}
}

func (t *ShardReindexTaskGeneric) Name() string {
	return t.name
}

// RunOnShard runs the full reindex lifecycle on a live shard: OnAfterLsmInit
// followed by repeated OnAfterLsmInitAsync calls until the task is complete.
// This is intended for debug/runtime-triggered migrations on an already-running shard.
// The shard may be a *Shard or *LazyLoadShard.
func (t *ShardReindexTaskGeneric) RunOnShard(ctx context.Context, shard ShardLike) error {
	concreteShard, err := unwrapShard(ctx, shard)
	if err != nil {
		return fmt.Errorf("unwrapping shard %q: %w", shard.Name(), err)
	}

	if err := t.OnAfterLsmInit(ctx, concreteShard); err != nil {
		return fmt.Errorf("OnAfterLsmInit: %w", err)
	}

	for {
		rerunAt, _, err := t.OnAfterLsmInitAsync(ctx, shard)
		if err != nil {
			return fmt.Errorf("OnAfterLsmInitAsync: %w", err)
		}
		if rerunAt.IsZero() {
			return nil
		}
	}
}

// RunReindexOnlyOnShard runs the reindex iteration WITHOUT swap/tidy.
// After this returns, the shard has:
//   - ingest bucket with double-written data
//   - reindex bucket with reindexed data
//   - main bucket unchanged (still serving queries)
//   - sentinel state: IsReindexed()
//
// This is used for barrier semantics: all shards must finish reindexing
// before any shard swaps. Call RunSwapOnShard after all shards are done.
func (t *ShardReindexTaskGeneric) RunReindexOnlyOnShard(ctx context.Context, shard ShardLike) error {
	t.skipSwapOnFinish = true
	defer func() { t.skipSwapOnFinish = false }()

	concreteShard, err := unwrapShard(ctx, shard)
	if err != nil {
		return fmt.Errorf("unwrapping shard %q: %w", shard.Name(), err)
	}

	if err := t.OnAfterLsmInit(ctx, concreteShard); err != nil {
		return fmt.Errorf("OnAfterLsmInit: %w", err)
	}

	for {
		rerunAt, _, err := t.OnAfterLsmInitAsync(ctx, shard)
		if err != nil {
			return fmt.Errorf("OnAfterLsmInitAsync: %w", err)
		}
		if rerunAt.IsZero() {
			return nil
		}
	}
}

// RunSwapOnShard runs the swap+tidy+OnMigrationComplete phase.
// Precondition: RunReindexOnlyOnShard completed (IsReindexed() == true).
func (t *ShardReindexTaskGeneric) RunSwapOnShard(ctx context.Context, shard ShardLike) error {
	concreteShard, err := unwrapShard(ctx, shard)
	if err != nil {
		return fmt.Errorf("unwrapping shard %q: %w", shard.Name(), err)
	}

	logger := t.logger.WithFields(map[string]any{
		"collection": concreteShard.Index().Config.ClassName.String(),
		"shard":      concreteShard.Name(),
		"method":     "RunSwapOnShard",
	})

	rt, err := t.newReindexTracker(concreteShard.pathLSM())
	if err != nil {
		return fmt.Errorf("creating reindex tracker: %w", err)
	}

	if !rt.IsReindexed() {
		return fmt.Errorf("shard %q is not in reindexed state", concreteShard.Name())
	}

	props, err := t.readPropsToReindex(rt)
	if err != nil {
		return fmt.Errorf("reading props: %w", err)
	}
	if len(props) == 0 {
		return fmt.Errorf("no props found for swap on shard %q", concreteShard.Name())
	}

	logger.WithField("props", props).Info("starting swap phase")

	if err := t.runtimeSwap(ctx, logger, shard, rt, props); err != nil {
		return fmt.Errorf("runtime swap: %w", err)
	}

	return nil
}

// unwrapShard extracts the concrete *Shard from a ShardLike,
// handling both *Shard and *LazyLoadShard.
func unwrapShard(ctx context.Context, shard ShardLike) (*Shard, error) {
	switch s := shard.(type) {
	case *Shard:
		return s, nil
	case *LazyLoadShard:
		return s.Unwrap(ctx)
	default:
		return nil, fmt.Errorf("unsupported shard type %T", shard)
	}
}

func (t *ShardReindexTaskGeneric) OnBeforeLsmInit(ctx context.Context, shard *Shard) (err error) {
	collectionName := shard.Index().Config.ClassName.String()
	shardName := shard.Name()
	logger := t.logger.WithFields(map[string]any{
		"collection": collectionName,
		"shard":      shardName,
		"method":     "OnBeforeLsmInit",
	})
	logger.Info("starting")
	defer func(started time.Time) {
		logger = logger.WithField("took", time.Since(started))
		if err != nil {
			logger.WithError(err).Error("finished with error")
		} else {
			logger.Info("finished")
		}
	}(time.Now())

	if !t.isShardSelected(collectionName, shardName) {
		logger.Debug("different collection/shard selected. nothing to do")
		return nil
	}

	rt, err := t.newReindexTracker(shard.pathLSM())
	if err != nil {
		err = fmt.Errorf("creating reindex tracker: %w", err)
		return err
	}

	rt.checkOverrides(logger, &t.config)

	if rt.IsRollback() {
		// make it so it "survives" the rt.reset()
		t.config.rollback = true
	}

	if rt.IsReset() && rt.IsTidied() {
		rt.reset()
		err = fmt.Errorf("reset was manually triggered")
		return err
	}

	if t.config.conditionalStart && !rt.HasStartCondition() {
		err = fmt.Errorf("conditional start is set, but file trigger is not found")
		return err
	}

	props, err := t.readPropsToReindex(rt)
	if err != nil {
		err = fmt.Errorf("reading reindexable props: %w", err)
		return err
	}

	if t.config.rollback {
		logger.Debugf("rollback started: config=%v, runtime=%v", t.config.rollback, rt.IsRollback())

		if rt.IsTidied() {
			err = fmt.Errorf("rollback: backup buckets are deleted, can not restore")
			return err
		}
		if rt.IsSwapped() {
			if err = t.unswapIngestAndBackupBuckets(ctx, logger, shard, rt, props); err != nil {
				err = fmt.Errorf("rollback: unswapping buckets: %w", err)
				return err
			}
		}
		if err = t.removeReindexBucketsDirs(ctx, logger, shard, props); err != nil {
			err = fmt.Errorf("rollback: removing reindex buckets: %w", err)
			return err
		}
		if err = t.removeIngestBucketsDirs(ctx, logger, shard, props); err != nil {
			err = fmt.Errorf("rollback: removing ingest buckets: %w", err)
			return err
		}
		if err = rt.reset(); err != nil {
			err = fmt.Errorf("rollback: removing migration files: %w", err)
			return err
		}

		logger.Debug("rollback completed")
		return nil
	}

	if len(props) == 0 {
		logger.Debug("no props read. nothing to do")
		return nil
	}

	if err = ctx.Err(); err != nil {
		err = fmt.Errorf("context check (1): %w / %w", err, context.Cause(ctx))
		return err
	}

	isMerged := rt.IsMerged()
	if !isMerged && rt.IsReindexed() {
		if rt.IsPrepended() {
			// Runtime swap already prepended reindex segments into ingest.
			// Just clean up the reindex dirs (may already be gone) and mark merged.
			logger.Debug("reindexed and prepended, not merged. removing reindex dirs")

			if err = t.removeReindexBucketsDirs(ctx, logger, shard, props); err != nil {
				err = fmt.Errorf("removing reindex bucket dirs: %w", err)
				return err
			}
			if err = rt.markMerged(); err != nil {
				err = fmt.Errorf("marking merged after prepend recovery: %w", err)
				return err
			}
		} else {
			logger.Debug("reindexed, not merged. merging buckets")

			if err = t.mergeReindexAndIngestBuckets(ctx, logger, shard, rt, props); err != nil {
				err = fmt.Errorf("merging reindex and ingest buckets:%w", err)
				return err
			}
		}
		isMerged = true
	}

	if err = ctx.Err(); err != nil {
		err = fmt.Errorf("context check (2): %w / %w", err, context.Cause(ctx))
		return err
	}

	isSwapped := rt.IsSwapped()
	isTidied := rt.IsTidied()
	isPrepended := rt.IsPrepended()
	if isMerged {
		if isSwapped {
			if t.config.unswapBuckets && !isPrepended {
				if isTidied {
					logger.Debug("swapped and tidied. can not be unswapped")
				} else {
					logger.Debug("swapped, not tidied. unswapping buckets")

					if err = t.unswapIngestAndBackupBuckets(ctx, logger, shard, rt, props); err != nil {
						err = fmt.Errorf("unswapping ingest and backup buckets:%w", err)
						return err
					}
					isSwapped = false
				}
			}
		} else {
			if isPrepended {
				// Crash recovery for runtime swap: the merge was done via
				// PrependSegmentsFromBucket so the on-disk layout may differ
				// from the restart-based swap (old main may already be renamed
				// to backup for some props). Use a recovery-aware swap.
				logger.Debug("prepended, merged, not swapped. recovering runtime swap")

				if err = t.recoverRuntimeSwapBuckets(ctx, logger, shard, rt, props); err != nil {
					err = fmt.Errorf("recovering runtime swap buckets: %w", err)
					return err
				}
				isSwapped = true
			} else if t.config.swapBuckets {
				logger.Debug("merged, not swapped. swapping buckets")

				if err = t.swapIngestAndBackupBuckets(ctx, logger, shard, rt, props); err != nil {
					err = fmt.Errorf("swapping ingest and backup buckets:%w", err)
					return err
				}
				isSwapped = true
			}
		}
	}

	if err = ctx.Err(); err != nil {
		err = fmt.Errorf("context check (3): %w / %w", err, context.Cause(ctx))
		return err
	}

	if isSwapped {
		if isTidied {
			// Runtime swap completed: in-memory swap done, dirs deferred.
			// FinalizeCompletedMigrations (called before us in shard_init)
			// already renamed the dirs. Nothing left to do.
			logger.Debug("tidied. nothing to do")
			return nil
		}

		if t.config.tidyBuckets {
			logger.Debug("swapped, not tidied. tidying buckets")

			if err = t.tidyBackupBuckets(ctx, logger, shard, rt, props); err != nil {
				err = fmt.Errorf("tidying backup buckets:%w", err)
				return err
			}
		}
	}

	return nil
}

func (t *ShardReindexTaskGeneric) OnAfterLsmInit(ctx context.Context, shard *Shard) (err error) {
	collectionName := shard.Index().Config.ClassName.String()
	shardName := shard.Name()
	logger := t.logger.WithFields(map[string]any{
		"collection": collectionName,
		"shard":      shardName,
		"method":     "OnAfterLsmInit",
	})
	logger.Info("starting")
	defer func(started time.Time) {
		logger = logger.WithField("took", time.Since(started))
		if err != nil {
			logger.WithError(err).Error("finished with error")
		} else {
			logger.Info("finished")
		}
	}(time.Now())

	// skip shard only if not started or rollback requested
	// otherwise double writes have to be enabled if migration was already started
	isShardSelected := t.isShardSelected(collectionName, shardName)

	if t.config.rollback && isShardSelected {
		logger.Debug("rollback. nothing to do")
		return nil
	}

	rt, err := t.newReindexTracker(shard.pathLSM())
	if err != nil {
		err = fmt.Errorf("creating reindex tracker: %w", err)
		return err
	}

	rt.checkOverrides(logger, &t.config)

	if rt.IsRollback() {
		logger.Debug("rollback. nothing to do")
		return err
	}

	if t.config.conditionalStart && !rt.HasStartCondition() {
		err = fmt.Errorf("conditional start is set, but file trigger is not found")
		return err
	}

	isStarted := rt.IsStarted()
	if !isStarted && !isShardSelected {
		logger.Debug("different collection/shard selected. nothing to do")
		return nil
	}

	props, err := t.getPropsToReindex(shard, rt)
	if err != nil {
		err = fmt.Errorf("getting reindexable props: %w", err)
		return err
	}
	logger.WithField("props", props).Debug("props found")
	if len(props) == 0 {
		logger.Debug("no props found. nothing to do")
		return nil
	}

	if !isStarted {
		if err = rt.markStarted(time.Now()); err != nil {
			err = fmt.Errorf("marking reindex started: %w", err)
			return err
		}
	}

	if rt.IsSwapped() {
		if !rt.IsTidied() {
			logger.Debug("swapped, not tidied. starting backup buckets")

			if err = t.loadBackupBuckets(ctx, logger, shard, props); err != nil {
				err = fmt.Errorf("starting backup buckets:%w", err)
				return err
			}
			if err = t.registerDoubleWriteCallbacks(shard, props, t.backupBucketName, false); err != nil {
				err = fmt.Errorf("registering backup callbacks:%w", err)
				return err
			}
		}
	} else {
		isMerged := rt.IsMerged()
		if isMerged {
			logger.Debug("merged, not swapped. starting ingest buckets")
		} else {
			if !rt.IsReindexed() {
				logger.Debug("not reindexed. starting reindex buckets")

				if err = t.loadReindexBuckets(ctx, logger, shard, props); err != nil {
					err = fmt.Errorf("starting reindex buckets: %w", err)
					return err
				}
			}

			logger.Debug("not merged. starting ingest buckets")
		}

		t.strategy.PreReindexHook(shard, props)

		// since reindex bucket will be merged into ingest bucket with reindex segments being before ingest,
		// ingest segments should not be compacted and tombstones should be kept
		if err = t.loadIngestBuckets(ctx, logger, shard, props, !isMerged, !isMerged); err != nil {
			err = fmt.Errorf("starting ingest buckets:%w", err)
			return err
		}
		if err = t.registerDoubleWriteCallbacks(shard, props, t.ingestBucketName, true); err != nil {
			err = fmt.Errorf("registering ingest callbacks:%w", err)
			return err
		}
	}

	return nil
}

func (t *ShardReindexTaskGeneric) OnAfterLsmInitAsync(ctx context.Context, shard ShardLike,
) (rerunAt time.Time, reloadShard bool, err error) {
	collectionName := shard.Index().Config.ClassName.String()
	shardName := shard.Name()
	logger := t.logger.WithFields(map[string]any{
		"collection": collectionName,
		"shard":      shardName,
		"method":     "OnAfterLsmInitAsync",
	})
	logger.Info("starting")
	defer func(started time.Time) {
		logger = logger.WithField("took", time.Since(started))
		if err != nil {
			logger.WithError(err).Error("finished with error")
		} else {
			logger.Info("finished")
		}
	}(time.Now())

	zerotime := time.Time{}

	if !t.isShardSelected(collectionName, shardName) {
		logger.Debug("different collection/shard selected. nothing to do")
		return zerotime, false, nil
	}

	rt, err := t.newReindexTracker(shard.pathLSM())
	if err != nil {
		err = fmt.Errorf("creating reindex tracker: %w", err)
		return zerotime, false, err
	}

	rt.checkOverrides(logger, &t.config)

	// rollback initiated by the user after restart, stop double writes
	if rt.IsRollback() {
		logger.Debug("rollback started")
		props, err2 := t.readPropsToReindex(rt)
		if err2 != nil {
			err = fmt.Errorf("reading reindexable props for rollback: %w", err2)
			return zerotime, false, err
		}
		err = nil

		if !rt.IsSwapped() {
			err = t.unloadReindexBuckets(ctx, logger, shard, props)
			if err != nil {
				err = fmt.Errorf("unloading reindex buckets: %w", err)
				return zerotime, false, err
			}
			logger.Info("reindex buckets unloaded")
			err = t.unloadIngestBuckets(ctx, logger, shard, props)
			if err != nil {
				err = fmt.Errorf("unloading ingest buckets: %w", err)
				return zerotime, false, err
			}
			logger.Info("ingest buckets unloaded")
		} else {
			logger.Warnf("inverted bucket is being used for search, will not be unloaded: %s. Rollback will proceed on restart", shard.Name())
		}
		// return early to stop ingestion
		return zerotime, false, nil
	}

	if t.config.rollback {
		logger.Debug("rollback. nothing to do")
		return zerotime, false, nil
	}

	if t.config.conditionalStart && !rt.HasStartCondition() {
		err = fmt.Errorf("conditional start is set, but file trigger is not found")
		return zerotime, false, err
	}

	props, err := t.readPropsToReindex(rt)
	if err != nil {
		err = fmt.Errorf("reading reindexable props: %w", err)
		return zerotime, false, err
	}

	if rt.IsPaused() {
		logger.Debug("paused. waiting for resuming")
		return time.Now().Add(t.config.pauseDuration), false, nil
	}

	if rt.IsTidied() {
		err = t.strategy.OnMigrationComplete(ctx, shard.Index().Config.ClassName.String())
		if err != nil {
			err = fmt.Errorf("updating inverted index config: %w", err)
		}
		return zerotime, false, err
	}

	if len(props) == 0 {
		logger.Debug("no props read. nothing to do")
		return zerotime, false, nil
	}

	if rt.IsReindexed() {
		logger.Debug("reindexed. nothing to do")
		return zerotime, false, nil
	}

	var reindexStarted time.Time
	if !rt.IsStarted() {
		err = fmt.Errorf("missing reindex started")
		return zerotime, false, err
	} else if reindexStarted, err = rt.getStarted(); err != nil {
		err = fmt.Errorf("getting reindex started: %w", err)
		return zerotime, false, err
	}

	var lastStoredKey indexKey
	if lastStoredKey, _, err = rt.GetProgress(); err != nil {
		err = fmt.Errorf("getting reindex progress: %w", err)
		return zerotime, false, err
	}

	logger.WithFields(map[string]any{
		"last_stored_key": lastStoredKey,
		"reindex_started": reindexStarted,
	}).Debug("reindexing")

	if err = ctx.Err(); err != nil {
		err = fmt.Errorf("context check (1): %w / %w", err, context.Cause(ctx))
		return zerotime, false, err
	}

	processedCount := 0
	indexedCount := 0
	lastProcessedKey := lastStoredKey.Clone()

	defer func() {
		if err != nil && !bytes.Equal(lastStoredKey.Bytes(), lastProcessedKey.Bytes()) {
			logger.WithField("last_processed_key", lastProcessedKey).Debug("marking progress on error")
			rt.markProgress(lastProcessedKey, processedCount, indexedCount)
		}
	}()

	store := shard.Store()
	propExtraction := storobj.NewPropExtraction()
	bucketsByPropName := map[string]*lsmkv.Bucket{}
	for _, prop := range props {
		propExtraction.Add(prop)
		bucketName := t.reindexBucketName(prop)
		bucketsByPropName[prop] = store.Bucket(bucketName)
	}

	breakCh := make(chan bool, 1)
	breakCh <- false
	finished := false

	// Flush the objects bucket so all objects are on disk before the
	// CursorOnDisk scan begins. In the restart-based flow, this isn't needed
	// because the shard shutdown flushes all memtables. In the runtime path,
	// objects may still be in the active memtable.
	objectsBucket := store.Bucket(helpers.ObjectsBucketLSM)
	if objectsBucket != nil {
		if err = objectsBucket.FlushAndSwitch(); err != nil {
			err = fmt.Errorf("flushing objects bucket before reindex: %w", err)
			return zerotime, false, err
		}
	}

	err = store.PauseObjectBucketCompaction(ctx)
	if err != nil {
		return zerotime, false, err
	}
	defer store.ResumeObjectBucketCompaction(ctx)

	processingStarted, mdCh := t.objectsIteratorAsync(logger, shard, lastStoredKey, t.keyParser.FromBytes,
		propExtraction, reindexStarted, breakCh)

	for md := range mdCh {
		if md == nil {
			finished = true
		} else if md.err != nil {
			err = md.err
			return zerotime, false, err
		} else if err = ctx.Err(); err != nil {
			breakCh <- true
			err = fmt.Errorf("context check (loop): %w / %w", err, context.Cause(ctx))
			return zerotime, false, err
		} else {
			if len(md.props) > 0 {
				for _, invprop := range md.props {
					if bucket, ok := bucketsByPropName[invprop.Name]; ok {
						if err := t.strategy.WriteToReindexBucket(shard, bucket, md.docID, invprop); err != nil {
							breakCh <- true
							err = fmt.Errorf("adding object '%s' prop '%s': %w", md.key.String(), invprop.Name, err)
							return zerotime, false, err
						}
					}
				}
				indexedCount++
			}
			processedCount++
			lastProcessedKey = md.key

			breakCh <- processedCount%t.config.checkProcessingEveryNoObjects == 0 && (time.Since(processingStarted) > t.config.processingDuration || rt.IsPaused())
			time.Sleep(t.config.perObjectDelay)
		}
	}
	if !bytes.Equal(lastStoredKey.Bytes(), lastProcessedKey.Bytes()) {
		if err := rt.markProgress(lastProcessedKey, processedCount, indexedCount); err != nil {
			err = fmt.Errorf("marking reindex progress: %w", err)
			return zerotime, false, err
		}
		lastStoredKey = lastProcessedKey.Clone()
	}
	if finished {
		if err = rt.markReindexed(); err != nil {
			err = fmt.Errorf("marking reindexed: %w", err)
			return zerotime, false, err
		}
		if t.config.reloadShards {
			return zerotime, true, nil
		}
		if t.skipSwapOnFinish {
			logger.Info("reindex complete (swap deferred for barrier)")
			return zerotime, false, nil
		}
		// Runtime swap: merge, swap, tidy all inline — no shard restart needed.
		if err = t.runtimeSwap(ctx, logger, shard, rt, props); err != nil {
			err = fmt.Errorf("runtime swap: %w", err)
			return zerotime, false, err
		}
		return zerotime, false, nil
	}
	return time.Now().Add(t.config.pauseDuration), false, nil
}

// runtimeSwap performs the merge→swap→tidy lifecycle inline after the reindex
// iteration completes, without requiring a shard restart.
//
// Steps:
//  1. Shut down the reindex bucket (makes its segments immutable for prepend).
//  2. Prepend reindex segments into the ingest bucket.
//  3. Atomically swap the ingest bucket pointer into the main bucket slot.
//  4. Shut down the old main bucket, rename its dir to _bak for crash safety.
//  5. Disable double-write callbacks (no longer needed).
//  6. Remove the reindex bucket directory.
//  7. Mark tidied and call OnMigrationComplete.
//
// On crash at any step, OnBeforeLsmInit on next restart will pick up from the
// last completed sentinel state (reindexed/merged/swapped) and finish.
func (t *ShardReindexTaskGeneric) runtimeSwap(ctx context.Context,
	logger logrus.FieldLogger, shard ShardLike, rt reindexTracker, props []string,
) error {
	store := shard.Store()
	lsmPath := shard.pathLSM()

	for _, propName := range props {
		reindexName := t.reindexBucketName(propName)
		ingestName := t.ingestBucketName(propName)

		reindexBucket := store.Bucket(reindexName)
		if reindexBucket == nil {
			return fmt.Errorf("reindex bucket %q not found", reindexName)
		}
		ingestBucket := store.Bucket(ingestName)
		if ingestBucket == nil {
			return fmt.Errorf("ingest bucket %q not found", ingestName)
		}

		// Step 1: Flush and shut down the reindex bucket so its segments are
		// immutable and safe to copy.
		if err := reindexBucket.FlushAndSwitch(); err != nil {
			return fmt.Errorf("flushing reindex bucket %q: %w", reindexName, err)
		}
		reindexDir := reindexBucket.GetDir()
		if err := store.ShutdownBucket(ctx, reindexName); err != nil {
			return fmt.Errorf("shutting down reindex bucket %q: %w", reindexName, err)
		}

		// Step 2: Prepend reindex segments into the ingest bucket. After this,
		// ingest contains all reindexed + double-written data.
		if err := ingestBucket.PrependSegmentsFromBucket(ctx, reindexDir); err != nil {
			return fmt.Errorf("prepending segments from %q to %q: %w", reindexName, ingestName, err)
		}
	}

	// Mark prepended before removing the reindex dirs. On crash recovery,
	// OnBeforeLsmInit sees IsPrepended() and skips the file-move merge
	// (segments are already in ingest via prepend).
	if err := rt.markPrepended(); err != nil {
		return fmt.Errorf("marking prepended: %w", err)
	}

	// Remove reindex bucket directories — their segments have been copied
	// into ingest, so the originals are no longer needed.
	if err := t.removeReindexBucketsDirs(ctx, logger, shard, props); err != nil {
		return fmt.Errorf("removing reindex bucket dirs: %w", err)
	}

	if err := rt.markMerged(); err != nil {
		return fmt.Errorf("marking merged: %w", err)
	}
	logger.Debug("runtime swap: all props merged")

	// Step 3: Swap each ingest bucket into the main bucket slot.
	// The old main dir is renamed to the backup suffix (same naming as the
	// restart-based swap) so that crash recovery in OnBeforeLsmInit can pick
	// up from any partially-swapped state using the same tidy logic.
	for _, propName := range props {
		ingestName := t.ingestBucketName(propName)
		mainName := t.strategy.SourceBucketName(propName)
		backupName := t.backupBucketName(propName)
		backupDir := filepath.Join(lsmPath, backupName)

		oldMainBucket, err := store.SwapBucketPointer(ctx, mainName, ingestName)
		if err != nil {
			return fmt.Errorf("swapping bucket pointer %q <- %q: %w", mainName, ingestName, err)
		}

		// Shut down the old main bucket and rename its directory to the backup
		// location. This must happen before markSwappedProp so that on crash
		// recovery the restart path sees the same on-disk layout it expects.
		if err := oldMainBucket.Shutdown(ctx); err != nil {
			return fmt.Errorf("shutting down old main bucket %q: %w", mainName, err)
		}
		oldMainDir := oldMainBucket.GetDir()
		if err := os.Rename(oldMainDir, backupDir); err != nil {
			return fmt.Errorf("renaming old main dir %q -> %q: %w", oldMainDir, backupDir, err)
		}

		if err := rt.markSwappedProp(propName); err != nil {
			return fmt.Errorf("marking swapped prop %q: %w", propName, err)
		}
	}

	if err := rt.markSwapped(); err != nil {
		return fmt.Errorf("marking swapped: %w", err)
	}
	logger.Debug("runtime swap: all props swapped")

	// Step 5: Disable double-write callbacks — writes now go directly to the
	// (formerly ingest) main bucket.
	t.disableCallbacks()

	// Step 6: Mark tidied. The in-memory swap is complete — the ingest bucket
	// is now serving queries under the main bucket name. The directory on disk
	// still has the ingest name, and the old main dir is renamed to _bak.
	// These filesystem renames (ingest→main, remove _bak) happen on next
	// startup in OnBeforeLsmInit, which reads the sentinel files and performs
	// the renames BEFORE bucket loading. This keeps the runtime swap
	// side-effect-free on the filesystem, avoiding issues with renaming
	// directories of live/initialized buckets.
	if err := rt.markTidied(); err != nil {
		return fmt.Errorf("marking tidied: %w", err)
	}
	logger.Debug("runtime swap: tidy complete (fs cleanup deferred to next restart)")

	// Step 7: Signal migration complete (e.g. update schema).
	if err := t.strategy.OnMigrationComplete(ctx, shard.Index().Config.ClassName.String()); err != nil {
		return fmt.Errorf("on migration complete: %w", err)
	}
	logger.Info("runtime swap: migration complete")

	return nil
}

// -----------------------------------------------------------------------------
// Bucket operations
// -----------------------------------------------------------------------------

func (t *ShardReindexTaskGeneric) reindexBucketName(propName string) string {
	return t.strategy.SourceBucketName(propName) + t.strategy.ReindexSuffix()
}

func (t *ShardReindexTaskGeneric) ingestBucketName(propName string) string {
	return t.strategy.SourceBucketName(propName) + t.strategy.IngestSuffix()
}

func (t *ShardReindexTaskGeneric) backupBucketName(propName string) string {
	return t.strategy.SourceBucketName(propName) + t.strategy.BackupSuffix()
}

func (t *ShardReindexTaskGeneric) mergeReindexAndIngestBuckets(ctx context.Context,
	logger logrus.FieldLogger, shard *Shard, rt reindexTracker, props []string,
) error {
	lsmPath := shard.pathLSM()
	segmentPathsToMove := [][2]string{}
	bucketPathsToRemove := make([]string, 0, len(props))
	lock := new(sync.Mutex)

	eg, gctx := enterrors.NewErrorGroupWithContextWrapper(logger, ctx)
	eg.SetLimit(t.config.concurrency)
	for i := range props {
		propName := props[i]

		eg.Go(func() error {
			reindexBucketName := t.reindexBucketName(propName)
			reindexBucketPath := filepath.Join(lsmPath, reindexBucketName)
			ingestBucketName := t.ingestBucketName(propName)
			ingestBucketPath := filepath.Join(lsmPath, ingestBucketName)

			for {
				propSegmentPathsToMove, needsRecover, err := t.getSegmentPathsToMove(reindexBucketPath, ingestBucketPath)
				if err != nil {
					return fmt.Errorf("buckets '%s' & '%s': %w", reindexBucketName, ingestBucketName, err)
				}

				if needsRecover {
					if err := t.recoverReindexBucket(gctx, logger, shard, reindexBucketName); err != nil {
						return err
					}
				} else {
					lock.Lock()
					bucketPathsToRemove = append(bucketPathsToRemove, reindexBucketPath)
					segmentPathsToMove = append(segmentPathsToMove, propSegmentPathsToMove...)
					lock.Unlock()
					return nil
				}
			}
		})
	}
	if err := eg.Wait(); err != nil {
		return err
	}

	logger.WithField("segments_to_move", segmentPathsToMove).WithField("buckets_to_remove", bucketPathsToRemove).
		Debug("merging reindex and ingest buckets")

	eg, _ = enterrors.NewErrorGroupWithContextWrapper(logger, ctx)
	eg.SetLimit(t.config.concurrency)
	for i := range segmentPathsToMove {
		i := i
		eg.Go(func() error {
			return os.Rename(segmentPathsToMove[i][0], segmentPathsToMove[i][1])
		})
	}
	if err := eg.Wait(); err != nil {
		return fmt.Errorf("moving segment: %w", err)
	}

	eg, _ = enterrors.NewErrorGroupWithContextWrapper(logger, ctx)
	eg.SetLimit(t.config.concurrency)
	for i := range bucketPathsToRemove {
		i := i
		eg.Go(func() error {
			return os.RemoveAll(bucketPathsToRemove[i])
		})
	}
	if err := eg.Wait(); err != nil {
		return fmt.Errorf("removing bucket: %w", err)
	}

	if err := rt.markMerged(); err != nil {
		return fmt.Errorf("marking reindex merged: %w", err)
	}
	return nil
}

func (t *ShardReindexTaskGeneric) getSegmentPathsToMove(bucketPathSrc, bucketPathDst string,
) ([][2]string, bool, error) {
	segmentPaths := [][2]string{}
	needsRecover := false

	err := filepath.WalkDir(bucketPathSrc, func(path string, d os.DirEntry, err error) error {
		if d.IsDir() {
			return nil
		}
		if isSegmentDb(d.Name()) || isSegmentBloom(d.Name()) {
			ext := filepath.Ext(d.Name())
			idAndData := strings.Split(strings.TrimSuffix(strings.TrimPrefix(d.Name(), "segment-"), ext), ".")
			timestamp, err := strconv.ParseInt(idAndData[0], 10, 64)
			if err != nil {
				return err
			}
			timestampPast := time.Unix(0, timestamp).AddDate(-23, 0, 0).UnixNano()
			idAndData[0] = strconv.FormatInt(timestampPast, 10)
			segmentPaths = append(segmentPaths, [2]string{
				path, filepath.Join(bucketPathDst, fmt.Sprintf("segment-%s%s", strings.Join(idAndData, "."), ext)),
			})
		} else if isSegmentWal(d.Name()) {
			if info, err := d.Info(); err != nil {
				return err
			} else if info.Size() > 0 {
				needsRecover = true
				return filepath.SkipAll
			}
		}
		return nil
	})
	if err != nil {
		return nil, false, err
	}
	if needsRecover {
		return nil, true, nil
	}
	return segmentPaths, false, nil
}

// recoverRuntimeSwapBuckets handles crash recovery for a runtime swap that
// was interrupted. For each property, the on-disk state may be:
//
//   - main exists, ingest exists, no backup → swap not started → do full swap
//   - main gone, ingest exists, backup exists → halfway: main renamed to backup
//     but ingest not yet renamed → rename ingest to main
//   - main exists (from ingest→main rename), backup exists → swap done, just
//     needs markSwappedProp
//
// Props already marked via markSwappedProp are skipped.
func (t *ShardReindexTaskGeneric) recoverRuntimeSwapBuckets(ctx context.Context,
	logger logrus.FieldLogger, shard ShardLike, rt reindexTracker, props []string,
) error {
	lsmPath := shard.pathLSM()

	for _, propName := range props {
		if rt.IsSwappedProp(propName) {
			continue
		}

		mainName := t.strategy.SourceBucketName(propName)
		mainDir := filepath.Join(lsmPath, mainName)
		ingestDir := filepath.Join(lsmPath, t.ingestBucketName(propName))
		backupDir := filepath.Join(lsmPath, t.backupBucketName(propName))

		mainExists := dirExists(mainDir)
		backupExists := dirExists(backupDir)

		switch {
		case mainExists && !backupExists:
			// Swap not started for this prop. Do the full rename sequence.
			if err := os.Rename(mainDir, backupDir); err != nil {
				return fmt.Errorf("recovery rename main->backup for %q: %w", propName, err)
			}
			if err := os.Rename(ingestDir, mainDir); err != nil {
				return fmt.Errorf("recovery rename ingest->main for %q: %w", propName, err)
			}
		case !mainExists && backupExists:
			// Main was renamed to backup, but ingest not yet renamed to main.
			if err := os.Rename(ingestDir, mainDir); err != nil {
				return fmt.Errorf("recovery rename ingest->main for %q: %w", propName, err)
			}
		case mainExists && backupExists:
			// Both exist — ingest was already renamed to main. Nothing to do.
		default:
			return fmt.Errorf("unexpected state for prop %q: main=%v backup=%v", propName, mainExists, backupExists)
		}

		if err := rt.markSwappedProp(propName); err != nil {
			return fmt.Errorf("marking swapped prop %q: %w", propName, err)
		}
	}

	if err := rt.markSwapped(); err != nil {
		return fmt.Errorf("marking swapped: %w", err)
	}
	return nil
}

func (t *ShardReindexTaskGeneric) swapIngestAndBackupBuckets(ctx context.Context,
	logger logrus.FieldLogger, shard ShardLike, rt reindexTracker, props []string,
) error {
	lsmPath := shard.pathLSM()

	eg, _ := enterrors.NewErrorGroupWithContextWrapper(logger, ctx)
	eg.SetLimit(t.config.concurrency)
	for i := range props {
		propName := props[i]

		if !rt.IsSwappedProp(props[i]) {
			eg.Go(func() error {
				bucketName := t.strategy.SourceBucketName(propName)
				bucketPath := filepath.Join(lsmPath, bucketName)
				ingestBucketName := t.ingestBucketName(propName)
				ingestBucketPath := filepath.Join(lsmPath, ingestBucketName)
				backupBucketName := t.backupBucketName(propName)
				backupBucketPath := filepath.Join(lsmPath, backupBucketName)

				logger.WithFields(map[string]any{
					"bucket":        bucketName,
					"ingest_bucket": ingestBucketName,
					"backup_bucket": backupBucketName,
				}).Debug("swapping buckets")

				if err := os.Rename(bucketPath, backupBucketPath); err != nil {
					return err
				}
				if err := os.Rename(ingestBucketPath, bucketPath); err != nil {
					return err
				}
				if err := rt.markSwappedProp(propName); err != nil {
					return fmt.Errorf("marking reindex swapped for '%s': %w", propName, err)
				}
				return nil
			})
		}
	}
	if err := eg.Wait(); err != nil {
		return err
	}
	if err := rt.markSwapped(); err != nil {
		return fmt.Errorf("marking reindex swapped: %w", err)
	}

	logger.Debug("swapped buckets")

	return nil
}

func (t *ShardReindexTaskGeneric) unswapIngestAndBackupBuckets(ctx context.Context,
	logger logrus.FieldLogger, shard ShardLike, rt reindexTracker, props []string,
) error {
	lsmPath := shard.pathLSM()

	eg, _ := enterrors.NewErrorGroupWithContextWrapper(logger, ctx)
	eg.SetLimit(t.config.concurrency)
	for i := range props {
		propName := props[i]

		if rt.IsSwappedProp(props[i]) {
			eg.Go(func() error {
				bucketName := t.strategy.SourceBucketName(propName)
				bucketPath := filepath.Join(lsmPath, bucketName)
				ingestBucketName := t.ingestBucketName(propName)
				ingestBucketPath := filepath.Join(lsmPath, ingestBucketName)
				backupBucketName := t.backupBucketName(propName)
				backupBucketPath := filepath.Join(lsmPath, backupBucketName)

				logger.WithFields(map[string]any{
					"bucket":        bucketName,
					"ingest_bucket": ingestBucketName,
					"backup_bucket": backupBucketName,
				}).Debug("unswapping buckets")

				if err := os.Rename(bucketPath, ingestBucketPath); err != nil {
					return err
				}
				if err := os.Rename(backupBucketPath, bucketPath); err != nil {
					return err
				}
				if err := rt.unmarkSwappedProp(propName); err != nil {
					return fmt.Errorf("unmarking reindex swapped for '%s': %w", propName, err)
				}
				return nil
			})
		}
	}
	if err := eg.Wait(); err != nil {
		return err
	}
	if err := rt.unmarkSwapped(); err != nil {
		return fmt.Errorf("unmarking reindex swapped: %w", err)
	}

	logger.Debug("unswapped buckets")

	return nil
}

func (t *ShardReindexTaskGeneric) tidyBackupBuckets(ctx context.Context,
	logger logrus.FieldLogger, shard ShardLike, rt reindexTracker, props []string,
) error {
	lsmPath := shard.pathLSM()

	eg, _ := enterrors.NewErrorGroupWithContextWrapper(logger, ctx)
	eg.SetLimit(t.config.concurrency)
	for i := range props {
		propName := props[i]

		eg.Go(func() error {
			bucketName := t.backupBucketName(propName)
			bucketPath := filepath.Join(lsmPath, bucketName)
			if err := os.RemoveAll(bucketPath); err != nil {
				return err
			}
			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		return err
	}
	if err := rt.markTidied(); err != nil {
		return fmt.Errorf("marking reindex tidied: %w", err)
	}
	return nil
}

func (t *ShardReindexTaskGeneric) loadReindexBuckets(ctx context.Context,
	logger logrus.FieldLogger, shard *Shard, props []string,
) error {
	bucketOpts := t.bucketOptions(shard, t.strategy.TargetStrategy(), false, false, t.config.memtableOptFactor)
	return t.loadBuckets(ctx, logger, shard, props, t.reindexBucketName, bucketOpts)
}

func (t *ShardReindexTaskGeneric) loadIngestBuckets(ctx context.Context,
	logger logrus.FieldLogger, shard *Shard, props []string,
	keepLevelCompaction, keepTombstones bool,
) error {
	bucketOpts := t.bucketOptions(shard, t.strategy.TargetStrategy(), keepLevelCompaction, keepTombstones, t.config.memtableOptFactor)
	return t.loadBuckets(ctx, logger, shard, props, t.ingestBucketName, bucketOpts)
}

func (t *ShardReindexTaskGeneric) loadBackupBuckets(ctx context.Context,
	logger logrus.FieldLogger, shard *Shard, props []string,
) error {
	bucketOpts := t.bucketOptions(shard, t.strategy.BackupStrategy(), false, false, t.config.backupMemtableOptFactor)
	return t.loadBuckets(ctx, logger, shard, props, t.backupBucketName, bucketOpts)
}

func (t *ShardReindexTaskGeneric) loadBuckets(ctx context.Context,
	logger logrus.FieldLogger, shard ShardLike, props []string, bucketNamer func(string) string,
	bucketOpts []lsmkv.BucketOption,
) error {
	store := shard.Store()

	eg, gctx := enterrors.NewErrorGroupWithContextWrapper(logger, ctx)
	eg.SetLimit(t.config.concurrency)
	for i := range props {
		propName := props[i]

		eg.Go(func() error {
			bucketName := bucketNamer(propName)
			logger.WithField("bucket", bucketName).Debug("loading bucket")
			if err := store.CreateOrLoadBucket(gctx, bucketName, bucketOpts...); err != nil {
				return err
			}
			logger.WithField("bucket", bucketName).Debug("bucket loaded")
			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		return err
	}
	return nil
}

func (t *ShardReindexTaskGeneric) unloadIngestBuckets(ctx context.Context,
	logger logrus.FieldLogger, shard ShardLike, props []string,
) error {
	return t.unloadBuckets(ctx, logger, shard, props, t.ingestBucketName)
}

func (t *ShardReindexTaskGeneric) unloadReindexBuckets(ctx context.Context,
	logger logrus.FieldLogger, shard ShardLike, props []string,
) error {
	return t.unloadBuckets(ctx, logger, shard, props, t.reindexBucketName)
}

func (t *ShardReindexTaskGeneric) unloadBuckets(ctx context.Context,
	logger logrus.FieldLogger, shard ShardLike, props []string, bucketNamer func(string) string,
) error {
	store := shard.Store()

	eg, gctx := enterrors.NewErrorGroupWithContextWrapper(logger, ctx)
	eg.SetLimit(t.config.concurrency)
	for i := range props {
		propName := props[i]

		eg.Go(func() error {
			bucketName := bucketNamer(propName)
			logger.WithField("bucket", bucketName).Debug("unloading bucket")
			if err := store.ShutdownBucket(gctx, bucketName); err != nil {
				return err
			}
			logger.WithField("bucket", bucketName).Debug("bucket unloaded")
			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		return err
	}
	return nil
}

func (t *ShardReindexTaskGeneric) recoverReindexBucket(ctx context.Context,
	logger logrus.FieldLogger, shard *Shard, bucketName string,
) error {
	store := shard.Store()
	bucketOpts := t.bucketOptions(shard, t.strategy.TargetStrategy(), true, false, t.config.memtableOptFactor)

	logger.WithField("bucket", bucketName).Debug("recover wals, loading bucket")
	if err := store.CreateOrLoadBucket(ctx, bucketName, bucketOpts...); err != nil {
		return fmt.Errorf("bucket '%s': %w", bucketName, err)
	}
	logger.WithField("bucket", bucketName).Debug("recover wals, shutting down bucket")
	if err := store.ShutdownBucket(ctx, bucketName); err != nil {
		return fmt.Errorf("bucket '%s': %w", bucketName, err)
	}
	logger.WithField("bucket", bucketName).Debug("recover wals, shut down bucket")

	return nil
}

func (t *ShardReindexTaskGeneric) removeReindexBucketsDirs(ctx context.Context, logger logrus.FieldLogger,
	shard ShardLike, props []string,
) error {
	return t.removeBucketsDirs(ctx, logger, shard, props, t.reindexBucketName)
}

func (t *ShardReindexTaskGeneric) removeIngestBucketsDirs(ctx context.Context, logger logrus.FieldLogger,
	shard ShardLike, props []string,
) error {
	return t.removeBucketsDirs(ctx, logger, shard, props, t.ingestBucketName)
}

func (t *ShardReindexTaskGeneric) removeBucketsDirs(ctx context.Context, logger logrus.FieldLogger,
	shard ShardLike, props []string, bucketNamer func(string) string,
) error {
	lsmPath := shard.pathLSM()
	eg, _ := enterrors.NewErrorGroupWithContextWrapper(logger, ctx)
	eg.SetLimit(t.config.concurrency)
	for i := range props {
		propName := props[i]

		eg.Go(func() error {
			bucketName := bucketNamer(propName)
			bucketPath := filepath.Join(lsmPath, bucketName)

			logger.WithField("bucket", bucketName).Debug("removing bucket")

			return os.RemoveAll(bucketPath)
		})
	}
	return eg.Wait()
}

func dirExists(path string) bool {
	info, err := os.Stat(path)
	return err == nil && info.IsDir()
}

func (t *ShardReindexTaskGeneric) registerDoubleWriteCallbacks(shard *Shard, props []string,
	bucketNamer func(string) string, forTargetStrategy bool,
) error {
	propsByName := map[string]struct{}{}
	for i := range props {
		propsByName[props[i]] = struct{}{}
	}

	disableAdd := shard.registerAddToPropertyValueIndex(
		t.strategy.MakeAddCallback(bucketNamer, propsByName, forTargetStrategy))
	disableDelete := shard.registerDeleteFromPropertyValueIndex(
		t.strategy.MakeDeleteCallback(bucketNamer, propsByName, forTargetStrategy))

	t.callbackDisableFuncsMu.Lock()
	t.callbackDisableFuncs = append(t.callbackDisableFuncs, disableAdd, disableDelete)
	t.callbackDisableFuncsMu.Unlock()

	return nil
}

// disableCallbacks calls all stored callback disable functions collected
// during registerDoubleWriteCallbacks, then clears the list.
func (t *ShardReindexTaskGeneric) disableCallbacks() {
	t.callbackDisableFuncsMu.Lock()
	defer t.callbackDisableFuncsMu.Unlock()

	for _, fn := range t.callbackDisableFuncs {
		fn()
	}
	t.callbackDisableFuncs = nil
}

func (t *ShardReindexTaskGeneric) bucketOptions(shard *Shard, strategy string,
	keepLevelCompaction, keepTombstones bool, memtableOptFactor int,
) []lsmkv.BucketOption {
	cfg := shard.Index().Config

	return shard.makeDefaultBucketOptions(strategy,
		lsmkv.WithKeepLevelCompaction(keepLevelCompaction),
		lsmkv.WithKeepTombstones(keepTombstones),
		// overwrite DynamicMemtableSizing
		lsmkv.WithDynamicMemtableSizing(
			memtableOptFactor*cfg.MemtablesInitialSizeMB,
			memtableOptFactor*cfg.MemtablesMaxSizeMB,
			memtableOptFactor*cfg.MemtablesMinActiveSeconds,
			memtableOptFactor*cfg.MemtablesMaxActiveSeconds,
		),
	)
}

// -----------------------------------------------------------------------------
// Property discovery and selection
// -----------------------------------------------------------------------------

func (t *ShardReindexTaskGeneric) findPropsToReindex(shard ShardLike) (props []string, save bool) {
	collectionName := shard.Index().Config.ClassName.String()
	shardName := shard.Name()
	propNames := []string{}

	if !t.isShardSelected(collectionName, shardName) {
		return propNames, false
	}

	checkPropSelected := func(propName string) bool { return true }
	if t.config.selectionEnabled {
		if selectedProps := t.config.selectedPropsByCollection[collectionName]; len(selectedProps) > 0 {
			checkPropSelected = func(propName string) bool {
				_, ok := selectedProps[propName]
				return ok
			}
		}
	}

	for name, bucket := range shard.Store().GetBucketsByName() {
		if bucket.Strategy() == t.strategy.SourceStrategy() {
			propName, indexType := GetPropNameAndIndexTypeFromBucketName(name)

			if indexType == t.strategy.SourceIndexType() && checkPropSelected(propName) {
				propNames = append(propNames, propName)
			}
		}
	}
	return propNames, true
}

func (t *ShardReindexTaskGeneric) getPropsToReindex(shard ShardLike, rt reindexTracker) ([]string, error) {
	if rt.HasProps() {
		props, err := rt.GetProps()
		if err != nil {
			return nil, err
		}
		return props, nil
	}
	props, save := t.findPropsToReindex(shard)
	if save {
		if err := rt.saveProps(props); err != nil {
			return nil, err
		}
	}
	return props, nil
}

func (t *ShardReindexTaskGeneric) readPropsToReindex(rt reindexTracker) ([]string, error) {
	if rt.HasProps() {
		props, err := rt.GetProps()
		if err != nil {
			return nil, err
		}
		return props, nil
	}
	return []string{}, nil
}

func (t *ShardReindexTaskGeneric) isShardSelected(collectionName, shardName string) bool {
	if t.config.selectionEnabled {
		selectedShards, isCollectionSelected := t.config.selectedShardsByCollection[collectionName]
		if !isCollectionSelected {
			return false
		}

		if len(selectedShards) > 0 {
			if _, isShardSelected := selectedShards[shardName]; !isShardSelected {
				return false
			}
		}
	}
	return true
}

// -----------------------------------------------------------------------------
// Segment helpers
// -----------------------------------------------------------------------------

func isSegmentDb(filename string) bool {
	return strings.HasPrefix(filename, "segment-") && strings.HasSuffix(filename, ".db")
}

func isSegmentBloom(filename string) bool {
	return strings.HasPrefix(filename, "segment-") && strings.HasSuffix(filename, ".bloom")
}

func isSegmentWal(filename string) bool {
	return strings.HasPrefix(filename, "segment-") && strings.HasSuffix(filename, ".wal")
}

// -----------------------------------------------------------------------------
// Migration data and object iterator
// -----------------------------------------------------------------------------

type migrationData struct {
	key   indexKey
	docID uint64
	props []inverted.Property
	err   error
}

type objectsIteratorAsync func(logger logrus.FieldLogger, shard ShardLike, lastKey indexKey, keyParse func([]byte) indexKey, propExtraction *storobj.PropertyExtraction, reindexStarted time.Time, breakCh <-chan bool,
) (time.Time, <-chan *migrationData)

func uuidObjectsIteratorAsync(logger logrus.FieldLogger, shard ShardLike, lastKey indexKey, keyParse func([]byte) indexKey,
	propExtraction *storobj.PropertyExtraction, reindexStarted time.Time, breakCh <-chan bool,
) (time.Time, <-chan *migrationData) {
	startedCh := make(chan time.Time)
	mdCh := make(chan *migrationData)

	enterrors.GoWrapper(func() {
		cursor := shard.Store().Bucket(helpers.ObjectsBucketLSM).CursorOnDisk()
		defer cursor.Close()

		startedCh <- time.Now() // after cursor created (necessary locks acquired)
		addProps := additional.Properties{}

		var k, v []byte
		if lastKey == nil {
			k, v = cursor.First()
		} else {
			key := lastKey.Bytes()
			k, v = cursor.Seek(key)
			if bytes.Equal(k, key) {
				k, v = cursor.Next()
			}
		}

		for ; k != nil; k, v = cursor.Next() {
			ik := keyParse(k)
			obj, err := storobj.FromBinaryOptional(v, addProps, propExtraction)
			if err != nil {
				mdCh <- &migrationData{err: fmt.Errorf("unmarshalling object '%s': %w", ik.String(), err)}
				break
			}

			if obj.LastUpdateTimeUnix() < reindexStarted.UnixMilli() {
				props, _, err := shard.AnalyzeObject(obj)
				if err != nil {
					mdCh <- &migrationData{err: fmt.Errorf("analyzing object '%s': %w", ik.String(), err)}
					break
				}

				if <-breakCh {
					break
				}
				mdCh <- &migrationData{key: ik.Clone(), props: props, docID: obj.DocID}
			} else {
				if <-breakCh {
					break
				}
				mdCh <- &migrationData{key: ik.Clone()}
			}
		}
		if k == nil {
			<-breakCh
			mdCh <- nil
		}
		close(mdCh)
	}, logger)

	return <-startedCh, mdCh
}

// -----------------------------------------------------------------------------
// Index key types
// -----------------------------------------------------------------------------

type indexKey interface {
	String() string
	Bytes() []byte
	Clone() indexKey
}

type uuidBytes []byte

func (b uuidBytes) String() string {
	if b == nil {
		return "nil"
	}
	uid, err := uuid.FromBytes(b)
	if err != nil {
		return err.Error()
	}
	return uid.String()
}

func (b uuidBytes) Bytes() []byte {
	return b
}

func (b uuidBytes) Clone() indexKey {
	buf := make([]byte, len(b))
	copy(buf, b)
	return uuidBytes(buf)
}

type indexKeyParser interface {
	FromString(key string) (indexKey, error)
	FromBytes(key []byte) indexKey
}

// UuidKeyParser parses index keys as UUIDs.
type UuidKeyParser struct{}

func (p *UuidKeyParser) FromString(key string) (indexKey, error) {
	uid, err := uuid.Parse(key)
	if err != nil {
		return nil, err
	}
	buf, err := uid.MarshalBinary()
	if err != nil {
		return nil, err
	}
	return uuidBytes(buf), nil
}

func (p *UuidKeyParser) FromBytes(key []byte) indexKey {
	return uuidBytes(key)
}

// -----------------------------------------------------------------------------
// Reindex tracker interface and file-based implementation
// -----------------------------------------------------------------------------

type reindexTracker interface {
	HasStartCondition() bool
	IsStarted() bool
	markStarted(time.Time) error
	getStarted() (time.Time, error)

	markProgress(lastProcessedKey indexKey, processedCount, indexedCount int) error
	GetProgress() (indexKey, *time.Time, error)

	IsReindexed() bool
	markReindexed() error

	IsPrepended() bool
	markPrepended() error

	IsMerged() bool
	markMerged() error

	IsSwapped() bool
	markSwapped() error
	unmarkSwapped() error
	IsSwappedProp(propName string) bool
	markSwappedProp(propName string) error
	unmarkSwappedProp(propName string) error

	IsTidied() bool
	markTidied() error

	HasProps() bool
	GetProps() ([]string, error)
	saveProps([]string) error

	IsPaused() bool
	IsRollback() bool
	IsReset() bool

	reset() error

	checkOverrides(logger logrus.FieldLogger, config *reindexTaskConfig)
}

// NewFileReindexTracker creates a file-based reindex tracker under
// <lsmPath>/.migrations/<migrationDirName>/
func NewFileReindexTracker(lsmPath, migrationDirName string, keyParser indexKeyParser) *fileReindexTracker {
	return &fileReindexTracker{
		progressCheckpoint: 1,
		keyParser:          keyParser,
		config: fileReindexTrackerConfig{
			filenameStart:      "start.mig",
			filenameStarted:    "started.mig",
			filenameProgress:   "progress.mig",
			filenameReindexed:  "reindexed.mig",
			filenamePrepended:  "prepended.mig",
			filenameMerged:     "merged.mig",
			filenameSwapped:    "swapped.mig",
			filenameTidied:     "tidied.mig",
			filenameProperties: "properties.mig",
			filenameRollback:   "rollback.mig",
			filenameReset:      "reset.mig",
			filenamePaused:     "paused.mig",
			filenameOverrides:  "overrides.mig",
			migrationPath:      filepath.Join(lsmPath, ".migrations", migrationDirName),
		},
	}
}

type fileReindexTracker struct {
	progressCheckpoint int
	keyParser          indexKeyParser
	config             fileReindexTrackerConfig
}

type fileReindexTrackerConfig struct {
	filenameStart      string
	filenameStarted    string
	filenameProgress   string
	filenameReindexed  string
	filenamePrepended  string
	filenameMerged     string
	filenameSwapped    string
	filenameTidied     string
	filenameProperties string
	filenameRollback   string
	filenameReset      string
	filenamePaused     string
	filenameOverrides  string
	migrationPath      string
}

func (t *fileReindexTracker) init() error {
	if err := os.MkdirAll(t.config.migrationPath, 0o777); err != nil {
		return err
	}
	return nil
}

func (t *fileReindexTracker) HasStartCondition() bool {
	return t.fileExists(t.config.filenameStart)
}

func (t *fileReindexTracker) IsStarted() bool {
	return t.fileExists(t.config.filenameStarted)
}

func (t *fileReindexTracker) markStarted(started time.Time) error {
	return t.createFile(t.config.filenameStarted, []byte(t.encodeTime(started)))
}

func (t *fileReindexTracker) getTime(filePath string) (time.Time, error) {
	path := t.filepath(filePath)
	content, err := os.ReadFile(path)
	if err != nil {
		return time.Time{}, err
	}
	return t.decodeTime(string(content))
}

func (t *fileReindexTracker) getStarted() (time.Time, error) {
	return t.getTime(t.config.filenameStarted)
}

func (t *fileReindexTracker) findLastProgressFile() (string, error) {
	prefix := t.config.filenameProgress + "."
	expectedLen := len(prefix) + 9 // 9 digits

	lastProgressFilename := ""
	err := filepath.WalkDir(t.config.migrationPath, func(path string, d os.DirEntry, err error) error {
		// skip parent and children dirs
		if path != t.config.migrationPath {
			if d.IsDir() {
				return filepath.SkipDir
			}
			if name := d.Name(); len(name) == expectedLen && strings.HasPrefix(name, prefix) {
				lastProgressFilename = name
			}
		}
		return nil
	})

	return lastProgressFilename, err
}

func (t *fileReindexTracker) markProgress(lastProcessedKey indexKey, processedCount, indexedCount int) error {
	filename := fmt.Sprintf("%s.%09d", t.config.filenameProgress, t.progressCheckpoint)
	content := strings.Join([]string{
		t.encodeTime(time.Now()),
		lastProcessedKey.String(),
		fmt.Sprintf("all %d", processedCount),
		fmt.Sprintf("idx %d", indexedCount),
	}, "\n")

	if err := t.createFile(filename, []byte(content)); err != nil {
		return err
	}
	t.progressCheckpoint++
	return nil
}

func (t *fileReindexTracker) GetProgress() (indexKey, *time.Time, error) {
	filename, err := t.findLastProgressFile()
	if err != nil {
		return nil, nil, err
	}
	if filename == "" {
		return t.keyParser.FromBytes(nil), nil, nil
	}

	checkpoint, err := strconv.Atoi(strings.TrimPrefix(filename, t.config.filenameProgress+"."))
	if err != nil {
		return nil, nil, err
	}

	path := t.filepath(filename)
	content, err := os.ReadFile(path)
	if err != nil {
		return nil, nil, err
	}

	split := strings.Split(string(content), "\n")
	key, err := t.keyParser.FromString(split[1])
	if err != nil {
		return nil, nil, err
	}

	timeStr := strings.TrimSpace(split[0])
	if timeStr == "" {
		return key, nil, fmt.Errorf("progress file '%s' is empty", filename)
	}

	tm, err := t.decodeTime(timeStr)
	if err != nil {
		return nil, nil, fmt.Errorf("decoding time from '%s': %w", timeStr, err)
	}

	t.progressCheckpoint = checkpoint + 1
	return key, &tm, nil
}

func (t *fileReindexTracker) parseProgressFile(filename string) (lastProcessedKey indexKey, tm time.Time, allCount int, idxCount int, err error) {
	progressFilePath := filename
	progressFile, err := os.ReadFile(progressFilePath)
	if err != nil {
		err = fmt.Errorf("failed to read %s: %w", progressFilePath, err)
		return lastProcessedKey, tm, allCount, idxCount, err
	}

	if len(progressFile) == 0 {
		err = fmt.Errorf("progress file %s is empty", progressFilePath)
		return lastProcessedKey, tm, allCount, idxCount, err
	}

	progressFileFields := strings.Split(string(progressFile), "\n")
	if len(progressFileFields) != 4 {
		err = fmt.Errorf("progress file %s has unexpected format, expected 4 lines, got %d", progressFilePath, len(progressFileFields))
		return lastProcessedKey, tm, allCount, idxCount, err
	}

	tm, err = t.decodeTime(strings.TrimSpace(progressFileFields[0]))
	if err != nil {
		err = fmt.Errorf("failed to parse timestamp from %s: %w", progressFilePath, err)
		return lastProcessedKey, tm, allCount, idxCount, err
	}

	lastProcessedKey, err = t.keyParser.FromString(progressFileFields[1])
	if err != nil {
		err = fmt.Errorf("failed to parse last processed key from %s: %w", progressFilePath, err)
		return lastProcessedKey, tm, allCount, idxCount, err
	}

	allCount, err = strconv.Atoi(strings.Split(progressFileFields[2], " ")[1])
	if err != nil {
		err = fmt.Errorf("failed to parse objects migrated count from %s: %w", progressFilePath, err)
		return lastProcessedKey, tm, allCount, idxCount, err
	}

	idxCount, err = strconv.Atoi(strings.Split(progressFileFields[3], " ")[1])
	if err != nil {
		err = fmt.Errorf("failed to parse index count from %s: %w", progressFilePath, err)
		return lastProcessedKey, tm, allCount, idxCount, err
	}

	return lastProcessedKey, tm, allCount, idxCount, err
}

func (t *fileReindexTracker) GetMigratedCount() (objectsMigratedCountTotal int, snapshots []map[string]string, err error) {
	snapshots = make([]map[string]string, 0)
	files, err := os.ReadDir(t.config.migrationPath)
	objectsMigratedCountTotal = 0
	progressCount := 0

	if err != nil {
		return objectsMigratedCountTotal, snapshots, err
	}
	for _, file := range files {
		if strings.HasPrefix(file.Name(), "progress.mig.") {
			snapshot := map[string]string{
				"checkpoint": strings.TrimPrefix(file.Name(), "progress.mig."),
			}
			progressCount++
			progressFilePath := t.config.migrationPath + "/" + file.Name()
			key, tm, allCount, idxCount, err2 := t.parseProgressFile(progressFilePath)
			if err2 != nil {
				err = fmt.Errorf("failed to parse progress file %s: %w", progressFilePath, err2)
				return objectsMigratedCountTotal, snapshots, err
			}

			objectsMigratedCountTotal += allCount
			snapshot["lastProcessedKey"] = key.String()
			snapshot["timestamp"] = tm.Format(time.RFC3339)
			snapshot["allCount"] = fmt.Sprintf("%d", allCount)
			snapshot["idxCount"] = fmt.Sprintf("%d", idxCount)
			snapshots = append(snapshots, snapshot)
		}
	}
	return objectsMigratedCountTotal, snapshots, err
}

func (t *fileReindexTracker) IsReindexed() bool {
	return t.fileExists(t.config.filenameReindexed)
}

func (t *fileReindexTracker) markReindexed() error {
	return t.createFile(t.config.filenameReindexed, []byte(t.encodeTimeNow()))
}

func (t *fileReindexTracker) getReindexed() (time.Time, error) {
	return t.getTime(t.config.filenameReindexed)
}

func (t *fileReindexTracker) IsPrepended() bool {
	return t.fileExists(t.config.filenamePrepended)
}

func (t *fileReindexTracker) markPrepended() error {
	return t.createFile(t.config.filenamePrepended, []byte(t.encodeTimeNow()))
}

func (t *fileReindexTracker) IsMerged() bool {
	return t.fileExists(t.config.filenameMerged)
}

func (t *fileReindexTracker) markMerged() error {
	return t.createFile(t.config.filenameMerged, []byte(t.encodeTimeNow()))
}

func (t *fileReindexTracker) getMerged() (time.Time, error) {
	return t.getTime(t.config.filenameMerged)
}

func (t *fileReindexTracker) IsSwappedProp(propName string) bool {
	return t.fileExists(t.config.filenameSwapped + "." + propName)
}

func (t *fileReindexTracker) markSwappedProp(propName string) error {
	return t.createFile(t.config.filenameSwapped+"."+propName, []byte(t.encodeTimeNow()))
}

func (t *fileReindexTracker) unmarkSwappedProp(propName string) error {
	return t.removeFile(t.config.filenameSwapped + "." + propName)
}

func (t *fileReindexTracker) IsSwapped() bool {
	return t.fileExists(t.config.filenameSwapped)
}

func (t *fileReindexTracker) markSwapped() error {
	return t.createFile(t.config.filenameSwapped, []byte(t.encodeTimeNow()))
}

func (t *fileReindexTracker) unmarkSwapped() error {
	return t.removeFile(t.config.filenameSwapped)
}

func (t *fileReindexTracker) getSwapped() (time.Time, error) {
	return t.getTime(t.config.filenameSwapped)
}

func (t *fileReindexTracker) IsTidied() bool {
	return t.fileExists(t.config.filenameTidied)
}

func (t *fileReindexTracker) getTidied() (time.Time, error) {
	return t.getTime(t.config.filenameTidied)
}

func (t *fileReindexTracker) markTidied() error {
	return t.createFile(t.config.filenameTidied, []byte(t.encodeTimeNow()))
}

func (t *fileReindexTracker) filepath(filename string) string {
	return filepath.Join(t.config.migrationPath, filename)
}

func (t *fileReindexTracker) fileExists(filename string) bool {
	if _, err := os.Stat(t.filepath(filename)); err == nil {
		return true
	} else if errors.Is(err, os.ErrNotExist) {
		return false
	}
	return false
}

func (t *fileReindexTracker) createFile(filename string, content []byte) error {
	path := t.filepath(filename)
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o777)
	if err != nil {
		return err
	}
	defer file.Close()

	if len(content) > 0 {
		_, err = file.Write(content)
		return err
	}
	return nil
}

func (t *fileReindexTracker) removeFile(filename string) error {
	if err := os.Remove(t.filepath(filename)); err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	return nil
}

func (t *fileReindexTracker) encodeTimeNow() string {
	return t.encodeTime(time.Now())
}

func (t *fileReindexTracker) encodeTime(tm time.Time) string {
	return tm.UTC().Format(time.RFC3339Nano)
}

func (t *fileReindexTracker) decodeTime(tm string) (time.Time, error) {
	return time.Parse(time.RFC3339Nano, tm)
}

func (t *fileReindexTracker) HasProps() bool {
	return t.fileExists(t.config.filenameProperties)
}

func (t *fileReindexTracker) saveProps(propNames []string) error {
	props := []byte(strings.Join(propNames, ","))
	return t.createFile(t.config.filenameProperties, props)
}

func (t *fileReindexTracker) GetProps() ([]string, error) {
	content, err := os.ReadFile(t.filepath(t.config.filenameProperties))
	if err != nil {
		return nil, err
	}
	if len(content) == 0 {
		return []string{}, nil
	}
	return strings.Split(strings.TrimSpace(string(content)), ","), nil
}

func (t *fileReindexTracker) IsReset() bool {
	return t.fileExists(t.config.filenameReset)
}

func (t *fileReindexTracker) reset() error {
	return os.RemoveAll(t.config.migrationPath)
}

func (t *fileReindexTracker) IsRollback() bool {
	return t.fileExists(t.config.filenameRollback)
}

func (t *fileReindexTracker) IsPaused() bool {
	return t.fileExists(t.config.filenamePaused)
}

func (t *fileReindexTracker) GetStatusStrings() (status string, message string, action string) {
	if !t.IsStarted() {
		status = "not started"
		message = "reindexing not started"
		action = "enable relevant REINDEX_MAP_TO_BLOCKMAX_* env vars"
		if t.HasStartCondition() {
			message = "reindexing will start on next restart"
			action = "restart"
		}
		return status, message, action
	}
	message = "reindexing started"
	action = "wait"

	if !t.HasProps() {
		status = "computing properties"
		message = "computing properties to reindex"
		return status, message, action
	}

	count, _, err := t.GetMigratedCount()
	if err != nil {
		status = "error"
		message = fmt.Sprintf("failed to get migrated count: %v", err)
		return status, message, action
	}

	status = "in progress"

	if count == 0 {
		message = "reindexing just started, no snapshots yet"
	}

	if t.IsReindexed() {
		status = "reindexed"
		message = "reindexing done, needs restart to merge buckets"
		action = "restart"
	}

	if t.IsPrepended() {
		status = "prepended"
		message = "reindexing done, segments prepended at runtime"
		action = "wait"
	}

	if t.IsMerged() {
		status = "merged"
		message = "reindexing done, buckets merged"
		action = "restart"
	}

	if t.IsSwapped() {
		status = "swapped"
		message = "reindexing done, buckets swapped"
		action = "restart"
	}

	if t.IsPaused() {
		status = "paused"
		message = "reindexing paused, needs resume or rollback"
		action = "resume or rollback"
	}

	if t.IsRollback() {
		status = "rollback"
		message = "reindexing rollback in progress, will finish on next restart"
		action = "restart"
	}

	if t.IsTidied() {
		status = "tidied"
		message = "reindexing done, buckets tidied"
		action = "nothing to do"
	}

	return status, message, action
}

func (t *fileReindexTracker) GetTimes() map[string]string {
	times := map[string]string{}

	started, err := t.getStarted()
	if err != nil {
		times["started"] = ""
	} else {
		times["started"] = t.encodeTime(started)
	}
	_, tm, _ := t.GetProgress()
	if tm == nil {
		times["reindexSnapshot"] = ""
	} else {
		times["reindexSnapshot"] = t.encodeTime(*tm)
	}

	reindexed, err := t.getReindexed()
	if err != nil {
		times["reindexFinished"] = ""
	} else {
		times["reindexFinished"] = t.encodeTime(reindexed)
	}
	merged, err := t.getMerged()
	if err != nil {
		times["merged"] = ""
	} else {
		times["merged"] = t.encodeTime(merged)
	}

	swapped, err := t.getSwapped()
	if err != nil {
		times["swapped"] = ""
	} else {
		times["swapped"] = t.encodeTime(swapped)
	}

	tidied, err := t.getTidied()
	if err != nil {
		times["tidied"] = ""
	} else {
		times["tidied"] = t.encodeTime(tidied)
	}

	return times
}

func (t *fileReindexTracker) checkOverrides(logger logrus.FieldLogger, config *reindexTaskConfig) {
	if !t.fileExists(t.config.filenameOverrides) {
		return
	}
	if config == nil {
		return
	}
	content, err := os.ReadFile(t.filepath(t.config.filenameOverrides))
	if err != nil {
		return
	}
	lines := strings.Split(strings.TrimSpace(string(content)), "\n")
	if len(lines) == 0 {
		return
	}

	for _, line := range lines {
		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			logger.WithField("line", line).Warn("invalid override line, expected 'key=value'")
			continue
		}
		key := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])
		logger.WithFields(logrus.Fields{
			"key":   key,
			"value": value,
		}).Info("processing override")

		switch key {
		case "swapBuckets":
			config.swapBuckets = entcfg.Enabled(value)
		case "unswapBuckets":
			config.unswapBuckets = entcfg.Enabled(value)
		case "tidyBuckets":
			config.tidyBuckets = entcfg.Enabled(value)
		case "reloadShards":
			config.reloadShards = entcfg.Enabled(value)
		case "rollback":
			config.rollback = entcfg.Enabled(value)
		case "conditionalStart":
			config.conditionalStart = entcfg.Enabled(value)
		case "concurrency":
			concurrency, err := strconv.Atoi(value)
			if err != nil {
				logger.WithField("value", value).Warn("invalid concurrency value, must be an integer")
				continue
			}
			if concurrency <= 0 {
				logger.WithField("value", value).Warn("invalid concurrency value, must be greater than 0")
				continue
			}
			config.concurrency = concurrency
		case "memtableOptBlockmaxFactor", "memtableOptFactor":
			memtableOptFactor, err := strconv.Atoi(value)
			if err != nil {
				logger.WithField("value", value).Warn("invalid memtableOptFactor value, must be an integer")
				continue
			}
			if memtableOptFactor <= 0 {
				logger.WithField("value", value).Warn("invalid memtableOptFactor value, must be greater than 0")
				continue
			}
			config.memtableOptFactor = memtableOptFactor
		case "processingDuration":
			processingDuration, err := time.ParseDuration(value)
			if err != nil {
				logger.WithField("value", value).Warnf("invalid processingDuration value: %v", err)
				continue
			}
			if processingDuration <= 0 {
				logger.WithField("value", value).Warn("invalid processingDuration value, must be greater than 0")
				continue
			}
			config.processingDuration = processingDuration
		case "pauseDuration":
			pauseDuration, err := time.ParseDuration(value)
			if err != nil {
				logger.WithField("value", value).Warnf("invalid pauseDuration value: %v", err)
				continue
			}
			if pauseDuration <= 0 {
				logger.WithField("value", value).Warn("invalid pauseDuration value, must be greater than 0")
				continue
			}
			config.pauseDuration = pauseDuration
		case "perObjectDelay":
			perObjectDelay, err := time.ParseDuration(value)
			if err != nil {
				logger.WithField("value", value).Warnf("invalid perObjectDelay value: %v", err)
				continue
			}
			if perObjectDelay < 0 {
				logger.WithField("value", value).Warn("invalid perObjectDelay value, must be greater than or equal to 0")
				continue
			}
			config.perObjectDelay = perObjectDelay
		case "checkProcessingEveryNoObjects":
			checkProcessingEveryNoObjects, err := strconv.Atoi(value)
			if err != nil {
				logger.WithField("value", value).Warnf("invalid checkProcessingEveryNoObjects value: %v", err)
				continue
			}
			if checkProcessingEveryNoObjects <= 0 {
				logger.WithField("value", value).Warn("invalid checkProcessingEveryNoObjects value, must be greater than 0")
				continue
			}
			config.checkProcessingEveryNoObjects = checkProcessingEveryNoObjects
		default:
			logger.WithField("key", key).Warnf("unknown override key, ignoring: %s", key)
			continue
		}
	}

	logger.WithField("config", fmt.Sprintf("%+v", config)).Debug("reindex config overrides applied")
}

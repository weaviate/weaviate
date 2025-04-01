//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
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
	"github.com/weaviate/weaviate/entities/concurrency"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/storobj"
	schema "github.com/weaviate/weaviate/usecases/schema"
)

func NewShardInvertedReindexTaskMapToBlockmax(logger logrus.FieldLogger,
	swapBuckets, unswapBuckets, tidyBuckets, rollback bool,
	processingDuration, pauseDuration time.Duration,
	schemaManager *schema.Manager,
) *ShardReindexTask_MapToBlockmax {
	name := "MapToBlockmax"
	keyParser := &uuidKeyParser{}
	objectsIteratorAsync := uuidObjectsIteratorAsync

	logger = logger.WithField("task", name)
	newReindexTracker := func(lsmPath string) (mapToBlockmaxReindexTracker, error) {
		rt := newFileMapToBlockmaxReindexTracker(lsmPath, keyParser)
		if err := rt.init(); err != nil {
			return nil, err
		}
		return rt, nil
	}

	concurrentMigratorCpuRatio := 0.5

	// TODO amourao: move to config and clean up code
	if os.Getenv("REINDEX_MAP_TO_BLOCKMAX_CPU_RATIO") != "" {
		// parse float from env var
		cpuRatio, err := strconv.ParseFloat(os.Getenv("REINDEX_MAP_TO_BLOCKMAX_CPU_RATIO"), 64)
		if err == nil {
			concurrentMigratorCpuRatio = cpuRatio
		}

		if err != nil || cpuRatio <= 0 {
			concurrentMigratorCpuRatio = 0.5
			logger.Warn("Invalid REINDEX_MAP_TO_BLOCKMAX_CPU_RATIO value \"%v\", using default of 0.5", os.Getenv("REINDEX_MAP_TO_BLOCKMAX_CPU_RATIO"))
		}
	}

	// defaults to 0.5*NUMCPU, rounded at middle point, but min of 1 if concurrentMigratorCpuRatio > 0
	concurrentMigratorCount := concurrency.TimesFloatNUMCPU(concurrentMigratorCpuRatio)
	logger.WithField("concurrent_migrator_count", concurrentMigratorCount).Info("Concurrent migrator count set")

	return &ShardReindexTask_MapToBlockmax{
		name:                 name,
		logger:               logger,
		newReindexTracker:    newReindexTracker,
		keyParser:            keyParser,
		objectsIteratorAsync: objectsIteratorAsync,
		config: mapToBlockmaxConfig{
			swapBuckets:                   swapBuckets,
			unswapBuckets:                 unswapBuckets,
			tidyBuckets:                   tidyBuckets,
			rollback:                      rollback,
			concurrency:                   concurrentMigratorCount,
			memtableOptBlockmaxFactor:     4,
			processingDuration:            processingDuration,
			pauseDuration:                 pauseDuration,
			checkProcessingEveryNoObjects: 1000,
		},
		schemaManager: schemaManager,
	}
}

type ShardReindexTask_MapToBlockmax struct {
	name                 string
	logger               logrus.FieldLogger
	newReindexTracker    func(lsmPath string) (mapToBlockmaxReindexTracker, error)
	keyParser            indexKeyParser
	objectsIteratorAsync objectsIteratorAsync
	config               mapToBlockmaxConfig
	schemaManager        *schema.Manager
}

type mapToBlockmaxConfig struct {
	swapBuckets                   bool
	unswapBuckets                 bool
	tidyBuckets                   bool
	rollback                      bool
	concurrency                   int
	memtableOptBlockmaxFactor     int
	processingDuration            time.Duration
	pauseDuration                 time.Duration
	checkProcessingEveryNoObjects int
}

func (t *ShardReindexTask_MapToBlockmax) Name() string {
	return t.name
}

func (t *ShardReindexTask_MapToBlockmax) OnBeforeLsmInit(ctx context.Context, shard *Shard) (err error) {
	collectionName := shard.Index().Config.ClassName.String()
	logger := t.logger.WithFields(map[string]any{
		"collection": collectionName,
		"shard":      shard.Name(),
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

	rt, err := t.newReindexTracker(shard.pathLSM())
	if err != nil {
		err = fmt.Errorf("creating reindex tracker: %w", err)
		return
	}

	props, err := t.readPropsToReindex(rt)
	if err != nil {
		err = fmt.Errorf("reading reindexable props: %w", err)
		return
	}

	if t.config.rollback {
		logger.Debug("rollback started")

		if rt.isTidied() {
			err = fmt.Errorf("rollback: searchable map buckets are deleted, can not restore")
			return
		}
		if err = t.unswapIngestAndMapBuckets(ctx, logger, shard, rt, props); err != nil {
			err = fmt.Errorf("rollback: unswapping buckets: %w", err)
			return
		}
		if err = t.removeReindexBucketsDirs(ctx, logger, shard, props); err != nil {
			err = fmt.Errorf("rollback: removing reindex buckets: %w", err)
			return
		}
		if err = t.removeIngestBucketsDirs(ctx, logger, shard, props); err != nil {
			err = fmt.Errorf("rollback: removing ingest buckets: %w", err)
			return
		}
		if err = rt.reset(); err != nil {
			err = fmt.Errorf("rollback: removing migration files: %w", err)
			return
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
		return
	}

	isMerged := rt.isMerged()
	if !isMerged && rt.isReindexed() {
		logger.Debug("reindexed, not merged. merging buckets")

		if err = t.mergeReindexAndIngestBuckets(ctx, logger, shard, rt, props); err != nil {
			err = fmt.Errorf("merging reindex and ingest buckets:%w", err)
			return
		}
		isMerged = true
	}

	if err = ctx.Err(); err != nil {
		err = fmt.Errorf("context check (2): %w / %w", err, context.Cause(ctx))
		return
	}

	isSwapped := rt.isSwapped()
	isTidied := rt.isTidied()
	if isMerged {
		if isSwapped {
			if t.config.unswapBuckets {
				if isTidied {
					logger.Debug("swapped and tidied. can not be unswapped")
				} else {
					logger.Debug("swapped, not tidied. unswapping buckets")

					if err = t.unswapIngestAndMapBuckets(ctx, logger, shard, rt, props); err != nil {
						err = fmt.Errorf("unswapping ingest and map buckets:%w", err)
						return
					}
					isSwapped = false
				}
			}
		} else {
			if t.config.swapBuckets {
				logger.Debug("merged, not swapped. swapping buckets")

				if err = t.swapIngestAndMapBuckets(ctx, logger, shard, rt, props); err != nil {
					err = fmt.Errorf("swapping ingest and map buckets:%w", err)
					return
				}
				isSwapped = true
			}
		}
	}

	if err = ctx.Err(); err != nil {
		err = fmt.Errorf("context check (3): %w / %w", err, context.Cause(ctx))
		return
	}

	if isSwapped {
		if isTidied {
			logger.Debug("tidied. nothing to do")
			return nil
		}

		if t.config.tidyBuckets {
			logger.Debug("swapped, not tidied. tidying buckets")

			if err = t.tidyMapBuckets(ctx, logger, shard, rt, props); err != nil {
				err = fmt.Errorf("tidying map buckets:%w", err)
				return
			}

			err = updateToBlockMaxInvertedIndexConfig(ctx, t.schemaManager, shard.Index().Config.ClassName.String())
			if err != nil {
				err = fmt.Errorf("updating inverted index config: %w", err)
				return err
			}
		}
	}

	return nil
}

func (t *ShardReindexTask_MapToBlockmax) OnAfterLsmInit(ctx context.Context, shard *Shard) (err error) {
	collectionName := shard.Index().Config.ClassName.String()
	logger := t.logger.WithFields(map[string]any{
		"collection": collectionName,
		"shard":      shard.Name(),
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

	if t.config.rollback {
		logger.Debug("rollback. nothing to do")
		return nil
	}

	rt, err := t.newReindexTracker(shard.pathLSM())
	if err != nil {
		err = fmt.Errorf("creating reindex tracker: %w", err)
		return
	}

	props, err := t.getPropsToReindex(shard, rt)
	if err != nil {
		err = fmt.Errorf("getting reindexable props: %w", err)
		return
	}
	logger.WithField("props", props).Debug("props found")
	if len(props) == 0 {
		logger.Debug("no props found. nothing to do")
		return nil
	}

	if rt.isSwapped() {
		if !rt.isTidied() {
			logger.Debug("swapped, not tidied. starting map buckets")

			if err = t.loadMapSearchBuckets(ctx, logger, shard, props); err != nil {
				err = fmt.Errorf("starting map buckets:%w", err)
				return
			}
			if err = t.duplicateToMapBuckets(shard, props); err != nil {
				err = fmt.Errorf("duplicating map buckets:%w", err)
				return
			}
		}
	} else {
		isMerged := rt.isMerged()
		if isMerged {
			logger.Debug("merged, not swapped. starting ingest buckets")
		} else {
			if !rt.isReindexed() {
				logger.Debug("not reindexed. starting reindex buckets")

				if err = t.loadReindexSearchBuckets(ctx, logger, shard, props); err != nil {
					err = fmt.Errorf("starting reindex buckets: %w", err)
					return
				}
			}

			logger.Debug("not merged. starting ingest buckets")
		}

		shard.markSearchableBlockmaxProperties(props...)

		// since reindex bucket will be merged into ingest bucket with reindex segments being before ingest,
		// ingest segments should not be compacted and tombstones should be kept
		if err = t.loadIngestSearchBuckets(ctx, logger, shard, props, !isMerged, !isMerged); err != nil {
			err = fmt.Errorf("starting ingest buckets:%w", err)
			return
		}
		if err = t.duplicateToIngestBuckets(shard, props); err != nil {
			err = fmt.Errorf("duplicating ingest buckets:%w", err)
			return
		}
	}

	return nil
}

func (t *ShardReindexTask_MapToBlockmax) OnAfterLsmInitAsync(ctx context.Context, shard ShardLike,
) (rerunAt time.Time, err error) {
	collectionName := shard.Index().Config.ClassName.String()
	logger := t.logger.WithFields(map[string]any{
		"collection": collectionName,
		"shard":      shard.Name(),
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

	if t.config.rollback {
		logger.Debug("rollback. nothing to do")
		return zerotime, nil
	}

	rt, err := t.newReindexTracker(shard.pathLSM())
	if err != nil {
		err = fmt.Errorf("creating reindex tracker: %w", err)
		return zerotime, err
	}

	props, err := t.readPropsToReindex(rt)
	if err != nil {
		err = fmt.Errorf("reading reindexable props: %w", err)
		return zerotime, err
	}
	if len(props) == 0 {
		logger.Debug("no props read. nothing to do")
		return zerotime, nil
	}

	if rt.isReindexed() {
		logger.Debug("reindexed. nothing to do")
		return zerotime, nil
	}

	reindexStarted := time.Now()
	if rt.isStarted() {
		if reindexStarted, err = rt.getStarted(); err != nil {
			err = fmt.Errorf("getting reindex started: %w", err)
			return zerotime, err
		}
	} else if err = rt.markStarted(reindexStarted); err != nil {
		err = fmt.Errorf("marking reindex started: %w", err)
		return zerotime, err
	}

	var lastStoredKey indexKey
	if lastStoredKey, err = rt.getProgress(); err != nil {
		err = fmt.Errorf("getting reindex progress: %w", err)
		return zerotime, err
	}

	logger.WithFields(map[string]any{
		"last_stored_key": lastStoredKey,
		"reindex_started": reindexStarted,
	}).Debug("reindexing")

	if err = ctx.Err(); err != nil {
		err = fmt.Errorf("context check (1): %w / %w", err, context.Cause(ctx))
		return zerotime, err
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

	processingStarted, mdCh := t.objectsIteratorAsync(logger, shard, lastStoredKey, t.keyParser.FromBytes,
		propExtraction, reindexStarted, breakCh)

	for md := range mdCh {
		if md == nil {
			finished = true
		} else if md.err != nil {
			err = md.err
			return zerotime, err
		} else if err = ctx.Err(); err != nil {
			breakCh <- true
			err = fmt.Errorf("context check (loop): %w / %w", err, context.Cause(ctx))
			return zerotime, err
		} else {
			if len(md.props) > 0 {
				for _, invprop := range md.props {
					if bucket, ok := bucketsByPropName[invprop.Name]; ok {
						propLen := t.calcPropLenInverted(invprop.Items)
						for _, item := range invprop.Items {
							pair := shard.pairPropertyWithFrequency(md.docID, item.TermFrequency, propLen)
							if err := shard.addToPropertyMapBucket(bucket, pair, item.Data); err != nil {
								breakCh <- true
								err = fmt.Errorf("adding object '%s' prop '%s': %w", md.key.String(), invprop.Name, err)
								return zerotime, err
							}
						}
					}
				}
				indexedCount++
			}
			processedCount++
			lastProcessedKey = md.key

			// check execution time every X objects processed to close the cursor and pause shard's migration
			breakCh <- processedCount%t.config.checkProcessingEveryNoObjects == 0 &&
				time.Since(processingStarted) > t.config.processingDuration
		}
	}
	if !bytes.Equal(lastStoredKey.Bytes(), lastProcessedKey.Bytes()) {
		if err := rt.markProgress(lastProcessedKey, processedCount, indexedCount); err != nil {
			err = fmt.Errorf("marking reindex progress: %w", err)
			return zerotime, err
		}
		lastStoredKey = lastProcessedKey.Clone()
	}
	if finished {
		if err = rt.markReindexed(); err != nil {
			err = fmt.Errorf("marking reindexed: %w", err)
			return zerotime, err
		}
		return zerotime, nil
	}
	return time.Now().Add(t.config.pauseDuration), nil
}

func (t *ShardReindexTask_MapToBlockmax) mergeReindexAndIngestBuckets(ctx context.Context,
	logger logrus.FieldLogger, shard ShardLike, rt mapToBlockmaxReindexTracker, props []string,
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

func (t *ShardReindexTask_MapToBlockmax) getSegmentPathsToMove(bucketPathSrc, bucketPathDst string,
) ([][2]string, bool, error) {
	segmentPaths := [][2]string{}
	needsRecover := false

	err := filepath.WalkDir(bucketPathSrc, func(path string, d os.DirEntry, err error) error {
		if d.IsDir() {
			return nil
		}
		if t.isSegmentDb(d.Name()) || t.isSegmentBloom(d.Name()) {
			ext := filepath.Ext(d.Name())
			id := strings.TrimSuffix(strings.TrimPrefix(d.Name(), "segment-"), ext)
			timestamp, err := strconv.ParseInt(id, 10, 64)
			if err != nil {
				return err
			}
			timestampPast := time.Unix(0, timestamp).AddDate(-23, 0, 0).UnixNano()

			segmentPaths = append(segmentPaths, [2]string{
				path, filepath.Join(bucketPathDst, fmt.Sprintf("segment-%d%s", timestampPast, ext)),
			})
		} else if t.isSegmentWal(d.Name()) {
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

func (t *ShardReindexTask_MapToBlockmax) swapIngestAndMapBuckets(ctx context.Context,
	logger logrus.FieldLogger, shard ShardLike, rt mapToBlockmaxReindexTracker, props []string,
) error {
	lsmPath := shard.pathLSM()

	eg, _ := enterrors.NewErrorGroupWithContextWrapper(logger, ctx)
	eg.SetLimit(t.config.concurrency)
	for i := range props {
		propName := props[i]

		if !rt.isSwappedProp(props[i]) {
			eg.Go(func() error {
				bucketName := helpers.BucketSearchableFromPropNameLSM(propName)
				bucketPath := filepath.Join(lsmPath, bucketName)
				ingestBucketName := t.ingestBucketName(propName)
				ingestBucketPath := filepath.Join(lsmPath, ingestBucketName)
				mapBucketName := t.mapBucketName(propName)
				mapBucketPath := filepath.Join(lsmPath, mapBucketName)

				logger.WithFields(map[string]any{
					"bucket":        bucketName,
					"ingest_bucket": ingestBucketName,
					"map_bucket":    mapBucketName,
				}).Debug("swapping buckets")

				if err := os.Rename(bucketPath, mapBucketPath); err != nil {
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

	logger.Debug("swapped searchable buckets")

	return nil
}

func (t *ShardReindexTask_MapToBlockmax) unswapIngestAndMapBuckets(ctx context.Context,
	logger logrus.FieldLogger, shard ShardLike, rt mapToBlockmaxReindexTracker, props []string,
) error {
	lsmPath := shard.pathLSM()

	eg, _ := enterrors.NewErrorGroupWithContextWrapper(logger, ctx)
	eg.SetLimit(t.config.concurrency)
	for i := range props {
		propName := props[i]

		if rt.isSwappedProp(props[i]) {
			eg.Go(func() error {
				bucketName := helpers.BucketSearchableFromPropNameLSM(propName)
				bucketPath := filepath.Join(lsmPath, bucketName)
				ingestBucketName := t.ingestBucketName(propName)
				ingestBucketPath := filepath.Join(lsmPath, ingestBucketName)
				mapBucketName := t.mapBucketName(propName)
				mapBucketPath := filepath.Join(lsmPath, mapBucketName)

				logger.WithFields(map[string]any{
					"bucket":        bucketName,
					"ingest_bucket": ingestBucketName,
					"map_bucket":    mapBucketName,
				}).Debug("unswapping buckets")

				if err := os.Rename(bucketPath, ingestBucketPath); err != nil {
					return err
				}
				if err := os.Rename(mapBucketPath, bucketPath); err != nil {
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

	logger.Debug("unswapped searchable buckets")

	return nil
}

func (t *ShardReindexTask_MapToBlockmax) tidyMapBuckets(ctx context.Context,
	logger logrus.FieldLogger, shard ShardLike, rt mapToBlockmaxReindexTracker, props []string,
) error {
	lsmPath := shard.pathLSM()

	eg, _ := enterrors.NewErrorGroupWithContextWrapper(logger, ctx)
	eg.SetLimit(t.config.concurrency)
	for i := range props {
		propName := props[i]

		eg.Go(func() error {
			bucketName := t.mapBucketName(propName)
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

func (t *ShardReindexTask_MapToBlockmax) loadReindexSearchBuckets(ctx context.Context,
	logger logrus.FieldLogger, shard ShardLike, props []string,
) error {
	bucketOpts := t.bucketOptions(shard, lsmkv.StrategyInverted, false, false, t.config.memtableOptBlockmaxFactor)
	return t.loadBuckets(ctx, logger, shard, props, t.reindexBucketName, bucketOpts)
}

func (t *ShardReindexTask_MapToBlockmax) loadIngestSearchBuckets(ctx context.Context,
	logger logrus.FieldLogger, shard ShardLike, props []string,
	disableCompaction, keepTombstones bool,
) error {
	bucketOpts := t.bucketOptions(shard, lsmkv.StrategyInverted, disableCompaction, keepTombstones, t.config.memtableOptBlockmaxFactor)
	return t.loadBuckets(ctx, logger, shard, props, t.ingestBucketName, bucketOpts)
}

func (t *ShardReindexTask_MapToBlockmax) loadMapSearchBuckets(ctx context.Context,
	logger logrus.FieldLogger, shard ShardLike, props []string,
) error {
	bucketOpts := t.bucketOptions(shard, lsmkv.StrategyMapCollection, false, false, 1)
	return t.loadBuckets(ctx, logger, shard, props, t.mapBucketName, bucketOpts)
}

func (t *ShardReindexTask_MapToBlockmax) loadBuckets(ctx context.Context,
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

func (t *ShardReindexTask_MapToBlockmax) recoverReindexBucket(ctx context.Context,
	logger logrus.FieldLogger, shard ShardLike, bucketName string,
) error {
	store := shard.Store()
	bucketOpts := t.bucketOptions(shard, lsmkv.StrategyInverted, true, false, t.config.memtableOptBlockmaxFactor)

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

func (t *ShardReindexTask_MapToBlockmax) removeReindexBucketsDirs(ctx context.Context, logger logrus.FieldLogger,
	shard ShardLike, props []string,
) error {
	return t.removeBucketsDirs(ctx, logger, shard, props, t.reindexBucketName)
}

func (t *ShardReindexTask_MapToBlockmax) removeIngestBucketsDirs(ctx context.Context, logger logrus.FieldLogger,
	shard ShardLike, props []string,
) error {
	return t.removeBucketsDirs(ctx, logger, shard, props, t.ingestBucketName)
}

func (t *ShardReindexTask_MapToBlockmax) removeBucketsDirs(ctx context.Context, logger logrus.FieldLogger,
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

func (t *ShardReindexTask_MapToBlockmax) duplicateToIngestBuckets(shard *Shard, props []string,
) error {
	return t.duplicateToBuckets(shard, props, t.ingestBucketName, t.calcPropLenInverted)
}

func (t *ShardReindexTask_MapToBlockmax) duplicateToMapBuckets(shard *Shard, props []string,
) error {
	return t.duplicateToBuckets(shard, props, t.mapBucketName, t.calcPropLenMap)
}

func (t *ShardReindexTask_MapToBlockmax) duplicateToBuckets(shard *Shard, props []string,
	bucketNamer func(string) string, calcPropLen func([]inverted.Countable) float32,
) error {
	propsByName := map[string]struct{}{}
	for i := range props {
		propsByName[props[i]] = struct{}{}
	}

	shard.registerAddToPropertyValueIndex(func(s *Shard, docID uint64, property *inverted.Property) error {
		if !property.HasSearchableIndex {
			return nil
		}
		if _, ok := propsByName[property.Name]; !ok {
			return nil
		}

		bucketName := bucketNamer(property.Name)
		bucket := s.store.Bucket(bucketName)
		propLen := calcPropLen(property.Items)
		for _, item := range property.Items {
			pair := s.pairPropertyWithFrequency(docID, item.TermFrequency, propLen)
			if err := s.addToPropertyMapBucket(bucket, pair, item.Data); err != nil {
				return fmt.Errorf("adding prop '%s' to bucket '%s': %w", item.Data, bucketName, err)
			}
		}
		return nil
	})
	shard.registerDeleteFromPropertyValueIndex(func(s *Shard, docID uint64, property *inverted.Property) error {
		if !property.HasSearchableIndex {
			return nil
		}
		if _, ok := propsByName[property.Name]; !ok {
			return nil
		}

		bucketName := bucketNamer(property.Name)
		bucket := s.store.Bucket(bucketName)
		for _, item := range property.Items {
			if err := s.deleteInvertedIndexItemWithFrequencyLSM(bucket, item, docID); err != nil {
				return fmt.Errorf("deleting prop '%s' from bucket '%s': %w", item.Data, bucketName, err)
			}
		}
		return nil
	})

	return nil
}

func (t *ShardReindexTask_MapToBlockmax) isSegmentDb(filename string) bool {
	return strings.HasPrefix(filename, "segment-") && strings.HasSuffix(filename, ".db")
}

func (t *ShardReindexTask_MapToBlockmax) isSegmentBloom(filename string) bool {
	return strings.HasPrefix(filename, "segment-") && strings.HasSuffix(filename, ".bloom")
}

func (t *ShardReindexTask_MapToBlockmax) isSegmentWal(filename string) bool {
	return strings.HasPrefix(filename, "segment-") && strings.HasSuffix(filename, ".wal")
}

func (t *ShardReindexTask_MapToBlockmax) calcPropLenMap(items []inverted.Countable) float32 {
	return float32(len(items))
}

func (t *ShardReindexTask_MapToBlockmax) calcPropLenInverted(items []inverted.Countable) float32 {
	propLen := float32(0)
	for _, item := range items {
		propLen += item.TermFrequency
	}
	return propLen
}

func (t *ShardReindexTask_MapToBlockmax) bucketOptions(shard ShardLike, strategy string,
	disableCompaction, keepTombstones bool, memtableOptFactor int,
) []lsmkv.BucketOption {
	index := shard.Index()

	opts := []lsmkv.BucketOption{
		lsmkv.WithDirtyThreshold(time.Duration(index.Config.MemtablesFlushDirtyAfter) * time.Second),
		lsmkv.WithDynamicMemtableSizing(
			index.Config.MemtablesInitialSizeMB*memtableOptFactor,
			index.Config.MemtablesMaxSizeMB*memtableOptFactor,
			index.Config.MemtablesMinActiveSeconds*memtableOptFactor,
			index.Config.MemtablesMaxActiveSeconds*memtableOptFactor,
		),
		lsmkv.WithPread(index.Config.AvoidMMap),
		lsmkv.WithAllocChecker(index.allocChecker),
		lsmkv.WithMaxSegmentSize(index.Config.MaxSegmentSize),
		lsmkv.WithSegmentsChecksumValidationEnabled(index.Config.LSMEnableSegmentsChecksumValidation),
		lsmkv.WithStrategy(strategy),
		lsmkv.WithDisableCompaction(disableCompaction),
		lsmkv.WithKeepTombstones(keepTombstones),
	}

	if strategy == lsmkv.StrategyMapCollection && shard.Versioner().Version() < 2 {
		opts = append(opts, lsmkv.WithLegacyMapSorting())
	}

	return opts
}

func (t *ShardReindexTask_MapToBlockmax) reindexBucketName(propName string) string {
	return helpers.BucketSearchableFromPropNameLSM(propName) + "__blockmax_reindex"
}

func (t *ShardReindexTask_MapToBlockmax) ingestBucketName(propName string) string {
	return helpers.BucketSearchableFromPropNameLSM(propName) + "__blockmax_ingest"
}

func (t *ShardReindexTask_MapToBlockmax) mapBucketName(propName string) string {
	return helpers.BucketSearchableFromPropNameLSM(propName) + "__blockmax_map"
}

func (t *ShardReindexTask_MapToBlockmax) findPropsToReindex(shard ShardLike) []string {
	propNames := []string{}
	for name, bucket := range shard.Store().GetBucketsByName() {
		if bucket.Strategy() == lsmkv.StrategyMapCollection {
			propName, indexType := GetPropNameAndIndexTypeFromBucketName(name)

			switch indexType {
			case IndexTypePropSearchableValue:
				propNames = append(propNames, propName)
			default:
				// skip remaining types
			}
		}
	}
	return propNames
}

func (t *ShardReindexTask_MapToBlockmax) getPropsToReindex(shard ShardLike, rt mapToBlockmaxReindexTracker) ([]string, error) {
	if rt.hasProps() {
		props, err := rt.getProps()
		if err != nil {
			return nil, err
		}
		return props, nil
	}
	props := t.findPropsToReindex(shard)
	if err := rt.saveProps(props); err != nil {
		return nil, err
	}
	return props, nil
}

func (T *ShardReindexTask_MapToBlockmax) readPropsToReindex(rt mapToBlockmaxReindexTracker) ([]string, error) {
	if rt.hasProps() {
		props, err := rt.getProps()
		if err != nil {
			return nil, err
		}
		return props, nil
	}
	return []string{}, nil
}

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

type mapToBlockmaxReindexTracker interface {
	isStarted() bool
	markStarted(time.Time) error
	getStarted() (time.Time, error)

	markProgress(lastProcessedKey indexKey, processedCount, indexedCount int) error
	getProgress() (indexKey, error)

	isReindexed() bool
	markReindexed() error

	isMerged() bool
	markMerged() error

	isSwapped() bool
	markSwapped() error
	unmarkSwapped() error
	isSwappedProp(propName string) bool
	markSwappedProp(propName string) error
	unmarkSwappedProp(propName string) error

	isTidied() bool
	markTidied() error

	hasProps() bool
	getProps() ([]string, error)
	saveProps([]string) error

	reset() error
}

func newFileMapToBlockmaxReindexTracker(lsmPath string, keyParser indexKeyParser) *fileMapToBlockmaxReindexTracker {
	return &fileMapToBlockmaxReindexTracker{
		progressCheckpoint: 1,
		keyParser:          keyParser,
		config: fileReindexTrackerConfig{
			filenameStarted:    "started.mig",
			filenameProgress:   "progress.mig",
			filenameReindexed:  "reindexed.mig",
			filenameMerged:     "merged.mig",
			filenameSwapped:    "swapped.mig",
			filenameTidied:     "tidied.mig",
			filenameProperties: "properties.mig",
			migrationPath:      filepath.Join(lsmPath, ".migrations", "searchable_map_to_blockmax"),
		},
	}
}

type fileMapToBlockmaxReindexTracker struct {
	progressCheckpoint int
	keyParser          indexKeyParser
	config             fileReindexTrackerConfig
}

type fileReindexTrackerConfig struct {
	filenameStarted    string
	filenameProgress   string
	filenameReindexed  string
	filenameMerged     string
	filenameSwapped    string
	filenameTidied     string
	filenameProperties string
	migrationPath      string
}

func (t *fileMapToBlockmaxReindexTracker) init() error {
	if err := os.MkdirAll(t.config.migrationPath, 0o777); err != nil {
		return err
	}
	return nil
}

func (t *fileMapToBlockmaxReindexTracker) isStarted() bool {
	return t.fileExists(t.config.filenameStarted)
}

func (t *fileMapToBlockmaxReindexTracker) markStarted(started time.Time) error {
	return t.createFile(t.config.filenameStarted, []byte(t.encodeTime(started)))
}

func (t *fileMapToBlockmaxReindexTracker) getStarted() (time.Time, error) {
	path := t.filepath(t.config.filenameStarted)
	content, err := os.ReadFile(path)
	if err != nil {
		return time.Time{}, err
	}
	return t.decodeTime(string(content))
}

func (t *fileMapToBlockmaxReindexTracker) findLastProgressFile() (string, error) {
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

func (t *fileMapToBlockmaxReindexTracker) markProgress(lastProcessedKey indexKey, processedCount, indexedCount int) error {
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

func (t *fileMapToBlockmaxReindexTracker) getProgress() (indexKey, error) {
	filename, err := t.findLastProgressFile()
	if err != nil {
		return nil, err
	}
	if filename == "" {
		return t.keyParser.FromBytes(nil), nil
	}

	checkpoint, err := strconv.Atoi(strings.TrimPrefix(filename, t.config.filenameProgress+"."))
	if err != nil {
		return nil, err
	}

	path := t.filepath(filename)
	content, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	split := strings.Split(string(content), "\n")
	key, err := t.keyParser.FromString(split[1])
	if err != nil {
		return nil, err
	}

	t.progressCheckpoint = checkpoint + 1
	return key, nil
}

func (t *fileMapToBlockmaxReindexTracker) isReindexed() bool {
	return t.fileExists(t.config.filenameReindexed)
}

func (t *fileMapToBlockmaxReindexTracker) markReindexed() error {
	return t.createFile(t.config.filenameReindexed, []byte(t.encodeTimeNow()))
}

func (t *fileMapToBlockmaxReindexTracker) isMerged() bool {
	return t.fileExists(t.config.filenameMerged)
}

func (t *fileMapToBlockmaxReindexTracker) markMerged() error {
	return t.createFile(t.config.filenameMerged, []byte(t.encodeTimeNow()))
}

func (t *fileMapToBlockmaxReindexTracker) isSwappedProp(propName string) bool {
	return t.fileExists(t.config.filenameSwapped + "." + propName)
}

func (t *fileMapToBlockmaxReindexTracker) markSwappedProp(propName string) error {
	return t.createFile(t.config.filenameSwapped+"."+propName, []byte(t.encodeTimeNow()))
}

func (t *fileMapToBlockmaxReindexTracker) unmarkSwappedProp(propName string) error {
	return t.removeFile(t.config.filenameSwapped + "." + propName)
}

func (t *fileMapToBlockmaxReindexTracker) isSwapped() bool {
	return t.fileExists(t.config.filenameSwapped)
}

func (t *fileMapToBlockmaxReindexTracker) markSwapped() error {
	return t.createFile(t.config.filenameSwapped, []byte(t.encodeTimeNow()))
}

func (t *fileMapToBlockmaxReindexTracker) unmarkSwapped() error {
	return t.removeFile(t.config.filenameSwapped)
}

func (t *fileMapToBlockmaxReindexTracker) isTidied() bool {
	return t.fileExists(t.config.filenameTidied)
}

func (t *fileMapToBlockmaxReindexTracker) markTidied() error {
	return t.createFile(t.config.filenameTidied, []byte(t.encodeTimeNow()))
}

func (t *fileMapToBlockmaxReindexTracker) filepath(filename string) string {
	return filepath.Join(t.config.migrationPath, filename)
}

func (t *fileMapToBlockmaxReindexTracker) fileExists(filename string) bool {
	if _, err := os.Stat(t.filepath(filename)); err == nil {
		return true
	} else if errors.Is(err, os.ErrNotExist) {
		return false
	}
	return false
}

func (t *fileMapToBlockmaxReindexTracker) createFile(filename string, content []byte) error {
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

func (t *fileMapToBlockmaxReindexTracker) removeFile(filename string) error {
	return os.Remove(t.filepath(filename))
}

func (t *fileMapToBlockmaxReindexTracker) encodeTimeNow() string {
	return t.encodeTime(time.Now())
}

func (t *fileMapToBlockmaxReindexTracker) encodeTime(tm time.Time) string {
	return tm.UTC().Format(time.RFC3339Nano)
}

func (t *fileMapToBlockmaxReindexTracker) decodeTime(tm string) (time.Time, error) {
	return time.Parse(time.RFC3339Nano, tm)
}

func (t *fileMapToBlockmaxReindexTracker) hasProps() bool {
	return t.fileExists(t.config.filenameProperties)
}

func (t *fileMapToBlockmaxReindexTracker) saveProps(propNames []string) error {
	props := []byte(strings.Join(propNames, ","))
	return t.createFile(t.config.filenameProperties, props)
}

func (t *fileMapToBlockmaxReindexTracker) getProps() ([]string, error) {
	content, err := os.ReadFile(t.filepath(t.config.filenameProperties))
	if err != nil {
		return nil, err
	}
	if len(content) == 0 {
		return []string{}, nil
	}
	return strings.Split(string(content), ","), nil
}

func (t *fileMapToBlockmaxReindexTracker) reset() error {
	return os.RemoveAll(t.config.migrationPath)
}

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

// type uint64Bytes []byte

// func (b uint64Bytes) String() string {
// 	if b == nil {
// 		return "nil"
// 	}
// 	return fmt.Sprint(binary.LittleEndian.Uint64(b))
// }

// func (b uint64Bytes) Bytes() []byte {
// 	return b
// }

// func (b uint64Bytes) Clone() indexKey {
// 	buf := make([]byte, len(b))
// 	copy(buf, b)
// 	return uint64Bytes(buf)
// }

// -----------------------------------------------------------------------------

type indexKeyParser interface {
	FromString(key string) (indexKey, error)
	FromBytes(key []byte) indexKey
}

type uuidKeyParser struct{}

func (p *uuidKeyParser) FromString(key string) (indexKey, error) {
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

func (p *uuidKeyParser) FromBytes(key []byte) indexKey {
	return uuidBytes(key)
}

// type uint64KeyParser struct{}

// func (p *uint64KeyParser) FromString(key string) (indexKey, error) {
// 	u, err := strconv.ParseUint(key, 10, 64)
// 	if err != nil {
// 		return nil, err
// 	}
// 	buf := make([]byte, 8)
// 	binary.LittleEndian.PutUint64(buf, u)
// 	return uint64Bytes(buf), nil
// }

// func (p *uint64KeyParser) FromBytes(key []byte) indexKey {
// 	return uint64Bytes(key)
// }

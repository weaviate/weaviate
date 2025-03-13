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
)

type mapToBlockmaxConfig struct {
	swapBuckets               bool
	tidyBuckets               bool
	concurrency               int
	memtableOptBlockmaxFactor int

	processingInterval            time.Duration
	pauseInterval                 time.Duration
	checkProcessingEveryNoObjects int
}

type ShardInvertedReindexTask_MapToBlockmax struct {
	logger            logrus.FieldLogger
	newReindexTracker func(lsmPath string) (reindexTracker, error)
	keyParser         indexKeyParser
	objectsIterator   func(objectsBucket *lsmkv.Bucket, lastKey indexKey, callback func(key, value []byte) (proceed bool, err error)) (finished bool, err error)
	config            mapToBlockmaxConfig
}

func NewShardInvertedReindexTaskMapToBlockmax(logger logrus.FieldLogger, swapBuckets bool, tidyBuckets bool,
) *ShardInvertedReindexTask_MapToBlockmax {
	keyParser := &uuidKeyParser{}
	objectsIterator := uuidObjectsIterator

	logger = logger.WithField("task", "MapToBlockmax")
	newReindexTracker := func(lsmPath string) (reindexTracker, error) {
		rt := newFileReindexTracker(lsmPath, keyParser)
		if err := rt.init(); err != nil {
			return nil, err
		}
		return rt, nil
	}

	return &ShardInvertedReindexTask_MapToBlockmax{
		logger:            logger,
		newReindexTracker: newReindexTracker,
		keyParser:         keyParser,
		objectsIterator:   objectsIterator,
		config: mapToBlockmaxConfig{
			swapBuckets:                   swapBuckets,
			tidyBuckets:                   tidyBuckets,
			concurrency:                   concurrency.NUMCPU_2,
			memtableOptBlockmaxFactor:     4,
			processingInterval:            15 * time.Minute,
			pauseInterval:                 1 * time.Second,
			checkProcessingEveryNoObjects: 1000,

			// processingInterval:            100 * time.Millisecond,
			// pauseInterval:                 2 * time.Second,
			// checkProcessingEveryNoObjects: 100,
		},
	}
}

func (t *ShardInvertedReindexTask_MapToBlockmax) HasOnBefore() bool {
	return true
}

func (t *ShardInvertedReindexTask_MapToBlockmax) OnBefore(ctx context.Context) (err error) {
	logger := t.logger.WithField("method", "OnBefore")
	logger.Debug("starting")
	defer func(started time.Time) {
		logger = logger.WithField("took", time.Since(started))
		if err != nil {
			logger.WithError(err).Error("finished with error")
		} else {
			logger.Debug("finished")
		}
	}(time.Now())

	return nil
}

func (t *ShardInvertedReindexTask_MapToBlockmax) OnBeforeByShard(ctx context.Context, shard ShardLike) (err error) {
	collectionName := shard.Index().Config.ClassName.String()
	logger := t.logger.WithFields(map[string]any{
		"collection": collectionName,
		"shard":      shard.Name(),
		"method":     "OnBeforeShard",
	})
	logger.Debug("starting")
	defer func(started time.Time) {
		logger = logger.WithField("took", time.Since(started))
		if err != nil {
			logger.WithError(err).Error("finished with error")
		} else {
			logger.Debug("finished")
		}
	}(time.Now())

	rt, err := t.newReindexTracker(shard.pathLSM())
	if err != nil {
		return fmt.Errorf("creating reindex tracker: %w", err)
	}

	props, err := t.getPropsToReindex(shard, rt)
	if err != nil {
		return fmt.Errorf("getting reindexable props: %w", err)
	}
	logger.WithField("props", props).Debug("props found")
	if len(props) == 0 {
		return nil
	}

	if err = ctx.Err(); err != nil {
		return fmt.Errorf("context check (1): %w", err)
	}

	isMerged := rt.isMerged()
	if !isMerged {
		if rt.isReindexed() {
			logger.Debug("reindexed, not merged. merging buckets")

			if err = t.mergeReindexAndIngestBuckets(ctx, logger, shard, rt, props); err != nil {
				return fmt.Errorf("merging reindex and ingest buckets:%w", err)
			}
			isMerged = true
		}
	}

	if err = ctx.Err(); err != nil {
		return fmt.Errorf("context check (2): %w", err)
	}

	isSwapped := rt.isSwapped()
	if t.config.swapBuckets && !isSwapped && isMerged {
		logger.Debug("merged, not swapped. swapping buckets")

		if err = t.swapIngestAndMapBuckets(ctx, logger, shard, rt, props); err != nil {
			return fmt.Errorf("swapping ingest and map buckets:%w", err)
		}
		isSwapped = true
		shard.markSearchableBlockmaxProperties(props...)
	}

	if err = ctx.Err(); err != nil {
		return fmt.Errorf("context check (3): %w", err)
	}

	if isSwapped {
		if !rt.isTidied() {
			if t.config.tidyBuckets {
				logger.Debug("swapped, not tidied. tidying buckets")

				if err = t.tidyMapBuckets(ctx, logger, shard, rt, props); err != nil {
					return fmt.Errorf("tidying map buckets:%w", err)
				}
			} else {
				logger.Debug("swapped, not tidied. starting map buckets")

				if err = t.loadMapSearchBuckets(ctx, logger, shard, props); err != nil {
					return fmt.Errorf("starting map buckets:%w", err)
				}
				if err = t.duplicateToMapBuckets(ctx, logger, shard, props); err != nil {
					return fmt.Errorf("duplicating map buckets:%w", err)
				}
			}
		} else {
			logger.Debug("swapped and tidied. nothing to do")
		}
	} else {
		logger.Debug("merged, not swapped. starting ingest buckets")

		// since reindex bucket will be merged into ingest bucket with reindex segments being before ingest,
		// ingest segments should not be compacted and tombstones kept
		if err = t.loadIngestSearchBuckets(ctx, logger, shard, props, !isMerged, !isMerged); err != nil {
			return fmt.Errorf("starting ingest buckets:%w", err)
		}
		shard.markSearchableBlockmaxProperties(props...)
		if err = t.duplicateToIngestBuckets(ctx, logger, shard, props); err != nil {
			return fmt.Errorf("duplicating ingest buckets:%w", err)
		}
	}

	return nil
}

func (t *ShardInvertedReindexTask_MapToBlockmax) mergeReindexAndIngestBuckets(ctx context.Context,
	logger logrus.FieldLogger, shard ShardLike, rt reindexTracker, props []string,
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
					return fmt.Errorf("buckets %q & %q: %w", reindexBucketName, ingestBucketName, err)
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

	eg, gctx = enterrors.NewErrorGroupWithContextWrapper(logger, ctx)
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

	eg, gctx = enterrors.NewErrorGroupWithContextWrapper(logger, ctx)
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

func (t *ShardInvertedReindexTask_MapToBlockmax) getSegmentPathsToMove(bucketPathSrc, bucketPathDst string,
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

func (t *ShardInvertedReindexTask_MapToBlockmax) swapIngestAndMapBuckets(ctx context.Context,
	logger logrus.FieldLogger, shard ShardLike, rt reindexTracker, props []string,
) error {
	lsmPath := shard.pathLSM()
	store := shard.Store()
	propsToSwap := make([]string, 0, len(props))

	eg, gctx := enterrors.NewErrorGroupWithContextWrapper(logger, ctx)
	eg.SetLimit(t.config.concurrency)
	for i := range props {
		propName := props[i]

		if !rt.isSwappedProp(props[i]) {
			propsToSwap = append(propsToSwap, propName)

			eg.Go(func() error {
				bucketName := helpers.BucketSearchableFromPropNameLSM(propName)

				logger.WithField("bucket", bucketName).Debug("shutting down bucket")
				if err := store.ShutdownBucket(gctx, bucketName); err != nil {
					return err
				}
				return nil
			})
		}
	}
	if err := eg.Wait(); err != nil {
		return err
	}

	logger.Debug("shut down searchable buckets for swap")

	// TODO aliszka:blockmax handle partially swapped buckets?
	eg, gctx = enterrors.NewErrorGroupWithContextWrapper(logger, ctx)
	eg.SetLimit(t.config.concurrency)
	for i := range propsToSwap {
		propName := propsToSwap[i]

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
				return fmt.Errorf("marking reindex swapped for %q: %w", propName, err)
			}
			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		return err
	}
	if err := rt.markSwapped(); err != nil {
		return fmt.Errorf("marking reindex swapped: %w", err)
	}

	logger.Debug("swapped searchable buckets")

	if err := t.loadSwappedSearchBuckets(ctx, logger, shard, props); err != nil {
		return err
	}
	logger.Debug("loaded searchable buckets after swap")

	return nil
}

func (t *ShardInvertedReindexTask_MapToBlockmax) tidyMapBuckets(ctx context.Context,
	logger logrus.FieldLogger, shard ShardLike, rt reindexTracker, props []string,
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

func (t *ShardInvertedReindexTask_MapToBlockmax) loadReindexSearchBuckets(ctx context.Context,
	logger logrus.FieldLogger, shard ShardLike, props []string,
) error {
	bucketOpts := t.bucketOptions(shard, lsmkv.StrategyInverted, false, false, t.config.memtableOptBlockmaxFactor)
	return t.loadBuckets(ctx, logger, shard, props, t.reindexBucketName, bucketOpts)
}

func (t *ShardInvertedReindexTask_MapToBlockmax) loadIngestSearchBuckets(ctx context.Context,
	logger logrus.FieldLogger, shard ShardLike, props []string,
	disableCompaction, keepTombstones bool,
) error {
	bucketOpts := t.bucketOptions(shard, lsmkv.StrategyInverted, disableCompaction, keepTombstones, t.config.memtableOptBlockmaxFactor)
	return t.loadBuckets(ctx, logger, shard, props, t.ingestBucketName, bucketOpts)
}

func (t *ShardInvertedReindexTask_MapToBlockmax) loadMapSearchBuckets(ctx context.Context,
	logger logrus.FieldLogger, shard ShardLike, props []string,
) error {
	bucketOpts := t.bucketOptions(shard, lsmkv.StrategyMapCollection, false, false, 1)
	return t.loadBuckets(ctx, logger, shard, props, t.mapBucketName, bucketOpts)
}

func (t *ShardInvertedReindexTask_MapToBlockmax) loadSwappedSearchBuckets(ctx context.Context,
	logger logrus.FieldLogger, shard ShardLike, props []string,
) error {
	bucketOpts := t.bucketOptions(shard, lsmkv.StrategyInverted, false, false, 1)
	return t.loadBuckets(ctx, logger, shard, props, helpers.BucketSearchableFromPropNameLSM, bucketOpts)
}

func (t *ShardInvertedReindexTask_MapToBlockmax) loadBuckets(ctx context.Context,
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

func (t *ShardInvertedReindexTask_MapToBlockmax) recoverReindexBucket(ctx context.Context,
	logger logrus.FieldLogger, shard ShardLike, bucketName string,
) error {
	store := shard.Store()
	bucketOpts := t.bucketOptions(shard, lsmkv.StrategyInverted, true, false, t.config.memtableOptBlockmaxFactor)

	logger.WithField("bucket", bucketName).Debug("loading bucket")
	if err := store.CreateOrLoadBucket(ctx, bucketName, bucketOpts...); err != nil {
		return fmt.Errorf("bucket %q: %w", bucketName, err)
	}
	logger.WithField("bucket", bucketName).Debug("shutting down bucket")
	if err := store.ShutdownBucket(ctx, bucketName); err != nil {
		return fmt.Errorf("bucket %q: %w", bucketName, err)
	}
	logger.WithField("bucket", bucketName).Debug("shut down bucket")

	return nil
}

func (t *ShardInvertedReindexTask_MapToBlockmax) duplicateToIngestBuckets(ctx context.Context,
	logger logrus.FieldLogger, shard ShardLike, props []string,
) error {
	return t.duplicateToBuckets(ctx, logger, shard, props, t.ingestBucketName, t.calcPropLenInverted)
}

func (t *ShardInvertedReindexTask_MapToBlockmax) duplicateToMapBuckets(ctx context.Context,
	logger logrus.FieldLogger, shard ShardLike, props []string,
) error {
	return t.duplicateToBuckets(ctx, logger, shard, props, t.mapBucketName, t.calcPropLenMap)
}

func (t *ShardInvertedReindexTask_MapToBlockmax) duplicateToBuckets(ctx context.Context,
	logger logrus.FieldLogger, shard ShardLike, props []string, bucketNamer func(string) string,
	calcPropLen func([]inverted.Countable) float32,
) error {
	propsByName := map[string]struct{}{}
	for i := range props {
		propsByName[props[i]] = struct{}{}
	}

	shard.RegisterAddToPropertyValueIndex(func(s *Shard, docID uint64, property *inverted.Property) error {
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
				return fmt.Errorf("adding prop %q to bucket %q: %w", item.Data, bucketName, err)
			}
		}
		return nil
	})
	shard.RegisterDeleteFromPropertyValueIndex(func(s *Shard, docID uint64, property *inverted.Property) error {
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
				return fmt.Errorf("deleting prop %q from bucket %q: %w", item.Data, bucketName, err)
			}
		}
		return nil
	})

	return nil
}

func (t *ShardInvertedReindexTask_MapToBlockmax) isSegmentDb(filename string) bool {
	return strings.HasPrefix(filename, "segment-") && strings.HasSuffix(filename, ".db")
}

func (t *ShardInvertedReindexTask_MapToBlockmax) isSegmentBloom(filename string) bool {
	return strings.HasPrefix(filename, "segment-") && strings.HasSuffix(filename, ".bloom")
}

func (t *ShardInvertedReindexTask_MapToBlockmax) isSegmentWal(filename string) bool {
	return strings.HasPrefix(filename, "segment-") && strings.HasSuffix(filename, ".wal")
}

func (t *ShardInvertedReindexTask_MapToBlockmax) calcPropLenMap(items []inverted.Countable) float32 {
	return float32(len(items))
}

func (t *ShardInvertedReindexTask_MapToBlockmax) calcPropLenInverted(items []inverted.Countable) float32 {
	propLen := float32(0)
	for _, item := range items {
		propLen += item.TermFrequency
	}
	return propLen
}

func (t *ShardInvertedReindexTask_MapToBlockmax) bucketOptions(shard ShardLike, strategy string,
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

func (t *ShardInvertedReindexTask_MapToBlockmax) ReindexByShard(ctx context.Context, shard ShardLike,
) (rerunAt time.Time, err error) {
	collectionName := shard.Index().Config.ClassName.String()
	logger := t.logger.WithFields(map[string]any{
		"collection": collectionName,
		"shard":      shard.Name(),
		"method":     "ReindexByShard",
	})
	logger.Debug("starting")
	defer func(started time.Time) {
		logger = logger.WithField("took", time.Since(started))
		if err != nil {
			logger.WithError(err).Error("finished with error")
		} else {
			logger.Debug("finished")
		}
	}(time.Now())

	zerotime := time.Time{}
	rt, err := t.newReindexTracker(shard.pathLSM())
	if err != nil {
		return zerotime, fmt.Errorf("creating reindex tracker: %w", err)
	}

	props, err := t.getPropsToReindex(shard, rt)
	if err != nil {
		return zerotime, fmt.Errorf("getting reindexable props: %w", err)
	}
	logger.WithField("props", props).Debug("props found")
	if len(props) == 0 {
		return zerotime, nil
	}

	if rt.isReindexed() {
		logger.Debug("reindexed. nothing to do")
		return zerotime, nil
	}

	reindexStarted := time.Now()
	if rt.isStarted() {
		if reindexStarted, err = rt.getStarted(); err != nil {
			return zerotime, fmt.Errorf("getting reindex started: %w", err)
		}
	} else if err = rt.markStarted(reindexStarted); err != nil {
		return zerotime, fmt.Errorf("marking reindex started: %w", err)
	}

	var lastStoredKey indexKey
	if lastStoredKey, err = rt.getProgress(); err != nil {
		return zerotime, fmt.Errorf("getting reindex progress: %w", err)
	}

	logger.WithFields(map[string]any{
		"last_stored_key": lastStoredKey,
		"reindex_started": reindexStarted,
	}).Debug("reindexing")

	if err = ctx.Err(); err != nil {
		return zerotime, fmt.Errorf("context check (1): %w", err)
	}

	if err = t.loadReindexSearchBuckets(ctx, logger, shard, props); err != nil {
		return zerotime, fmt.Errorf("starting reindex buckets: %w", err)
	}

	if err = ctx.Err(); err != nil {
		return zerotime, fmt.Errorf("context check (2): %w", err)
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

	addProps := additional.Properties{}
	propExtraction := storobj.NewPropExtraction()

	store := shard.Store()
	objectsBucket := store.Bucket(helpers.ObjectsBucketLSM)
	bucketsByPropName := map[string]*lsmkv.Bucket{}
	for _, prop := range props {
		propExtraction.Add(prop)
		bucketName := t.reindexBucketName(prop)
		bucketsByPropName[prop] = store.Bucket(bucketName)
	}

	var processingStarted time.Time
	finished, err := t.objectsIterator(objectsBucket, lastStoredKey, func(key, value []byte) (bool, error) {
		if err := ctx.Err(); err != nil {
			return false, fmt.Errorf("context check (iterator): %w", err)
		}
		if processingStarted.IsZero() {
			processingStarted = time.Now()
		}

		ik := t.keyParser.FromBytes(key)
		obj, err := storobj.FromBinaryOptional(value, addProps, propExtraction)
		if err != nil {
			return false, fmt.Errorf("unmarshalling object %q: %w", ik.String(), err)
		}

		if obj.LastUpdateTimeUnix() < reindexStarted.UnixMilli() {
			props, _, err := shard.AnalyzeObject(obj)
			if err != nil {
				return false, fmt.Errorf("analyzing object %q: %w", ik.String(), err)
			}

			for _, invprop := range props {
				if bucket, ok := bucketsByPropName[invprop.Name]; ok {
					propLen := t.calcPropLenInverted(invprop.Items)
					for _, item := range invprop.Items {
						pair := shard.pairPropertyWithFrequency(obj.DocID, item.TermFrequency, propLen)
						if err := shard.addToPropertyMapBucket(bucket, pair, item.Data); err != nil {
							return false, fmt.Errorf("adding object %q prop %q: %w", ik.String(), invprop.Name, err)
						}
					}
				}
			}
			indexedCount++
		}

		lastProcessedKey = ik.Clone()
		processedCount++

		// check execution time every X objects processed to pause
		if processedCount%t.config.checkProcessingEveryNoObjects == 0 {
			if time.Since(processingStarted) > t.config.processingInterval {
				if err := rt.markProgress(lastProcessedKey, processedCount, indexedCount); err != nil {
					return false, fmt.Errorf("marking reindex progress (iterator): %w", err)
				}
				lastStoredKey = lastProcessedKey.Clone()
				processedCount = 0
				indexedCount = 0
				return false, nil
			}
		}
		return true, nil
	})
	if err != nil {
		return zerotime, err
	}
	if !bytes.Equal(lastStoredKey.Bytes(), lastProcessedKey.Bytes()) {
		if err := rt.markProgress(lastProcessedKey, processedCount, indexedCount); err != nil {
			return zerotime, fmt.Errorf("marking reindex progress: %w", err)
		}
		lastStoredKey = lastProcessedKey.Clone()
		processedCount = 0
		indexedCount = 0
	}
	if finished {
		if err = rt.markReindexed(); err != nil {
			return zerotime, fmt.Errorf("marking reindexed: %w", err)
		}
		return zerotime, nil
	}
	return time.Now().Add(t.config.pauseInterval), nil
}

func (t *ShardInvertedReindexTask_MapToBlockmax) reindexBucketName(propName string) string {
	return helpers.BucketSearchableFromPropNameLSM(propName) + "__blockmax_reindex"
}

func (t *ShardInvertedReindexTask_MapToBlockmax) ingestBucketName(propName string) string {
	return helpers.BucketSearchableFromPropNameLSM(propName) + "__blockmax_ingest"
}

func (t *ShardInvertedReindexTask_MapToBlockmax) mapBucketName(propName string) string {
	return helpers.BucketSearchableFromPropNameLSM(propName) + "__blockmax_map"
}

func (t *ShardInvertedReindexTask_MapToBlockmax) findPropsToReindex(shard ShardLike) []string {
	propNames := []string{}
	for name, bucket := range shard.Store().GetBucketsByName() {
		if bucket.Strategy() == lsmkv.StrategyMapCollection && bucket.DesiredStrategy() == lsmkv.StrategyInverted {
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

func (t *ShardInvertedReindexTask_MapToBlockmax) getPropsToReindex(shard ShardLike, rt reindexTracker) ([]string, error) {
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

func uuidObjectsIterator(objectsBucket *lsmkv.Bucket, lastKey indexKey,
	callback func(key, value []byte) (proceed bool, err error),
) (finished bool, err error) {
	cursor := objectsBucket.Cursor()
	defer cursor.Close()

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
		proceed, err := callback(k, v)
		if err != nil {
			return false, err
		}
		if !proceed {
			k, _ = cursor.Next()
			break
		}
	}
	return k == nil, nil
}

type reindexTracker interface {
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
	isSwappedProp(propName string) bool
	markSwappedProp(propName string) error

	isTidied() bool
	markTidied() error

	hasProps() bool
	getProps() ([]string, error)
	saveProps([]string) error
}

func newFileReindexTracker(lsmPath string, keyParser indexKeyParser) *fileReindexTracker {
	return &fileReindexTracker{
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

type fileReindexTracker struct {
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

func (t *fileReindexTracker) init() error {
	if err := os.MkdirAll(t.config.migrationPath, 0o777); err != nil {
		return err
	}
	return nil
}

func (t *fileReindexTracker) isStarted() bool {
	return t.fileExists(t.config.filenameStarted)
}

func (t *fileReindexTracker) markStarted(started time.Time) error {
	return t.createFile(t.config.filenameStarted, []byte(t.encodeTime(started)))
}

func (t *fileReindexTracker) getStarted() (time.Time, error) {
	path := t.filepath(t.config.filenameStarted)
	content, err := os.ReadFile(path)
	if err != nil {
		return time.Time{}, err
	}
	return t.decodeTime(string(content))
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

func (t *fileReindexTracker) getProgress() (indexKey, error) {
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

func (t *fileReindexTracker) isReindexed() bool {
	return t.fileExists(t.config.filenameReindexed)
}

func (t *fileReindexTracker) markReindexed() error {
	return t.createFile(t.config.filenameReindexed, []byte(t.encodeTimeNow()))
}

func (t *fileReindexTracker) isMerged() bool {
	return t.fileExists(t.config.filenameMerged)
}

func (t *fileReindexTracker) markMerged() error {
	return t.createFile(t.config.filenameMerged, []byte(t.encodeTimeNow()))
}

func (t *fileReindexTracker) isSwappedProp(propName string) bool {
	return t.fileExists(t.config.filenameSwapped + "." + propName)
}

func (t *fileReindexTracker) markSwappedProp(propName string) error {
	return t.createFile(t.config.filenameSwapped+"."+propName, []byte(t.encodeTimeNow()))
}

func (t *fileReindexTracker) isSwapped() bool {
	return t.fileExists(t.config.filenameSwapped)
}

func (t *fileReindexTracker) markSwapped() error {
	return t.createFile(t.config.filenameSwapped, []byte(t.encodeTimeNow()))
}

func (t *fileReindexTracker) isTidied() bool {
	return t.fileExists(t.config.filenameTidied)
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

func (t *fileReindexTracker) encodeTimeNow() string {
	return t.encodeTime(time.Now())
}

func (t *fileReindexTracker) encodeTime(tm time.Time) string {
	return tm.UTC().Format(time.RFC3339Nano)
}

func (t *fileReindexTracker) decodeTime(tm string) (time.Time, error) {
	return time.Parse(time.RFC3339Nano, tm)
}

func (t *fileReindexTracker) hasProps() bool {
	return t.fileExists(t.config.filenameProperties)
}

func (t *fileReindexTracker) saveProps(propNames []string) error {
	props := []byte(strings.Join(propNames, ","))
	return t.createFile(t.config.filenameProperties, props)
}

func (t *fileReindexTracker) getProps() ([]string, error) {
	content, err := os.ReadFile(t.filepath(t.config.filenameProperties))
	if err != nil {
		return nil, err
	}
	if len(content) == 0 {
		return []string{}, nil
	}
	return strings.Split(string(content), ","), nil
}

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

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
	entcfg "github.com/weaviate/weaviate/entities/config"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/config"
	schema "github.com/weaviate/weaviate/usecases/schema"
)

func NewShardInvertedReindexTaskMapToBlockmax(logger logrus.FieldLogger,
	swapBuckets, unswapBuckets, tidyBuckets, reloadShards, rollback, conditionalStart bool,
	processingDuration, pauseDuration time.Duration, perObjectDelay time.Duration, concurrency int,
	cptSelected []config.CollectionPropsTenants, schemaManager *schema.Manager,
) *ShardReindexTask_MapToBlockmax {
	name := "MapToBlockmax"
	keyParser := &UuidKeyParser{}
	objectsIteratorAsync := uuidObjectsIteratorAsync

	logger = logger.WithField("task", name)
	newReindexTracker := func(lsmPath string) (mapToBlockmaxReindexTracker, error) {
		rt := NewFileMapToBlockmaxReindexTracker(lsmPath, keyParser)
		if err := rt.init(); err != nil {
			return nil, err
		}
		return rt, nil
	}

	selectionEnabled := false
	var selectedPropsByCollection, selectedShardsByCollection map[string]map[string]struct{}
	if count := len(cptSelected); count > 0 {
		selectionEnabled = true
		selectedPropsByCollection = make(map[string]map[string]struct{}, count)
		selectedShardsByCollection = make(map[string]map[string]struct{}, count)

		for _, cpt := range cptSelected {
			var props, shards map[string]struct{}
			if countp := len(cpt.Props); countp > 0 {
				props = make(map[string]struct{}, countp)
				for _, prop := range cpt.Props {
					props[prop] = struct{}{}
				}
			}
			if counts := len(cpt.Tenants); counts > 0 {
				shards = make(map[string]struct{}, counts)
				for _, shard := range cpt.Tenants {
					shards[shard] = struct{}{}
				}
			}
			selectedPropsByCollection[cpt.Collection] = props
			selectedShardsByCollection[cpt.Collection] = shards
		}
	}

	config := mapToBlockmaxConfig{
		swapBuckets:                   swapBuckets,
		unswapBuckets:                 unswapBuckets,
		tidyBuckets:                   tidyBuckets,
		reloadShards:                  reloadShards,
		rollback:                      rollback,
		concurrency:                   concurrency,
		conditionalStart:              conditionalStart,
		memtableOptBlockmaxFactor:     4,
		processingDuration:            processingDuration,
		pauseDuration:                 pauseDuration,
		checkProcessingEveryNoObjects: 1000,
		selectionEnabled:              selectionEnabled,
		selectedPropsByCollection:     selectedPropsByCollection,
		selectedShardsByCollection:    selectedShardsByCollection,
		perObjectDelay:                perObjectDelay,
	}

	logger.WithField("config", fmt.Sprintf("%+v", config)).Debug("task created")

	return &ShardReindexTask_MapToBlockmax{
		name:                 name,
		logger:               logger,
		newReindexTracker:    newReindexTracker,
		keyParser:            keyParser,
		objectsIteratorAsync: objectsIteratorAsync,
		schemaManager:        schemaManager,
		config:               config,
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
	reloadShards                  bool
	rollback                      bool
	conditionalStart              bool
	concurrency                   int
	memtableOptBlockmaxFactor     int
	processingDuration            time.Duration
	pauseDuration                 time.Duration
	perObjectDelay                time.Duration
	checkProcessingEveryNoObjects int
	selectionEnabled              bool
	selectedPropsByCollection     map[string]map[string]struct{}
	selectedShardsByCollection    map[string]map[string]struct{}
}

func (t *ShardReindexTask_MapToBlockmax) Name() string {
	return t.name
}

func (t *ShardReindexTask_MapToBlockmax) OnBeforeLsmInit(ctx context.Context, shard *Shard) (err error) {
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
		return
	}

	rt.checkOverrides(logger, &t.config)

	if rt.IsRollback() {
		// make it so it "survives" the rt.reset()
		t.config.rollback = true
	}

	if t.config.conditionalStart && !rt.HasStartCondition() {
		err = fmt.Errorf("conditional start is set, but file trigger is not found")
		return
	}

	props, err := t.readPropsToReindex(rt)
	if err != nil {
		err = fmt.Errorf("reading reindexable props: %w", err)
		return
	}

	if t.config.rollback {
		logger.Debugf("rollback started: config=%v, runtime=%v", t.config.rollback, rt.IsRollback())

		if rt.IsTidied() {
			err = fmt.Errorf("rollback: searchable map buckets are deleted, can not restore")
			return
		}
		if rt.IsSwapped() {
			if err = t.unswapIngestAndMapBuckets(ctx, logger, shard, rt, props); err != nil {
				err = fmt.Errorf("rollback: unswapping buckets: %w", err)
				return
			}
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

	isMerged := rt.IsMerged()
	if !isMerged && rt.IsReindexed() {
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

	isSwapped := rt.IsSwapped()
	isTidied := rt.IsTidied()
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
		}
	}

	return nil
}

func (t *ShardReindexTask_MapToBlockmax) OnAfterLsmInit(ctx context.Context, shard *Shard) (err error) {
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
		return
	}

	rt.checkOverrides(logger, &t.config)

	if rt.IsRollback() {
		logger.Debug("rollback. nothing to do")
		return
	}

	if t.config.conditionalStart && !rt.HasStartCondition() {
		err = fmt.Errorf("conditional start is set, but file trigger is not found")
		return
	}

	isStarted := rt.IsStarted()
	if !isStarted && !isShardSelected {
		logger.Debug("different collection/shard selected. nothing to do")
		return nil
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

	if !isStarted {
		if err = rt.markStarted(time.Now()); err != nil {
			err = fmt.Errorf("marking reindex started: %w", err)
			return
		}
	}

	if rt.IsSwapped() {
		if !rt.IsTidied() {
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
		isMerged := rt.IsMerged()
		if isMerged {
			logger.Debug("merged, not swapped. starting ingest buckets")
		} else {
			if !rt.IsReindexed() {
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
		err = updateToBlockMaxInvertedIndexConfig(ctx, t.schemaManager, shard.Index().Config.ClassName.String())
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
						propLen := t.calcPropLenInverted(invprop.Items)
						for _, item := range invprop.Items {
							pair := shard.pairPropertyWithFrequency(md.docID, item.TermFrequency, propLen)
							if err := shard.addToPropertyMapBucket(bucket, pair, item.Data); err != nil {
								breakCh <- true
								err = fmt.Errorf("adding object '%s' prop '%s': %w", md.key.String(), invprop.Name, err)
								return zerotime, false, err
							}
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
		return zerotime, t.config.reloadShards, nil
	}
	return time.Now().Add(t.config.pauseDuration), false, nil
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

		if !rt.IsSwappedProp(props[i]) {
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

		if rt.IsSwappedProp(props[i]) {
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
	keepLevelCompaction, keepTombstones bool,
) error {
	bucketOpts := t.bucketOptions(shard, lsmkv.StrategyInverted, keepLevelCompaction, keepTombstones, t.config.memtableOptBlockmaxFactor)
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

func (t *ShardReindexTask_MapToBlockmax) unloadIngestBuckets(ctx context.Context,
	logger logrus.FieldLogger, shard ShardLike, props []string,
) error {
	return t.unloadBuckets(ctx, logger, shard, props, t.ingestBucketName)
}

func (t *ShardReindexTask_MapToBlockmax) unloadReindexBuckets(ctx context.Context,
	logger logrus.FieldLogger, shard ShardLike, props []string,
) error {
	return t.unloadBuckets(ctx, logger, shard, props, t.reindexBucketName)
}

func (t *ShardReindexTask_MapToBlockmax) unloadBuckets(ctx context.Context,
	logger logrus.FieldLogger, shard ShardLike, props []string, bucketNamer func(string) string,
) error {
	store := shard.Store()

	eg, gctx := enterrors.NewErrorGroupWithContextWrapper(logger, ctx)
	eg.SetLimit(t.config.concurrency)
	for i := range props {
		propName := props[i]

		eg.Go(func() error {
			bucketName := bucketNamer(propName)
			logger.WithField("bucket", bucketName).Debug("loading bucket")
			if err := store.ShutdownBucket(gctx, bucketName); err != nil {
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
	keepLevelCompaction, keepTombstones bool, memtableOptFactor int,
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
		lsmkv.WithKeepLevelCompaction(keepLevelCompaction),
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

func (t *ShardReindexTask_MapToBlockmax) findPropsToReindex(shard ShardLike) (props []string, save bool) {
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
		if bucket.Strategy() == lsmkv.StrategyMapCollection {
			propName, indexType := GetPropNameAndIndexTypeFromBucketName(name)

			if indexType == IndexTypePropSearchableValue && checkPropSelected(propName) {
				propNames = append(propNames, propName)
			}
		}
	}
	return propNames, true
}

func (t *ShardReindexTask_MapToBlockmax) getPropsToReindex(shard ShardLike, rt mapToBlockmaxReindexTracker) ([]string, error) {
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

func (t *ShardReindexTask_MapToBlockmax) readPropsToReindex(rt mapToBlockmaxReindexTracker) ([]string, error) {
	if rt.HasProps() {
		props, err := rt.GetProps()
		if err != nil {
			return nil, err
		}
		return props, nil
	}
	return []string{}, nil
}

func (t *ShardReindexTask_MapToBlockmax) isShardSelected(collectionName, shardName string) bool {
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
	HasStartCondition() bool
	IsStarted() bool
	markStarted(time.Time) error
	getStarted() (time.Time, error)

	markProgress(lastProcessedKey indexKey, processedCount, indexedCount int) error
	GetProgress() (indexKey, *time.Time, error)

	IsReindexed() bool
	markReindexed() error

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

	reset() error

	checkOverrides(logger logrus.FieldLogger, config *mapToBlockmaxConfig)
}

func NewFileMapToBlockmaxReindexTracker(lsmPath string, keyParser indexKeyParser) *fileMapToBlockmaxReindexTracker {
	return &fileMapToBlockmaxReindexTracker{
		progressCheckpoint: 1,
		keyParser:          keyParser,
		config: fileReindexTrackerConfig{
			filenameStart:      "start.mig",
			filenameStarted:    "started.mig",
			filenameProgress:   "progress.mig",
			filenameReindexed:  "reindexed.mig",
			filenameMerged:     "merged.mig",
			filenameSwapped:    "swapped.mig",
			filenameTidied:     "tidied.mig",
			filenameProperties: "properties.mig",
			filenameRollback:   "rollback.mig",
			filenamePaused:     "paused.mig",
			filenameOverrides:  "overrides.mig",
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
	filenameStart      string
	filenameStarted    string
	filenameProgress   string
	filenameReindexed  string
	filenameMerged     string
	filenameSwapped    string
	filenameTidied     string
	filenameProperties string
	filenameRollback   string
	filenamePaused     string
	filenameOverrides  string
	migrationPath      string
}

func (t *fileMapToBlockmaxReindexTracker) init() error {
	if err := os.MkdirAll(t.config.migrationPath, 0o777); err != nil {
		return err
	}
	return nil
}

func (t *fileMapToBlockmaxReindexTracker) HasStartCondition() bool {
	return t.fileExists(t.config.filenameStart)
}

func (t *fileMapToBlockmaxReindexTracker) IsStarted() bool {
	return t.fileExists(t.config.filenameStarted)
}

func (t *fileMapToBlockmaxReindexTracker) markStarted(started time.Time) error {
	return t.createFile(t.config.filenameStarted, []byte(t.encodeTime(started)))
}

func (t *fileMapToBlockmaxReindexTracker) getTime(filePath string) (time.Time, error) {
	path := t.filepath(filePath)
	content, err := os.ReadFile(path)
	if err != nil {
		return time.Time{}, err
	}
	return t.decodeTime(string(content))
}

func (t *fileMapToBlockmaxReindexTracker) getStarted() (time.Time, error) {
	return t.getTime(t.config.filenameStarted)
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

func (t *fileMapToBlockmaxReindexTracker) GetProgress() (indexKey, *time.Time, error) {
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

func (t *fileMapToBlockmaxReindexTracker) parseProgressFile(filename string) (lastProcessedKey indexKey, tm time.Time, allCount int, idxCount int, err error) {
	progressFilePath := filename
	progressFile, err := os.ReadFile(progressFilePath)
	if err != nil {
		err = fmt.Errorf("failed to read %s: %w", progressFilePath, err)
		return
	}

	if len(progressFile) == 0 {
		err = fmt.Errorf("progress file %s is empty", progressFilePath)
		return
	}

	progressFileFields := strings.Split(string(progressFile), "\n")
	if len(progressFileFields) != 4 {
		err = fmt.Errorf("progress file %s has unexpected format, expected 4 lines, got %d", progressFilePath, len(progressFileFields))
		return
	}

	tm, err = t.decodeTime(strings.TrimSpace(progressFileFields[0]))
	if err != nil {
		err = fmt.Errorf("failed to parse timestamp from %s: %w", progressFilePath, err)
		return
	}

	lastProcessedKey, err = t.keyParser.FromString(progressFileFields[1])
	if err != nil {
		err = fmt.Errorf("failed to parse last processed key from %s: %w", progressFilePath, err)
		return
	}

	allCount, err = strconv.Atoi(strings.Split(progressFileFields[2], " ")[1])
	if err != nil {
		err = fmt.Errorf("failed to parse objects migrated count from %s: %w", progressFilePath, err)
		return
	}

	idxCount, err = strconv.Atoi(strings.Split(progressFileFields[3], " ")[1])
	if err != nil {
		err = fmt.Errorf("failed to parse index count from %s: %w", progressFilePath, err)
		return
	}

	return
}

func (t *fileMapToBlockmaxReindexTracker) GetMigratedCount() (objectsMigratedCountTotal int, snapshots []map[string]string, err error) {
	snapshots = make([]map[string]string, 0)
	files, err := os.ReadDir(t.config.migrationPath)
	objectsMigratedCountTotal = 0
	progressCount := 0

	if err != nil {
		return
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
				return
			}

			objectsMigratedCountTotal += allCount
			snapshot["lastProcessedKey"] = key.String()
			snapshot["timestamp"] = tm.Format(time.RFC3339)
			snapshot["allCount"] = fmt.Sprintf("%d", allCount)
			snapshot["idxCount"] = fmt.Sprintf("%d", idxCount)
			snapshots = append(snapshots, snapshot)
		}
	}
	return
}

func (t *fileMapToBlockmaxReindexTracker) IsReindexed() bool {
	return t.fileExists(t.config.filenameReindexed)
}

func (t *fileMapToBlockmaxReindexTracker) markReindexed() error {
	return t.createFile(t.config.filenameReindexed, []byte(t.encodeTimeNow()))
}

func (t *fileMapToBlockmaxReindexTracker) getReindexed() (time.Time, error) {
	return t.getTime(t.config.filenameReindexed)
}

func (t *fileMapToBlockmaxReindexTracker) IsMerged() bool {
	return t.fileExists(t.config.filenameMerged)
}

func (t *fileMapToBlockmaxReindexTracker) markMerged() error {
	return t.createFile(t.config.filenameMerged, []byte(t.encodeTimeNow()))
}

func (t *fileMapToBlockmaxReindexTracker) getMerged() (time.Time, error) {
	return t.getTime(t.config.filenameMerged)
}

func (t *fileMapToBlockmaxReindexTracker) IsSwappedProp(propName string) bool {
	return t.fileExists(t.config.filenameSwapped + "." + propName)
}

func (t *fileMapToBlockmaxReindexTracker) markSwappedProp(propName string) error {
	return t.createFile(t.config.filenameSwapped+"."+propName, []byte(t.encodeTimeNow()))
}

func (t *fileMapToBlockmaxReindexTracker) unmarkSwappedProp(propName string) error {
	return t.removeFile(t.config.filenameSwapped + "." + propName)
}

func (t *fileMapToBlockmaxReindexTracker) IsSwapped() bool {
	return t.fileExists(t.config.filenameSwapped)
}

func (t *fileMapToBlockmaxReindexTracker) markSwapped() error {
	return t.createFile(t.config.filenameSwapped, []byte(t.encodeTimeNow()))
}

func (t *fileMapToBlockmaxReindexTracker) unmarkSwapped() error {
	return t.removeFile(t.config.filenameSwapped)
}

func (t *fileMapToBlockmaxReindexTracker) getSwapped() (time.Time, error) {
	return t.getTime(t.config.filenameSwapped)
}

func (t *fileMapToBlockmaxReindexTracker) IsTidied() bool {
	return t.fileExists(t.config.filenameTidied)
}

func (t *fileMapToBlockmaxReindexTracker) getTidied() (time.Time, error) {
	return t.getTime(t.config.filenameTidied)
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
	if err := os.Remove(t.filepath(filename)); err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	return nil
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

func (t *fileMapToBlockmaxReindexTracker) HasProps() bool {
	return t.fileExists(t.config.filenameProperties)
}

func (t *fileMapToBlockmaxReindexTracker) saveProps(propNames []string) error {
	props := []byte(strings.Join(propNames, ","))
	return t.createFile(t.config.filenameProperties, props)
}

func (t *fileMapToBlockmaxReindexTracker) GetProps() ([]string, error) {
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

func (t *fileMapToBlockmaxReindexTracker) IsRollback() bool {
	return t.fileExists(t.config.filenameRollback)
}

func (t *fileMapToBlockmaxReindexTracker) IsPaused() bool {
	return t.fileExists(t.config.filenamePaused)
}

func (t *fileMapToBlockmaxReindexTracker) GetStatusStrings() (status string, message string, action string) {
	if !t.IsStarted() {
		status = "not started"
		message = "reindexing not started"
		action = "enable relevant REINDEX_MAP_TO_BLOCKMAX_* env vars"
		if t.HasStartCondition() {
			message = "reindexing will start on next restart"
			action = "restart"
		}
		return
	}
	message = "reindexing started"
	action = "wait"

	if !t.HasProps() {
		status = "computing properties"
		message = "computing properties to reindex"
		return
	}

	count, _, err := t.GetMigratedCount()
	if err != nil {
		status = "error"
		message = fmt.Sprintf("failed to get migrated count: %v", err)
		return
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

	return
}

func (t *fileMapToBlockmaxReindexTracker) GetTimes() map[string]string {
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

func (t *fileMapToBlockmaxReindexTracker) checkOverrides(logger logrus.FieldLogger, config *mapToBlockmaxConfig) {
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
		case "memtableOptBlockmaxFactor":
			memtableOptBlockmaxFactor, err := strconv.Atoi(value)
			if err != nil {
				logger.WithField("value", value).Warn("invalid memtableOptBlockmaxFactor value, must be an integer")
				continue
			}
			if memtableOptBlockmaxFactor <= 0 {
				logger.WithField("value", value).Warn("invalid memtableOptBlockmaxFactor value, must be greater than 0")
				continue
			}
			config.memtableOptBlockmaxFactor = memtableOptBlockmaxFactor
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

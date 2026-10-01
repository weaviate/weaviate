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
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"sync"
	"time"

	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/indexcounter"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	shardusage "github.com/weaviate/weaviate/adapters/repos/db/shard_usage"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/diskio"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storagestate"
	"github.com/weaviate/weaviate/entities/storobj"
)

// dimensionsReindex is the REINDEX_VECTOR_DIMENSIONS_AT_STARTUP state all indexes
// of a db share. A shard is rebuilt once per process, not on every lazy reload.
type dimensionsReindex struct {
	enabled bool

	mu sync.Mutex
	// by shard id, whether its rebuild succeeded
	outcomes map[string]bool
	objects  int64
}

func (r *dimensionsReindex) pending(shardID string) bool {
	if r == nil || !r.enabled {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return !r.outcomes[shardID]
}

func (r *dimensionsReindex) record(shardID string, objects int, ok bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.outcomes == nil {
		r.outcomes = map[string]bool{}
	}
	r.outcomes[shardID] = ok
	if ok {
		r.objects += int64(objects)
	}
}

func (r *dimensionsReindex) done(shardID string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.outcomes[shardID]
}

func (r *dimensionsReindex) summary() (rebuilt, failed int, objects int64) {
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

// prepareDimensionsOfShards runs before the index loads its shards, for active and cold
// tenants alike, without loading them. It migrates their dimensions buckets with
// REINDEX_VECTOR_DIMENSIONS_TO_ROARINGSET_AT_STARTUP and rebuilds them with
// REINDEX_VECTOR_DIMENSIONS_AT_STARTUP. A shard that fails is retried when it loads.
func (i *Index) prepareDimensionsOfShards(ctx context.Context, class *models.Class, shardNames []string) {
	reindex := i.Config.DimensionsReindex
	if !i.Config.TrackVectorDimensions || len(shardNames) == 0 ||
		(!i.Config.MigrateDimensionsToRoaringSet && (reindex == nil || !reindex.enabled)) {
		return
	}

	start := time.Now()
	eg := enterrors.NewErrorGroupWrapper(i.logger)
	eg.SetLimit(_NUMCPU)
	for _, name := range shardNames {
		eg.Go(func() error {
			i.prepareShardDimensions(ctx, class, name)
			return nil
		}, name)
	}
	_ = eg.Wait()
	i.logger.WithField("action", "prepare_dimensions").WithField("shards", len(shardNames)).
		WithField("took", time.Since(start)).Info("dimensions buckets prepared")
}

func (i *Index) prepareShardDimensions(ctx context.Context, class *models.Class, name string) {
	logger := i.logger.WithField("action", "prepare_dimensions").WithField("shard", name)
	if _, err := os.Stat(shardPath(i.path(), name)); err != nil {
		if !os.IsNotExist(err) {
			logger.Errorf("could not prepare dimensions: %v", err)
		}
		return
	}

	shardID := shardId(i.ID(), name)
	reindex := i.Config.DimensionsReindex
	rebuild := reindex.pending(shardID)

	unlock, err := shardusage.LockUnloadedDimensionsBucket(ctx, i.path(), name)
	if err != nil {
		logger.Errorf("could not prepare dimensions: %v", err)
		return
	}
	// a rebuild writes a RoaringSet bucket anyway
	err = shardusage.PrepareDimensionsBucket(ctx, i.logger, i.path(), name,
		i.Config.MigrateDimensionsToRoaringSet && !rebuild)
	unlock()
	if err != nil {
		logger.Errorf("could not prepare dimensions: %v", err)
		return
	}
	if !rebuild {
		return
	}

	start := time.Now()
	objects, err := i.rebuildUnloadedShardDimensions(ctx, class, name)
	reindex.record(shardID, objects, err == nil)
	if err != nil {
		logger.Errorf("could not reindex dimensions: %v", err)
		return
	}
	logger.WithField("objects", objects).WithField("took", time.Since(start)).Info("dimensions reindexed")
}

// rebuildUnloadedShardDimensions rebuilds the dimensions bucket of a shard that is not
// loaded, on a shard that has only its objects and dimensions buckets open, with the
// options a load opens them with.
func (i *Index) rebuildUnloadedShardDimensions(ctx context.Context, class *models.Class, name string) (objects int, err error) {
	if i.unloadedShardIsEmpty(name) {
		return 0, nil
	}
	s := &Shard{
		index:         i,
		class:         class,
		name:          name,
		status:        ShardStatus{Status: storagestate.StatusReady},
		bitmapBufPool: i.bitmapBufPool,
	}
	// the options of a MapCollection bucket depend on the shard version
	count, err := indexcounter.Read(s.path())
	if err != nil {
		return 0, fmt.Errorf("read index counter: %w", err)
	}
	if s.versioner, err = newShardVersioner(path.Join(s.path(), "version"), count > 0); err != nil {
		return 0, fmt.Errorf("init shard versioner: %w", err)
	}

	noop := cyclemanager.NewCallbackGroupNoop()
	store, err := lsmkv.New(s.pathLSM(), s.path(), i.logger.WithField("shard", name), nil,
		i.bucketLoadLimiter, noop, noop, noop)
	if err != nil {
		return 0, fmt.Errorf("init store: %w", err)
	}
	s.store = store
	defer func() {
		if shutdownErr := store.Shutdown(context.WithoutCancel(ctx)); shutdownErr != nil && err == nil {
			err = fmt.Errorf("shutdown store: %w", shutdownErr)
		}
	}()

	if err := s.initObjectBucket(ctx); err != nil {
		return 0, err
	}
	if err := s.loadDimensionsBucket(ctx, helpers.DimensionsBucketLSM); err != nil {
		return 0, err
	}
	return s.recalculateDimensions(ctx)
}

type dimensionsRows map[string]*sroar.Bitmap

func (r dimensionsRows) set(key []byte, docID uint64) {
	bm, ok := r[string(key)]
	if !ok {
		bm = sroar.NewBitmap()
		r[string(key)] = bm
	}
	bm.Set(docID)
}

// reindexDimensionsOnLoad runs while the shard loads, before anything can read or
// write it. A failure that leaves the bucket in place is only logged, the shard
// serves with the dimensions it had; one that leaves none fails the load, and the
// next load recovers the bucket from its dirs.
func (s *Shard) reindexDimensionsOnLoad(ctx context.Context) error {
	reindex := s.index.Config.DimensionsReindex
	if !s.index.Config.TrackVectorDimensions || !reindex.pending(s.ID()) {
		return nil
	}
	logger := s.index.logger.WithField("action", "reindex_vector_dimensions").WithField("shard", s.ID())

	start := time.Now()
	objects, err := s.recalculateDimensions(ctx)
	reindex.record(s.ID(), objects, err == nil)
	if err == nil {
		logger.WithField("objects", objects).WithField("took", time.Since(start)).Info("dimensions reindexed")
		return nil
	}
	if s.store.Bucket(helpers.DimensionsBucketLSM) == nil {
		return fmt.Errorf("reindex dimensions: %w", err)
	}
	logger.Errorf("could not reindex dimensions, keeping them as they were: %v", err)
	return nil
}

// recalculateDimensions rebuilds the dimensions bucket from the objects. It must not
// run while the shard serves. The bucket is replaced only once complete, so an
// interruption changes nothing.
func (s *Shard) recalculateDimensions(ctx context.Context) (objects int, err error) {
	// a usage scan of the unloaded shard runs migration recovery, which removes a
	// replacement bucket that has no bucket moved aside next to it
	unlock, err := shardusage.LockUnloadedDimensionsBucket(ctx, s.index.path(), s.name)
	if err != nil {
		return 0, err
	}
	defer unlock()

	rows, objects, err := s.scanObjectDimensions(ctx)
	if err != nil {
		return 0, err
	}
	// not to be interrupted from here on: a switch given up halfway has to be rolled back
	ctx = context.WithoutCancel(ctx)
	name, err := s.createDimensionsReplacement(ctx)
	if err != nil {
		return 0, err
	}
	if err := s.replaceDimensionsBucket(ctx, name, rows); err != nil {
		return 0, err
	}
	return objects, nil
}

func (s *Shard) scanObjectDimensions(ctx context.Context) (dimensionsRows, int, error) {
	bucket := s.store.Bucket(helpers.ObjectsBucketLSM)
	if bucket == nil {
		return nil, 0, fmt.Errorf("objects bucket of shard %q: %w", s.ID(), lsmkv.ErrBucketNotFound)
	}

	// Only the vector lengths are needed. Properties are not decoded and the
	// legacy vector is skipped, its length is kept regardless.
	var namedVectors []string
	for targetVector := range s.index.GetVectorIndexConfigs() {
		if targetVector != "" {
			namedVectors = append(namedVectors, targetVector)
		}
	}
	decode := additional.Properties{NoProps: true, Vectors: namedVectors}
	className := s.index.Config.ClassName.String()

	rows := dimensionsRows{}
	objects := 0
	key := make([]byte, 0, 64)
	track := func(targetVector string, dims int, docID uint64) {
		key = append(key[:0], targetVector...)
		key = binary.LittleEndian.AppendUint32(key, uint32(dims))
		rows.set(key, docID)
	}

	cursor := bucket.Cursor()
	defer cursor.Close()
	for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
		if objects%1000 == 0 && ctx.Err() != nil {
			return nil, 0, fmt.Errorf("scan objects of shard %q: %w", s.ID(), context.Cause(ctx))
		}
		object, err := storobj.FromBinaryOptionalDisk(v, className, decode, nil)
		if err != nil {
			return nil, 0, fmt.Errorf("unmarshal object %d of shard %q: %w", objects, s.ID(), err)
		}
		objects++

		// as [storobj.Object.IterateThroughVectorDimensions] has it for a fully decoded object
		if object.VectorLen > 0 {
			track("", object.VectorLen, object.DocID)
		}
		for targetVector, vector := range object.Vectors {
			track(targetVector, len(vector), object.DocID)
		}
		for targetVector, vectors := range object.MultiVectors {
			dims := 0
			for _, vector := range vectors {
				dims += len(vector)
			}
			track(targetVector, dims, object.DocID)
		}
	}
	return rows, objects, nil
}

func (s *Shard) createDimensionsReplacement(ctx context.Context) (name string, err error) {
	if s.store.Bucket(helpers.DimensionsBucketLSM) == nil {
		return "", errors.New("no bucket dimensions")
	}

	// Named as the migration names its replacement, so that a switch interrupted
	// between its two renames is recovered the same way when the shard loads next.
	name = helpers.DimensionsBucketLSM + shardusage.DimensionsReplacementBucketSuffix
	bucketPath := filepath.Join(s.pathLSM(), helpers.DimensionsBucketLSM)
	if s.store.Bucket(name) != nil {
		if err := s.store.ShutdownBucket(ctx, name); err != nil {
			return "", fmt.Errorf("shutdown stale bucket %q: %w", name, err)
		}
	}
	// Left over by a switch that failed. The shard has its dimensions bucket in
	// use, so neither is needed, and one moved aside would fail the switch.
	for _, stale := range []string{
		bucketPath + shardusage.DimensionsReplacementBucketSuffix,
		bucketPath + shardusage.DimensionsReplacedBucketSuffix,
	} {
		if err := os.RemoveAll(stale); err != nil {
			return "", fmt.Errorf("remove stale bucket %q: %w", stale, err)
		}
	}

	if err := s.store.CreateOrLoadBucket(ctx, name, s.makeDefaultBucketOptions(lsmkv.StrategyRoaringSet)...); err != nil {
		return "", fmt.Errorf("create bucket %q: %w", name, err)
	}
	return name, nil
}

func (s *Shard) replaceDimensionsBucket(ctx context.Context, name string, rows dimensionsRows) error {
	bucketPath := filepath.Join(s.pathLSM(), helpers.DimensionsBucketLSM)
	if err := s.fillDimensionsBucket(name, rows); err != nil {
		s.discardDimensionsReplacement(ctx, name, bucketPath+shardusage.DimensionsReplacementBucketSuffix)
		return err
	}
	return s.switchToDimensionsReplacement(ctx, name, bucketPath)
}

func (s *Shard) fillDimensionsBucket(name string, rows dimensionsRows) error {
	replacement := s.store.Bucket(name)
	for key, docIDs := range rows {
		if docIDs.IsEmpty() {
			continue
		}
		if err := replacement.RoaringSetAddBitmap([]byte(key), docIDs); err != nil {
			return fmt.Errorf("write dimensions key %x: %w", key, err)
		}
	}
	// replacing a bucket drops what it has not flushed
	if err := replacement.FlushAndSwitch(); err != nil {
		return fmt.Errorf("flush bucket %q: %w", name, err)
	}
	return nil
}

// discardDimensionsReplacement leaves the shard on the dimensions bucket it has.
// What it fails to remove the next switch or load removes.
func (s *Shard) discardDimensionsReplacement(ctx context.Context, name, readyPath string) {
	if err := s.store.ShutdownBucket(ctx, name); err != nil {
		s.index.logger.WithField("bucket", name).Warnf("failed to shutdown bucket: %v", err)
	}
	if err := os.RemoveAll(readyPath); err != nil {
		s.index.logger.WithField("path", readyPath).Warnf("failed to remove dir: %v", err)
	}
}

func (s *Shard) switchToDimensionsReplacement(ctx context.Context, name, bucketPath string) error {
	err := s.store.ReplaceBuckets(ctx, helpers.DimensionsBucketLSM, name)
	if err == nil {
		if err := diskio.Fsync(s.pathLSM()); err != nil {
			return fmt.Errorf("fsync %q: %w", s.pathLSM(), err)
		}
		return nil
	}
	err = fmt.Errorf("replace dimensions bucket: %w", err)

	readyPath := bucketPath + shardusage.DimensionsReplacementBucketSuffix
	if s.store.Bucket(name) != nil {
		s.discardDimensionsReplacement(ctx, name, readyPath)
		return err
	}
	return s.recoverFailedDimensionsSwitch(ctx, bucketPath, err)
}

// recoverFailedDimensionsSwitch handles ReplaceBuckets failing after it took the name,
// before, between or after its two renames: it puts the dirs in order and loads the
// bucket again. Where that is unsafe or fails, it leaves the shard without one, and
// the next load recovers it from the dirs.
func (s *Shard) recoverFailedDimensionsSwitch(ctx context.Context, bucketPath string, switchErr error) error {
	readyPath := bucketPath + shardusage.DimensionsReplacementBucketSuffix
	delPath := bucketPath + shardusage.DimensionsReplacedBucketSuffix
	logger := s.index.logger.WithField("action", "reindex_vector_dimensions").WithField("path", bucketPath)

	if err := s.store.ShutdownBucket(ctx, helpers.DimensionsBucketLSM); err != nil {
		return fmt.Errorf("%w: shutdown bucket: %w", switchErr, err)
	}
	if errors.Is(switchErr, lsmkv.ErrReplacedBucketNotShutDown) {
		return switchErr
	}

	// Which dirs are there says how far the switch got. Unless that is known for
	// sure, nothing is touched, the next load recovers the dirs.
	readyExists, err := diskio.DirExists(readyPath)
	if err != nil {
		return fmt.Errorf("%w: stat %q: %w", switchErr, readyPath, err)
	}
	switched := !readyExists
	if switched {
		if err := os.RemoveAll(delPath); err != nil {
			logger.Warnf("failed to remove dir %q: %v", delPath, err)
		}
	} else {
		bucketExists, err := diskio.DirExists(bucketPath)
		if err != nil {
			return fmt.Errorf("%w: stat %q: %w", switchErr, bucketPath, err)
		}
		if !bucketExists {
			if err := os.Rename(delPath, bucketPath); err != nil {
				// loading now would start an empty bucket, the next load finishes the switch
				return fmt.Errorf("%w: move dimensions bucket back: %w", switchErr, err)
			}
		}
		if err := os.RemoveAll(readyPath); err != nil {
			logger.Warnf("failed to remove dir %q: %v", readyPath, err)
		}
	}

	if err := s.loadDimensionsBucket(ctx, helpers.DimensionsBucketLSM); err != nil {
		return fmt.Errorf("%w: load dimensions bucket again: %w", switchErr, err)
	}
	if switched {
		logger.Warnf("dimensions bucket replaced, cleaning up after it failed: %v", switchErr)
		return nil
	}
	return switchErr
}

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
	"time"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/indexcounter"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	shardusage "github.com/weaviate/weaviate/adapters/repos/db/shard_usage"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/diskio"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storagestate"
	"github.com/weaviate/weaviate/entities/storobj"
)

// prepareShardDimensions migrates the dimensions bucket of a shard that is not loaded,
// if asked to. It reports whether the dimensions of the shard are to be reindexed.
func (i *Index) prepareShardDimensions(ctx context.Context, logger logrus.FieldLogger, name string) bool {
	reindex := i.Config.DimensionsReindex.pending(i.ID(), name)

	// a reindex writes a RoaringSet bucket anyway
	migrated, err := i.prepareUnloadedDimensionsBucket(ctx, name, i.Config.MigrateDimensionsToRoaringSet && !reindex)
	if err != nil {
		logger.Errorf("could not prepare dimensions: %v", err)
		return false
	}
	if migrated {
		i.removeComputedUsage(logger, name)
	}
	return reindex
}

func (i *Index) prepareUnloadedDimensionsBucket(ctx context.Context, name string, migrate bool) (bool, error) {
	unlock, err := shardusage.LockUnloadedDimensionsBucket(ctx, i.path(), name)
	if err != nil {
		return false, err
	}
	// a panic must not keep the bucket locked, its load and usage scans would wait forever
	defer unlock()
	return shardusage.PrepareDimensionsBucket(ctx, i.logger, i.path(), name, migrate)
}

func (i *Index) removeComputedUsage(logger logrus.FieldLogger, name string) {
	if err := shardusage.RemoveComputedUsageDataForUnloadedShard(i.path(), name); err != nil {
		logger.Warnf("failed to remove computed usage: %v", err)
	}
}

// unloadedDimensionsReindex reindexes the dimensions of a shard before it loads.
type unloadedDimensionsReindex struct {
	index   *Index
	name    string
	logger  logrus.FieldLogger
	rebuild *dimensionsRebuild
}

func (r *unloadedDimensionsReindex) empty() error {
	return nil
}

func (r *unloadedDimensionsReindex) begin(ctx context.Context, s *Shard) (objectsVisitor, error) {
	if err := s.loadDimensionsBucket(ctx, helpers.DimensionsBucketLSM); err != nil {
		return nil, err
	}
	rebuild, err := s.beginDimensionsRebuild(ctx)
	if err != nil {
		return nil, err
	}
	r.rebuild = rebuild
	return rebuild, nil
}

func (r *unloadedDimensionsReindex) end(ctx context.Context, walkErr error) error {
	return r.rebuild.end(ctx, walkErr)
}

func (r *unloadedDimensionsReindex) conclude(objects int, took time.Duration, err error) {
	r.index.Config.DimensionsReindex.record(r.index.ID(), r.name, objects, err == nil)
	if err != nil {
		r.logger.Errorf("could not reindex dimensions: %v", err)
		return
	}
	r.index.removeComputedUsage(r.logger, r.name)
	r.logger.WithField("objects", objects).WithField("took", took).Info("dimensions reindexed")
}

// openUnloadedShard opens only the objects bucket of a shard that is not loaded,
// for a rebuild before the shard loads. closeShard shuts its store down.
func (i *Index) openUnloadedShard(ctx context.Context, class *models.Class, name string,
) (s *Shard, closeShard func() error, err error) {
	s = &Shard{
		index:         i,
		class:         class,
		name:          name,
		status:        ShardStatus{Status: storagestate.StatusReady},
		bitmapBufPool: i.bitmapBufPool,
	}
	// the options of a MapCollection bucket depend on the shard version
	count, err := indexcounter.Read(s.path())
	if err != nil {
		return nil, nil, fmt.Errorf("read index counter: %w", err)
	}
	if s.versioner, err = newShardVersioner(path.Join(s.path(), "version"), count > 0); err != nil {
		return nil, nil, fmt.Errorf("init shard versioner: %w", err)
	}

	noop := cyclemanager.NewCallbackGroupNoop()
	store, err := lsmkv.New(s.pathLSM(), s.path(), i.logger.WithField("shard", name), nil,
		i.bucketLoadLimiter, noop, noop, noop)
	if err != nil {
		return nil, nil, fmt.Errorf("init store: %w", err)
	}
	s.store = store
	closeShard = func() error {
		if err := store.Shutdown(context.WithoutCancel(ctx)); err != nil {
			return fmt.Errorf("shutdown store: %w", err)
		}
		return nil
	}

	if err := s.initObjectBucket(ctx); err != nil {
		_ = closeShard()
		return nil, nil, err
	}
	return s, closeShard, nil
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

// reindexDimensionsOnLoad runs before the shard serves. A failure that leaves the
// bucket in place is only logged; one that leaves none fails the load.
func (s *Shard) reindexDimensionsOnLoad(ctx context.Context) error {
	reindex := s.index.Config.DimensionsReindex
	if !s.index.Config.TrackVectorDimensions || !reindex.pending(s.index.ID(), s.name) {
		return nil
	}
	// such as a tenant created after startup
	if s.counter.PreviewNext() == 0 {
		reindex.record(s.index.ID(), s.name, 0, true)
		return nil
	}
	logger := s.index.logger.WithField("action", "reindex_vector_dimensions").WithField("shard", s.ID())

	start := time.Now()
	objects, err := s.recalculateDimensions(ctx)
	reindex.record(s.index.ID(), s.name, objects, err == nil)
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

// recalculateDimensions must not run while the shard serves.
func (s *Shard) recalculateDimensions(ctx context.Context) (int, error) {
	rebuild, err := s.beginDimensionsRebuild(ctx)
	if err != nil {
		return 0, err
	}
	objects, errs := s.walkObjects(ctx, rebuild)
	if err := rebuild.end(ctx, errs[0]); err != nil {
		return 0, err
	}
	return objects, nil
}

// dimensionsRebuild collects the dimensions of the objects of a shard, to replace its
// dimensions bucket with.
type dimensionsRebuild struct {
	shard  *Shard
	unlock func()
	decode additional.Properties
	rows   dimensionsRows
	key    []byte
}

// beginDimensionsRebuild keeps usage scans of the unloaded shard out until end. A
// usage scan runs migration recovery, which removes a replacement bucket that has no
// bucket moved aside next to it.
func (s *Shard) beginDimensionsRebuild(ctx context.Context) (*dimensionsRebuild, error) {
	unlock, err := shardusage.LockUnloadedDimensionsBucket(ctx, s.index.path(), s.name)
	if err != nil {
		return nil, err
	}

	// the legacy vector's length is kept regardless
	var namedVectors []string
	for targetVector := range s.index.GetVectorIndexConfigs() {
		if targetVector != "" {
			namedVectors = append(namedVectors, targetVector)
		}
	}
	return &dimensionsRebuild{
		shard:  s,
		unlock: unlock,
		decode: additional.Properties{NoProps: true, Vectors: namedVectors},
		rows:   dimensionsRows{},
		key:    make([]byte, 0, 64),
	}, nil
}

func (d *dimensionsRebuild) decodeOptions() additional.Properties {
	return d.decode
}

// visit tracks the vectors as [storobj.Object.IterateThroughVectorDimensions] does for
// a fully decoded object.
func (d *dimensionsRebuild) visit(object *storobj.Object) error {
	if object.VectorLen > 0 {
		d.track("", object.VectorLen, object.DocID)
	}
	for targetVector, vector := range object.Vectors {
		d.track(targetVector, len(vector), object.DocID)
	}
	for targetVector, vectors := range object.MultiVectors {
		dims := 0
		for _, vector := range vectors {
			dims += len(vector)
		}
		d.track(targetVector, dims, object.DocID)
	}
	return nil
}

func (d *dimensionsRebuild) track(targetVector string, dims int, docID uint64) {
	d.key = append(d.key[:0], targetVector...)
	d.key = binary.LittleEndian.AppendUint32(d.key, uint32(dims))
	d.rows.set(d.key, docID)
}

// end replaces the dimensions bucket with what the walk found, unless walkErr is set,
// and lets usage scans in again.
func (d *dimensionsRebuild) end(ctx context.Context, walkErr error) error {
	defer d.unlock()
	if walkErr != nil {
		return walkErr
	}
	// not to be interrupted from here on: a switch given up halfway has to be rolled back
	ctx = context.WithoutCancel(ctx)
	name, err := d.shard.createDimensionsReplacement(ctx)
	if err != nil {
		return err
	}
	return d.shard.replaceDimensionsBucket(ctx, name, d.rows)
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
	// left over by a failed switch; one moved aside would fail the next switch
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
	// flushed here so ReplaceBuckets does not flush under the store-wide lock
	if err := replacement.FlushAndSwitch(); err != nil {
		return fmt.Errorf("flush bucket %q: %w", name, err)
	}
	return nil
}

// discardDimensionsReplacement leaves what it fails to remove to the next switch or load.
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

// recoverFailedDimensionsSwitch puts the dirs in order after ReplaceBuckets failed and
// loads the bucket again. Where that is unsafe it leaves the shard without one, for the
// next load to recover.
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

	// unless it is certain how far the switch got, the next load recovers the dirs
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

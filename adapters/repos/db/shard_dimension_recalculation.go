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
	"path/filepath"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	shardusage "github.com/weaviate/weaviate/adapters/repos/db/shard_usage"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/diskio"
	"github.com/weaviate/weaviate/entities/storobj"
)

// dimensionsRows holds the doc ids of each row of a dimensions bucket, by row key.
type dimensionsRows map[string]*sroar.Bitmap

func (r dimensionsRows) set(key []byte, docID uint64) {
	bm, ok := r[string(key)]
	if !ok {
		bm = sroar.NewBitmap()
		r[string(key)] = bm
	}
	bm.Set(docID)
}

func (r dimensionsRows) remove(key []byte, docID uint64) {
	if bm, ok := r[string(key)]; ok {
		bm.Remove(docID)
	}
}

// dimensionsRecalculation keeps the dimension writes that reach a shard while its
// objects are scanned. The scan may have read an object before or after such a
// write, so what it found is corrected by them at the end. A doc id can be removed
// from a row and added to it again, by an update that keeps the doc id, therefore
// the last write to a doc id of a row is the one that counts.
type dimensionsRecalculation struct {
	mu      sync.Mutex
	added   dimensionsRows
	removed dimensionsRows
}

func newDimensionsRecalculation() *dimensionsRecalculation {
	return &dimensionsRecalculation{added: dimensionsRows{}, removed: dimensionsRows{}}
}

func (r *dimensionsRecalculation) record(key []byte, docID uint64, tombstone bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if tombstone {
		r.added.remove(key, docID)
		r.removed.set(key, docID)
		return
	}
	r.removed.remove(key, docID)
	r.added.set(key, docID)
}

// applyTo must not run concurrently with record.
func (r *dimensionsRecalculation) applyTo(rows dimensionsRows) {
	for key, removed := range r.removed {
		if bm, ok := rows[key]; ok {
			bm.AndNot(removed)
		}
	}
	for key, added := range r.added {
		if bm, ok := rows[key]; ok {
			bm.Or(added)
		} else {
			rows[key] = added
		}
	}
}

// recalculateShardDimensions recalculates the dimensions of a shard the index
// has loaded, or loads lazily. skipped reports a shard that is gone, or that went
// away meanwhile: a tenant deactivated, a shard dropped, the index shut down.
func (i *Index) recalculateShardDimensions(ctx context.Context, shardName string) (objects int, skipped bool, err error) {
	shard, err := i.shardForDimensionsRecalculation(ctx, shardName)
	if err != nil {
		return 0, false, err
	}
	if shard == nil {
		return 0, true, nil
	}

	objects, err = shard.recalculateDimensions(ctx)
	if err != nil && (shard.shutOrDropped() || errors.Is(err, errShutdownInProgress) ||
		errors.Is(err, errDropInProgress) || errors.Is(err, errAlreadyShutdown)) {
		return 0, true, nil
	}
	return objects, false, err
}

// shardForDimensionsRecalculation returns the loaded shard itself, never its lazy
// wrapper. Whatever unloads a shard takes shardCreateLocks for write, so holding
// it over both the lookup and the load keeps the wrapper from loading a shard that
// left i.shards in between, which nothing could shut down anymore.
//
// The shard is returned without a reference held, as a recalculation can take
// long and must not keep the shard from shutting down. It ends by itself then.
func (i *Index) shardForDimensionsRecalculation(ctx context.Context, shardName string) (*Shard, error) {
	if err := i.enterRead(); err != nil {
		return nil, nil
	}
	defer i.exitRead()

	i.shardCreateLocks.RLock(shardName)
	defer i.shardCreateLocks.RUnlock(shardName)

	switch shard := i.shards.Load(shardName).(type) {
	case nil:
		return nil, nil
	case *Shard:
		return shard, nil
	case *LazyLoadShard:
		if err := shard.Load(ctx); err != nil {
			return nil, err
		}
		return shard.shard, nil
	default:
		return nil, fmt.Errorf("shard %q: unexpected type %T", shardName, shard)
	}
}

// recalculateDimensions rebuilds the dimensions bucket from the objects of the
// shard, which keeps serving reads and writes meanwhile. The bucket in use is
// replaced only once the new one is complete, so an interrupted recalculation
// changes nothing. The new bucket is a roaring set one, whatever the old one was.
//
// It ends with an error when the shard is shut down or dropped.
func (s *Shard) recalculateDimensions(ctx context.Context) (objects int, err error) {
	if err := s.isReadOnly(); err != nil {
		return 0, err
	}

	// shutting down the store waits for the scan to let go of the objects bucket
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	if s.shutCtx != nil {
		stop := context.AfterFunc(s.shutCtx, func() { cancel(context.Cause(s.shutCtx)) })
		defer stop()
		// AfterFunc calls back from a goroutine of its own, too late for a short scan
		if s.shutCtx.Err() != nil {
			cancel(context.Cause(s.shutCtx))
		}
	}

	recalculation := newDimensionsRecalculation()
	s.dimensionsLock.Lock()
	if s.dimensionsRecalculation != nil {
		s.dimensionsLock.Unlock()
		return 0, errors.New("dimensions are being recalculated already")
	}
	// from here on, before the scan opens its cursor: a write not recorded has
	// stored its object by now, and the scan will find it
	s.dimensionsRecalculation = recalculation
	s.dimensionsLock.Unlock()
	defer func() {
		s.dimensionsLock.Lock()
		s.dimensionsRecalculation = nil
		s.dimensionsLock.Unlock()
	}()

	rows, objects, err := s.scanObjectDimensions(ctx)
	if err != nil {
		return 0, err
	}
	if err := s.switchToRecalculatedDimensions(ctx, recalculation, rows); err != nil {
		return 0, err
	}
	return objects, nil
}

// switchToRecalculatedDimensions renames bucket dirs, which must not happen to a
// shard whose files are being copied, to a store that is shutting down, or to a
// bucket a usage scan has open or is recovering. It is short, and it keeps all of
// them out until it is done, as well as the writes to the bucket.
func (s *Shard) switchToRecalculatedDimensions(ctx context.Context,
	recalculation *dimensionsRecalculation, rows dimensionsRows,
) error {
	if ctx.Err() != nil {
		return fmt.Errorf("switch dimensions bucket of shard %q: %w", s.ID(), context.Cause(ctx))
	}
	release, err := s.lockNotHaltedForTransfer(ctx)
	if err != nil {
		return err
	}
	// Deferred first, so it runs last: a shutdown requested meanwhile runs within
	// release, and takes haltForTransferMux.
	defer release()
	defer s.haltForTransferMux.Unlock()

	unlockBucket, err := shardusage.LockUnloadedDimensionsBucket(ctx, s.index.path(), s.name)
	if err != nil {
		return err
	}
	defer unlockBucket()

	s.dimensionsLock.Lock()
	defer s.dimensionsLock.Unlock()

	recalculation.applyTo(rows)
	// not to be interrupted: a switch given up halfway has to be rolled back
	return s.replaceDimensionsBucket(context.WithoutCancel(ctx), rows)
}

// lockNotHaltedForTransfer returns holding haltForTransferMux and a reference to
// the shard, once the shard is not halted. A halt in turn waits for the mutex, so
// none can start meanwhile. The reference is taken first, as performShutdown takes
// its lock before the mutex. Both are let go while the shard is halted, so that it
// can be shut down or dropped meanwhile.
func (s *Shard) lockNotHaltedForTransfer(ctx context.Context) (release func(), err error) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		release, err := s.preventShutdown()
		if err != nil {
			return nil, err
		}
		s.haltForTransferMux.Lock()
		if !s.haltedForTransfer() {
			return release, nil
		}
		s.haltForTransferMux.Unlock()
		release()

		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("wait for transfer of shard %q to end: %w", s.ID(), context.Cause(ctx))
		case <-ticker.C:
		}
	}
}

func (s *Shard) scanObjectDimensions(ctx context.Context) (dimensionsRows, int, error) {
	bucket, release := s.store.AcquireBucketForRead(helpers.ObjectsBucketLSM)
	if bucket == nil {
		return nil, 0, fmt.Errorf("objects bucket of shard %q: %w", s.ID(), lsmkv.ErrBucketNotFound)
	}
	defer release()

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

// replaceDimensionsBucket must run under the locks of [Shard.switchToRecalculatedDimensions].
func (s *Shard) replaceDimensionsBucket(ctx context.Context, rows dimensionsRows) error {
	if s.store.Bucket(helpers.DimensionsBucketLSM) == nil {
		return errors.New("no bucket dimensions")
	}

	// Named as the migration names its replacement, so that a switch interrupted
	// between its two renames is recovered the same way when the shard loads next.
	name := helpers.DimensionsBucketLSM + shardusage.DimensionsReplacementBucketSuffix
	bucketPath := filepath.Join(s.pathLSM(), helpers.DimensionsBucketLSM)
	if s.store.Bucket(name) != nil {
		if err := s.store.ShutdownBucket(ctx, name); err != nil {
			return fmt.Errorf("shutdown stale bucket %q: %w", name, err)
		}
	}
	// Left over by a switch that failed. The shard has its dimensions bucket in
	// use, so neither is needed, and one moved aside would fail the switch.
	for _, stale := range []string{
		bucketPath + shardusage.DimensionsReplacementBucketSuffix,
		bucketPath + shardusage.DimensionsReplacedBucketSuffix,
	} {
		if err := os.RemoveAll(stale); err != nil {
			return fmt.Errorf("remove stale bucket %q: %w", stale, err)
		}
	}

	if err := s.store.CreateOrLoadBucket(ctx, name, s.makeDefaultBucketOptions(lsmkv.StrategyRoaringSet)...); err != nil {
		return fmt.Errorf("create bucket %q: %w", name, err)
	}
	// Set read only while the scan ran, possibly as the disk is running full. A
	// status set from now on reaches the replacement, and fails the fill.
	if err := s.isReadOnly(); err != nil {
		s.discardDimensionsReplacement(ctx, name, bucketPath+shardusage.DimensionsReplacementBucketSuffix)
		return err
	}
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

// switchToDimensionsReplacement puts the filled replacement in place of the
// dimensions bucket.
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

// recoverFailedDimensionsSwitch deals with ReplaceBuckets failing after it gave the
// replacement the name of the dimensions bucket, which it does not take back. It
// can have failed before, between or after its two renames, and the bucket now
// going by the name may still be on a dir the next load removes, or have been
// left halfway. So the dirs are put in order first, and the bucket in place is
// loaded again. Where that is not safe or fails, the shard is left without one, see
// [Shard.leaveWithoutDimensionsBucket]. switchErr is returned unless only the
// cleanup after the switch failed.
func (s *Shard) recoverFailedDimensionsSwitch(ctx context.Context, bucketPath string, switchErr error) error {
	readyPath := bucketPath + shardusage.DimensionsReplacementBucketSuffix
	delPath := bucketPath + shardusage.DimensionsReplacedBucketSuffix
	logger := s.index.logger.WithField("action", "reindex_vector_dimensions").WithField("path", bucketPath)

	if err := s.store.ShutdownBucket(ctx, helpers.DimensionsBucketLSM); err != nil {
		return s.leaveWithoutDimensionsBucket(logger, fmt.Errorf("%w: shutdown bucket: %w", switchErr, err))
	}
	if errors.Is(switchErr, lsmkv.ErrReplacedBucketNotShutDown) {
		return s.leaveWithoutDimensionsBucket(logger, switchErr)
	}

	// Which dirs are there says how far the switch got. Unless that is known for
	// sure, nothing is touched, the next load recovers the dirs.
	readyExists, err := diskio.DirExists(readyPath)
	if err != nil {
		return s.leaveWithoutDimensionsBucket(logger, fmt.Errorf("%w: stat %q: %w", switchErr, readyPath, err))
	}
	switched := !readyExists
	if switched {
		if err := os.RemoveAll(delPath); err != nil {
			logger.Warnf("failed to remove dir %q: %v", delPath, err)
		}
	} else {
		bucketExists, err := diskio.DirExists(bucketPath)
		if err != nil {
			return s.leaveWithoutDimensionsBucket(logger, fmt.Errorf("%w: stat %q: %w", switchErr, bucketPath, err))
		}
		if !bucketExists {
			if err := os.Rename(delPath, bucketPath); err != nil {
				// loading now would start an empty bucket, the next load finishes the switch
				return s.leaveWithoutDimensionsBucket(logger, fmt.Errorf("%w: move dimensions bucket back: %w", switchErr, err))
			}
		}
		if err := os.RemoveAll(readyPath); err != nil {
			logger.Warnf("failed to remove dir %q: %v", readyPath, err)
		}
	}

	if err := s.loadDimensionsBucket(ctx, helpers.DimensionsBucketLSM); err != nil {
		return s.leaveWithoutDimensionsBucket(logger, fmt.Errorf("%w: load dimensions bucket again: %w", switchErr, err))
	}
	if switched {
		logger.Warnf("dimensions bucket replaced, cleaning up after it failed: %v", switchErr)
		return nil
	}
	return switchErr
}

// leaveWithoutDimensionsBucket is for a switch that failed with no dimensions bucket
// left loaded. A write would store its object and then fail on the missing bucket,
// before it reaches the vector index, and a retry keeping the doc id would not
// reach it either. So the shard is set read only, until it is loaded again and
// recovers the bucket from the dirs. Writes that passed the read only check
// already skip the dimensions then, see [Shard.addToDimensionBucket].
//
// It must run under dimensionsLock.
func (s *Shard) leaveWithoutDimensionsBucket(logger logrus.FieldLogger, err error) error {
	s.dimensionsBucketLost = true
	if statusErr := s.SetStatusReadonly("dimensions bucket lost by a failed recalculation, load the shard again"); statusErr != nil {
		logger.Errorf("failed to set shard read only: %v", statusErr)
	}
	logger.Errorf("shard left without dimensions bucket and set read only, load it again: %v", err)
	return err
}

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

// recalculateDimensions rebuilds the dimensions bucket from the objects of the
// shard, which keeps serving reads and writes meanwhile. The bucket in use is
// replaced only once the new one is complete, so an interrupted recalculation
// changes nothing. The new bucket is a roaring set one, whatever the old one was.
func (s *Shard) recalculateDimensions(ctx context.Context) (objects int, err error) {
	if err := s.isReadOnly(); err != nil {
		return 0, err
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

	rows, objects, scanErr := s.scanObjectDimensions(ctx)

	s.dimensionsLock.Lock()
	defer s.dimensionsLock.Unlock()
	s.dimensionsRecalculation = nil
	if scanErr != nil {
		return 0, scanErr
	}

	recalculation.applyTo(rows)
	if err := s.replaceDimensionsBucket(ctx, rows); err != nil {
		return 0, err
	}
	return objects, nil
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

// replaceDimensionsBucket must run with dimensionsLock write-held: a write that
// reaches either bucket while they are switched is lost.
func (s *Shard) replaceDimensionsBucket(ctx context.Context, rows dimensionsRows) error {
	if s.store.Bucket(helpers.DimensionsBucketLSM) == nil {
		return errors.New("no bucket dimensions")
	}

	// Named as the migration names its replacement, so that a switch interrupted
	// between its two renames is recovered the same way when the shard loads next.
	name := helpers.DimensionsBucketLSM + shardusage.DimensionsReplacementBucketSuffix
	if s.store.Bucket(name) != nil {
		if err := s.store.ShutdownBucket(ctx, name); err != nil {
			return fmt.Errorf("shutdown stale bucket %q: %w", name, err)
		}
	}
	if err := os.RemoveAll(filepath.Join(s.pathLSM(), name)); err != nil {
		return fmt.Errorf("remove stale bucket %q: %w", name, err)
	}

	if err := s.store.CreateOrLoadBucket(ctx, name, s.makeDefaultBucketOptions(lsmkv.StrategyRoaringSet)...); err != nil {
		return fmt.Errorf("create bucket %q: %w", name, err)
	}
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

	if err := s.store.ReplaceBuckets(ctx, helpers.DimensionsBucketLSM, name); err != nil {
		return fmt.Errorf("replace dimensions bucket: %w", err)
	}
	if err := diskio.Fsync(s.pathLSM()); err != nil {
		return fmt.Errorf("fsync %q: %w", s.pathLSM(), err)
	}
	return nil
}

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

package flat

import (
	"bytes"
	"encoding/binary"
	"sync"
	"sync/atomic"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	enterrors "github.com/weaviate/weaviate/entities/errors"
)

type compressedParallelIterator struct {
	bucket    *lsmkv.Bucket
	parallel  int
	logger    logrus.FieldLogger
	cacheSize int
	dimension int
}

func NewCompressedParallelIterator(bucket *lsmkv.Bucket, parallel int, cacheSize int, dimension int,
	logger logrus.FieldLogger,
) *compressedParallelIterator {
	return &compressedParallelIterator{
		bucket:    bucket,
		parallel:  parallel,
		logger:    logger,
		cacheSize: cacheSize,
		dimension: dimension,
	}
}

func (cpi *compressedParallelIterator) IterateAll() chan []BQVecAndID {
	if cpi.parallel <= 1 {
		// caller explicitly wants no parallelism, fallback to regular cursor
		return cpi.iterateAllNoConcurrency()
	}

	// We need one fewer seed than our desired parallel factor, that is because
	// we will add one routine that starts with cursor.First() and reads to the
	// first checkpoint, therefore we will have len(checkpoints) + 1 routines in
	// total.
	seedCount := cpi.parallel - 1
	seeds := cpi.bucket.QuantileKeys(seedCount)
	if len(seeds) == 0 {
		return nil
	}

	wg := sync.WaitGroup{}
	out := make(chan []BQVecAndID)

	var cacheVecs []uint64
	var cacheCounter atomic.Uint32
	// preallocate a slice to store all the vectors in - this saves many (expensive) small allocations.
	// Note that there was a bug that the dimensions were not restored when reloading the index, so we must check that
	// both values are non-zero before pre-allocating. Otherwise, fall back to small allocations which will load everything
	// correctly - the next shutdown will then write the correct values.
	if cpi.cacheSize != 0 && cpi.dimension != 0 {
		vecLength := cpi.dimension / 8 // 8 bytes per uint64
		cacheVecs = make([]uint64, cpi.cacheSize*vecLength)
		cacheCounter.Store(0)
	}

	// There are three scenarios:
	// 1. Read from beginning to first checkpoint
	// 2. Read from checkpoint n to checkpoint n+1
	// 3. Read from last checkpoint to end
	extract := func(k, v []byte) BQVecAndID {
		id := binary.BigEndian.Uint64(k)
		var vec []uint64
		if cacheVecs != nil {
			length := uint32(len(v) / 8)
			end := cacheCounter.Add(length)
			vec = uint64SliceFromByteSlice(v, cacheVecs[end-length:end])
		} else {
			vec = uint64SliceFromByteSlice(v, make([]uint64, len(v)/8))
		}
		return BQVecAndID{id: id, vec: vec}
	}

	// S1: Read from beginning to first checkpoint:
	wg.Add(1)
	enterrors.GoWrapper(func() {
		c := cpi.bucket.Cursor()
		localResults := make([]BQVecAndID, 0, 10_000)
		defer c.Close()
		defer wg.Done()

		for k, v := c.First(); k != nil && bytes.Compare(k, seeds[0]) < 0; k, v = c.Next() {
			localResults = append(localResults, extract(k, v))
		}

		out <- localResults
	}, cpi.logger)

	// S2: Read from checkpoint n to checkpoint n+1, stop at last checkpoint:
	for i := 0; i < len(seeds)-1; i++ {
		wg.Add(1)
		start := seeds[i]
		end := seeds[i+1]

		enterrors.GoWrapper(func() {
			defer wg.Done()
			localResults := make([]BQVecAndID, 0, 10_000)
			c := cpi.bucket.Cursor()
			defer c.Close()

			for k, v := c.Seek(start); k != nil && bytes.Compare(k, end) < 0; k, v = c.Next() {
				localResults = append(localResults, extract(k, v))
			}

			out <- localResults
		}, cpi.logger)
	}

	// S3: Read from last checkpoint to end:
	wg.Add(1)
	enterrors.GoWrapper(func() {
		c := cpi.bucket.Cursor()
		defer c.Close()
		defer wg.Done()
		localResults := make([]BQVecAndID, 0, 10_000)

		for k, v := c.Seek(seeds[len(seeds)-1]); k != nil; k, v = c.Next() {
			localResults = append(localResults, extract(k, v))
		}

		out <- localResults
	}, cpi.logger)

	enterrors.GoWrapper(func() {
		wg.Wait()
		close(out)
	}, cpi.logger)

	return out
}

func (cpi *compressedParallelIterator) iterateAllNoConcurrency() chan []BQVecAndID {
	out := make(chan []BQVecAndID)
	enterrors.GoWrapper(func() {
		defer close(out)
		c := cpi.bucket.Cursor()
		defer c.Close()

		localResults := make([]BQVecAndID, 0, 10_000)
		for k, v := c.First(); k != nil; k, v = c.Next() {
			id := binary.BigEndian.Uint64(k)
			vec := uint64SliceFromByteSlice(v, make([]uint64, len(v)/8))
			localResults = append(localResults, BQVecAndID{id: id, vec: vec})
		}

		out <- localResults
	}, cpi.logger)

	return out
}

type BQVecAndID struct {
	id  uint64
	vec []uint64
}

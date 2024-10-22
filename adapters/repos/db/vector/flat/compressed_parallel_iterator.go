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

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	enterrors "github.com/weaviate/weaviate/entities/errors"
)

type parallelIterator struct {
	bucket   *lsmkv.Bucket
	parallel int
	logger   logrus.FieldLogger
}

func NewParallelIterator(bucket *lsmkv.Bucket, parallel int,
	logger logrus.FieldLogger,
) *parallelIterator {
	return &parallelIterator{
		bucket:   bucket,
		parallel: parallel,
		logger:   logger,
	}
}

func (cpi *parallelIterator) IterateAll() chan []BQVecAndID {
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

	// There are three scenarios:
	// 1. Read from beginning to first checkpoint
	// 2. Read from checkpoint n to checkpoint n+1
	// 3. Read from last checkpoint to end

	extract := func(k, v []byte, vecBuf []uint64) BQVecAndID {
		id := binary.BigEndian.Uint64(k)
		vec := uint64SliceFromByteSlice(v, vecBuf)
		return BQVecAndID{id: id, vec: vec}
	}

	// S1: Read from beginning to first checkpoint:
	wg.Add(1)
	enterrors.GoWrapper(func() {
		c := cpi.bucket.Cursor()
		localResults := make([]BQVecAndID, 0, 10_000)
		defer c.Close()
		defer wg.Done()

		var vecBuffer []uint64
		for k, v := c.First(); k != nil && bytes.Compare(k, seeds[0]) < 0; k, v = c.Next() {
			length := len(v) / 8
			if len(vecBuffer) < length {
				vecBuffer = make([]uint64, length*1000)
			}

			localResults = append(localResults, extract(k, v, vecBuffer[:length]))
			vecBuffer = vecBuffer[length:]
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

			var vecBuffer []uint64
			for k, v := c.Seek(start); k != nil && bytes.Compare(k, end) < 0; k, v = c.Next() {
				length := len(v) / 8
				if len(vecBuffer) < length {
					vecBuffer = make([]uint64, length*1000)
				}

				localResults = append(localResults, extract(k, v, vecBuffer[:length]))
				vecBuffer = vecBuffer[length:]
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

		var vecBuffer []uint64
		for k, v := c.Seek(seeds[len(seeds)-1]); k != nil; k, v = c.Next() {
			length := len(v) / 8
			if len(vecBuffer) < length {
				vecBuffer = make([]uint64, length*1000)
			}

			localResults = append(localResults, extract(k, v, vecBuffer[:length]))
			vecBuffer = vecBuffer[length:]
		}

		out <- localResults
	}, cpi.logger)

	enterrors.GoWrapper(func() {
		wg.Wait()
		close(out)
	}, cpi.logger)

	return out
}

func (cpi *parallelIterator) iterateAllNoConcurrency() chan []BQVecAndID {
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

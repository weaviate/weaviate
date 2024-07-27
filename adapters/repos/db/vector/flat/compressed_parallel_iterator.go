package flat

import (
	"bytes"
	"encoding/binary"
	"sync"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
)

type compressedParallelIterator struct {
	bucket   *lsmkv.Bucket
	parallel int
}

func NewCompressedParallelIterator(bucket *lsmkv.Bucket, parallel int) *compressedParallelIterator {
	return &compressedParallelIterator{
		bucket:   bucket,
		parallel: parallel,
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

	// There are three scenarios:
	// 1. Read from beginning to first checkpoint
	// 2. Read from checkpoint n to checkpoint n+1
	// 3. Read from last checkpoint to end

	extract := func(k, v []byte) BQVecAndID {
		id := binary.BigEndian.Uint64(k)
		vec := uint64SliceFromByteSlice(v, make([]uint64, len(v)/8))
		return BQVecAndID{id: id, vec: vec}
	}

	// S1: Read from beginning to first checkpoint:
	wg.Add(1)
	go func() {
		c := cpi.bucket.Cursor()
		localResults := make([]BQVecAndID, 0, 10_000)
		defer c.Close()
		defer wg.Done()

		for k, v := c.First(); k != nil && bytes.Compare(k, seeds[0]) < 0; k, v = c.Next() {
			localResults = append(localResults, extract(k, v))
		}

		out <- localResults
	}()

	// S2: Read from checkpoint n to checkpoint n+1, stop at last checkpoint:
	for i := 0; i < len(seeds)-1; i++ {
		wg.Add(1)
		go func(start, end []byte) {
			defer wg.Done()
			localResults := make([]BQVecAndID, 0, 10_000)
			c := cpi.bucket.Cursor()
			defer c.Close()

			for k, v := c.Seek(start); k != nil && bytes.Compare(k, end) < 0; k, v = c.Next() {
				localResults = append(localResults, extract(k, v))
			}

			out <- localResults
		}(seeds[i], seeds[i+1])
	}

	// S3: Read from last checkpoint to end:
	wg.Add(1)
	go func() {
		c := cpi.bucket.Cursor()
		defer c.Close()
		defer wg.Done()
		localResults := make([]BQVecAndID, 0, 10_000)

		for k, v := c.Seek(seeds[len(seeds)-1]); k != nil; k, v = c.Next() {
			localResults = append(localResults, extract(k, v))
		}

		out <- localResults
	}()

	go func() {
		wg.Wait()
		close(out)
	}()

	return out
}

func (cpi *compressedParallelIterator) iterateAllNoConcurrency() chan []BQVecAndID {
	out := make(chan []BQVecAndID)
	go func() {
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
	}()

	return out
}

type BQVecAndID struct {
	id  uint64
	vec []uint64
}

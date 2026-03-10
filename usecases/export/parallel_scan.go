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

package export

import (
	"bytes"
	"context"
	"fmt"
	"runtime"
	"sync"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/storobj"
)

const scanBatchSize = 1000

// keyRange represents a contiguous range of keys [start, end) in a bucket.
// nil start means from the beginning; nil end means to the end.
type keyRange struct {
	start, end []byte
}

// scanJob is a self-contained unit of work: scan one key range from one shard
// and send resulting rows to rowsCh.
type scanJob struct {
	ctx        context.Context // per-shard context, canceled on writer error
	bucket     *lsmkv.Bucket
	keyRange   keyRange
	rowsCh     chan<- []ParquetRow
	wg         *sync.WaitGroup // per-shard WaitGroup, Done() called after scan
	setScanErr func(error)
}

func (j *scanJob) execute() {
	defer j.wg.Done()
	if err := scanRange(j.ctx, j.bucket, j.keyRange.start, j.keyRange.end, j.rowsCh); err != nil {
		j.setScanErr(err)
	}
}

// computeRanges splits a bucket's key space into roughly parallelism ranges
// using QuantileKeys.
func computeRanges(bucket *lsmkv.Bucket) []keyRange {
	parallelism := runtime.GOMAXPROCS(0) * 2
	if parallelism < 2 {
		parallelism = 2
	}

	quantileKeys := bucket.QuantileKeys(parallelism - 1)

	if len(quantileKeys) == 0 {
		return []keyRange{{start: nil, end: nil}}
	}

	ranges := make([]keyRange, 0, len(quantileKeys)+1)
	ranges = append(ranges, keyRange{start: nil, end: quantileKeys[0]})
	for i := 1; i < len(quantileKeys); i++ {
		ranges = append(ranges, keyRange{start: quantileKeys[i-1], end: quantileKeys[i]})
	}
	ranges = append(ranges, keyRange{start: quantileKeys[len(quantileKeys)-1], end: nil})

	return ranges
}

// scanRange scans [startKey, endKey) using a Cursor and sends batches
// of ParquetRows to rowsCh. If endKey is nil, scans to the end.
func scanRange(
	ctx context.Context,
	bucket *lsmkv.Bucket,
	startKey, endKey []byte,
	rowsCh chan<- []ParquetRow,
) error {
	cursor := bucket.Cursor()
	defer cursor.Close()

	var key, val []byte
	if startKey == nil {
		key, val = cursor.First()
	} else {
		key, val = cursor.Seek(startKey)
	}

	batch := make([]ParquetRow, 0, scanBatchSize)

	for key != nil {
		// Boundary check: stop if we've reached endKey.
		if endKey != nil && bytes.Compare(key, endKey) >= 0 {
			break
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		obj, err := storobj.FromBinary(val)
		if err != nil {
			return fmt.Errorf("deserialize object: %w", err)
		}

		row, err := convertToParquetRow(obj)
		if err != nil {
			return fmt.Errorf("convert object to parquet row: %w", err)
		}

		batch = append(batch, row)

		if len(batch) >= scanBatchSize {
			select {
			case rowsCh <- batch:
			case <-ctx.Done():
				return ctx.Err()
			}
			// Fresh allocation required: the writer goroutine still
			// holds a reference to the previous batch slice.
			batch = make([]ParquetRow, 0, scanBatchSize)
		}

		key, val = cursor.Next()
	}

	// Send remaining rows.
	if len(batch) > 0 {
		select {
		case rowsCh <- batch:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return nil
}

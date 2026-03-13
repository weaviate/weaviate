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
	"io"
	"sync"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/storobj"
)

// keyRange represents a contiguous range of keys [start, end) in a bucket.
// nil start means from the beginning; nil end means to the end.
type keyRange struct {
	start, end []byte
}

// scanJob is a self-contained unit of work: scan one key range, create a
// per-range writer pipeline, write directly to it, and upload.
type scanJob struct {
	ctx        context.Context // per-shard context
	bucket     *lsmkv.Bucket
	keyRange   keyRange
	rangeIndex int
	writerCfg  *rangeWriterConfig
	wg         *sync.WaitGroup // per-shard WaitGroup, Done() called after scan
	setErr     func(error)
	addWritten func(int64)
}

func (j *scanJob) execute() {
	defer j.wg.Done()

	pipeline, err := startRangeWriter(j.ctx, j.writerCfg, j.rangeIndex)
	if err != nil {
		j.setErr(fmt.Errorf("start range writer %d: %w", j.rangeIndex, err))
		return
	}

	scanErr := scanRangeToWriter(j.ctx, j.bucket, j.keyRange.start, j.keyRange.end, pipeline.writer)

	written, shutdownErr := pipeline.Shutdown(scanErr)
	if shutdownErr != nil {
		j.setErr(shutdownErr)
		return
	}

	j.addWritten(written)
}

const (
	// minObjectsPerRange is the minimum number of objects each key range should
	// contain. When the bucket has fewer objects than parallelism * this value,
	// we reduce the number of ranges so that each range has meaningful work.
	minObjectsPerRange = 10_000

	// maxObjectsPerRange bounds the maximum objects per range, ensuring each
	// parquet file stays a manageable size for upload and retry. With a typical
	// row of ~7-8 KB (1536-dim vector + properties), 500K rows produce roughly
	// 3.5-4 GB uncompressed. Zstd compression brings this down to ~2-3 GB per
	// file, which keeps individual uploads recoverable without excessive memory
	// or retry cost.
	maxObjectsPerRange = 500_000
)

// computeRanges splits a bucket's key space into ranges using QuantileKeys.
// The number of ranges is bounded by both minObjectsPerRange (lower bound on
// range size) and maxObjectsPerRange (upper bound), and can exceed parallelism
// for very large shards.
func computeRanges(bucket *lsmkv.Bucket, parallelism int) []keyRange {
	count := bucket.CountAsync()
	numRanges := computeNumRanges(count, parallelism)

	if numRanges < 2 {
		return []keyRange{{start: nil, end: nil}}
	}

	quantileKeys := bucket.QuantileKeys(numRanges - 1)

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

// computeNumRanges determines how many key ranges to create given the object
// count and desired parallelism. The result is bounded by minObjectsPerRange
// (so ranges aren't too small) and maxObjectsPerRange (so files aren't too large).
func computeNumRanges(count, parallelism int) int {
	numRanges := parallelism
	if count > 0 {
		// Don't create ranges smaller than minObjectsPerRange.
		numRanges = min(numRanges, count/minObjectsPerRange)
		// Ensure no range exceeds maxObjectsPerRange.
		minRequired := (count + maxObjectsPerRange - 1) / maxObjectsPerRange // ceil division
		numRanges = max(numRanges, minRequired)
	}
	return max(numRanges, 1)
}

// scanRangeToWriter scans [startKey, endKey) using a Cursor and writes
// rows directly to a ParquetWriter. If endKey is nil, scans to the end.
func scanRangeToWriter(
	ctx context.Context,
	bucket *lsmkv.Bucket,
	startKey, endKey []byte,
	writer *ParquetWriter,
) error {
	cursor := bucket.Cursor()
	defer cursor.Close()

	var key, val []byte
	if startKey == nil {
		key, val = cursor.First()
	} else {
		key, val = cursor.Seek(startKey)
	}

	for key != nil {
		if endKey != nil && bytes.Compare(key, endKey) >= 0 {
			break
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		fields, err := storobj.ExportFieldsFromBinary(val)
		if err != nil {
			return fmt.Errorf("extract export fields: %w", err)
		}

		row := ParquetRow{
			ID:           fields.ID,
			CreationTime: fields.CreateTime,
			UpdateTime:   fields.UpdateTime,
			Vector:       fields.VectorBytes,
			NamedVectors: fields.NamedVectors,
			MultiVectors: fields.MultiVectors,
			Properties:   fields.Properties,
		}

		if err := writer.WriteRow(row); err != nil {
			return fmt.Errorf("write row to parquet: %w", err)
		}

		key, val = cursor.Next()
	}

	return nil
}

// rangeWriterConfig holds shared configuration for all range writers of a shard.
type rangeWriterConfig struct {
	backend   modulecapabilities.BackupBackend
	req       *ExportRequest
	className string
	shardName string
	isMT      bool
	logger    logrus.FieldLogger
}

// rangePipeline bundles a per-range ParquetWriter, io.Pipe, and upload goroutine.
type rangePipeline struct {
	pw         *io.PipeWriter
	writer     *ParquetWriter
	uploadDone <-chan error
}

// Shutdown closes the writer pipeline and waits for the upload to finish.
// If scanErr is non-nil, the pipeline is torn down and scanErr is returned.
func (rp *rangePipeline) Shutdown(scanErr error) (int64, error) {
	if scanErr != nil {
		_ = rp.writer.Close()
		rp.pw.CloseWithError(scanErr)
		<-rp.uploadDone
		return 0, scanErr
	}

	if err := rp.writer.Close(); err != nil {
		rp.pw.CloseWithError(err)
		<-rp.uploadDone
		return 0, err
	}

	if err := rp.pw.Close(); err != nil {
		<-rp.uploadDone
		return 0, err
	}

	if uploadErr := <-rp.uploadDone; uploadErr != nil {
		return 0, uploadErr
	}

	return rp.writer.ObjectsWritten(), nil
}

// startRangeWriter creates a rangePipeline for a single key range.
func startRangeWriter(ctx context.Context, cfg *rangeWriterConfig, rangeIndex int) (*rangePipeline, error) {
	pr, pw := io.Pipe()

	fileName := fmt.Sprintf("%s_%s_%04d.parquet", cfg.className, cfg.shardName, rangeIndex)

	uploadDone := make(chan error, 1)
	enterrors.GoWrapper(func() {
		var err error
		defer func() { uploadDone <- err }()
		_, err = cfg.backend.Write(ctx, cfg.req.ID, fileName, cfg.req.Bucket, cfg.req.Path, pr)
	}, cfg.logger)

	writer, err := NewParquetWriter(pw)
	if err != nil {
		pw.CloseWithError(err)
		<-uploadDone
		return nil, fmt.Errorf("create parquet writer: %w", err)
	}

	writer.SetFileMetadata("collection", cfg.className)
	if cfg.isMT {
		writer.SetFileMetadata("tenant", cfg.shardName)
	}

	return &rangePipeline{
		pw:         pw,
		writer:     writer,
		uploadDone: uploadDone,
	}, nil
}

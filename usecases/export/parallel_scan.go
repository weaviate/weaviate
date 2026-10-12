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
	"sync"
	"sync/atomic"
	"time"

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

// scanJob scans one key range into one or more parquet files and uploads
// them.
type scanJob struct {
	ctx        context.Context // per-shard context
	bucket     *lsmkv.Bucket
	keyRange   keyRange
	rangeIndex int
	writerCfg  *rangeWriterConfig
	wg         *sync.WaitGroup // per-shard WaitGroup, Done() called after scan
	setErr     func(error)
}

func (j *scanJob) execute() {
	defer j.wg.Done()

	// getWriter opens a file on the first row and on the first row after the
	// open file reaches maxFileBytes, so a range never uploads an empty file.
	// It waits for the full file's upload, so an upload error stops the scan.
	// A cap of zero or below means the default; otherwise every row would get
	// its own file.
	fileCap := j.writerCfg.maxFileBytes
	if fileCap <= 0 {
		fileCap = maxFileBytes
	}
	var pipeline *rangePipeline
	getWriter := func() (*ParquetWriter, error) {
		if pipeline != nil && pipeline.writer.Size() >= fileCap {
			// Detach the full file before shutting it down. If Shutdown fails, its
			// error comes back as scanErr with pipeline nil, so execute passes it
			// to setErr and doesn't shut the same pipeline down a second time.
			full := pipeline
			pipeline = nil
			if err := full.Shutdown(nil); err != nil {
				return nil, err
			}
		}
		if pipeline == nil {
			p, err := startRangeWriter(j.ctx, j.writerCfg)
			if err != nil {
				return nil, fmt.Errorf("start range writer %d: %w", j.rangeIndex, err)
			}
			pipeline = p
		}
		return pipeline.writer, nil
	}

	scanErr := scanRangeToWriter(j.ctx, j.bucket, j.keyRange.start, j.keyRange.end, getWriter)

	if pipeline == nil {
		// No file is open, because the range was empty, the scan failed before its
		// first row, or opening a file or shutting down the full one failed.
		if scanErr != nil {
			j.setErr(scanErr)
		}
		return
	}

	if shutdownErr := pipeline.Shutdown(scanErr); shutdownErr != nil {
		j.setErr(shutdownErr)
	}
}

const (
	// minObjectsPerRange is the minimum number of objects each key range should
	// contain. When the bucket has fewer objects than parallelism * this value,
	// we reduce the number of ranges so that each range has meaningful work.
	minObjectsPerRange = 50_000

	// maxObjectsPerRange bounds the objects per range, so a large shard splits
	// into more ranges than there are workers.
	maxObjectsPerRange = 500_000

	// maxFileBytes caps a parquet file's size. parquet-go keeps a file's page
	// index and footer metadata in memory until Close, and both grow with the
	// file.
	maxFileBytes = 1 << 30 // 1 GiB
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
// count and desired parallelism. minObjectsPerRange and maxObjectsPerRange
// bound the objects per range.
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

// scanRangeToWriter scans [startKey, endKey) using a Cursor and writes each
// row to the writer getWriter returns for it. An empty range never calls
// getWriter, so it starts no upload. If endKey is nil, it scans to the end.
//
// The writer's onFlush callback reports progress when a batch enters the open
// row group, up to one row group before its bytes reach the upload pipe.
func scanRangeToWriter(
	ctx context.Context,
	bucket *lsmkv.Bucket,
	startKey, endKey []byte,
	getWriter func() (*ParquetWriter, error),
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

		writer, err := getWriter()
		if err != nil {
			return err
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
	backend      modulecapabilities.BackupBackend
	req          *ExportRequest
	className    string
	shardName    string
	isMT         bool
	maxFileBytes int64 // scanJob.execute starts a new file once the open one reaches this size; <= 0 means maxFileBytes
	logger       logrus.FieldLogger
	onFlush      func(int64)  // called after each successful ParquetWriter flush
	filesOpened  atomic.Int32 // numbers the shard's files across all its ranges
}

// rangePipeline bundles one file's ParquetWriter, buffered pipe, and upload
// goroutine. Once the pipe is full (defaultPipeBufferSize), the scan waits for
// the upload with its LSM cursor open.
type rangePipeline struct {
	pw         *bufferedPipeWriter
	writer     *ParquetWriter
	uploadDone <-chan error
}

// Shutdown closes the writer pipeline and waits for the upload to finish.
// If scanErr is non-nil, Shutdown clears onFlush first, so rows still in the
// batch buffer are not counted. On any error, including a failed upload,
// onFlush may already have counted batches that never reached the backend.
func (rp *rangePipeline) Shutdown(scanErr error) error {
	if scanErr != nil {
		rp.writer.onFlush = nil
		_ = rp.writer.Close()
		rp.pw.CloseWithError(scanErr)
		<-rp.uploadDone
		return scanErr
	}

	if err := rp.writer.Close(); err != nil {
		rp.pw.CloseWithError(err)
		<-rp.uploadDone
		return err
	}

	if err := rp.pw.Close(); err != nil {
		<-rp.uploadDone
		return err
	}

	if uploadErr := <-rp.uploadDone; uploadErr != nil {
		return uploadErr
	}

	return nil
}

// startRangeWriter creates a rangePipeline for the shard's next file.
func startRangeWriter(ctx context.Context, cfg *rangeWriterConfig) (*rangePipeline, error) {
	pr, pw := newBufferedPipe(defaultPipeBufferSize)

	fileIndex := cfg.filesOpened.Add(1) - 1
	fileName := fmt.Sprintf("%s_%s_%04d.parquet", cfg.className, cfg.shardName, fileIndex)

	uploadDone := make(chan error, 1)
	uploadStart := time.Now()
	enterrors.GoWrapper(func() {
		var err error
		defer func() {
			if err != nil {
				cfg.logger.WithField("action", "export_upload").
					WithField("export_id", cfg.req.ID).
					WithField("class", cfg.className).
					WithField("shard", cfg.shardName).
					WithField("file", fileName).
					WithField("duration_ms", time.Since(uploadStart).Milliseconds()).
					Errorf("upload failed: %v", err)
			} else {
				cfg.logger.WithField("action", "export_upload").
					WithField("export_id", cfg.req.ID).
					WithField("class", cfg.className).
					WithField("shard", cfg.shardName).
					WithField("file", fileName).
					WithField("duration_ms", time.Since(uploadStart).Milliseconds()).
					Info("upload completed")
			}
			uploadDone <- err
			close(uploadDone)
		}()
		_, err = cfg.backend.Write(ctx, cfg.req.ID, fileName, cfg.req.Bucket, cfg.req.Path, pr)
	}, cfg.logger)

	writer, err := NewParquetWriter(pw)
	if err != nil {
		pw.CloseWithError(err)
		<-uploadDone
		return nil, fmt.Errorf("create parquet writer: %w", err)
	}
	writer.onFlush = cfg.onFlush

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

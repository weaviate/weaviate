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
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"runtime"
	"sync"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/parquet-go/parquet-go"
	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storobj"
)

type testShard struct {
	store *lsmkv.Store
	name  string
}

func (s *testShard) Store() *lsmkv.Store { return s.store }
func (s *testShard) Name() string        { return s.name }

func createTestStore(t *testing.T, numObjects int) (*lsmkv.Store, int) {
	t.Helper()
	dir := t.TempDir()
	ctx := context.Background()
	logger, _ := test.NewNullLogger()

	store, err := lsmkv.New(dir, dir, logger, nil, nil,
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop())
	require.NoError(t, err)

	err = store.CreateOrLoadBucket(ctx, helpers.ObjectsBucketLSM,
		lsmkv.WithStrategy(lsmkv.StrategyReplace), lsmkv.WithCalcCountNetAdditions(true))
	require.NoError(t, err)

	bucket := store.Bucket(helpers.ObjectsBucketLSM)
	require.NotNil(t, bucket)

	inserted := 0
	// Insert objects in batches across multiple segments.
	batchSize := numObjects / 3
	if batchSize == 0 {
		batchSize = numObjects
	}

	for i := 0; i < numObjects; i++ {
		obj := createTestStorObj(t, uint64(i), "TestClass")
		data, err := obj.MarshalBinary()
		require.NoError(t, err)

		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, uint64(i))
		require.NoError(t, bucket.Put(key, data))
		inserted++

		if inserted%batchSize == 0 && inserted < numObjects {
			require.NoError(t, bucket.FlushAndSwitch())
		}
	}

	if inserted > 0 {
		require.NoError(t, bucket.FlushAndSwitch())
	}

	return store, inserted
}

func createTestStorObj(t *testing.T, id uint64, className string) *storobj.Object {
	t.Helper()
	uid := strfmt.UUID(fmt.Sprintf("00000000-0000-0000-0000-%012d", id))
	obj := &models.Object{
		ID:    uid,
		Class: className,
	}
	return storobj.FromObject(obj, nil, nil, nil)
}

// scanShardWithWorkers exercises the worker pool pattern end-to-end for a
// single shard: it computes ranges, submits scanJobs to workers, and collects
// results via a ParquetWriter.
func scanShardWithWorkers(ctx context.Context, shard ShardLike, writer *ParquetWriter, numWorkers int, logger logrus.FieldLogger) error {
	store := shard.Store()
	if store == nil {
		return fmt.Errorf("store not found for shard %s", shard.Name())
	}

	bucket := store.Bucket(helpers.ObjectsBucketLSM)
	if bucket == nil {
		return fmt.Errorf("objects bucket not found for shard %s", shard.Name())
	}

	ranges := computeRanges(bucket, numWorkers)

	rowsCh := make(chan []ParquetRow, numWorkers)

	scanCtx, scanCancel := context.WithCancel(ctx)
	defer scanCancel()

	var writerErr error
	writerDone := make(chan struct{})
	enterrors.GoWrapper(func() {
		defer close(writerDone)
		for batch := range rowsCh {
			for i := range batch {
				if err := writer.WriteRow(batch[i]); err != nil {
					writerErr = fmt.Errorf("write row to parquet: %w", err)
					scanCancel()
					for range rowsCh {
					}
					return
				}
			}
		}
	}, logger)

	var scanMu sync.Mutex
	var scanErr error
	setScanErr := func(err error) {
		scanMu.Lock()
		if scanErr == nil {
			scanErr = err
		}
		scanMu.Unlock()
	}

	jobCh := make(chan scanJob, numWorkers)

	var workerWg sync.WaitGroup
	for range numWorkers {
		workerWg.Add(1)
		enterrors.GoWrapper(func() {
			defer workerWg.Done()
			for job := range jobCh {
				job.execute()
			}
		}, logger)
	}

	var shardWg sync.WaitGroup
	for _, r := range ranges {
		shardWg.Add(1)
		jobCh <- scanJob{
			ctx:        scanCtx,
			bucket:     bucket,
			keyRange:   r,
			rowsCh:     rowsCh,
			wg:         &shardWg,
			setScanErr: setScanErr,
		}
	}

	close(jobCh)
	workerWg.Wait()
	shardWg.Wait()
	close(rowsCh)
	<-writerDone

	if writerErr != nil {
		return writerErr
	}
	return scanErr
}

// TestScanShardWithWorkers verifies correctness and uniqueness across
// different dataset sizes: empty, single object, small, and exceeding the
// internal scanBatchSize (1000).
func TestScanShardWithWorkers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		numObjects int
	}{
		{name: "empty bucket", numObjects: 0},
		{name: "single object", numObjects: 1},
		{name: "small dataset", numObjects: 200},
		{name: "exceeds scan batch size", numObjects: 2500},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			logger, _ := test.NewNullLogger()

			store, inserted := createTestStore(t, tc.numObjects)
			defer store.Shutdown(context.Background())
			require.Equal(t, tc.numObjects, inserted)

			shard := &testShard{store: store, name: "test-shard"}

			var buf bytes.Buffer
			writer, err := NewParquetWriter(&buf)
			require.NoError(t, err)

			err = scanShardWithWorkers(context.Background(), shard, writer, 4, logger)
			require.NoError(t, err)
			require.NoError(t, writer.Close())

			assert.Equal(t, int64(tc.numObjects), writer.ObjectsWritten())

			if tc.numObjects > 0 {
				readBack := readParquetRows(t, buf.Bytes())
				assert.Len(t, readBack, tc.numObjects)
				assertUniqueIDs(t, readBack)
			}
		})
	}
}

func TestScanShardWithWorkers_NilStore(t *testing.T) {
	t.Parallel()
	logger, _ := test.NewNullLogger()

	shard := &testShard{store: nil, name: "nil-store-shard"}

	var buf bytes.Buffer
	writer, err := NewParquetWriter(&buf)
	require.NoError(t, err)

	err = scanShardWithWorkers(context.Background(), shard, writer, 4, logger)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "store not found")
}

func TestScanShardWithWorkers_NilBucket(t *testing.T) {
	t.Parallel()
	logger, _ := test.NewNullLogger()

	// Create a store without the objects bucket.
	dir := t.TempDir()
	store, err := lsmkv.New(dir, dir, logger, nil, nil,
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop())
	require.NoError(t, err)
	defer store.Shutdown(context.Background())

	shard := &testShard{store: store, name: "no-bucket-shard"}

	var buf bytes.Buffer
	writer, err := NewParquetWriter(&buf)
	require.NoError(t, err)

	err = scanShardWithWorkers(context.Background(), shard, writer, 4, logger)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "objects bucket not found")
}

func TestScanShardWithWorkers_ContextCanceled(t *testing.T) {
	t.Parallel()
	logger, _ := test.NewNullLogger()

	store, _ := createTestStore(t, 200)
	defer store.Shutdown(context.Background())

	shard := &testShard{store: store, name: "test-shard"}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately.

	var buf bytes.Buffer
	writer, err := NewParquetWriter(&buf)
	require.NoError(t, err)

	err = scanShardWithWorkers(ctx, shard, writer, 4, logger)
	// Context cancellation is racy: the export may complete before
	// workers notice the cancellation.
	if err != nil {
		assert.ErrorIs(t, err, context.Canceled)
	}
}

// TestScanShardWithWorkers_SingleWorker exercises the single-worker path
// to verify correctness when all ranges are processed sequentially.
func TestScanShardWithWorkers_SingleWorker(t *testing.T) {
	t.Parallel()
	logger, _ := test.NewNullLogger()

	numObjects := 500
	store, inserted := createTestStore(t, numObjects)
	defer store.Shutdown(context.Background())
	require.Equal(t, numObjects, inserted)

	shard := &testShard{store: store, name: "test-shard"}

	var buf bytes.Buffer
	writer, err := NewParquetWriter(&buf)
	require.NoError(t, err)

	err = scanShardWithWorkers(context.Background(), shard, writer, 1, logger)
	require.NoError(t, err)
	require.NoError(t, writer.Close())

	assert.Equal(t, int64(numObjects), writer.ObjectsWritten())

	readBack := readParquetRows(t, buf.Bytes())
	assert.Len(t, readBack, numObjects)
	assertUniqueIDs(t, readBack)
}

func TestComputeRanges(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		objects   int
		maxRanges int // upper bound on expected range count
		minRanges int // lower bound on expected range count
	}{
		{
			name:      "empty bucket returns single range",
			objects:   0,
			minRanges: 1,
			maxRanges: 1,
		},
		{
			name:      "below min threshold returns single range",
			objects:   1000,
			minRanges: 1,
			maxRanges: 1,
		},
		{
			name:      "at threshold allows limited splitting",
			objects:   20_000,
			minRanges: 1,
			maxRanges: 2,
		},
		{
			name:      "large bucket allows multiple ranges",
			objects:   50_000,
			minRanges: 2,
			maxRanges: 50_000 / minObjectsPerRange,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			store, _ := createTestStore(t, tc.objects)
			defer store.Shutdown(context.Background())

			bucket := store.Bucket(helpers.ObjectsBucketLSM)
			require.NotNil(t, bucket)

			ranges := computeRanges(bucket, runtime.GOMAXPROCS(0)*2)
			require.NotEmpty(t, ranges)
			assert.GreaterOrEqual(t, len(ranges), tc.minRanges, "too few ranges")
			assert.LessOrEqual(t, len(ranges), tc.maxRanges, "too many ranges")

			// First range always starts at nil, last always ends at nil.
			assert.Nil(t, ranges[0].start)
			assert.Nil(t, ranges[len(ranges)-1].end)

			// Ranges must be contiguous: each range's end == next range's start.
			for i := 1; i < len(ranges); i++ {
				assert.Equal(t, ranges[i-1].end, ranges[i].start,
					"gap between range %d and %d", i-1, i)
			}
		})
	}
}

// readParquetRows reads all ParquetRow entries from a parquet file in memory.
func readParquetRows(t *testing.T, data []byte) []ParquetRow {
	t.Helper()

	reader := parquet.NewGenericReader[ParquetRow](bytes.NewReader(data))
	defer reader.Close()

	rows := make([]ParquetRow, reader.NumRows())
	n, err := reader.Read(rows)
	if err != nil && !errors.Is(err, io.EOF) {
		require.NoError(t, err, "failed to read parquet rows")
	}
	require.Equal(t, int(reader.NumRows()), n, "did not read all parquet rows")
	return rows[:n]
}

// assertUniqueIDs verifies that all ParquetRow IDs are unique.
func assertUniqueIDs(t *testing.T, rows []ParquetRow) {
	t.Helper()
	seen := make(map[string]struct{}, len(rows))
	for _, r := range rows {
		if _, exists := seen[r.ID]; exists {
			t.Errorf("duplicate ID found: %s", r.ID)
		}
		seen[r.ID] = struct{}{}
	}
}

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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"runtime"
	"sync"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/parquet-go/parquet-go"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/byteops"
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

	for i := range numObjects {
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

// TestComputeRanges exercises the integration with real LSM buckets,
// verifying that QuantileKeys produces contiguous, well-formed ranges.
// The range-count math is covered by TestComputeNumRanges.
func TestComputeRanges(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		objects     int
		parallelism int
		minRanges   int // lower bound on expected range count
		maxRanges   int // upper bound on expected range count
	}{
		{
			name:        "single object falls back to one range",
			objects:     1,
			parallelism: runtime.GOMAXPROCS(0) * 2,
			minRanges:   1,
			maxRanges:   1,
		},
		{
			name:        "few objects falls back to one range",
			objects:     5,
			parallelism: runtime.GOMAXPROCS(0) * 2,
			minRanges:   1,
			maxRanges:   1,
		},
		{
			name:        "splitting with two ranges",
			objects:     100_000,
			parallelism: runtime.GOMAXPROCS(0) * 2,
			minRanges:   1,
			maxRanges:   2,
		},
		{
			name:        "multiple ranges from large bucket",
			objects:     250_000,
			parallelism: runtime.GOMAXPROCS(0) * 2,
			minRanges:   2,
			maxRanges:   250_000 / minObjectsPerRange,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			store, _ := createTestStore(t, tc.objects)
			defer store.Shutdown(context.Background())

			bucket := store.Bucket(helpers.ObjectsBucketLSM)
			require.NotNil(t, bucket)

			ranges := computeRanges(bucket, tc.parallelism)
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

func TestComputeNumRanges(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		count       int
		parallelism int
		expected    int
	}{
		{
			name:        "empty bucket returns parallelism",
			count:       0,
			parallelism: 8,
			expected:    8, // computeRanges handles the empty-bucket fallback via QuantileKeys
		},
		{
			name:        "zero parallelism returns 1",
			count:       100_000,
			parallelism: 0,
			expected:    1,
		},
		{
			name:        "below minObjectsPerRange",
			count:       5_000,
			parallelism: 8,
			expected:    1,
		},
		{
			name:        "parallelism capped by minObjectsPerRange",
			count:       150_000,
			parallelism: 16,
			expected:    3, // 150k / 50k = 3
		},
		{
			name:        "parallelism is the limit",
			count:       1_000_000,
			parallelism: 4,
			expected:    4, // 1M / 50k = 20, but parallelism caps at 4
		},
		{
			name:        "maxObjectsPerRange forces more ranges than parallelism",
			count:       1_500_001,
			parallelism: 2,
			expected:    4, // ceil(1_500_001 / 500_000) = 4, exceeds parallelism
		},
		{
			name:        "exactly at maxObjectsPerRange boundary",
			count:       1_000_000,
			parallelism: 1,
			expected:    2, // 1M / 500K = 2
		},
		{
			name:        "just over one maxObjectsPerRange",
			count:       500_001,
			parallelism: 1,
			expected:    2, // ceil(500_001 / 500_000) = 2
		},
		{
			name:        "exactly one maxObjectsPerRange",
			count:       500_000,
			parallelism: 1,
			expected:    1, // 500K / 500K = 1
		},
		{
			name:        "very large shard",
			count:       5_000_000,
			parallelism: 4,
			expected:    10, // ceil(5M / 500K) = 10, exceeds parallelism of 4
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.expected, computeNumRanges(tc.count, tc.parallelism))
		})
	}
}

// TestScanAllRanges verifies that splitting a bucket into multiple ranges and
// scanning each range independently produces the exact same set of objects as
// the original bucket — no duplicates from overlapping boundaries, no gaps
// from missed keys.
func TestScanAllRanges(t *testing.T) {
	t.Parallel()

	const numObjects = 250_000
	const parallelism = 10

	store, inserted := createTestStore(t, numObjects)
	defer store.Shutdown(context.Background())
	require.Equal(t, numObjects, inserted)

	bucket := store.Bucket(helpers.ObjectsBucketLSM)
	require.NotNil(t, bucket)

	ranges := computeRanges(bucket, parallelism)
	require.Greater(t, len(ranges), 1, "test requires multiple ranges to be meaningful")

	// Scan each range into its own in-memory parquet file.
	var allRows []ParquetRow
	for i, r := range ranges {
		var buf bytes.Buffer
		writer, err := NewParquetWriter(&buf)
		require.NoError(t, err)

		err = scanRangeToWriter(context.Background(), bucket, r.start, r.end, writer)
		require.NoError(t, err)
		require.NoError(t, writer.Close())

		rows := readParquetRows(t, buf.Bytes())
		t.Logf("range %d: %d rows", i, len(rows))
		allRows = append(allRows, rows...)
	}

	assert.Len(t, allRows, numObjects, "total rows should match inserted objects")

	assertUniqueIDs(t, allRows)

	// Verify every expected ID is present (no gaps).
	expected := make(map[string]struct{}, numObjects)
	for i := range numObjects {
		expected[fmt.Sprintf("00000000-0000-0000-0000-%012d", i)] = struct{}{}
	}
	for _, row := range allRows {
		delete(expected, row.ID)
	}
	assert.Empty(t, expected, "missing %d IDs from scan output", len(expected))
}

// scanToRows is a test helper that calls scanRangeToWriter with an in-memory
// ParquetWriter and returns the decoded rows (or error).
func scanToRows(t *testing.T, ctx context.Context, bucket *lsmkv.Bucket, start, end []byte) ([]ParquetRow, error) {
	t.Helper()
	var buf bytes.Buffer
	writer, err := NewParquetWriter(&buf)
	require.NoError(t, err)

	scanErr := scanRangeToWriter(ctx, bucket, start, end, writer)
	require.NoError(t, writer.Close())
	if scanErr != nil {
		return nil, scanErr
	}
	return readParquetRows(t, buf.Bytes()), nil
}

func TestScanRangeToWriter(t *testing.T) {
	t.Parallel()

	const numObjects = 100
	store, _ := createTestStore(t, numObjects)
	t.Cleanup(func() { store.Shutdown(context.Background()) })

	bucket := store.Bucket(helpers.ObjectsBucketLSM)
	require.NotNil(t, bucket)

	makeKey := func(i int) []byte {
		k := make([]byte, 8)
		binary.BigEndian.PutUint64(k, uint64(i))
		return k
	}

	expectedID := func(i int) string {
		return fmt.Sprintf("00000000-0000-0000-0000-%012d", i)
	}

	t.Run("full scan with nil start and nil end", func(t *testing.T) {
		t.Parallel()
		rows, err := scanToRows(t, context.Background(), bucket, nil, nil)
		require.NoError(t, err)
		assert.Len(t, rows, numObjects)
	})

	t.Run("bounded range excludes endKey", func(t *testing.T) {
		t.Parallel()
		rows, err := scanToRows(t, context.Background(), bucket, makeKey(10), makeKey(20))
		require.NoError(t, err)
		require.Len(t, rows, 10)
		assert.Equal(t, expectedID(10), rows[0].ID)
		assert.Equal(t, expectedID(19), rows[len(rows)-1].ID)
	})

	t.Run("nil start scans from beginning", func(t *testing.T) {
		t.Parallel()
		rows, err := scanToRows(t, context.Background(), bucket, nil, makeKey(5))
		require.NoError(t, err)
		require.Len(t, rows, 5)
		assert.Equal(t, expectedID(0), rows[0].ID)
		assert.Equal(t, expectedID(4), rows[len(rows)-1].ID)
	})

	t.Run("nil end scans to end of bucket", func(t *testing.T) {
		t.Parallel()
		rows, err := scanToRows(t, context.Background(), bucket, makeKey(95), nil)
		require.NoError(t, err)
		require.Len(t, rows, 5)
		assert.Equal(t, expectedID(95), rows[0].ID)
		assert.Equal(t, expectedID(99), rows[len(rows)-1].ID)
	})

	t.Run("start equals end produces zero rows", func(t *testing.T) {
		t.Parallel()
		rows, err := scanToRows(t, context.Background(), bucket, makeKey(50), makeKey(50))
		require.NoError(t, err)
		assert.Empty(t, rows)
	})

	t.Run("start beyond last key produces zero rows", func(t *testing.T) {
		t.Parallel()
		rows, err := scanToRows(t, context.Background(), bucket, makeKey(numObjects+10), nil)
		require.NoError(t, err)
		assert.Empty(t, rows)
	})

	t.Run("adjacent ranges have no overlap and no gaps", func(t *testing.T) {
		t.Parallel()
		boundary := makeKey(50)
		left, err := scanToRows(t, context.Background(), bucket, nil, boundary)
		require.NoError(t, err)
		right, err := scanToRows(t, context.Background(), bucket, boundary, nil)
		require.NoError(t, err)

		combined := append(left, right...)
		assert.Len(t, combined, numObjects)
		assertUniqueIDs(t, combined)
	})

	t.Run("canceled context returns context.Canceled", func(t *testing.T) {
		t.Parallel()
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		_, err := scanToRows(t, ctx, bucket, nil, nil)
		require.ErrorIs(t, err, context.Canceled)
	})
}

// TestScanFieldRoundTrip verifies that all ParquetRow fields (vectors,
// named vectors, multi vectors, properties, timestamps) survive the full
// pipeline: bucket → cursor scan → ExportFieldsFromBinary → ParquetRow →
// parquet write → parquet read.
func TestScanFieldRoundTrip(t *testing.T) {
	t.Parallel()

	type objectSpec struct {
		name       string
		createTime int64
		updateTime int64
		vector     []float32
		namedVecs  map[string][]float32
		multiVecs  map[string][][]float32
		props      map[string]any
	}

	specs := []objectSpec{
		{
			name: "minimal",
		},
		{
			name:   "with vector",
			vector: []float32{1.5, 2.5, 3.5, 4.5},
		},
		{
			name:  "with properties",
			props: map[string]any{"title": "Hello", "count": float64(42), "active": true},
		},
		{
			name:      "with named vectors",
			namedVecs: map[string][]float32{"title": {0.1, 0.2, 0.3}, "body": {0.4, 0.5, 0.6}},
		},
		{
			name:      "with multi vectors",
			multiVecs: map[string][][]float32{"colbert": {{1.0, 2.0}, {3.0, 4.0}}},
		},
		{
			name:       "all fields",
			createTime: 1700000000000,
			updateTime: 1700000001000,
			vector:     []float32{0.1, 0.2, 0.3},
			namedVecs:  map[string][]float32{"semantic": {0.4, 0.5}},
			multiVecs:  map[string][][]float32{"colbert": {{0.6, 0.7}}},
			props:      map[string]any{"name": "full", "score": float64(0.99)},
		},
	}

	// Build a store with one object per spec.
	dir := t.TempDir()
	logger, _ := test.NewNullLogger()
	store, err := lsmkv.New(dir, dir, logger, nil, nil,
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop())
	require.NoError(t, err)
	t.Cleanup(func() { store.Shutdown(context.Background()) })

	err = store.CreateOrLoadBucket(context.Background(), helpers.ObjectsBucketLSM,
		lsmkv.WithStrategy(lsmkv.StrategyReplace))
	require.NoError(t, err)
	bucket := store.Bucket(helpers.ObjectsBucketLSM)
	require.NotNil(t, bucket)

	ids := make([]string, len(specs))
	for i, spec := range specs {
		uid := strfmt.UUID(fmt.Sprintf("00000000-0000-0000-0000-%012d", i))
		ids[i] = string(uid)

		obj := &models.Object{
			ID:                 uid,
			Class:              "TestClass",
			CreationTimeUnix:   spec.createTime,
			LastUpdateTimeUnix: spec.updateTime,
			Properties:         spec.props,
		}
		sobj := storobj.FromObject(obj, spec.vector, spec.namedVecs, spec.multiVecs)
		data, marshalErr := sobj.MarshalBinary()
		require.NoError(t, marshalErr)

		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, uint64(i))
		require.NoError(t, bucket.Put(key, data))
	}
	require.NoError(t, bucket.FlushAndSwitch())

	rows, err := scanToRows(t, context.Background(), bucket, nil, nil)
	require.NoError(t, err)
	require.Len(t, rows, len(specs))

	rowByID := make(map[string]ParquetRow, len(rows))
	for _, r := range rows {
		rowByID[r.ID] = r
	}

	for i, spec := range specs {
		t.Run(spec.name, func(t *testing.T) {
			t.Parallel()
			row, ok := rowByID[ids[i]]
			require.True(t, ok, "row not found for ID %s", ids[i])

			assert.Equal(t, spec.createTime, row.CreationTime)
			assert.Equal(t, spec.updateTime, row.UpdateTime)

			if spec.vector == nil {
				assert.Empty(t, row.Vector)
			} else {
				assert.Equal(t, byteops.Fp32SliceToBytes(spec.vector), row.Vector)
			}

			if spec.namedVecs == nil {
				assert.Empty(t, row.NamedVectors)
			} else {
				var actual map[string][]float32
				require.NoError(t, json.Unmarshal(row.NamedVectors, &actual))
				assert.Equal(t, spec.namedVecs, actual)
			}

			if spec.multiVecs == nil {
				assert.Empty(t, row.MultiVectors)
			} else {
				var actual map[string][][]float32
				require.NoError(t, json.Unmarshal(row.MultiVectors, &actual))
				assert.Equal(t, spec.multiVecs, actual)
			}

			if spec.props == nil {
				assert.Empty(t, row.Properties)
			} else {
				expected, jsonErr := json.Marshal(spec.props)
				require.NoError(t, jsonErr)
				assert.JSONEq(t, string(expected), string(row.Properties))
			}
		})
	}
}

// newTestPipeline creates a rangePipeline backed by a real io.Pipe and
// ParquetWriter. The upload goroutine drains the pipe reader and sends
// uploadErr to the done channel. The PipeReader is returned so callers
// can break the pipe for error-path tests.
func newTestPipeline(t *testing.T, uploadErr error) (*rangePipeline, *io.PipeReader) {
	t.Helper()
	pr, pw := io.Pipe()
	uploadDone := make(chan error, 1)
	go func() {
		_, err := io.Copy(io.Discard, pr)
		pr.CloseWithError(err)
		uploadDone <- uploadErr
	}()
	writer, err := NewParquetWriter(pw)
	require.NoError(t, err)
	return &rangePipeline{pw: pw, writer: writer, uploadDone: uploadDone}, pr
}

func TestRangePipelineShutdown(t *testing.T) {
	t.Parallel()

	t.Run("scanErr is returned as-is", func(t *testing.T) {
		t.Parallel()
		rp, _ := newTestPipeline(t, nil)
		scanErr := fmt.Errorf("scan failed")

		written, err := rp.Shutdown(scanErr)
		assert.Equal(t, int64(0), written)
		assert.Equal(t, scanErr, err)
	})

	t.Run("upload error propagated on clean scan", func(t *testing.T) {
		t.Parallel()
		uploadErr := fmt.Errorf("upload failed")
		rp, _ := newTestPipeline(t, uploadErr)

		written, err := rp.Shutdown(nil)
		assert.Equal(t, int64(0), written)
		assert.Equal(t, uploadErr, err)
	})

	t.Run("success returns written count", func(t *testing.T) {
		t.Parallel()
		rp, _ := newTestPipeline(t, nil)
		require.NoError(t, rp.writer.WriteRow(ParquetRow{ID: "a"}))
		require.NoError(t, rp.writer.WriteRow(ParquetRow{ID: "b"}))

		written, err := rp.Shutdown(nil)
		assert.NoError(t, err)
		assert.Equal(t, int64(2), written)
	})

	t.Run("writer close error propagated", func(t *testing.T) {
		t.Parallel()
		rp, pr := newTestPipeline(t, nil)
		// Buffer a row so Close has work to do (flush + footer).
		require.NoError(t, rp.writer.WriteRow(ParquetRow{ID: "x"}))
		// Break the pipe so the flush write fails.
		pr.Close()

		written, err := rp.Shutdown(nil)
		assert.Equal(t, int64(0), written)
		assert.Error(t, err)
	})
}

// failingWriteBackend embeds fakeBackend but returns an error from Write
// after draining the reader (so the pipe doesn't deadlock).
type failingWriteBackend struct {
	fakeBackend
	writeErr error
}

func (b *failingWriteBackend) Write(_ context.Context, _, _, _, _ string, r backup.ReadCloserWithError) (int64, error) {
	_, err := io.ReadAll(r)
	r.CloseWithError(err)
	return 0, b.writeErr
}

func TestScanJobExecute(t *testing.T) {
	t.Parallel()

	t.Run("happy path writes rows and reports count", func(t *testing.T) {
		t.Parallel()

		store, _ := createTestStore(t, 10)
		t.Cleanup(func() { store.Shutdown(context.Background()) })
		bucket := store.Bucket(helpers.ObjectsBucketLSM)

		backend := &fakeBackend{}
		logger, _ := test.NewNullLogger()

		cfg := &rangeWriterConfig{
			backend:   backend,
			req:       &ExportRequest{ID: "test", Bucket: "b", Path: "p"},
			className: "TestClass",
			shardName: "shard0",
			logger:    logger,
		}

		var wg sync.WaitGroup
		var written int64
		var gotErr error

		wg.Add(1)
		job := scanJob{
			ctx:        context.Background(),
			bucket:     bucket,
			keyRange:   keyRange{start: nil, end: nil},
			rangeIndex: 0,
			writerCfg:  cfg,
			wg:         &wg,
			setErr:     func(err error) { gotErr = err },
			addWritten: func(n int64) { written = n },
		}
		job.execute()
		wg.Wait()

		require.NoError(t, gotErr)
		assert.Equal(t, int64(10), written)

		data := backend.getWritten("TestClass_shard0_0000.parquet")
		require.NotNil(t, data)
		assert.Len(t, readParquetRows(t, data), 10)
	})

	t.Run("upload error calls setErr", func(t *testing.T) {
		t.Parallel()

		store, _ := createTestStore(t, 10)
		t.Cleanup(func() { store.Shutdown(context.Background()) })
		bucket := store.Bucket(helpers.ObjectsBucketLSM)

		logger, _ := test.NewNullLogger()
		cfg := &rangeWriterConfig{
			backend:   &failingWriteBackend{writeErr: fmt.Errorf("s3 upload failed")},
			req:       &ExportRequest{ID: "test", Bucket: "b", Path: "p"},
			className: "TestClass",
			shardName: "shard0",
			logger:    logger,
		}

		var wg sync.WaitGroup
		var written int64
		var gotErr error

		wg.Add(1)
		job := scanJob{
			ctx:        context.Background(),
			bucket:     bucket,
			keyRange:   keyRange{start: nil, end: nil},
			rangeIndex: 0,
			writerCfg:  cfg,
			wg:         &wg,
			setErr:     func(err error) { gotErr = err },
			addWritten: func(n int64) { written = n },
		}
		job.execute()
		wg.Wait()

		require.Error(t, gotErr)
		assert.Contains(t, gotErr.Error(), "s3 upload failed")
		assert.Equal(t, int64(0), written)
	})
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

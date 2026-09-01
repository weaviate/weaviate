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

package docid

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storobj"
)

var errScanFn = errors.New("scan fn failed")

func TestScanObjectsLSM(t *testing.T) {
	// enough objects that the scan splits them across several concurrent workers
	const manyObjects = 100

	tests := []struct {
		name        string
		objectCount int
		// the scanFn call that returns errScanFn, counted from one; 0 lets every
		// call succeed
		failOnCall int
	}{
		{name: "no objects", objectCount: 0, failOnCall: 0},
		{name: "every call succeeds", objectCount: manyObjects, failOnCall: 0},
		{name: "the only object fails", objectCount: 1, failOnCall: 1},
		{name: "the first scan call fails", objectCount: manyObjects, failOnCall: 1},
		{name: "a mid-scan call fails", objectCount: manyObjects, failOnCall: manyObjects / 2},
		{name: "the last scan call fails", objectCount: manyObjects, failOnCall: manyObjects},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, _ := test.NewNullLogger()
			store, pointers := storeWithObjects(t, tt.objectCount, logger, false)

			var calls atomic.Int64
			scan := func(_ context.Context, _ *models.PropertySchema, _ uint64) error {
				if int(calls.Add(1)) == tt.failOnCall {
					return errScanFn
				}
				return nil
			}

			err := ScanObjectsLSM(context.Background(), store, pointers, scan, []string{"name"}, logger)

			if tt.failOnCall == 0 {
				require.NoError(t, err)
				require.Equal(t, int64(tt.objectCount), calls.Load())
				return
			}
			require.ErrorIs(t, err, errScanFn)
		})
	}
}

func TestScanObjectsLSMCancellation(t *testing.T) {
	const someObjects = 100

	// ten doc IDs per worker, so the first worker's range holds the whole of what
	// the two part-way rows observe and no other worker can reach scanFn
	tenPerWorker := 10 * scanConcurrency()

	tests := []struct {
		name string
		// doc IDs beyond storedObjects resolve to no object, so the loop skips
		// them without ever reaching scanFn
		storedObjects int
		pointerCount  int
		// otherwise the first scanFn call cancels, part way through the scan
		cancelBeforeScan bool
		maxScanCalls     int
	}{
		{
			name:             "cancelled before the scan, every doc ID resolves",
			storedObjects:    someObjects,
			pointerCount:     someObjects,
			cancelBeforeScan: true,
			maxScanCalls:     0,
		},
		{
			name:             "cancelled before the scan, no doc ID resolves",
			storedObjects:    0,
			pointerCount:     someObjects,
			cancelBeforeScan: true,
			maxScanCalls:     0,
		},
		{
			name:             "cancelled before the scan, no doc IDs at all",
			storedObjects:    0,
			pointerCount:     0,
			cancelBeforeScan: true,
			maxScanCalls:     0,
		},
		{
			// only the first worker's range resolves, so it alone reaches scanFn and
			// the count below is its own behaviour rather than a race between workers
			name:          "cancelled part way through, every doc ID resolves",
			storedObjects: 10,
			pointerCount:  tenPerWorker,
			maxScanCalls:  1,
		},
		{
			name:          "cancelled part way through, only the first doc ID resolves",
			storedObjects: 1,
			pointerCount:  tenPerWorker,
			maxScanCalls:  1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, _ := test.NewNullLogger()
			store, pointers := storeWithObjects(t, tt.storedObjects, logger, false)
			for i := len(pointers); i < tt.pointerCount; i++ {
				pointers = append(pointers, uint64(i))
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if tt.cancelBeforeScan {
				cancel()
			}

			var calls atomic.Int64
			var sawCancel atomic.Bool
			scan := func(scanCtx context.Context, _ *models.PropertySchema, _ uint64) error {
				calls.Add(1)
				cancel()
				// cancelling the caller's context propagates to children before it
				// returns, so a scanCtx derived from it is already cancelled here
				sawCancel.Store(scanCtx.Err() != nil)
				return nil
			}

			err := ScanObjectsLSM(ctx, store, pointers, scan, []string{"name"}, logger)

			require.ErrorIs(t, err, context.Canceled)
			require.LessOrEqual(t, calls.Load(), int64(tt.maxScanCalls))
			if calls.Load() > 0 {
				require.True(t, sawCancel.Load(), "scanFn was handed a context the caller cannot cancel")
			}
		})
	}
}

// A worker's payload buffer is grown to the largest object it has read, so a
// smaller object is parsed out of a buffer whose spare capacity still holds a
// larger one. These rows pin the values scanFn is handed under that reuse.
func TestScanObjectsLSMPropertyValues(t *testing.T) {
	const (
		objectCount = 200
		sizeStep    = 64
	)
	propertyNames := []string{"name", "count", "flag", "tags", "scores", "meta"}

	tests := []struct {
		name string
		// largest object first, so the later ones meet an over-sized buffer
		descending bool
		// every nth pointer resolves to no object; 0 inserts none
		missingEvery int
		// every nth object is padded past maxRetainedBufferBytes; 0 pads none
		oversizedEvery int
	}{
		{name: "sizes descending", descending: true},
		{name: "sizes ascending", descending: false},
		{name: "sizes descending, doc IDs missing mid-scan", descending: true, missingEvery: 7},
		{name: "objects past the retained buffer cap", oversizedEvery: 13},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			props := make([]map[string]interface{}, objectCount)
			for i := range props {
				padding := (i + 1) * sizeStep
				if tt.descending {
					padding = (objectCount - i) * sizeStep
				}
				if tt.oversizedEvery > 0 && i%tt.oversizedEvery == 0 {
					padding = maxRetainedBufferBytes + sizeStep
				}
				props[i] = objectProperties(i, padding)
			}

			logger, _ := test.NewNullLogger()
			store, stored := storeWithProperties(t, logger, true, props)

			pointers := stored
			if tt.missingEvery > 0 {
				pointers = nil
				for i, id := range stored {
					if i%tt.missingEvery == 0 {
						pointers = append(pointers, uint64(objectCount+i))
					}
					pointers = append(pointers, id)
				}
			}
			require.Greater(t, len(pointers), scanConcurrency(),
				"a worker must read more than one object or its buffer is never reused")

			var lock sync.Mutex
			scanned := map[uint64]interface{}{}
			scan := func(_ context.Context, prop *models.PropertySchema, docID uint64) error {
				lock.Lock()
				defer lock.Unlock()
				scanned[docID] = *prop
				return nil
			}

			require.NoError(t, ScanObjectsLSM(context.Background(), store, pointers, scan,
				propertyNames, logger))

			require.Len(t, scanned, objectCount)
			for i, want := range props {
				require.Equal(t, want, scanned[uint64(i)], "properties of object %d", i)
			}
		})
	}
}

// objectProperties returns properties already in the shape UnmarshalProperties
// decodes them back into, so the fixture doubles as the expected value.
func objectProperties(i, padding int) map[string]interface{} {
	tags := make([]interface{}, 0, padding/512+1)
	for j := 0; j <= padding/512; j++ {
		tags = append(tags, fmt.Sprintf("tag-%d-%d", i, j))
	}
	return map[string]interface{}{
		"name":   strings.Repeat("x", padding),
		"count":  float64(i),
		"flag":   i%2 == 0,
		"tags":   tags,
		"scores": []interface{}{float64(i) + 0.5, float64(i) + 1.5},
		"meta":   map[string]interface{}{"owner": fmt.Sprintf("owner-%d", i), "rank": float64(i)},
	}
}

// Sweeps what the per-doc-ID context poll and the reused payload buffer cost,
// on the two shapes the loop takes: every doc ID resolving to an object, and
// none of them resolving.
func BenchmarkScanObjectsLSM(b *testing.B) {
	const docIDCount = 50000

	benchmarks := []struct {
		name          string
		storedObjects int
	}{
		{name: "every doc ID resolves", storedObjects: docIDCount},
		{name: "no doc ID resolves", storedObjects: 0},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			logger, _ := test.NewNullLogger()
			store, pointers := storeWithObjects(b, bm.storedObjects, logger, true)
			for i := len(pointers); i < docIDCount; i++ {
				pointers = append(pointers, uint64(i))
			}
			scan := func(_ context.Context, _ *models.PropertySchema, _ uint64) error { return nil }

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := ScanObjectsLSM(context.Background(), store, pointers, scan,
					[]string{"name"}, logger); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// storeWithObjects returns a store holding count objects keyed by their UUID,
// with the docID secondary index ScanObjectsLSM resolves pointers through, and
// the docIDs of those objects.
func storeWithObjects(tb testing.TB, count int, logger logrus.FieldLogger, flush bool) (*lsmkv.Store, []uint64) {
	props := make([]map[string]interface{}, count)
	for i := range props {
		props[i] = map[string]interface{}{"name": fmt.Sprintf("object-%d", i)}
	}
	return storeWithProperties(tb, logger, flush, props)
}

// storeWithProperties returns a store holding one object per entry in props and
// the docIDs of those objects. flush is what puts them in a disk segment, the
// only path on which GetBySecondaryWithBuffer fills the caller's buffer.
func storeWithProperties(tb testing.TB, logger logrus.FieldLogger, flush bool,
	props []map[string]interface{},
) (*lsmkv.Store, []uint64) {
	ctx := context.Background()
	dir := tb.TempDir()

	store, err := lsmkv.New(dir, dir, logger, nil, nil,
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop())
	require.NoError(tb, err)
	tb.Cleanup(func() { require.NoError(tb, store.Shutdown(ctx)) })

	require.NoError(tb, store.CreateOrLoadBucket(ctx, helpers.ObjectsBucketLSM,
		lsmkv.WithStrategy(lsmkv.StrategyReplace), lsmkv.WithSecondaryIndices(1)))
	bucket := store.Bucket(helpers.ObjectsBucketLSM)

	pointers := make([]uint64, len(props))
	for i := range pointers {
		docID := uint64(i)
		pointers[i] = docID

		id := uuid.New()
		obj := storobj.New(docID)
		obj.SetID(strfmt.UUID(id.String()))
		obj.Object.Properties = props[i]
		objBytes, err := obj.MarshalBinary()
		require.NoError(tb, err)

		// the shard write path keys objects by the UUID's 16 raw bytes
		idBytes, err := id.MarshalBinary()
		require.NoError(tb, err)

		docIDBytes := make([]byte, 8)
		binary.LittleEndian.PutUint64(docIDBytes, docID)
		require.NoError(tb, bucket.Put(idBytes, objBytes,
			lsmkv.WithSecondaryKey(helpers.ObjectsBucketLSMDocIDSecondaryIndex, docIDBytes)))
	}

	if flush && len(props) > 0 {
		require.NoError(tb, bucket.FlushMemtable())
		requireSegmentOnDisk(tb, bucket.GetDir())
	}

	return store, pointers
}

// requireSegmentOnDisk fails unless the bucket wrote a segment; without one a
// scan answers from the memtable and never reuses the buffer.
func requireSegmentOnDisk(tb testing.TB, dir string) {
	entries, err := os.ReadDir(dir)
	require.NoError(tb, err)
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".db" {
			return
		}
	}
	require.Fail(tb, "no segment file in "+dir)
}

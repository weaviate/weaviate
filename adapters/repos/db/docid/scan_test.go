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
			store, pointers := storeWithObjects(t, tt.objectCount, logger)

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
			store, pointers := storeWithObjects(t, tt.storedObjects, logger)
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

// Sweeps what the per-doc-ID context poll in scan costs, on the two shapes the
// loop takes: every doc ID resolving to an object, and none of them resolving.
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
			store, pointers := storeWithObjects(b, bm.storedObjects, logger)
			for i := len(pointers); i < docIDCount; i++ {
				pointers = append(pointers, uint64(i))
			}
			scan := func(_ context.Context, _ *models.PropertySchema, _ uint64) error { return nil }

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
func storeWithObjects(tb testing.TB, count int, logger logrus.FieldLogger) (*lsmkv.Store, []uint64) {
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

	pointers := make([]uint64, count)
	for i := range pointers {
		docID := uint64(i)
		pointers[i] = docID

		id := uuid.New()
		obj := storobj.New(docID)
		obj.SetID(strfmt.UUID(id.String()))
		obj.Object.Properties = map[string]interface{}{"name": fmt.Sprintf("object-%d", i)}
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

	return store, pointers
}

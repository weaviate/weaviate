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
	"maps"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/concurrency"
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

	// the memtable and the segment are separate reads: only the segment fills the
	// caller's buffer, so every failure position runs against both
	storages := []struct {
		name     string
		segments int
	}{
		{name: "memtable", segments: 0},
		{name: "segment", segments: 1},
	}

	for _, storage := range storages {
		t.Run(storage.name, func(t *testing.T) {
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					runScanObjectsLSMCase(t, tt.objectCount, tt.failOnCall, storage.segments)
				})
			}
		})
	}
}

func runScanObjectsLSMCase(t *testing.T, objectCount, failOnCall, segments int) {
	logger, _ := test.NewNullLogger()
	store, pointers := storeWithObjects(t, objectCount, logger, segments)

	var calls atomic.Int64
	scan := func(_ context.Context, _ *models.PropertySchema, _ uint64) error {
		if int(calls.Add(1)) == failOnCall {
			return errScanFn
		}
		return nil
	}

	err := ScanObjectsLSM(context.Background(), store, pointers, scan, []string{"name"}, logger)

	if failOnCall == 0 {
		require.NoError(t, err)
		require.Equal(t, int64(objectCount), calls.Load())
		return
	}
	require.ErrorIs(t, err, errScanFn)
}

func TestScanObjectsLSMCancellation(t *testing.T) {
	const someObjects = 100

	// ten times as many doc IDs as workers, so stopping early does far less work
	tenPerWorker := 10 * concurrency.CurrentGOMAXPROCSx2()

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
			// each worker past its check can still land one call before seeing the cancel
			name:          "cancelled part way through, every doc ID resolves",
			storedObjects: tenPerWorker,
			pointerCount:  tenPerWorker,
			maxScanCalls:  concurrency.CurrentGOMAXPROCSx2(),
		},
		{
			// one resolvable doc ID means one possible call whatever the workers do
			name:          "cancelled part way through, only the first doc ID resolves",
			storedObjects: 1,
			pointerCount:  tenPerWorker,
			maxScanCalls:  1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, _ := test.NewNullLogger()
			store, pointers := storeWithObjects(t, tt.storedObjects, logger, 0)
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

			if tt.cancelBeforeScan {
				require.ErrorIs(t, err, context.Canceled,
					"a scan handed a cancelled context must report it")
			} else if err != nil {
				// cancelling mid-scan races the per-doc-ID check, so success is a
				// valid outcome; an error that does come back is the cancellation
				require.ErrorIs(t, err, context.Canceled)
			}
			require.LessOrEqual(t, calls.Load(), int64(tt.maxScanCalls))
			if calls.Load() > 0 {
				require.True(t, sawCancel.Load(), "scanFn was handed a context the caller cannot cancel")
			}
		})
	}
}

// A worker reuses its payload buffer and its property map across objects, so
// each is parsed into a buffer and a map the previous object left behind.
// These rows pin the values scanFn is handed under that reuse.
func TestScanObjectsLSMPropertyValues(t *testing.T) {
	const (
		sizeStep = 64
		// a mean, not a floor: workers draw doc IDs from one shared counter
		objectsPerWorker = 8
	)
	objectCount := max(200, concurrency.CurrentGOMAXPROCSx2()*objectsPerWorker)
	propertyNames := []string{"name", "count", "flag", "tags", "scores", "meta"}

	tests := []struct {
		name string
		// largest object first, so the later ones meet an over-sized buffer
		descending bool
		// every nth pointer resolves to no object; 0 inserts none
		missingEvery int
		// every nth object is padded past maxRetainedBufferBytes; 0 pads none
		oversizedEvery int
		// objects carry different property sets, so a stale value shows up
		varyProperties bool
		// no property is requested at all, so scanFn is handed a nil schema
		noProperties bool
	}{
		{name: "sizes descending", descending: true},
		{name: "sizes ascending", descending: false},
		{name: "sizes descending, doc IDs missing mid-scan", descending: true, missingEvery: 7},
		{name: "objects past the retained buffer cap", oversizedEvery: 13},
		{name: "property sets differ", varyProperties: true},
		{name: "property sets differ, doc IDs missing mid-scan", varyProperties: true, missingEvery: 7},
		{name: "no property requested", noProperties: true},
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
				if tt.varyProperties {
					// bit j of i keeps property j, so objects fewer than
					// 1<<len(propertyNames) apart carry different sets
					for j, name := range propertyNames {
						if i&(1<<j) == 0 {
							delete(props[i], name)
						}
					}
				}
			}

			logger, _ := test.NewNullLogger()
			store, stored := storeWithProperties(t, logger, 1, props)

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
			var lock sync.Mutex
			scanned := map[uint64]map[string]interface{}{}
			// map addresses, so the count below pins one map per worker not per object
			handedOut := map[uintptr]bool{}
			scan := func(_ context.Context, prop *models.PropertySchema, docID uint64) error {
				lock.Lock()
				defer lock.Unlock()
				if *prop == nil {
					scanned[docID] = nil
					return nil
				}
				typed := (*prop).(map[string]interface{})
				handedOut[reflect.ValueOf(typed).Pointer()] = true
				// the worker refills this map for its next object
				scanned[docID] = maps.Clone(typed)
				return nil
			}

			requested := propertyNames
			if tt.noProperties {
				requested = nil
			}

			require.NoError(t, ScanObjectsLSM(context.Background(), store, pointers, scan,
				requested, logger))

			require.Len(t, scanned, objectCount)
			if !tt.noProperties {
				require.LessOrEqual(t, len(handedOut), min(concurrency.CurrentGOMAXPROCSx2(), len(pointers)),
					"a worker hands the same map to every object it reads")
			}
			for i, want := range props {
				if tt.noProperties {
					want = nil
				}
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

// Sweeps what the per-doc-ID context poll, the reused payload buffer and the
// consistent view cost. A read references every segment it may consult, so the
// several-segments row is the one that shows what one view per scan saves over
// one per doc ID.
func BenchmarkScanObjectsLSM(b *testing.B) {
	const (
		docIDCount   = 50000
		manySegments = 8
	)

	benchmarks := []struct {
		name          string
		storedObjects int
		segments      int
		// no property requested, as an IncludeMetaCount-only aggregation does
		noProperties bool
	}{
		{name: "every doc ID resolves, one segment", storedObjects: docIDCount, segments: 1},
		{name: "every doc ID resolves, several segments", storedObjects: docIDCount, segments: manySegments},
		{name: "no doc ID resolves", storedObjects: 0, segments: 0},
		{name: "every doc ID resolves, no property requested", storedObjects: docIDCount, segments: 1, noProperties: true},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			logger, _ := test.NewNullLogger()
			store, pointers := storeWithObjects(b, bm.storedObjects, logger, bm.segments)
			for i := len(pointers); i < docIDCount; i++ {
				pointers = append(pointers, uint64(i))
			}
			scan := func(_ context.Context, _ *models.PropertySchema, _ uint64) error { return nil }
			requested := []string{"name"}
			if bm.noProperties {
				requested = nil
			}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := ScanObjectsLSM(context.Background(), store, pointers, scan,
					requested, logger); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// storeWithObjects returns a store holding count objects keyed by their UUID,
// with the docID secondary index ScanObjectsLSM resolves pointers through, and
// the docIDs of those objects.
func storeWithObjects(tb testing.TB, count int, logger logrus.FieldLogger, segments int) (*lsmkv.Store, []uint64) {
	props := make([]map[string]interface{}, count)
	for i := range props {
		props[i] = map[string]interface{}{"name": fmt.Sprintf("object-%d", i)}
	}
	return storeWithProperties(tb, logger, segments, props)
}

// storeWithProperties returns a store holding one object per entry in props and
// the docIDs of those objects. segments is how many disk segments to spread them
// over, 0 leaving them in the memtable; only a disk segment fills a read's buffer
// and carries a reference count.
func storeWithProperties(tb testing.TB, logger logrus.FieldLogger, segments int,
	props []map[string]interface{},
) (*lsmkv.Store, []uint64) {
	ctx := context.Background()
	dir := tb.TempDir()

	store, err := lsmkv.New(dir, dir, logger, nil, nil,
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop())
	require.NoError(tb, err)
	tb.Cleanup(func() { requireShutdownCompletes(tb, store) })

	require.NoError(tb, store.CreateOrLoadBucket(ctx, helpers.ObjectsBucketLSM,
		lsmkv.WithStrategy(lsmkv.StrategyReplace), lsmkv.WithSecondaryIndices(1)))
	bucket := store.Bucket(helpers.ObjectsBucketLSM)

	// the last index of each segment's run of objects, so every flush below has a
	// non-empty memtable and lands one segment on disk
	flushAfter := map[int]bool{}
	if segments > 0 && len(props) > 0 {
		require.LessOrEqual(tb, segments, len(props), "each segment needs at least one object")
		for s := 1; s <= segments; s++ {
			flushAfter[s*len(props)/segments-1] = true
		}
	}

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

		if flushAfter[i] {
			require.NoError(tb, bucket.FlushMemtable())
		}
	}

	if len(flushAfter) > 0 {
		requireSegmentsOnDisk(tb, bucket.GetDir(), segments)
	}

	return store, pointers
}

// how long a store gets to shut down. Closing this fixture's segments is orders of
// magnitude faster, so reaching the bound means shutdown is waiting on a segment
// reference that was never dropped rather than doing work.
const shutdownBound = 5 * time.Second

// requireShutdownCompletes shuts the store down, failing the test rather than
// blocking its binary when the wait for every segment's reference count to reach
// zero does not finish.
func requireShutdownCompletes(tb testing.TB, store *lsmkv.Store) {
	done := make(chan error, 1)
	// a bare goroutine keeps a panic in Shutdown visible; GoWrapper recovers it
	go func() { done <- store.Shutdown(context.Background()) }()

	select {
	case err := <-done:
		require.NoError(tb, err)
	case <-time.After(shutdownBound):
		require.FailNow(tb, "store shutdown blocked", "a consistent view was never released")
	}
}

// requireSegmentsOnDisk fails unless the bucket wrote want segments. With none a
// scan answers from the memtable and never reuses its buffer; the count is how
// many references one consistent view takes.
func requireSegmentsOnDisk(tb testing.TB, dir string, want int) {
	entries, err := os.ReadDir(dir)
	require.NoError(tb, err)
	got := 0
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".db" {
			got++
		}
	}
	require.Equal(tb, want, got, "segment files in %s", dir)
}

// BenchmarkScanObjectsLSMSizeSkew moves only where the large objects sit in an
// ascending allow list, the order the filtered path passes. spread is the
// control: equal counts already hold equal bytes there, so both rows read alike.
func BenchmarkScanObjectsLSMSizeSkew(b *testing.B) {
	const (
		objectCount = 5000
		largeCount  = 200
		smallBytes  = 512
		largeBytes  = 200 * 1024
		segments    = 4
	)

	benchmarks := []struct {
		name string
		// large objects in one contiguous run of doc IDs rather than at a stride
		clustered bool
	}{
		{name: "large objects clustered", clustered: true},
		{name: "large objects spread", clustered: false},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			stride := objectCount / largeCount
			props := make([]map[string]interface{}, objectCount)
			large := 0
			for i := range props {
				padding := smallBytes
				isLarge := i < largeCount
				if !bm.clustered {
					isLarge = i%stride == 0
				}
				if isLarge {
					padding = largeBytes
					large++
				}
				props[i] = objectProperties(i, padding)
			}
			require.Equal(b, largeCount, large, "both rows must hold the same bytes")

			logger, _ := test.NewNullLogger()
			store, pointers := storeWithProperties(b, logger, segments, props)
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

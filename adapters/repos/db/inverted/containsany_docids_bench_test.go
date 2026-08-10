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

//go:build integrationTest

package inverted

import (
	"context"
	"fmt"
	"runtime"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted/stopwords"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/concurrency"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/filters"
	entinverted "github.com/weaviate/weaviate/entities/inverted"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/config"
	configRuntime "github.com/weaviate/weaviate/usecases/config/runtime"
)

// Go-level A/B instrument for ContainsAny/ContainsAll fan-out cost.
//
// Benchmarks Searcher.DocIDs (the full extract + resolve + merge path, no
// HNSW) on a filterable roaringset text property with strictly-unique values
// (1 value == 1 docID), the reported pathological shape. The primary metric is
// allocs/op + B/op via -benchmem: deterministic and thermal-independent, unlike
// server-level throughput. The optimization target is the O(N) per-value
// filters.Clause + propValuePair construction at extraction, so the benchmark
// deliberately includes extraction rather than only resolution.
//
// Run:
//   go test -tags integrationTest -run '^$' -bench 'DocIDs_Contains' \
//       -benchmem -benchtime 20x -count 6 ./adapters/repos/db/inverted/ | tee baseline.txt
// then compare A/B with: benchstat baseline.txt optimized.txt

const benchPropName = "inverted-text-roaringset"

// containsFixture is a shared, deterministic corpus reused across every
// sub-benchmark size so the (expensive) 300K-entry bucket build happens once.
type containsFixture struct {
	searcher *Searcher
	store    *lsmkv.Store
	numDocs  int
}

func newContainsFixture(tb testing.TB, numDocs int) *containsFixture {
	tb.Helper()
	dir := tb.TempDir()
	logger, _ := test.NewNullLogger()

	// Use the production pooled buffer pool (NewBitmapBufPoolDefault with the
	// server's default 32MB/128MB sizing), matching what the real server wires
	// in configure_api.go, so allocation/GC numbers reflect production behaviour
	// (pooled+reused read buffers) rather than the noop pool's per-read
	// allocations, which overstate cost.
	bufPool, bufPoolClose := roaringset.NewBitmapBufPoolDefault(logger, nil,
		config.DefaultQueryBitmapBufsMaxBufSize, config.DefaultQueryBitmapBufsMaxMemory)
	tb.Cleanup(bufPoolClose)

	store, err := lsmkv.New(dir, dir, logger, nil, nil,
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop())
	require.NoError(tb, err)
	tb.Cleanup(func() { store.Shutdown(context.Background()) })

	bucketName := helpers.BucketFromPropNameLSM(benchPropName)
	require.NoError(tb, store.CreateOrLoadBucket(context.Background(), bucketName,
		lsmkv.WithStrategy(lsmkv.StrategyRoaringSet),
		lsmkv.WithBitmapBufPool(bufPool),
	))
	bucket := store.Bucket(bucketName)

	// value i ("val_%08d") maps to exactly docID i: strictly unique, 1:1.
	for i := 0; i < numDocs; i++ {
		require.NoError(tb, bucket.RoaringSetAddList([]byte(benchValue(i)), []uint64{uint64(i)}))
	}
	// A few multi-doc values on top of the unique corpus: each shared value
	// is held by the same two docs, so ContainsAll over them is non-empty;
	// the padded value is stored trimmed, as the FIELD write path would
	// store it, so unnormalized query values must trim to match.
	for _, v := range containsSharedValues {
		require.NoError(tb, bucket.RoaringSetAddList([]byte(v), containsSharedDocIDs))
	}
	require.NoError(tb, bucket.RoaringSetAddList([]byte(containsPaddedValue), []uint64{containsPaddedDocID}))
	require.NoError(tb, bucket.FlushAndSwitch())

	// Small int and uuid corpora for the family end-to-end rows: value i maps
	// to docID i, plus one shared value held by two docs so ContainsAll is
	// non-empty. Keys are written with the same encoding the analyzer uses
	// for these property types, which the exact-docs assertions depend on.
	intBucketName := helpers.BucketFromPropNameLSM(benchIntPropName)
	require.NoError(tb, store.CreateOrLoadBucket(context.Background(), intBucketName,
		lsmkv.WithStrategy(lsmkv.StrategyRoaringSet),
		lsmkv.WithBitmapBufPool(bufPool),
	))
	intBucket := store.Bucket(intBucketName)
	for i := 0; i < containsFamilyDocs; i++ {
		key, err := entinverted.LexicographicallySortableInt64(int64(i))
		require.NoError(tb, err)
		require.NoError(tb, intBucket.RoaringSetAddList(key, []uint64{uint64(i)}))
	}
	sharedIntKey, err := entinverted.LexicographicallySortableInt64(containsSharedInt)
	require.NoError(tb, err)
	require.NoError(tb, intBucket.RoaringSetAddList(sharedIntKey, containsFamilySharedDocIDs))
	require.NoError(tb, intBucket.FlushAndSwitch())

	uuidBucketName := helpers.BucketFromPropNameLSM(benchUUIDPropName)
	require.NoError(tb, store.CreateOrLoadBucket(context.Background(), uuidBucketName,
		lsmkv.WithStrategy(lsmkv.StrategyRoaringSet),
		lsmkv.WithBitmapBufPool(bufPool),
	))
	uuidBucket := store.Bucket(uuidBucketName)
	for i := 0; i < containsFamilyDocs; i++ {
		parsed := uuid.MustParse(benchUUIDValue(i))
		require.NoError(tb, uuidBucket.RoaringSetAddList(parsed[:], []uint64{uint64(i)}))
	}
	sharedUUID := uuid.MustParse(containsSharedUUIDValue)
	require.NoError(tb, uuidBucket.RoaringSetAddList(sharedUUID[:], containsFamilySharedDocIDs))
	require.NoError(tb, uuidBucket.FlushAndSwitch())

	numberBucketName := helpers.BucketFromPropNameLSM(benchNumberPropName)
	require.NoError(tb, store.CreateOrLoadBucket(context.Background(), numberBucketName,
		lsmkv.WithStrategy(lsmkv.StrategyRoaringSet),
		lsmkv.WithBitmapBufPool(bufPool),
	))
	numberBucket := store.Bucket(numberBucketName)
	for i := 0; i < containsFamilyDocs; i++ {
		key := make([]byte, 8)
		entinverted.PutLexicographicallySortableFloat64(key, benchNumberValue(i))
		require.NoError(tb, numberBucket.RoaringSetAddList(key, []uint64{uint64(i)}))
	}
	sharedNumberKey := make([]byte, 8)
	entinverted.PutLexicographicallySortableFloat64(sharedNumberKey, containsSharedNumber)
	require.NoError(tb, numberBucket.RoaringSetAddList(sharedNumberKey, containsFamilySharedDocIDs))
	require.NoError(tb, numberBucket.FlushAndSwitch())

	dateBucketName := helpers.BucketFromPropNameLSM(benchDatePropName)
	require.NoError(tb, store.CreateOrLoadBucket(context.Background(), dateBucketName,
		lsmkv.WithStrategy(lsmkv.StrategyRoaringSet),
		lsmkv.WithBitmapBufPool(bufPool),
	))
	dateBucket := store.Bucket(dateBucketName)
	for i := 0; i < containsFamilyDocs; i++ {
		key := make([]byte, 8)
		entinverted.PutLexicographicallySortableInt64(key, benchDateTime(i).UnixNano())
		require.NoError(tb, dateBucket.RoaringSetAddList(key, []uint64{uint64(i)}))
	}
	sharedDateKey := make([]byte, 8)
	entinverted.PutLexicographicallySortableInt64(sharedDateKey, containsSharedDateTime().UnixNano())
	require.NoError(tb, dateBucket.RoaringSetAddList(sharedDateKey, containsFamilySharedDocIDs))
	require.NoError(tb, dateBucket.FlushAndSwitch())

	// Booleans have only two distinct keys however many values a filter names,
	// so it is the family where a batch is most likely to repeat one. Even docs
	// are false and odd ones true; the shared docs hold both, so ContainsAll
	// over true and false is non-empty.
	boolBucketName := helpers.BucketFromPropNameLSM(benchBoolPropName)
	require.NoError(tb, store.CreateOrLoadBucket(context.Background(), boolBucketName,
		lsmkv.WithStrategy(lsmkv.StrategyRoaringSet),
		lsmkv.WithBitmapBufPool(bufPool),
	))
	boolBucket := store.Bucket(boolBucketName)
	for i := 0; i < containsFamilyDocs; i++ {
		require.NoError(tb, boolBucket.RoaringSetAddList([]byte{byte(i % 2)}, []uint64{uint64(i)}))
	}
	require.NoError(tb, boolBucket.RoaringSetAddList([]byte{0}, containsFamilySharedDocIDs))
	require.NoError(tb, boolBucket.FlushAndSwitch())

	maxDocID := uint64(numDocs + 1)
	bitmapFactory := roaringset.NewBitmapFactory(bufPool, newFakeMaxIDGetter(maxDocID))
	searcher := NewSearcher(logger, store, createSchema().GetClass, nil, nil,
		stopwords.NewProvider(fakeStopwordDetector{}, nil), 2, func() bool { return false },
		func(string) bool { return false }, "",
		config.DefaultQueryNestedCrossReferenceLimit, bitmapFactory).
		WithBatchedContainsEnabled(configRuntime.NewDynamicValue(true))

	return &containsFixture{searcher: searcher, store: store, numDocs: numDocs}
}

func benchValue(i int) string { return fmt.Sprintf("val_%08d", i) }

var (
	containsSharedValues = []string{"shared_a", "shared_b", "shared_c"}
	containsSharedDocIDs = []uint64{11, 17}
)

const (
	containsPaddedValue = "padded-value"
	containsPaddedDocID = uint64(23)
)

// Per-family corpora for the end-to-end rows.
const (
	benchIntPropName     = "inverted-without-frequency-roaringset"
	benchUUIDPropName    = "inverted-uuid-roaringset"
	benchNumberPropName  = "inverted-number-roaringset"
	benchBoolPropName    = "inverted-bool-roaringset"
	benchDatePropName    = "inverted-date-roaringset"
	containsFamilyDocs   = 50
	containsSharedInt    = 1000
	containsSharedNumber = 1000.5
)

var (
	containsFamilySharedDocIDs = []uint64{7, 9}
	containsSharedUUIDValue    = benchUUIDValue(999_999)
)

func benchUUIDValue(i int) string {
	return fmt.Sprintf("00000000-0000-0000-0000-%012d", i)
}

// benchNumberValue keeps a fraction so the keys exercise the sign-and-mantissa
// flip the float encoding does rather than reading as small integers.
func benchNumberValue(i int) float64 { return float64(i) + 0.5 }

func benchDateTime(i int) time.Time {
	return time.Date(2021, time.January, 1, 0, 0, 0, 0, time.UTC).AddDate(0, 0, i)
}

func benchDateValue(i int) string { return benchDateTime(i).Format(time.RFC3339) }

func containsSharedDateTime() time.Time { return benchDateTime(9_999) }

func containsSharedDateValue() string { return containsSharedDateTime().Format(time.RFC3339) }

// sampleValues picks `size` values spread evenly across the corpus (strided),
// so the selection touches the whole keyspace. Deterministic and identical
// across A/B runs. Returns the values and the docIDs they resolve to.
func (f *containsFixture) sampleValues(size int) (values []string, docIDs []uint64) {
	stride := f.numDocs / size
	if stride < 1 {
		stride = 1
	}
	values = make([]string, 0, size)
	docIDs = make([]uint64, 0, size)
	for i := 0; i < f.numDocs && len(values) < size; i += stride {
		values = append(values, benchValue(i))
		docIDs = append(docIDs, uint64(i))
	}
	return values, docIDs
}

func containsFilter(op filters.Operator, values []string) *filters.LocalFilter {
	return containsFilterOn(op, benchPropName, schema.DataTypeText, values)
}

func containsFilterOn(op filters.Operator, prop string, dt schema.DataType, value interface{}) *filters.LocalFilter {
	return &filters.LocalFilter{
		Root: &filters.Clause{
			Operator: op,
			On:       &filters.Path{Class: className, Property: schema.PropertyName(prop)},
			Value:    &filters.Value{Value: value, Type: dt},
		},
	}
}

const benchCorpusSize = 300_000

var benchSizes = []int{100, 1_000, 10_000, 100_000}

// clusteredValues picks `size` values whose docIDs are consecutive, so the
// whole result lands in one or two 64K containers — the Accumulator's
// best-case staging shape. sampleValues (strided) is the opposite extreme:
// every doc in its own 64K range, one staging block per doc.
func (f *containsFixture) clusteredValues(size int) []string {
	start := f.numDocs / 2
	values := make([]string, 0, size)
	for i := start; i < start+size && i < f.numDocs; i++ {
		values = append(values, benchValue(i))
	}
	return values
}

// BenchmarkDocIDs_ContainsAnyAccumulatorGate sweeps the
// containsAnyAccumulatorMinKeys crossover: the same DocIDs call at small key
// counts with the gate forced to always-accumulator vs always-incremental,
// over both result-spread extremes. Run:
//
//	go test -tags integrationTest -run '^$' -bench 'AccumulatorGate' \
//	    -benchmem -count 6 ./adapters/repos/db/inverted/ | tee gate.txt
func BenchmarkDocIDs_ContainsAnyAccumulatorGate(b *testing.B) {
	f := newContainsFixture(b, benchCorpusSize)
	ctx := context.Background()
	oldGate := containsAnyAccumulatorMinKeys
	defer func() { containsAnyAccumulatorMinKeys = oldGate }()

	shapes := []struct {
		name   string
		values func(size int) []string
	}{
		{"spread", func(size int) []string { v, _ := f.sampleValues(size); return v }},
		{"clustered", f.clusteredValues},
	}
	modes := []struct {
		name string
		gate int
	}{
		{"incremental", int(^uint(0) >> 1)}, // MaxInt: fold never uses the Accumulator
		{"accumulator", 2},                  // every batched fold uses the Accumulator
	}

	for _, shape := range shapes {
		for _, size := range []int{2, 4, 8, 16, 32, 64, 128, 256, 512, 1024} {
			values := shape.values(size)
			filter := containsFilter(filters.ContainsAny, values)
			for _, mode := range modes {
				b.Run(fmt.Sprintf("%s/N=%04d/%s", shape.name, size, mode.name), func(b *testing.B) {
					containsAnyAccumulatorMinKeys = mode.gate
					b.ReportAllocs()
					for i := 0; i < b.N; i++ {
						al, err := f.searcher.DocIDs(ctx, filter, additional.Properties{}, className)
						if err != nil {
							b.Fatal(err)
						}
						al.Close()
					}
				})
			}
		}
	}
}

func BenchmarkDocIDs_ContainsAny(b *testing.B) {
	f := newContainsFixture(b, benchCorpusSize)
	ctx := context.Background()
	for _, size := range benchSizes {
		values, _ := f.sampleValues(size)
		filter := containsFilter(filters.ContainsAny, values)
		b.Run(fmt.Sprintf("N=%d", size), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				al, err := f.searcher.DocIDs(ctx, filter, additional.Properties{}, className)
				if err != nil {
					b.Fatal(err)
				}
				al.Close()
			}
		})
	}
}

func BenchmarkDocIDs_ContainsAll(b *testing.B) {
	f := newContainsFixture(b, benchCorpusSize)
	ctx := context.Background()
	for _, size := range benchSizes {
		values, _ := f.sampleValues(size)
		filter := containsFilter(filters.ContainsAll, values)
		b.Run(fmt.Sprintf("N=%d", size), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				al, err := f.searcher.DocIDs(ctx, filter, additional.Properties{}, className)
				if err != nil {
					b.Fatal(err)
				}
				al.Close()
			}
		})
	}
}

// resolveDocIDs runs DocIDs with filter and returns the sorted doc-ID slice.
func (f *containsFixture) resolveDocIDs(t *testing.T, ctx context.Context, filter *filters.LocalFilter) []uint64 {
	t.Helper()
	al, err := f.searcher.DocIDs(ctx, filter, additional.Properties{}, className)
	require.NoError(t, err)
	defer al.Close()
	got := al.Slice()
	sorted := append([]uint64(nil), got...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	return sorted
}

// equalCompoundFilter builds the compound the desugared path would produce
// for the same values: OperatorEqual leaves under Or (ContainsAny) / And
// (ContainsAll) / Not-of-Or (ContainsNone). The batch gate only intercepts
// Contains clauses, so resolving this compound always exercises the per-value
// path.
func equalCompoundFilter(op filters.Operator, values []string) *filters.LocalFilter {
	leafValues := make([]interface{}, len(values))
	for i, v := range values {
		leafValues[i] = v
	}
	return equalCompoundFilterOn(op, benchPropName, schema.DataTypeText, leafValues)
}

func equalCompoundFilterOn(op filters.Operator, prop string, dt schema.DataType, values []interface{}) *filters.LocalFilter {
	compound := filters.OperatorOr
	if op == filters.ContainsAll {
		compound = filters.OperatorAnd
	}
	operands := make([]filters.Clause, len(values))
	for i, v := range values {
		operands[i] = filters.Clause{
			Operator: filters.OperatorEqual,
			On:       &filters.Path{Class: className, Property: schema.PropertyName(prop)},
			Value:    &filters.Value{Value: v, Type: dt},
		}
	}
	root := &filters.Clause{Operator: compound, Operands: operands}
	if op == filters.ContainsNone {
		root = &filters.Clause{Operator: filters.OperatorNot, Operands: []filters.Clause{*root}}
	}
	return &filters.LocalFilter{Root: root}
}

// TestDocIDs_BatchedMatchesDesugared is the differential gate for the batched
// Contains path: the same value set resolved through the Contains filter
// (batched) and through a hand-built compound of OperatorEqual leaves (never
// intercepted by the gate) must produce identical doc-ID sets. This is also
// what makes mixed execution safe: within one logical query, one shard can
// take the batch path while another (e.g. a non-roaringset bucket) desugars,
// and their results are combined.
func TestDocIDs_BatchedMatchesDesugared(t *testing.T) {
	f := newContainsFixture(t, 5_000)
	ctx := context.Background()

	sampled, _ := f.sampleValues(200)
	cases := []struct {
		name   string
		values []string
	}{
		{"unique corpus values", sampled},
		{"shared values, non-empty ContainsAll", containsSharedValues},
		{"shared plus unique", append([]string{benchValue(11)}, containsSharedValues...)},
		{
			"unnormalized values need FIELD trimming",
			[]string{"  " + containsPaddedValue + " ", " " + benchValue(3), containsSharedValues[0]},
		},
		{"absent values", []string{"absent_1", "absent_2", benchValue(5)}},
		// "" and "   " both FIELD-tokenize to the empty-string token — the
		// degenerate boundary of the one-token-per-value invariant, and a
		// duplicate key within one batch.
		{"empty and whitespace-only values", []string{"", "   ", benchValue(5)}},
	}

	for _, op := range []filters.Operator{filters.ContainsAny, filters.ContainsAll, filters.ContainsNone} {
		for _, tc := range cases {
			t.Run(op.Name()+"/"+tc.name, func(t *testing.T) {
				batched := f.resolveDocIDs(t, ctx, containsFilter(op, tc.values))
				desugared := f.resolveDocIDs(t, ctx, equalCompoundFilter(op, tc.values))
				require.Equal(t, desugared, batched,
					"batched Contains must resolve the same doc IDs as the desugared Equal compound")
			})
		}
	}

	// Every case above runs against a fully flushed corpus, which is exactly
	// the shape where the batch reader skips the active memtable — so batched
	// and desugared can agree by both reading only disk. This case leaves a
	// write unflushed so the active memtable is non-empty, the one shape where
	// the two paths could diverge: the batch reader must probe it per key like
	// the desugared path does.
	// The corpus rows above are all small or uniform-width, so they reach only
	// three of the sort's branches. These two are sized and shaped to reach the
	// two-word radix and the variable-width radix with its collision repair,
	// which unit tests cover but nothing had exercised through a real bucket.
	//
	// Each list mixes present values with absent ones: the absent values set the
	// branch, the present ones keep the result non-empty so the two paths are
	// compared on something.
	t.Run("large batches reach the radix branches", func(t *testing.T) {
		// 250 uuids with no shared prefix, past the two-word cutoff.
		uuids := make([]interface{}, 0, 250)
		for i := 0; i < containsFamilyDocs; i++ {
			uuids = append(uuids, benchUUIDValue(i))
		}
		for i := 0; len(uuids) < 250; i++ {
			uuids = append(uuids, fmt.Sprintf("%08x-0000-0000-0000-%012d", uint32(i)*2654435761, i))
		}

		// 100 text values of differing lengths sharing a prefix longer than the
		// packed word, so the first pass cannot separate them and the repair
		// has to run.
		texts := make([]interface{}, 0, 100)
		for i := 0; i < 20; i++ {
			texts = append(texts, benchValue(i))
		}
		for i := 0; len(texts) < 100; i++ {
			group := "user_profile_settings_"
			if i%2 == 1 {
				group = "user_profile_avatars_"
			}
			texts = append(texts, fmt.Sprintf("%s%d", group, i*7919))
		}

		for _, tc := range []struct {
			name   string
			prop   string
			dt     schema.DataType
			values []interface{}
		}{
			{"uuid, two-word radix", benchUUIDPropName, schema.DataTypeText, uuids},
			{"text, variable radix with collision repair", benchPropName, schema.DataTypeText, texts},
		} {
			for _, op := range []filters.Operator{filters.ContainsAny, filters.ContainsAll, filters.ContainsNone} {
				t.Run(tc.name+"/"+op.Name(), func(t *testing.T) {
					batched := f.resolveDocIDs(t, ctx, containsFilterOn(op, tc.prop, tc.dt, tc.values))
					desugared := f.resolveDocIDs(t, ctx, equalCompoundFilterOn(op, tc.prop, tc.dt, tc.values))
					require.Equal(t, desugared, batched,
						"batched Contains must resolve the same doc IDs as the desugared Equal compound")
					if op == filters.ContainsAny {
						require.NotEmpty(t, batched,
							"the present values must keep the comparison non-vacuous")
					}
				})
			}
		}
	})

	t.Run("unflushed write in the active memtable", func(t *testing.T) {
		g := newContainsFixture(t, 200)
		bucket := g.store.Bucket(helpers.BucketFromPropNameLSM(benchPropName))
		require.NotNil(t, bucket)

		// The doc ID must be inside the universe or ContainsNone's complement is
		// the same whether the write is seen or not, and it must land on both
		// values or ContainsAll's intersection excludes it either way — either
		// slip leaves two of the three operators asserting nothing.
		const unflushedDocID = 7
		for _, v := range []string{benchValue(3), benchValue(4)} {
			require.NoError(t, bucket.RoaringSetAddList([]byte(v), []uint64{unflushedDocID}))
		}

		values := []string{benchValue(3), benchValue(4)}
		for _, op := range []filters.Operator{filters.ContainsAny, filters.ContainsAll, filters.ContainsNone} {
			t.Run(op.Name(), func(t *testing.T) {
				batched := g.resolveDocIDs(t, ctx, containsFilter(op, values))
				desugared := g.resolveDocIDs(t, ctx, equalCompoundFilter(op, values))
				require.Equal(t, desugared, batched,
					"batched Contains must read the active memtable like the desugared path")
			})
		}

		// Without these the case is vacuous: if both paths skipped the active
		// memtable they would agree on results that are missing the write.
		require.Contains(t, g.resolveDocIDs(t, ctx, containsFilter(filters.ContainsAny, values)),
			uint64(unflushedDocID), "ContainsAny must see the unflushed write")
		require.Contains(t, g.resolveDocIDs(t, ctx, containsFilter(filters.ContainsAll, values)),
			uint64(unflushedDocID), "ContainsAll must see it on both values")
		require.NotContains(t, g.resolveDocIDs(t, ctx, containsFilter(filters.ContainsNone, values)),
			uint64(unflushedDocID), "ContainsNone must exclude it")
	})

	t.Run("[]interface{} values from the API layer", func(t *testing.T) {
		values := []string{containsSharedValues[0], containsSharedValues[1], benchValue(11)}
		iface := make([]interface{}, len(values))
		for i, v := range values {
			iface[i] = v
		}
		filter := &filters.LocalFilter{
			Root: &filters.Clause{
				Operator: filters.ContainsAll,
				On:       &filters.Path{Class: className, Property: schema.PropertyName(benchPropName)},
				Value:    &filters.Value{Value: iface, Type: schema.DataTypeText},
			},
		}
		batched := f.resolveDocIDs(t, ctx, filter)
		desugared := f.resolveDocIDs(t, ctx, equalCompoundFilter(filters.ContainsAll, values))
		require.Equal(t, desugared, batched)
		require.Equal(t, []uint64{11}, batched,
			"docID 11 holds its unique value plus every shared value")
	})

	// A batched ContainsNone resolves to a deny list: a bitmap of the docs to
	// EXCLUDE, which every parent And/Or/Not merge arm must honor. The rows
	// above only place Contains at the filter root, where the deny list is
	// inverted once against the universe, so the merge arms go unexercised.
	// The membership spot-checks pin the algebra itself, which the
	// differential alone cannot: both trees feed deny lists through the same
	// merge arms, so an arm bug would corrupt both sides identically.
	t.Run("deny-list composition under compound parents", func(t *testing.T) {
		// exclusion rows: valuesA -> docs {1, 2, 11, 17}, valuesB -> docs {2, 3, 11, 17}
		valuesA := []string{benchValue(1), benchValue(2), containsSharedValues[0]}
		valuesB := []string{benchValue(2), benchValue(3), containsSharedValues[1]}

		equalLeaf := func(v string) filters.Clause {
			return filters.Clause{
				Operator: filters.OperatorEqual,
				On:       &filters.Path{Class: className, Property: schema.PropertyName(benchPropName)},
				Value:    &filters.Value{Value: v, Type: schema.DataTypeText},
			}
		}
		tree := func(op filters.Operator, operands ...filters.Clause) *filters.LocalFilter {
			return &filters.LocalFilter{Root: &filters.Clause{Operator: op, Operands: operands}}
		}

		cases := []struct {
			name        string
			batched     *filters.LocalFilter
			desugared   *filters.LocalFilter
			exact       []uint64
			contains    []uint64
			notContains []uint64
		}{
			{
				name:      "And(ContainsNone, Equal)",
				batched:   tree(filters.OperatorAnd, *containsFilter(filters.ContainsNone, valuesA).Root, equalLeaf(benchValue(3))),
				desugared: tree(filters.OperatorAnd, *equalCompoundFilter(filters.ContainsNone, valuesA).Root, equalLeaf(benchValue(3))),
				exact:     []uint64{3},
			},
			{
				name:        "Or(ContainsNone, Equal) re-adds a denied doc",
				batched:     tree(filters.OperatorOr, *containsFilter(filters.ContainsNone, valuesA).Root, equalLeaf(benchValue(1))),
				desugared:   tree(filters.OperatorOr, *equalCompoundFilter(filters.ContainsNone, valuesA).Root, equalLeaf(benchValue(1))),
				contains:    []uint64{1, 3},
				notContains: []uint64{2, 11, 17},
			},
			{
				name:        "And(ContainsNone, ContainsNone)",
				batched:     tree(filters.OperatorAnd, *containsFilter(filters.ContainsNone, valuesA).Root, *containsFilter(filters.ContainsNone, valuesB).Root),
				desugared:   tree(filters.OperatorAnd, *equalCompoundFilter(filters.ContainsNone, valuesA).Root, *equalCompoundFilter(filters.ContainsNone, valuesB).Root),
				contains:    []uint64{4},
				notContains: []uint64{1, 2, 3, 11, 17},
			},
			{
				name:        "Or(ContainsNone, ContainsNone) keeps docs denied by only one side",
				batched:     tree(filters.OperatorOr, *containsFilter(filters.ContainsNone, valuesA).Root, *containsFilter(filters.ContainsNone, valuesB).Root),
				desugared:   tree(filters.OperatorOr, *equalCompoundFilter(filters.ContainsNone, valuesA).Root, *equalCompoundFilter(filters.ContainsNone, valuesB).Root),
				contains:    []uint64{1, 3, 4},
				notContains: []uint64{2, 11, 17},
			},
			{
				name:        "Not(ContainsAny) equals ContainsNone",
				batched:     tree(filters.OperatorNot, *containsFilter(filters.ContainsAny, valuesA).Root),
				desugared:   equalCompoundFilter(filters.ContainsNone, valuesA),
				contains:    []uint64{3},
				notContains: []uint64{1, 2, 11, 17},
			},
		}

		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				batched := f.resolveDocIDs(t, ctx, tc.batched)
				desugared := f.resolveDocIDs(t, ctx, tc.desugared)
				require.Equal(t, desugared, batched,
					"batched deny-list leaf must compose like its desugared reference")
				if tc.exact != nil {
					require.Equal(t, tc.exact, batched)
				}
				for _, id := range tc.contains {
					require.Contains(t, batched, id)
				}
				for _, id := range tc.notContains {
					require.NotContains(t, batched, id)
				}
			})
		}
	})

	// The rows above all read the text FIELD property, so the uuid and
	// primitive classify arms never cross a real bucket in this test. The
	// exact-docs assertions matter here: both query paths share one encoder
	// per family, so differential equality alone would survive an encoding
	// bug that breaks lookups on both sides.
	//
	// Every family that classifies as batchable appears, because each reaches a
	// different sorting arm — one word for int, number and date, two for uuid,
	// a counting pass for bool.
	t.Run("every family end-to-end", func(t *testing.T) {
		ints := func(vs ...int) []interface{} {
			out := make([]interface{}, len(vs))
			for i, v := range vs {
				out[i] = v
			}
			return out
		}

		cases := []struct {
			name string
			prop string
			dt   schema.DataType
			op   filters.Operator
			// filterValue is the Contains filter's value in the typed shape
			// the extractor accepts; leafValues are the same values as the
			// per-leaf Equal shape for the desugared reference.
			filterValue interface{}
			leafValues  []interface{}
			exact       []uint64
			contains    []uint64
			notContains []uint64
		}{
			{
				name: "int ContainsAny", prop: benchIntPropName, dt: schema.DataTypeInt,
				op:          filters.ContainsAny,
				filterValue: []int{1, 2, containsSharedInt}, leafValues: ints(1, 2, containsSharedInt),
				exact: []uint64{1, 2, 7, 9},
			},
			{
				name: "int ContainsAll", prop: benchIntPropName, dt: schema.DataTypeInt,
				op:          filters.ContainsAll,
				filterValue: []int{containsSharedInt, 7}, leafValues: ints(containsSharedInt, 7),
				exact: []uint64{7},
			},
			{
				name: "int ContainsNone", prop: benchIntPropName, dt: schema.DataTypeInt,
				op:          filters.ContainsNone,
				filterValue: []int{1, containsSharedInt}, leafValues: ints(1, containsSharedInt),
				contains: []uint64{2, 3}, notContains: []uint64{1, 7, 9},
			},
			{
				name: "uuid ContainsAny", prop: benchUUIDPropName, dt: schema.DataTypeText,
				op:          filters.ContainsAny,
				filterValue: []interface{}{benchUUIDValue(1), benchUUIDValue(2), containsSharedUUIDValue},
				leafValues:  []interface{}{benchUUIDValue(1), benchUUIDValue(2), containsSharedUUIDValue},
				exact:       []uint64{1, 2, 7, 9},
			},
			{
				name: "uuid ContainsAll", prop: benchUUIDPropName, dt: schema.DataTypeText,
				op:          filters.ContainsAll,
				filterValue: []interface{}{containsSharedUUIDValue, benchUUIDValue(7)},
				leafValues:  []interface{}{containsSharedUUIDValue, benchUUIDValue(7)},
				exact:       []uint64{7},
			},
			{
				name: "uuid ContainsNone", prop: benchUUIDPropName, dt: schema.DataTypeText,
				op:          filters.ContainsNone,
				filterValue: []interface{}{benchUUIDValue(1), containsSharedUUIDValue},
				leafValues:  []interface{}{benchUUIDValue(1), containsSharedUUIDValue},
				contains:    []uint64{2, 3}, notContains: []uint64{1, 7, 9},
			},
			{
				name: "number ContainsAny", prop: benchNumberPropName, dt: schema.DataTypeNumber,
				op:          filters.ContainsAny,
				filterValue: []float64{benchNumberValue(1), benchNumberValue(2), containsSharedNumber},
				leafValues: []interface{}{
					benchNumberValue(1), benchNumberValue(2), containsSharedNumber,
				},
				exact: []uint64{1, 2, 7, 9},
			},
			{
				name: "number ContainsAll", prop: benchNumberPropName, dt: schema.DataTypeNumber,
				op:          filters.ContainsAll,
				filterValue: []float64{containsSharedNumber, benchNumberValue(7)},
				leafValues:  []interface{}{containsSharedNumber, benchNumberValue(7)},
				exact:       []uint64{7},
			},
			{
				name: "number ContainsNone", prop: benchNumberPropName, dt: schema.DataTypeNumber,
				op:          filters.ContainsNone,
				filterValue: []float64{benchNumberValue(1), containsSharedNumber},
				leafValues:  []interface{}{benchNumberValue(1), containsSharedNumber},
				contains:    []uint64{2, 3}, notContains: []uint64{1, 7, 9},
			},
			{
				name: "date ContainsAny", prop: benchDatePropName, dt: schema.DataTypeDate,
				op:          filters.ContainsAny,
				filterValue: []string{benchDateValue(1), benchDateValue(2), containsSharedDateValue()},
				leafValues: []interface{}{
					benchDateValue(1), benchDateValue(2), containsSharedDateValue(),
				},
				exact: []uint64{1, 2, 7, 9},
			},
			{
				name: "date ContainsAll", prop: benchDatePropName, dt: schema.DataTypeDate,
				op:          filters.ContainsAll,
				filterValue: []string{containsSharedDateValue(), benchDateValue(7)},
				leafValues:  []interface{}{containsSharedDateValue(), benchDateValue(7)},
				exact:       []uint64{7},
			},
			{
				name: "date ContainsNone", prop: benchDatePropName, dt: schema.DataTypeDate,
				op:          filters.ContainsNone,
				filterValue: []string{benchDateValue(1), containsSharedDateValue()},
				leafValues:  []interface{}{benchDateValue(1), containsSharedDateValue()},
				contains:    []uint64{2, 3}, notContains: []uint64{1, 7, 9},
			},
			// Booleans draw on two distinct keys however many values a filter
			// names, and the rows below name more than two.
			{
				name: "bool ContainsAny, duplicate values", prop: benchBoolPropName, dt: schema.DataTypeBoolean,
				op:          filters.ContainsAny,
				filterValue: []bool{true, false, true, false},
				leafValues:  []interface{}{true, false, true, false},
				contains:    []uint64{0, 1, 2, 7, 9},
			},
			{
				name: "bool ContainsAny, one value repeated", prop: benchBoolPropName, dt: schema.DataTypeBoolean,
				op:          filters.ContainsAny,
				filterValue: []bool{true, true, true},
				leafValues:  []interface{}{true, true, true},
				contains:    []uint64{1, 3, 7, 9}, notContains: []uint64{0, 2, 4},
			},
			{
				name: "bool ContainsAll", prop: benchBoolPropName, dt: schema.DataTypeBoolean,
				op:          filters.ContainsAll,
				filterValue: []bool{true, false, true},
				leafValues:  []interface{}{true, false, true},
				exact:       []uint64{7, 9},
			},
			{
				name: "bool ContainsNone", prop: benchBoolPropName, dt: schema.DataTypeBoolean,
				op:          filters.ContainsNone,
				filterValue: []bool{true, true},
				leafValues:  []interface{}{true, true},
				contains:    []uint64{0, 2, 4}, notContains: []uint64{1, 3, 7, 9},
			},
		}

		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				batched := f.resolveDocIDs(t, ctx, containsFilterOn(tc.op, tc.prop, tc.dt, tc.filterValue))
				desugared := f.resolveDocIDs(t, ctx, equalCompoundFilterOn(tc.op, tc.prop, tc.dt, tc.leafValues))
				require.Equal(t, desugared, batched,
					"batched Contains must resolve the same doc IDs as the desugared Equal compound")
				if tc.exact != nil {
					require.Equal(t, tc.exact, batched)
				}
				for _, id := range tc.contains {
					require.Contains(t, batched, id)
				}
				for _, id := range tc.notContains {
					require.NotContains(t, batched, id)
				}
			})
		}
	})
}

// TestDocIDs_ContainsCorrectness is the correctness gate for the benchmark
// fixture: it pins that DocIDs returns exactly the expected doc-ID set on the
// same corpus the benchmark measures, so an optimization cannot "win" by
// returning wrong results. ContainsAny(sample) == the sampled docIDs;
// ContainsAll(sample) over strictly-unique values == empty (no doc holds >1
// value), which still fully exercises the AND-fold extraction/merge path.
func TestDocIDs_ContainsCorrectness(t *testing.T) {
	f := newContainsFixture(t, 20_000)
	ctx := context.Background()

	for _, size := range []int{1, 100, 1_000, 10_000} {
		values, wantAny := f.sampleValues(size)
		t.Run(fmt.Sprintf("ContainsAny_N=%d", size), func(t *testing.T) {
			al, err := f.searcher.DocIDs(ctx, containsFilter(filters.ContainsAny, values),
				additional.Properties{}, className)
			require.NoError(t, err)
			defer al.Close()
			got := al.Slice()
			sort.Slice(got, func(i, j int) bool { return got[i] < got[j] })
			require.Equal(t, wantAny, got)
		})
		t.Run(fmt.Sprintf("ContainsAll_N=%d", size), func(t *testing.T) {
			al, err := f.searcher.DocIDs(ctx, containsFilter(filters.ContainsAll, values),
				additional.Properties{}, className)
			require.NoError(t, err)
			defer al.Close()
			if size == 1 {
				require.Equal(t, wantAny, al.Slice()) // single value: AND == that value's docID
			} else {
				require.True(t, al.IsEmpty(),
					"ContainsAll over %d strictly-unique values must be empty", size)
			}
		})
		t.Run(fmt.Sprintf("ContainsNone_N=%d", size), func(t *testing.T) {
			// The deny list inverts against the BitmapFactory universe,
			// which is [0, maxDocID] inclusive with the fixture's
			// maxDocID = numDocs+1. Expected: the exact complement of the
			// sampled docIDs within that universe.
			sampledSet := make(map[uint64]struct{}, len(wantAny))
			for _, id := range wantAny {
				sampledSet[id] = struct{}{}
			}
			maxDocID := uint64(f.numDocs + 1)
			wantNone := make([]uint64, 0, maxDocID+1-uint64(len(wantAny)))
			for id := uint64(0); id <= maxDocID; id++ {
				if _, ok := sampledSet[id]; !ok {
					wantNone = append(wantNone, id)
				}
			}

			got := f.resolveDocIDs(t, ctx, containsFilter(filters.ContainsNone, values))
			require.Equal(t, wantNone, got)
		})
	}
}

// TestDocIDs_GoroutinePeak measures peak live goroutines during concurrent
// DocIDs(100K) resolution — the structural signal for the fan-out fix. The
// batched path spawns no per-value goroutines, only sroar's bounded merge
// workers (at most the per-query budget per caller), so the peak is asserted
// against a generous multiple of that structural bound; the old per-value
// fan-out (one goroutine per value per caller) exceeds it by orders of
// magnitude.
func TestDocIDs_GoroutinePeak(t *testing.T) {
	f := newContainsFixture(t, benchCorpusSize)
	ctx := context.Background()
	values, _ := f.sampleValues(100_000)
	filter := containsFilter(filters.ContainsAny, values)

	const concurrentCallers = 8

	stop := make(chan struct{})
	var peak int64
	var samplerWg sync.WaitGroup
	samplerWg.Add(1)
	go func() {
		defer samplerWg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				if n := int64(runtime.NumGoroutine()); n > peak {
					peak = n
				}
				time.Sleep(50 * time.Microsecond)
			}
		}
	}()

	baseline := runtime.NumGoroutine()
	var wg sync.WaitGroup
	for c := 0; c < concurrentCallers; c++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			al, err := f.searcher.DocIDs(ctx, filter, additional.Properties{}, className)
			require.NoError(t, err)
			al.Close()
		}()
	}
	wg.Wait()
	close(stop)
	samplerWg.Wait()

	t.Logf("goroutine peak: baseline=%d peak=%d (delta=%d) during %d concurrent DocIDs(100K), GOMAXPROCS=%d",
		baseline, peak, peak-int64(baseline), concurrentCallers, runtime.GOMAXPROCS(0))
	// structural bound: each caller may run at most SROAR_MERGE merge workers
	// at a time (plus its own goroutine); 4x headroom absorbs runtime and
	// test-infra goroutines without ever admitting a per-value fan-out
	bound := int64(baseline) + 4*concurrentCallers*int64(concurrency.SROAR_MERGE+1)
	require.LessOrEqual(t, peak, bound,
		"peak goroutines must stay within the bounded merge fan-out (per-value fan-out would exceed this by orders of magnitude)")
}

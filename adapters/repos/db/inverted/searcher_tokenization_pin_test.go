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

package inverted

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted/stopwords"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/config/runtime"
)

const (
	pinClass   = "TokPin"
	pinProp    = "title"
	pinAbsent  = "absent"
	pinValue   = "Hello World"
	pinStaged  = "property_title_staged"
	pinNumDocs = 3
)

var pinCanonical = helpers.BucketFromPropNameLSM(pinProp)

// tokPinFixture: the title bucket holds field terms, the staged bucket the same docs as word terms.
type tokPinFixture struct {
	store *lsmkv.Store
	class *models.Class

	mu       sync.RWMutex
	tok      string
	held     atomic.Int32
	releases []func()
}

// pinWithSchemaTokenization is the pinner of a shard with no overlay.
func pinWithSchemaTokenization(store *lsmkv.Store) TokenizationBucketPinner {
	return func(_, bucketName, schemaTokenization string) (string, *lsmkv.Bucket, func()) {
		bucket, release := store.AcquireBucketForRead(bucketName)
		return schemaTokenization, bucket, release
	}
}

func newTokPinFixture(t testing.TB) *tokPinFixture {
	t.Helper()
	dir := t.TempDir()
	store, err := lsmkv.New(dir, dir, logrus.New(), nil, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop())
	require.NoError(t, err)
	t.Cleanup(func() { store.Shutdown(context.Background()) })

	vTrue, vFalse := true, false
	textProp := func(name string) *models.Property {
		return &models.Property{
			Name: name, DataType: schema.DataTypeText.PropString(),
			Tokenization: models.PropertyTokenizationField, IndexFilterable: &vTrue, IndexSearchable: &vFalse,
		}
	}
	f := &tokPinFixture{
		store: store,
		class: &models.Class{Class: pinClass, Properties: []*models.Property{textProp(pinProp), textProp(pinAbsent)}},
		tok:   models.PropertyTokenizationField,
	}

	docIDs := []uint64{1, 2, 3}
	for bucket, terms := range map[string][]string{
		pinCanonical: {pinValue},
		pinStaged:    {"hello", "world"},
	} {
		require.NoError(t, store.CreateOrLoadBucket(context.Background(), bucket,
			lsmkv.WithStrategy(lsmkv.StrategyRoaringSet),
			lsmkv.WithBitmapBufPool(roaringset.NewBitmapBufPoolNoop())))
		for _, term := range terms {
			require.NoError(t, store.Bucket(bucket).RoaringSetAddList([]byte(term), docIDs))
		}
	}
	// A leaked pin would otherwise hang the store's shutdown.
	t.Cleanup(func() {
		for _, release := range f.releases {
			release()
		}
	})
	return f
}

func (f *tokPinFixture) pin(_, bucketName, _ string) (string, *lsmkv.Bucket, func()) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	bucket, release := f.store.AcquireBucketForRead(bucketName)
	f.held.Add(1)
	var once sync.Once
	counted := func() { once.Do(func() { f.held.Add(-1); release() }) }
	f.releases = append(f.releases, counted)
	return f.tok, bucket, counted
}

func (f *tokPinFixture) searcher(batched bool, pinner TokenizationBucketPinner) *Searcher {
	return NewSearcher(logrus.New(), f.store, func(string) *models.Class { return f.class }, nil, nil,
		stopwords.NewProvider(fakeStopwordDetector{}, nil), 2, func() bool { return false }, nil, "",
		config.DefaultQueryNestedCrossReferenceLimit,
		roaringset.NewBitmapFactory(roaringset.NewBitmapBufPoolNoop(), func() uint64 { return pinNumDocs + 1 }),
		pinner).
		WithBatchedContainsEnabled(runtime.NewDynamicValue(batched))
}

func (f *tokPinFixture) flip(t *testing.T) {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	_, err := f.store.SwapBucketPointer(context.Background(), pinCanonical, pinStaged)
	require.NoError(t, err)
	f.tok = models.PropertyTokenizationWord
}

func pinClause(op filters.Operator, prop string, value any) filters.Clause {
	return filters.Clause{
		Operator: op,
		On:       &filters.Path{Class: pinClass, Property: schema.PropertyName(prop)},
		Value:    &filters.Value{Type: schema.DataTypeText, Value: value},
	}
}

func pinAnd(clauses ...filters.Clause) *filters.LocalFilter {
	return &filters.LocalFilter{Root: &filters.Clause{Operator: filters.OperatorAnd, Operands: clauses}}
}

func pinFilter(c filters.Clause) *filters.LocalFilter { return &filters.LocalFilter{Root: &c} }

func TestFilterKeepsItsTokenizationAndBucketTogetherAcrossAFlip(t *testing.T) {
	tests := []struct {
		name    string
		batched bool
		filter  *filters.LocalFilter
	}{
		{name: "equal", filter: pinFilter(pinClause(filters.OperatorEqual, pinProp, pinValue))},
		{
			name: "batched contains any", batched: true,
			filter: pinFilter(pinClause(filters.ContainsAny, pinProp, []string{pinValue, "nope"})),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newTokPinFixture(t)
			ctx := context.Background()

			c := f.searcher(tc.batched, f.pin)
			defer c.pins.release()
			pv, err := c.extractPropValuePair(ctx, tc.filter.Root, pinClass)
			require.NoError(t, err)
			f.flip(t)
			dbm, err := pv.resolveDocIDs(ctx, c, 0)
			require.NoError(t, err)
			defer dbm.release()

			require.Equal(t, pinNumDocs, dbm.count())
		})
	}
}

// A pin left held blocks the bucket's shutdown forever, so every one is counted back.
func TestDocIDsReleasesItsPins(t *testing.T) {
	tests := []struct {
		name    string
		filter  *filters.LocalFilter
		wantErr string
	}{
		{name: "success", filter: pinFilter(pinClause(filters.OperatorEqual, pinProp, pinValue))},
		{
			name: "a later clause fails extraction",
			filter: pinAnd(pinClause(filters.OperatorEqual, pinProp, pinValue),
				pinClause(filters.OperatorEqual, "missing", pinValue)),
			wantErr: "missing",
		},
		{
			name: "a clause fails resolution",
			filter: pinAnd(pinClause(filters.OperatorEqual, pinProp, pinValue),
				pinClause(filters.OperatorEqual, pinAbsent, pinValue)),
			wantErr: "not found",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newTokPinFixture(t)
			allow, err := f.searcher(false, f.pin).DocIDs(context.Background(), tc.filter, additional.Properties{}, pinClass)
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
			} else {
				require.NoError(t, err)
				allow.Close()
			}
			require.Zero(t, f.held.Load())
		})
	}
}

func BenchmarkDocIDs_TextEqual(b *testing.B) {
	f := newTokPinFixture(b)
	s := f.searcher(false, pinWithSchemaTokenization(f.store))
	filter := pinFilter(pinClause(filters.OperatorEqual, pinProp, pinValue))
	ctx := context.Background()
	b.ReportAllocs()
	for b.Loop() {
		allow, err := s.DocIDs(ctx, filter, additional.Properties{}, pinClass)
		if err != nil {
			b.Fatal(err)
		}
		allow.Close()
	}
}

func BenchmarkDocIDs_ContainsAnyDesugaredText(b *testing.B) {
	f := newTokPinFixture(b)
	s := f.searcher(false, pinWithSchemaTokenization(f.store))
	values := make([]string, 1000)
	for i := range values {
		values[i] = fmt.Sprintf("value %d", i)
	}
	values[0] = pinValue
	filter := pinFilter(pinClause(filters.ContainsAny, pinProp, values))
	ctx := context.Background()
	b.ReportAllocs()
	for b.Loop() {
		allow, err := s.DocIDs(ctx, filter, additional.Properties{}, pinClass)
		if err != nil {
			b.Fatal(err)
		}
		allow.Close()
	}
}

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

package db

import (
	"context"
	stderrors "errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	resolver "github.com/weaviate/weaviate/adapters/repos/db/sharding"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/entities/searchparams"
	"github.com/weaviate/weaviate/entities/storobj"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/monitoring"
	"github.com/weaviate/weaviate/usecases/objects"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
)

type listedObject struct {
	class string
	id    int
	name  any
}

func listClass(name string, nameType schema.DataType) *models.Class {
	prop := &models.Property{Name: "name", DataType: nameType.PropString()}
	if nameType == schema.DataTypeText {
		prop.Tokenization = models.PropertyTokenizationField
	}
	return &models.Class{
		Class:               name,
		VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
		InvertedIndexConfig: invertedConfig(),
		Properties:          []*models.Property{prop},
	}
}

func listID(id int) strfmt.UUID {
	return strfmt.UUID(fmt.Sprintf("00000000-0000-0000-0000-%012d", id))
}

func listTestDB(t *testing.T, classes []*models.Class, objs []listedObject) *DB {
	t.Helper()
	db := createTestDatabaseWithClass(t, monitoring.GetMetrics(), classes...)
	for _, o := range objs {
		require.NoError(t, db.PutObject(t.Context(), &models.Object{
			Class: o.class, ID: listID(o.id), Properties: map[string]any{"name": o.name},
		}, []float32{1, 2, 3}, nil, nil, nil, 0))
	}
	return db
}

func listedIDs(res search.Results) []strfmt.UUID {
	ids := make([]strfmt.UUID, len(res))
	for i, r := range res {
		ids[i] = r.ID
	}
	return ids
}

func TestObjectSearchWithoutClass(t *testing.T) {
	alpha, beta := listClass("Alpha", schema.DataTypeText), listClass("Beta", schema.DataTypeText)
	gamma := listClass("Gamma", schema.DataTypeInt)
	byName := func(order string) []filters.Sort {
		return []filters.Sort{{Path: []string{"name"}, Order: order}}
	}

	cases := []struct {
		name    string
		classes []*models.Class
		objs    []listedObject
		sort    []filters.Sort
		offset  int
		limit   int
		want    []strfmt.UUID
		wantErr bool // an invalid user input
	}{
		{
			name:    "collections come in name order, each in id order",
			classes: []*models.Class{alpha, beta},
			objs:    []listedObject{{"Beta", 1, "b"}, {"Alpha", 3, "a"}, {"Alpha", 2, "a"}},
			limit:   10,
			want:    []strfmt.UUID{listID(2), listID(3), listID(1)},
		},
		{
			name:    "an ascending sort merges across collections",
			classes: []*models.Class{alpha, beta},
			objs:    []listedObject{{"Alpha", 1, "m"}, {"Alpha", 2, "z"}, {"Beta", 3, "a"}, {"Beta", 4, "n"}},
			sort:    byName("asc"),
			limit:   2,
			want:    []strfmt.UUID{listID(3), listID(1)},
		},
		{
			name:    "a descending sort merges across collections",
			classes: []*models.Class{alpha, beta},
			objs:    []listedObject{{"Alpha", 1, "m"}, {"Alpha", 2, "z"}, {"Beta", 3, "a"}, {"Beta", 4, "n"}},
			sort:    byName("desc"),
			limit:   3,
			want:    []strfmt.UUID{listID(2), listID(4), listID(1)},
		},
		{
			name:    "int and number sort properties merge",
			classes: []*models.Class{gamma, listClass("Delta", schema.DataTypeNumber)},
			objs:    []listedObject{{"Gamma", 1, 7}, {"Delta", 2, 2.5}, {"Gamma", 3, 1}},
			sort:    byName("asc"),
			limit:   3,
			want:    []strfmt.UUID{listID(3), listID(2), listID(1)},
		},
		{
			name:    "a sort property whose data type differs between collections is a user error",
			classes: []*models.Class{alpha, gamma},
			objs:    []listedObject{{"Alpha", 1, "m"}, {"Gamma", 2, 7}},
			sort:    byName("asc"),
			limit:   2,
			wantErr: true,
		},
		{
			name:    "a negative limit is a user error",
			classes: []*models.Class{alpha, beta},
			limit:   -1,
			wantErr: true,
		},
		{
			name:    "a negative offset is a user error",
			classes: []*models.Class{alpha, beta},
			offset:  -1,
			limit:   2,
			wantErr: true,
		},
		{
			name:    "a sort property a collection lacks is a user error",
			classes: []*models.Class{alpha, beta},
			sort:    []filters.Sort{{Path: []string{"missing"}, Order: "asc"}},
			limit:   2,
			wantErr: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db := listTestDB(t, tc.classes, tc.objs)
			// repeats because Go map order varies per walk
			for range 10 {
				res, err := db.ObjectSearch(t.Context(), tc.offset, tc.limit, nil, tc.sort, additional.Properties{}, "")
				if tc.wantErr {
					require.ErrorAs(t, err, &objects.ErrInvalidUserInput{})
					return
				}
				require.NoError(t, err)
				assert.Equal(t, tc.want, listedIDs(res))
			}
		})
	}
}

func TestObjectSearchWithoutClassPagesStably(t *testing.T) {
	alpha, beta := listClass("Alpha", schema.DataTypeText), listClass("Beta", schema.DataTypeText)
	var objs []listedObject
	for id := 1; id <= 6; id++ {
		objs = append(objs, listedObject{class: map[bool]string{true: "Alpha", false: "Beta"}[id%2 == 0], id: id, name: "n"})
	}
	db := listTestDB(t, []*models.Class{alpha, beta}, objs)

	all, err := db.ObjectSearch(t.Context(), 0, 10, nil, nil, additional.Properties{}, "")
	require.NoError(t, err)
	require.Len(t, all, 6)
	for range 10 {
		var paged []strfmt.UUID
		for offset := 0; offset < 6; offset += 2 {
			res, err := db.ObjectSearch(t.Context(), offset, 2, nil, nil, additional.Properties{}, "")
			require.NoError(t, err)
			paged = append(paged, listedIDs(res)...)
		}
		assert.Equal(t, listedIDs(all), paged)
	}
}

func TestObjectSearchWithoutClassCollectionLifecycle(t *testing.T) {
	classes := []*models.Class{
		listClass("A", schema.DataTypeText), listClass("B", schema.DataTypeText), listClass("C", schema.DataTypeText),
	}
	objs := []listedObject{{"B", 1, "x"}, {"B", 2, "y"}, {"C", 3, "z"}}
	byName := []filters.Sort{{Path: []string{"name"}, Order: "asc"}}

	cases := []struct {
		name       string
		class      string // the collection the case acts on
		unschema   bool   // removed from the schema, as a delete does before the drop
		unreadable bool   // its searches fail
		panics     bool   // its searches panic
		closeCause error
		sort       []filters.Sort
		limit      int
		want       []strfmt.UUID
		wantErr    error
	}{
		{
			name: "a collection gone from the schema is left out", class: "C", unschema: true,
			limit: 10, want: []strfmt.UUID{listID(1), listID(2)},
		},
		{
			name: "a collection gone from the schema is left out of a sort", class: "C", unschema: true,
			sort: byName, limit: 10, want: []strfmt.UUID{listID(1), listID(2)},
		},
		{
			name: "a collection being deleted is left out", class: "C", unreadable: true, closeCause: errIndexDropped,
			limit: 10, want: []strfmt.UUID{listID(1), listID(2)},
		},
		{
			name: "a collection being deleted is left out of a sort", class: "C", unreadable: true, closeCause: errIndexDropped,
			sort: byName, limit: 10, want: []strfmt.UUID{listID(1), listID(2)},
		},
		{
			name: "a collection whose search panics while being deleted is left out", class: "C", panics: true, closeCause: errIndexDropped,
			limit: 10, want: []strfmt.UUID{listID(1), listID(2)},
		},
		{
			name: "a search that panics fails the list", class: "C", panics: true,
			limit: 10, wantErr: errSearchPanicked,
		},
		{
			name: "a collection shutting down fails the list", class: "C", unreadable: true, closeCause: errIndexShutdown,
			limit: 10, wantErr: errAlreadyShutdown,
		},
		{
			name: "a failure before the page fills fails the list", class: "A", unreadable: true,
			limit: 2, wantErr: errAlreadyShutdown,
		},
		{
			name: "a failure after the page filled is ignored", class: "C", unreadable: true,
			limit: 2, want: []strfmt.UUID{listID(1), listID(2)},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db := listTestDB(t, classes, objs)
			if tc.unschema {
				sg := db.schemaGetter.(*fakeSchemaGetter)
				kept := slices.DeleteFunc(slices.Clone(classes), func(c *models.Class) bool { return c.Class == tc.class })
				sg.schema = schema.Schema{Objects: &models.Schema{Classes: kept}}
			}
			idx := db.GetIndex(schema.ClassName(tc.class))
			require.NotNil(t, idx)
			if tc.unreadable {
				wrapShards(t, idx, func(shard ShardLike) ShardLike { return failingSearchShard{shard} })
			}
			if tc.panics {
				t.Setenv("DISABLE_RECOVERY_ON_PANIC", "false") // the integration run disables it
				wrapShards(t, idx, func(shard ShardLike) ShardLike { return panickingSearchShard{shard} })
			}
			if tc.closeCause != nil {
				idx.signalCloseRequested(tc.closeCause)
			}

			res, err := db.ObjectSearch(t.Context(), 0, tc.limit, nil, tc.sort, additional.Properties{}, "")
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, listedIDs(res))
		})
	}
}

// Reads every collection outside indexLock, and only as many as the page needs.
func TestObjectSearchWithoutClassReads(t *testing.T) {
	classes := []*models.Class{
		listClass("A", schema.DataTypeText), listClass("B", schema.DataTypeText), listClass("C", schema.DataTypeText),
	}
	objs := []listedObject{{"A", 1, "x"}, {"A", 2, "y"}, {"B", 3, "z"}, {"C", 4, "w"}}

	cases := []struct {
		name      string
		sort      []filters.Sort
		limit     int
		wantReads int64
	}{
		{name: "a first collection that fills the page is the only one read", limit: 2, wantReads: 1},
		{name: "a page past the first collection reads on", limit: 3, wantReads: int64(1 + min(2, listBatchLimit))},
		{name: "a page of zero reads nothing", sort: []filters.Sort{{Path: []string{"name"}}}, wantReads: 0},
		{name: "a sort reads every collection", sort: []filters.Sort{{Path: []string{"name"}}}, limit: 1, wantReads: 3},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db := listTestDB(t, classes, objs)
			probe := &indexLockProbe{t: t, db: db}
			for _, c := range classes {
				idx := db.GetIndex(schema.ClassName(c.Class))
				require.NotNil(t, idx)
				wrapShards(t, idx, func(shard ShardLike) ShardLike { return lockProbeShard{ShardLike: shard, probe: probe} })
			}

			_, err := db.ObjectSearch(t.Context(), 0, tc.limit, nil, tc.sort, additional.Properties{}, "")
			require.NoError(t, err)
			assert.Equal(t, tc.wantReads, probe.reads.Load())
		})
	}
}

func wrapShards(t *testing.T, idx *Index, wrap func(ShardLike) ShardLike) {
	t.Helper()
	var names []string
	require.NoError(t, idx.ForEachShard(func(name string, _ ShardLike) error {
		names = append(names, name)
		return nil
	}))
	require.NotEmpty(t, names)
	for _, name := range names {
		idx.shards.Store(name, wrap(idx.shards.Load(name)))
	}
}

type failingSearchShard struct{ ShardLike }

func (failingSearchShard) ObjectSearch(context.Context, int, *filters.LocalFilter,
	*searchparams.KeywordRanking, []filters.Sort, *filters.Cursor, additional.Properties, []string,
) ([]*storobj.Object, []float32, error) {
	return nil, nil, errAlreadyShutdown
}

var errSearchPanicked = stderrors.New("search panicked")

type panickingSearchShard struct{ ShardLike }

func (panickingSearchShard) ObjectSearch(context.Context, int, *filters.LocalFilter,
	*searchparams.KeywordRanking, []filters.Sort, *filters.Cursor, additional.Properties, []string,
) ([]*storobj.Object, []float32, error) {
	panic(errSearchPanicked)
}

// Equal sort values must keep one order across offsets: by collection name,
// then the collection's own order.
func TestObjectSearchWithoutClassSortPagesStablyThroughTies(t *testing.T) {
	classes := []*models.Class{
		listClass("A", schema.DataTypeText), listClass("B", schema.DataTypeText), listClass("C", schema.DataTypeText),
	}
	var objs []listedObject
	for id := 1; id <= 24; id++ {
		objs = append(objs, listedObject{class: classes[(id-1)/8].Class, id: id, name: map[bool]string{true: "a", false: "b"}[id%2 == 0]})
	}
	db := listTestDB(t, classes, objs)
	byName := []filters.Sort{{Path: []string{"name"}, Order: "asc"}}

	all, err := db.ObjectSearch(t.Context(), 0, 30, nil, byName, additional.Properties{}, "")
	require.NoError(t, err)
	require.Len(t, all, 24)
	for range 5 {
		var paged []strfmt.UUID
		for offset := 0; offset < 24; offset += 5 {
			res, err := db.ObjectSearch(t.Context(), offset, 5, nil, byName, additional.Properties{}, "")
			require.NoError(t, err)
			paged = append(paged, listedIDs(res)...)
		}
		require.Equal(t, listedIDs(all), paged)
	}
}

// More collections than one batch: the merge across batches keeps the global top.
func TestObjectSearchWithoutClassSortAcrossBatches(t *testing.T) {
	n := 2*listSortedConcurrency + 1
	var classes []*models.Class
	var objs []listedObject
	for i := range n {
		class := fmt.Sprintf("C%03d", i)
		classes = append(classes, listClass(class, schema.DataTypeText))
		// later collections hold the smaller names
		objs = append(objs, listedObject{class: class, id: i + 1, name: fmt.Sprintf("n%03d", n-i)})
	}
	db := listTestDB(t, classes, objs)

	for _, tc := range []struct {
		order string
		want  []strfmt.UUID
	}{
		{"asc", []strfmt.UUID{listID(n), listID(n - 1), listID(n - 2)}},
		{"desc", []strfmt.UUID{listID(1), listID(2), listID(3)}},
	} {
		t.Run(tc.order, func(t *testing.T) {
			res, err := db.ObjectSearch(t.Context(), 0, 3, nil,
				[]filters.Sort{{Path: []string{"name"}, Order: tc.order}}, additional.Properties{}, "")
			require.NoError(t, err)
			assert.Equal(t, tc.want, listedIDs(res))
		})
	}
}

// Collections the request's tenant does not reach are neither validated nor searched.
func TestObjectSearchWithoutClassTenancy(t *testing.T) {
	alpha := listClass("Alpha", schema.DataTypeText)
	payroll := listClass("Payroll", schema.DataTypeInt)
	payroll.MultiTenancyConfig = &models.MultiTenancyConfig{Enabled: true}
	byName := []filters.Sort{{Path: []string{"name"}, Order: "asc"}}

	cases := []struct {
		name   string
		tenant string
		want   []strfmt.UUID
	}{
		{name: "a tenant-less request skips a multi-tenant collection", want: []strfmt.UUID{listID(1), listID(2)}},
		{name: "a tenant a multi-tenant collection lacks skips it", tenant: "t1", want: []strfmt.UUID{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db := listTestDB(t, []*models.Class{alpha, payroll}, []listedObject{{"Alpha", 1, "a"}, {"Alpha", 2, "b"}})
			idx := db.GetIndex("Payroll")
			require.NotNil(t, idx)
			var lookups atomic.Int32
			idx.shardResolver = resolver.NewShardResolver("Payroll", true,
				countingTenants{fakeSchemaGetter: db.schemaGetter.(*fakeSchemaGetter), calls: &lookups})

			res, err := db.ObjectSearch(t.Context(), 0, 10, nil, byName, additional.Properties{}, tc.tenant)
			require.NoError(t, err)
			assert.Equal(t, tc.want, listedIDs(res))
			assert.Zero(t, lookups.Load(), "a tenant lookup is a RAFT leader query")
		})
	}
}

type countingTenants struct {
	*fakeSchemaGetter
	calls *atomic.Int32
}

func (c countingTenants) TenantsShards(context.Context, string, ...string) (map[string]string, error) {
	c.calls.Add(1)
	return map[string]string{}, nil
}

// A cancelled caller gets an error, never a page that may be cut short.
func TestObjectSearchWithoutClassCancelled(t *testing.T) {
	byName := []filters.Sort{{Path: []string{"name"}, Order: "asc"}}
	cases := []struct {
		name    string
		classes []*models.Class
		sort    []filters.Sort
	}{
		{name: "without collections"},
		{name: "cancelled while a collection fills the page", classes: []*models.Class{listClass("A", schema.DataTypeText)}},
		{name: "cancelled during a sorted search", classes: []*models.Class{listClass("A", schema.DataTypeText)}, sort: byName},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db := listTestDB(t, tc.classes, []listedObject{})
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if len(tc.classes) == 0 {
				cancel()
			}
			for _, c := range tc.classes {
				require.NoError(t, db.PutObject(t.Context(), &models.Object{
					Class: c.Class, ID: listID(1), Properties: map[string]any{"name": "a"},
				}, []float32{1, 2, 3}, nil, nil, nil, 0))
				wrapShards(t, db.GetIndex(schema.ClassName(c.Class)), func(shard ShardLike) ShardLike {
					return cancelOnSearchShard{ShardLike: shard, cancel: cancel}
				})
			}

			_, err := db.ObjectSearch(ctx, 0, 1, nil, tc.sort, additional.Properties{}, "")
			require.ErrorIs(t, err, context.Canceled)
		})
	}
}

// cancelOnSearchShard cancels the caller, then answers: a local read ignores its context.
type cancelOnSearchShard struct {
	ShardLike
	cancel context.CancelFunc
}

func (s cancelOnSearchShard) ObjectSearch(ctx context.Context, limit int, filters *filters.LocalFilter,
	keywordRanking *searchparams.KeywordRanking, sort []filters.Sort, cursor *filters.Cursor,
	addl additional.Properties, properties []string,
) ([]*storobj.Object, []float32, error) {
	s.cancel()
	return s.ShardLike.ObjectSearch(ctx, limit, filters, keywordRanking, sort, cursor, addl, properties)
}

// A delete takes the class out of the schema, and only later signals the drop,
// while a sorted search of a multi-shard collection merges its shards: that
// collection is left out.
func TestObjectSearchWithoutClassSortLeavesOutACollectionDroppedMidSearch(t *testing.T) {
	classes := []*models.Class{listClass("A", schema.DataTypeText), listClass("B", schema.DataTypeText)}
	db := openTestDatabase(t, t.TempDir(), multiShardState(), monitoring.GetMetrics(), nil, nil, classes...)
	t.Cleanup(func() { require.NoError(t, db.Shutdown(context.Background())) })
	for _, o := range []listedObject{{"A", 1, "b"}, {"A", 2, "a"}, {"B", 3, "c"}, {"B", 4, "d"}, {"B", 5, "e"}, {"B", 6, "f"}} {
		require.NoError(t, db.PutObject(t.Context(), &models.Object{
			Class: o.class, ID: listID(o.id), Properties: map[string]any{"name": o.name},
		}, []float32{1, 2, 3}, nil, nil, nil, 0))
	}
	idx := db.GetIndex("B")
	require.NotNil(t, idx)
	var gone atomic.Bool
	var unread atomic.Int32
	idx.getSchema = classGoneAfter{SchemaGetter: idx.getSchema, class: "B", gone: &gone}
	wrapShards(t, idx, func(shard ShardLike) ShardLike {
		unread.Add(1)
		return dropAfterSearchShard{ShardLike: shard, gone: &gone, unread: &unread}
	})
	require.Greater(t, unread.Load(), int32(1), "the shard merge needs more than one shard")

	res, err := db.ObjectSearch(t.Context(), 0, 10, nil,
		[]filters.Sort{{Path: []string{"name"}, Order: "asc"}}, additional.Properties{}, "")
	require.NoError(t, err)
	assert.Equal(t, []strfmt.UUID{listID(2), listID(1)}, listedIDs(res))
	assert.True(t, gone.Load(), "the drop did not happen mid-search")
}

type classGoneAfter struct {
	schemaUC.SchemaGetter
	class string
	gone  *atomic.Bool
}

func (g classGoneAfter) ReadOnlyClass(name string) *models.Class {
	if name == g.class && g.gone.Load() {
		return nil
	}
	return g.SchemaGetter.ReadOnlyClass(name)
}

// dropAfterSearchShard takes the class out of the schema once the last shard was read.
type dropAfterSearchShard struct {
	ShardLike
	gone   *atomic.Bool
	unread *atomic.Int32
}

func (s dropAfterSearchShard) ObjectSearch(ctx context.Context, limit int, filters *filters.LocalFilter,
	keywordRanking *searchparams.KeywordRanking, sort []filters.Sort, cursor *filters.Cursor,
	addl additional.Properties, properties []string,
) ([]*storobj.Object, []float32, error) {
	res, scores, err := s.ShardLike.ObjectSearch(ctx, limit, filters, keywordRanking, sort, cursor, addl, properties)
	if s.unread.Add(-1) == 0 {
		s.gone.Store(true)
	}
	return res, scores, err
}

func TestReachedBy(t *testing.T) {
	cases := []struct {
		name        string
		multiTenant bool
		tenant      string
		want        bool
	}{
		{name: "a single-tenant collection without a tenant", want: true},
		{name: "a single-tenant collection with a tenant", tenant: "t1"},
		{name: "a multi-tenant collection without a tenant", multiTenant: true},
		{name: "a multi-tenant collection holding the tenant", multiTenant: true, tenant: "t1", want: true},
		{name: "a multi-tenant collection lacking the tenant", multiTenant: true, tenant: "t2"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			reader := schemaUC.NewMockSchemaReader(t)
			reader.EXPECT().Read("C", false, mock.Anything).RunAndReturn(
				func(_ string, _ bool, read func(*models.Class, *sharding.State) error) error {
					return read(&models.Class{Class: "C"}, &sharding.State{Physical: map[string]sharding.Physical{"t1": {Name: "t1"}}})
				}).Maybe()
			idx := &Index{Config: IndexConfig{ClassName: "C"}, partitioningEnabled: tc.multiTenant, schemaReader: reader}
			assert.Equal(t, tc.want, reachedBy(idx, tc.tenant))
		})
	}
}

// Searches in flight are capped, as each holds up to shards × (offset+limit)
// objects; an unsorted search asks only for what the page still needs.
func TestObjectSearchWithoutClassBoundsItsSearches(t *testing.T) {
	var classes []*models.Class
	var objs []listedObject
	for i := range 3 * listBatchLimit {
		class := fmt.Sprintf("C%03d", i)
		classes = append(classes, listClass(class, schema.DataTypeText))
	}
	// only the last collections hold objects, so batches grow to their cap
	for id := 1; id <= 5; id++ {
		objs = append(objs, listedObject{class: classes[len(classes)-1].Class, id: id, name: "x"})
	}
	objs = append(objs, listedObject{class: classes[len(classes)-2].Class, id: 6, name: "x"})

	cases := []struct {
		name        string
		sort        []filters.Sort
		maxInFlight int32
	}{
		{name: "unsorted", maxInFlight: listBatchLimit},
		{name: "sorted", sort: []filters.Sort{{Path: []string{"name"}}}, maxInFlight: listSortedConcurrency},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db := listTestDB(t, classes, objs)
			probe := &searchProbe{limits: map[string]int{}}
			for _, c := range classes {
				wrapShards(t, db.GetIndex(schema.ClassName(c.Class)), func(shard ShardLike) ShardLike {
					return probedSearchShard{ShardLike: shard, probe: probe, class: c.Class}
				})
			}

			res, err := db.ObjectSearch(t.Context(), 0, 3, nil, tc.sort, additional.Properties{}, "")
			require.NoError(t, err)
			require.Len(t, res, 3)
			assert.LessOrEqual(t, probe.maxInFlight.Load(), tc.maxInFlight)
			if tc.sort == nil {
				// the second-to-last collection fills one of three, so the last needs two
				assert.Equal(t, 2, probe.limit(classes[len(classes)-1].Class))
			}
		})
	}
}

type searchProbe struct {
	inFlight, maxInFlight atomic.Int32
	mu                    sync.Mutex
	limits                map[string]int
}

func (p *searchProbe) limit(class string) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.limits[class]
}

type probedSearchShard struct {
	ShardLike
	probe *searchProbe
	class string
}

func (s probedSearchShard) ObjectSearch(ctx context.Context, limit int, filters *filters.LocalFilter,
	keywordRanking *searchparams.KeywordRanking, sort []filters.Sort, cursor *filters.Cursor,
	addl additional.Properties, properties []string,
) ([]*storobj.Object, []float32, error) {
	n := s.probe.inFlight.Add(1)
	defer s.probe.inFlight.Add(-1)
	for {
		m := s.probe.maxInFlight.Load()
		if n <= m || s.probe.maxInFlight.CompareAndSwap(m, n) {
			break
		}
	}
	s.probe.mu.Lock()
	s.probe.limits[s.class] = limit
	s.probe.mu.Unlock()
	time.Sleep(20 * time.Millisecond) // overlap concurrent searches
	return s.ShardLike.ObjectSearch(ctx, limit, filters, keywordRanking, sort, cursor, addl, properties)
}

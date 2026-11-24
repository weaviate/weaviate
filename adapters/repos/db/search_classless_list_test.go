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
	"fmt"
	"slices"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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
			name:    "a sort property whose data type differs between collections is a user error",
			classes: []*models.Class{alpha, gamma},
			objs:    []listedObject{{"Alpha", 1, "m"}, {"Gamma", 2, 7}},
			sort:    byName("asc"),
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
				res, err := db.ObjectSearch(t.Context(), 0, tc.limit, nil, tc.sort, additional.Properties{}, "")
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
		{name: "a page past the first collection reads on", limit: 3, wantReads: int64(1 + min(2, _NUMCPU))},
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

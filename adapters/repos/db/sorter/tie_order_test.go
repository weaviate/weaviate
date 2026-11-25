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

package sorter

import (
	"bytes"
	"cmp"
	"context"
	"encoding/binary"
	"fmt"
	"slices"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/inverted"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/config/runtime"
)

type tiedObject struct {
	id        uuid.UUID
	int, int2 int64
}

// tieReplica holds the objects under its own docIDs, as each replica assigns them.
type tieReplica struct {
	store   *lsmkv.Store
	objects []tiedObject
	idOf    map[uint64]uuid.UUID
	docs    []uint64 // in object order
	deleted map[int]bool
}

func newTieReplica(t *testing.T, objects []tiedObject, docID func(i int) uint64) *tieReplica {
	ctx := context.Background()
	logger, _ := test.NewNullLogger()
	dir := t.TempDir()
	store, err := lsmkv.New(dir, dir, logger, nil, nil, cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop())
	require.NoError(t, err)
	t.Cleanup(func() { store.Shutdown(context.Background()) })
	require.NoError(t, store.CreateOrLoadBucket(ctx, helpers.ObjectsBucketLSM,
		lsmkv.WithStrategy(lsmkv.StrategyReplace), lsmkv.WithSecondaryIndices(1), lsmkv.WithCalcCountNetAdditions(true)))
	for _, prop := range []string{"int", "int2"} {
		require.NoError(t, store.CreateOrLoadBucket(ctx, helpers.BucketFromPropNameLSM(prop),
			lsmkv.WithStrategy(lsmkv.StrategyRoaringSet)))
	}

	r := &tieReplica{store: store, objects: objects, idOf: map[uint64]uuid.UUID{}, deleted: map[int]bool{}}
	for i := range objects {
		r.docs = append(r.docs, docID(i))
		r.put(t, i)
	}
	return r
}

func (r *tieReplica) put(t *testing.T, i int) {
	o, d := r.objects[i], r.docs[i]
	obj := storobj.FromObject(&models.Object{
		Class:      "DummyClass",
		ID:         strfmt.UUID(o.id.String()),
		Properties: map[string]any{"int": float64(o.int), "int2": float64(o.int2)},
	}, nil, nil, nil)
	obj.DocID = d
	data, err := obj.MarshalBinary()
	require.NoError(t, err)
	require.NoError(t, r.store.Bucket(helpers.ObjectsBucketLSM).Put(o.id[:], data, lsmkv.WithSecondaryKey(0, docIDKey(d))))
	r.index(t, i, d, true)
	r.idOf[d] = o.id
}

func (r *tieReplica) index(t *testing.T, i int, d uint64, add bool) {
	o := r.objects[i]
	for prop, v := range map[string]int64{"int": o.int, "int2": o.int2} {
		key, err := inverted.LexicographicallySortableInt64(v)
		require.NoError(t, err)
		b := r.store.Bucket(helpers.BucketFromPropNameLSM(prop))
		if add {
			require.NoError(t, b.RoaringSetAddOne(key, d))
		} else {
			require.NoError(t, b.RoaringSetRemoveOne(key, d))
		}
	}
}

// update rewrites the object under a new docID, as a shard does.
func (r *tieReplica) update(t *testing.T, i int, docID uint64) {
	r.index(t, i, r.docs[i], false)
	delete(r.idOf, r.docs[i])
	r.docs[i] = docID
	r.put(t, i)
}

func (r *tieReplica) delete(t *testing.T, i int) {
	r.index(t, i, r.docs[i], false)
	require.NoError(t, r.store.Bucket(helpers.ObjectsBucketLSM).Delete(r.objects[i].id[:],
		lsmkv.WithSecondaryKey(0, docIDKey(r.docs[i]))))
	r.deleted[i] = true
}

func docIDKey(d uint64) []byte {
	key := make([]byte, 8)
	binary.LittleEndian.PutUint64(key, d)
	return key
}

func (r *tieReplica) flush(t *testing.T) {
	for _, b := range []string{helpers.ObjectsBucketLSM, helpers.BucketFromPropNameLSM("int"), helpers.BucketFromPropNameLSM("int2")} {
		require.NoError(t, r.store.Bucket(b).FlushAndSwitch())
	}
}

func (r *tieReplica) uuids(docIDs []uint64) []uuid.UUID {
	out := make([]uuid.UUID, len(docIDs))
	for i, d := range docIDs {
		out[i] = r.idOf[d]
	}
	return out
}

// Replicas hold the same objects under different docIDs, so a tie broken by
// docID pages differently depending on the replica read.
func TestSortersOrderTiesByUUIDOnEveryReplica(t *testing.T) {
	const n = 300
	objects := make([]tiedObject, n)
	for i := range objects {
		// int: 3 values, groups of 100; int2: 60 values, groups of 5
		objects[i] = tiedObject{id: uuid.New(), int: int64(i % 3), int2: int64(i % 60)}
	}
	replicas := map[string]*tieReplica{
		"docIDs in insert order":   newTieReplica(t, objects, func(i int) uint64 { return uint64(i) }),
		"docIDs in reversed order": newTieReplica(t, objects, func(i int) uint64 { return uint64(n - 1 - i) }),
	}
	// leaves out every 7th object
	filtered := func(r *tieReplica) helpers.AllowList {
		var docs []uint64
		for i, d := range r.docs {
			if i%7 != 0 && !r.deleted[i] {
				docs = append(docs, d)
			}
		}
		return helpers.NewAllowList(docs...)
	}
	deleted := map[int]bool{}
	want := func(prop, order string, limit int, withFilter bool) []uuid.UUID {
		var in []tiedObject
		for i, o := range objects {
			if (!withFilter || i%7 != 0) && !deleted[i] {
				in = append(in, o)
			}
		}
		slices.SortFunc(in, func(a, b tiedObject) int {
			va, vb := a.int, b.int
			if prop == "int2" {
				va, vb = a.int2, b.int2
			}
			c := cmp.Compare(va, vb)
			if order == "desc" {
				c = -c
			}
			if c != 0 {
				return c
			}
			return bytes.Compare(a.id[:], b.id[:])
		})
		out := make([]uuid.UUID, 0, limit)
		for _, o := range in[:min(limit, len(in))] {
			out = append(out, o.id)
		}
		return out
	}

	class := func(string) *models.Class { return dummyClass() }
	strategies := []struct {
		name       string
		withFilter bool
		sort       func(ctx context.Context, r *tieReplica, limit int, s []filters.Sort) ([]uint64, error)
	}{
		{name: "inverted", sort: func(ctx context.Context, r *tieReplica, limit int, s []filters.Sort) ([]uint64, error) {
			return NewInvertedSorter(r.store, newDataTypesHelper(dummyClass())).SortDocIDs(ctx, limit, s, nil)
		}},
		{name: "inverted with a filter", withFilter: true, sort: func(ctx context.Context, r *tieReplica, limit int, s []filters.Sort) ([]uint64, error) {
			return NewInvertedSorter(r.store, newDataTypesHelper(dummyClass())).SortDocIDs(ctx, limit, s, filtered(r))
		}},
		{name: "objects bucket", sort: func(ctx context.Context, r *tieReplica, limit int, s []filters.Sort) ([]uint64, error) {
			ls, err := NewLSMSorter(r.store, class, "DummyClass", runtime.NewDynamicValue(true))
			require.NoError(t, err)
			return ls.Sort(ctx, limit, s)
		}},
		{name: "objects bucket with a filter", withFilter: true, sort: func(ctx context.Context, r *tieReplica, limit int, s []filters.Sort) ([]uint64, error) {
			ls, err := NewLSMSorter(r.store, class, "DummyClass", runtime.NewDynamicValue(true))
			require.NoError(t, err)
			return ls.SortDocIDs(ctx, limit, s, filtered(r))
		}},
		{name: "objects bucket with distances", sort: func(ctx context.Context, r *tieReplica, limit int, s []filters.Sort) ([]uint64, error) {
			ls, err := NewLSMSorter(r.store, class, "DummyClass", runtime.NewDynamicValue(true))
			require.NoError(t, err)
			var live []uint64
			for i, d := range r.docs {
				if !r.deleted[i] {
					live = append(live, d)
				}
			}
			docIDs, _, err := ls.SortDocIDsAndDists(ctx, limit, s, live, make([]float32, len(live)))
			return docIDs, err
		}},
		{name: "loaded objects", sort: func(ctx context.Context, r *tieReplica, limit int, s []filters.Sort) ([]uint64, error) {
			var objs []*storobj.Object
			for i, o := range objects {
				if r.deleted[i] {
					continue
				}
				obj := storobj.FromObject(&models.Object{
					Class: "DummyClass", ID: strfmt.UUID(o.id.String()),
					Properties: map[string]any{"int": float64(o.int), "int2": float64(o.int2)},
				}, nil, nil, nil)
				obj.DocID = r.docs[i]
				objs = append(objs, obj)
			}
			slices.SortFunc(objs, func(a, b *storobj.Object) int { return cmp.Compare(a.DocID, b.DocID) })
			sorted, _, err := NewObjectsSorter(class).Sort(objs, nil, limit, s)
			docIDs := make([]uint64, len(sorted))
			for i, o := range sorted {
				docIDs[i] = o.DocID
			}
			return docIDs, err
		}},
	}

	states := []struct {
		name  string
		apply func(t *testing.T)
	}{
		{name: "in memtables", apply: func(*testing.T) {}},
		{name: "flushed", apply: func(t *testing.T) {
			for _, r := range replicas {
				r.flush(t)
			}
		}},
		{name: "updated and deleted over a segment", apply: func(t *testing.T) {
			for i := range objects {
				switch {
				case i%11 == 0:
					deleted[i] = true
					for _, r := range replicas {
						r.delete(t, i)
					}
				case i%5 == 0:
					for name, r := range replicas {
						d := uint64(n + i) // each replica takes its own next docID
						if name == "docIDs in reversed order" {
							d = uint64(3*n - i)
						}
						r.update(t, i, d)
					}
				}
			}
		}},
		{name: "in two segments", apply: func(t *testing.T) {
			for _, r := range replicas {
				r.flush(t)
			}
		}},
	}
	for _, state := range states {
		state.apply(t)
		for _, st := range strategies {
			for _, prop := range []string{"int", "int2"} {
				for _, order := range []string{"asc", "desc"} {
					for _, limit := range []int{1, 7, 50, 100, 150, n} {
						t.Run(fmt.Sprintf("%s/%s/%s %s/limit=%d", state.name, st.name, prop, order, limit), func(t *testing.T) {
							s := []filters.Sort{{Path: []string{prop}, Order: order}}
							expected := want(prop, order, limit, st.withFilter)
							for name, r := range replicas {
								got, err := st.sort(helpers.InitSlowQueryDetails(context.Background()), r, limit, s)
								require.NoError(t, err)
								require.Equal(t, expected, r.uuids(got), name)
							}
						})
					}
				}
			}
		}
	}
}

// Ties break by id; only objects with the same id, from different
// collections, keep their input order.
func TestObjectsSorterBreaksTiesByID(t *testing.T) {
	id := func(i int) strfmt.UUID { return strfmt.UUID(fmt.Sprintf("00000000-0000-0000-0000-%012d", i)) }
	city := func(class string, i int, population float64) *storobj.Object {
		return &storobj.Object{Object: models.Object{
			Class: class, ID: id(i), Properties: map[string]any{"name": "Same", "population": population},
		}}
	}
	cityA, cityB := city("City", 1, 1), city("Town", 1, 1)

	for _, tc := range []struct {
		name string
		in   []*storobj.Object
		sort []filters.Sort
		want []*storobj.Object
	}{
		{
			name: "every value ties", sort: sort1("name", "asc"),
			in:   []*storobj.Object{city("City", 3, 0), city("City", 1, 0), city("City", 2, 0)},
			want: []*storobj.Object{city("City", 1, 0), city("City", 2, 0), city("City", 3, 0)},
		},
		{
			name: "ties within each value, descending", sort: sort1("population", "desc"),
			in:   []*storobj.Object{city("City", 4, 0), city("City", 3, 1), city("City", 2, 0), city("City", 1, 1)},
			want: []*storobj.Object{city("City", 1, 1), city("City", 3, 1), city("City", 2, 0), city("City", 4, 0)},
		},
		{
			name: "the same id in two collections keeps input order", sort: sort1("name", "asc"),
			in:   []*storobj.Object{cityB, city("City", 2, 0), cityA},
			want: []*storobj.Object{cityB, cityA, city("City", 2, 0)},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cities := func(string) *models.Class { return sorterCitySchema().GetClass("City") }
			got, _, err := NewObjectsSorter(cities).Sort(tc.in, nil, 0, tc.sort)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

// Time-ordered UUIDs can put a whole tie group at the end of the objects
// bucket. The scan must stop at its budget and the sorter fall back to lookups.
func TestInvertedSorterBoundsTheUUIDScan(t *testing.T) {
	// a scan of the top group stops after group*uuidScanStepsPerLookup = 800
	// objects, short of the group's first at 1900
	const n, group = 2000, 100
	objects := make([]tiedObject, n)
	for i := range objects {
		var id uuid.UUID
		binary.BigEndian.PutUint64(id[8:], uint64(i)) // ids ascend with the value
		objects[i] = tiedObject{id: id, int: int64(i / (n - group))}
	}
	var top []uint64
	for i := n - group; i < n; i++ {
		top = append(top, uint64(i))
	}

	// n spans several scan windows when the objects are still in the memtable
	for _, flushed := range []bool{true, false} {
		t.Run(fmt.Sprintf("flushed=%v", flushed), func(t *testing.T) {
			r := newTieReplica(t, objects, func(i int) uint64 { return uint64(i) })
			if flushed {
				r.flush(t)
			}

			_, complete, err := firstByUUIDScan(context.Background(), r.store.Bucket(helpers.ObjectsBucketLSM),
				sliceTieGroup(top).contains, 10, group*uuidScanStepsPerLookup)
			require.NoError(t, err)
			require.False(t, complete, "the group starts past the budget")

			got, err := NewInvertedSorter(r.store, newDataTypesHelper(dummyClass())).
				SortDocIDs(context.Background(), 10, []filters.Sort{{Path: []string{"int"}, Order: "desc"}}, nil)
			require.NoError(t, err)
			require.Equal(t, top[:10], got)
		})
	}
}

// A scan that ends early copies one window of the memtable, not all of it.
func TestUUIDScanCopiesOneWindowOfTheMemtable(t *testing.T) {
	const n, keep = 10 * uuidScanWindow, 5
	objects := make([]tiedObject, n)
	for i := range objects {
		var id uuid.UUID
		binary.BigEndian.PutUint64(id[8:], uint64(i))
		objects[i] = tiedObject{id: id}
	}
	r := newTieReplica(t, objects, func(i int) uint64 { return uint64(i) })
	bucket := r.store.Bucket(helpers.ObjectsBucketLSM)
	first := sliceTieGroup([]uint64{0, 1, 2, 3, 4}) // the smallest UUIDs

	allocs := testing.AllocsPerRun(5, func() {
		got, complete, err := firstByUUIDScan(context.Background(), bucket, first.contains, keep, n)
		require.NoError(t, err)
		require.True(t, complete)
		require.Equal(t, []uint64{0, 1, 2, 3, 4}, got)
	})
	assert.Less(t, allocs, float64(2*uuidScanWindow))
}

// A vector search sorted by a property keeps its ties in distance order, then
// UUID order.
func TestVectorSortBreaksTiesByDistance(t *testing.T) {
	ids := []uuid.UUID{
		uuid.MustParse("00000000-0000-0000-0000-000000000001"),
		uuid.MustParse("00000000-0000-0000-0000-000000000002"),
		uuid.MustParse("00000000-0000-0000-0000-000000000003"),
		uuid.MustParse("00000000-0000-0000-0000-000000000004"),
	}
	objects := []tiedObject{{id: ids[2], int: 1}, {id: ids[0], int: 1}, {id: ids[3], int: 1}, {id: ids[1], int: 0}}
	dists := []float32{0.2, 0.3, 0.2, 0.9}
	// value 0 first, then the value-1 ties: 0.2 (ids[2], ids[3] by UUID), then 0.3
	want := []uuid.UUID{ids[1], ids[2], ids[3], ids[0]}
	wantDists := []float32{0.9, 0.2, 0.2, 0.3}
	byInt := []filters.Sort{{Path: []string{"int"}, Order: "asc"}}
	class := func(string) *models.Class { return dummyClass() }

	t.Run("in a shard", func(t *testing.T) {
		r := newTieReplica(t, objects, func(i int) uint64 { return uint64(len(objects) - i) })
		ls, err := NewLSMSorter(r.store, class, "DummyClass", runtime.NewDynamicValue(true))
		require.NoError(t, err)
		docIDs, gotDists, err := ls.SortDocIDsAndDists(context.Background(), 0, byInt, slices.Clone(r.docs), slices.Clone(dists))
		require.NoError(t, err)
		require.Equal(t, want, r.uuids(docIDs))
		require.Equal(t, wantDists, gotDists)
	})

	loaded := func() []*storobj.Object {
		objs := make([]*storobj.Object, len(objects))
		for i, o := range objects {
			objs[i] = storobj.FromObject(&models.Object{
				Class: "DummyClass", ID: strfmt.UUID(o.id.String()), Properties: map[string]any{"int": float64(o.int)},
			}, nil, nil, nil)
		}
		return objs
	}
	uuidsOf := func(objs []*storobj.Object) []uuid.UUID {
		out := make([]uuid.UUID, len(objs))
		for i, o := range objs {
			out[i] = uuid.MustParse(o.ID().String())
		}
		return out
	}
	t.Run("merging shards", func(t *testing.T) {
		got, gotDists, err := NewObjectsSorter(class).TieByDistances().Sort(loaded(), slices.Clone(dists), 0, byInt)
		require.NoError(t, err)
		require.Equal(t, want, uuidsOf(got))
		require.Equal(t, wantDists, gotDists)
	})
	t.Run("scores that are not distances tie by UUID", func(t *testing.T) {
		got, _, err := NewObjectsSorter(class).Sort(loaded(), slices.Clone(dists), 0, byInt)
		require.NoError(t, err)
		require.Equal(t, []uuid.UUID{ids[1], ids[0], ids[2], ids[3]}, uuidsOf(got))
	})
}

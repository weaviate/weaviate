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
	"encoding/binary"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/concurrency"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/entities/vectorindex/common"
	"github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/config/runtime"
)

// textNameClass is a class with one whitespace-tokenized text property, name.
func textNameClass(name string) *models.Class {
	return &models.Class{
		Class: name,
		Properties: []*models.Property{{
			Name:         "name",
			DataType:     schema.DataTypeText.PropString(),
			Tokenization: models.PropertyTokenizationWhitespace,
		}},
	}
}

func nameEquals(class *models.Class, value string) *filters.LocalFilter {
	return &filters.LocalFilter{Root: &filters.Clause{
		Operator: filters.OperatorEqual,
		On:       &filters.Path{Class: schema.ClassName(class.Class), Property: "name"},
		Value:    &filters.Value{Value: value, Type: schema.DataTypeText},
	}}
}

func putNamedObject(t *testing.T, ctx context.Context, shard ShardLike, class *models.Class, name string) *storobj.Object {
	t.Helper()
	obj := &storobj.Object{
		MarshallerVersion: 1,
		Object: models.Object{
			ID:         strfmt.UUID(uuid.NewString()),
			Class:      class.Class,
			Properties: map[string]interface{}{"name": name},
		},
	}
	require.NoError(t, shard.PutObject(ctx, obj))
	return obj
}

// putNamedVectorObject is putNamedObject for the searches that need the object
// in the vector index as well as the inverted one.
func putNamedVectorObject(t *testing.T, ctx context.Context, shard ShardLike, class *models.Class,
	name string, vector []float32,
) *storobj.Object {
	t.Helper()
	obj := &storobj.Object{
		MarshallerVersion: 1,
		Object: models.Object{
			ID:         strfmt.UUID(uuid.NewString()),
			Class:      class.Class,
			Properties: map[string]interface{}{"name": name},
		},
		Vector: vector,
	}
	require.NoError(t, shard.PutObject(ctx, obj))
	return obj
}

// docIDKeyOf returns the objects bucket secondary key of a stored object.
func docIDKeyOf(t *testing.T, ctx context.Context, shard ShardLike, id strfmt.UUID) []byte {
	t.Helper()
	stored, err := shard.ObjectByID(ctx, id, nil, additional.Properties{})
	require.NoError(t, err)
	key := make([]byte, 8)
	binary.LittleEndian.PutUint64(key, stored.DocID)
	return key
}

func TestShardFindUUIDs(t *testing.T) {
	ctx := context.Background()
	class := textNameClass("FindUUIDsTest")

	type testCase struct {
		name              string
		matching          int
		other             int
		deletedMatching   int
		unreadable        int
		flushObjects      bool
		limit             int
		cancelCtx         bool
		objectsBucketGone bool
		wantCount         int
		wantWarnSkipped   int
		wantErr           error
		wantErrText       string
	}
	cases := []testCase{
		{name: "no match", other: 5, wantCount: 0},
		{name: "one match", matching: 1, other: 5, wantCount: 1},
		{name: "many matches", matching: 25, other: 5, wantCount: 25},
		{name: "more matches than one bucket call", matching: 1203, wantCount: 1203},
		{name: "limit below matches", matching: 25, limit: 10, wantCount: 10},
		{name: "objects deleted after the allow list was built are skipped", matching: 25, deletedMatching: 5, wantCount: 20},
		{name: "every match deleted after the allow list was built", matching: 5, deletedMatching: 5, wantCount: 0},
		{name: "objects without a readable id are skipped", matching: 40, other: 5, unreadable: 2, wantCount: 38, wantWarnSkipped: 2},
		{
			// Spans more than one 500-doc-id bucket call; the summary must still be
			// one line, not one per skip.
			name: "unreadable ids across more than one bucket call are warned about once", matching: 1203,
			unreadable: 3, wantCount: 1200, wantWarnSkipped: 3,
		},
		{
			// The only case resolved from a flushed segment, exercising the
			// worker read buffer.
			name: "matches resolved from a flushed segment", matching: 600, other: 5, flushObjects: true, wantCount: 600,
		},
		{name: "cancelled context", matching: 5, cancelCtx: true, wantErr: context.Canceled},
		{name: "a missing objects bucket fails the call instead of skipping", matching: 5, objectsBucketGone: true, wantErr: lsmkv.ErrBucketNotFound, wantErrText: "objects bucket"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			shard, _ := testShardWithSettings(t, ctx, class, hnsw.UserConfig{Distance: common.DefaultDistanceMetric}, false, false, false)

			matching := make([]*storobj.Object, tc.matching)
			for i := range matching {
				matching[i] = putNamedObject(t, ctx, shard, class, "match")
			}
			for range tc.other {
				putNamedObject(t, ctx, shard, class, "other")
			}

			// Tombstoning the object without touching the inverted index leaves its
			// doc id in the allow list, as a delete racing FindUUIDs would.
			bucket := shard.Store().Bucket(helpers.ObjectsBucketLSM)
			skipped := map[strfmt.UUID]bool{}
			for _, obj := range matching[:tc.deletedMatching] {
				require.NoError(t, bucket.Delete([]byte(obj.ID()), lsmkv.WithSecondaryKey(0, docIDKeyOf(t, ctx, shard, obj.ID()))))
				skipped[obj.ID()] = true
			}

			// Bytes shorter than the id header carry no readable id; spreading them
			// across the match set exercises the skip branch on several workers and calls.
			for i := range tc.unreadable {
				obj := matching[i*len(matching)/tc.unreadable]
				require.NoError(t, bucket.Put([]byte(obj.ID()), []byte("garbage"), lsmkv.WithSecondaryKey(0, docIDKeyOf(t, ctx, shard, obj.ID()))))
				skipped[obj.ID()] = true
			}

			if tc.flushObjects {
				require.NoError(t, bucket.FlushAndSwitch())
			}

			hook := test.NewLocal(shard.Index().logger.(*logrus.Logger))

			if tc.objectsBucketGone {
				require.NoError(t, shard.Store().ShutdownBucket(ctx, helpers.ObjectsBucketLSM))
			}
			findCtx, cancel := context.WithCancel(ctx)
			defer cancel()
			if tc.cancelCtx {
				cancel()
			}

			uuids, err := shard.FindUUIDs(findCtx, nameEquals(class, "match"), tc.limit)
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
				if tc.wantErrText != "" {
					require.ErrorContains(t, err, tc.wantErrText)
				}
				require.Nil(t, uuids)
				return
			}
			require.NoError(t, err)
			require.Len(t, uuids, tc.wantCount)
			requireSkipWarn(t, hook, tc.wantWarnSkipped)

			want := make([]strfmt.UUID, 0, len(matching))
			for _, obj := range matching {
				if !skipped[obj.ID()] {
					want = append(want, obj.ID())
				}
			}
			require.Subset(t, want, uuids)
			if tc.limit == 0 {
				require.ElementsMatch(t, want, uuids)
			}
		})
	}
}

// requireSkipWarn asserts the one summary line the unreadable-id branch writes,
// or its absence when nothing was skipped.
func requireSkipWarn(t *testing.T, hook *test.Hook, wantSkipped int) {
	t.Helper()
	var warns []string
	for _, entry := range hook.AllEntries() {
		if entry.Level == logrus.WarnLevel && entry.Data["op"] == "shard.find_uuids" {
			warns = append(warns, entry.Message)
		}
	}
	if wantSkipped == 0 {
		require.Empty(t, warns)
		return
	}
	require.Len(t, warns, 1, "the skips must be summarised once per call, not once per doc id")
	require.Contains(t, warns[0], fmt.Sprintf("skipped %d doc ids", wantSkipped))
	require.Contains(t, warns[0], "doc id ")
}

// stubDocIDBucket visits the first failAfter keys and then fails, as a bucket
// whose segment read breaks part way through a call does. It records the
// concurrency budget it was called with, which is what the real bucket sizes
// its lookup fan-out from; zero means the caller passed none.
type stubDocIDBucket struct {
	objects   map[uint64][]byte
	failAfter int
	err       error
	gotBudget int
}

func (b *stubDocIDBucket) GetBySecondaryBatch(ctx context.Context, _ int, keys [][]byte,
	visit func(i int, value []byte) error,
) error {
	b.gotBudget = concurrency.BudgetFromCtx(ctx, 0)
	for i := range keys {
		if i >= b.failAfter {
			return b.err
		}
		if object, ok := b.objects[binary.LittleEndian.Uint64(keys[i])]; ok {
			if err := visit(i, object); err != nil {
				return err
			}
		}
	}
	return nil
}

type sliceDocIDIterator struct {
	ids []uint64
	pos int
}

func (it *sliceDocIDIterator) Next() (uint64, bool) {
	if it.pos >= len(it.ids) {
		return 0, false
	}
	it.pos++
	return it.ids[it.pos-1], true
}

func (it *sliceDocIDIterator) Len() int { return len(it.ids) }

// TestResolveUUIDsFailsOnReadError pins that a read failing partway through a
// bucket call fails the whole resolve, rather than returning the ids read so far.
func TestResolveUUIDsFailsOnReadError(t *testing.T) {
	readErr := errors.New("segment read failed")
	object := func(id strfmt.UUID) []byte {
		obj := &storobj.Object{
			MarshallerVersion: 1,
			Object:            models.Object{ID: id, Class: "ReadErrorTest"},
		}
		data, err := obj.MarshalBinary()
		require.NoError(t, err)
		return data
	}

	cases := []struct {
		name      string
		failAfter int
	}{
		{name: "the read fails before any object is visited"},
		{name: "the read fails after some objects are visited", failAfter: 2},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			logger, _ := test.NewNullLogger()
			bucket := &stubDocIDBucket{objects: map[uint64][]byte{}, failAfter: tc.failAfter, err: readErr}
			ids := make([]uint64, 5)
			for i := range ids {
				ids[i] = uint64(i)
				bucket.objects[uint64(i)] = object(strfmt.UUID(uuid.NewString()))
			}

			uuids, err := resolveUUIDs(context.Background(), logger, bucket, &sliceDocIDIterator{ids: ids})
			require.ErrorIs(t, err, readErr)
			require.ErrorContains(t, err, "resolve uuids")
			require.Nil(t, uuids)
		})
	}
}

// TestResolveUUIDsSetsConcurrencyBudget pins the budget the resolve puts on the
// context it hands the bucket. The bucket sizes its lookup fan-out from that
// budget, so without it the resolve runs at whatever the bucket's own constant
// is rather than at the per-query share of this host's cores.
func TestResolveUUIDsSetsConcurrencyBudget(t *testing.T) {
	object := &storobj.Object{
		MarshallerVersion: 1,
		Object:            models.Object{ID: strfmt.UUID(uuid.NewString()), Class: "BudgetTest"},
	}
	data, err := object.MarshalBinary()
	require.NoError(t, err)

	cases := []struct {
		name         string
		callerBudget int
		wantBudget   int
	}{
		{name: "the caller's context carries no budget", wantBudget: concurrency.TimesGOMAXPROCS(2)},
		{name: "the caller's context carries one", callerBudget: 3, wantBudget: 3},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			logger, _ := test.NewNullLogger()
			const docID = 7
			bucket := &stubDocIDBucket{objects: map[uint64][]byte{docID: data}, failAfter: 1}

			ctx := context.Background()
			if tc.callerBudget > 0 {
				ctx = concurrency.CtxWithBudget(ctx, tc.callerBudget)
			}

			uuids, err := resolveUUIDs(ctx, logger, bucket, &sliceDocIDIterator{ids: []uint64{docID}})
			require.NoError(t, err)
			require.Equal(t, []strfmt.UUID{object.ID()}, uuids)
			require.Equal(t, tc.wantBudget, bucket.gotBudget)
		})
	}
}

// secondaryLookupSlowLogKeys is the pair the reduce gate owns: both are
// reduced when something reads them, and both are dropped when nothing does.
var secondaryLookupSlowLogKeys = []string{
	lsmkv.SlowLogKeyGetBySecondary,
	lsmkv.SlowLogKeyGetBySecondaryWithView,
}

func TestShardReducesSecondaryLookupSlowLogEntries(t *testing.T) {
	ctx := context.Background()
	class := textNameClass("SlowLogReduceTest")

	// Both search entry points carry the same gate, over both keys.
	searches := map[string]func(t *testing.T, shard ShardLike, ctx context.Context, profile bool){
		"ObjectSearch": func(t *testing.T, shard ShardLike, ctx context.Context, profile bool) {
			found, _, err := shard.ObjectSearch(ctx, 10, nameEquals(class, "match"), nil, nil, nil,
				additional.Properties{QueryProfile: profile}, nil)
			require.NoError(t, err)
			require.Len(t, found, 5, "the search must resolve the objects, or nothing records an entry")
		},
		"ObjectVectorSearch": func(t *testing.T, shard ShardLike, ctx context.Context, profile bool) {
			found, _, err := shard.ObjectVectorSearch(ctx, []models.Vector{[]float32{1, 0, 0}}, []string{""},
				0, 10, nameEquals(class, "match"), nil, nil,
				additional.Properties{QueryProfile: profile}, nil, nil)
			require.NoError(t, err)
			require.Len(t, found, 5, "the search must resolve the objects, or nothing records an entry")
		},
	}

	cases := []struct {
		name            string
		reporterEnabled bool
		queryProfile    bool
		wantReduced     bool
	}{
		{name: "the reporter is on", reporterEnabled: true, wantReduced: true},
		{name: "a query profile was asked for", queryProfile: true, wantReduced: true},
		{name: "nothing reads the entries", wantReduced: false},
	}
	for searchName, search := range searches {
		for _, tc := range cases {
			t.Run(searchName+"/"+tc.name, func(t *testing.T) {
				// The default config, not a bare one: a zero MaxConnections builds a
				// graph with no edges, and the vector search then returns only its
				// entry point however many objects match.
				shard, _ := testShardWithSettings(t, ctx, class, hnsw.NewDefaultUserConfig(), false, false, false)
				for i := range 5 {
					// Distinct vectors: identical ones collapse into one result.
					putNamedVectorObject(t, ctx, shard, class, "match", []float32{1, float32(i) / 10, 0})
				}
				// Only a segment read writes a per-lookup slow-log entry.
				require.NoError(t, shard.Store().Bucket(helpers.ObjectsBucketLSM).FlushAndSwitch())

				logger, _ := test.NewNullLogger()
				shard.(*Shard).slowQueryReporter = helpers.NewSlowQueryReporter(
					runtime.NewDynamicValue(tc.reporterEnabled),
					runtime.NewDynamicValue(helpers.DefaultSlowLogThreshold), logger,
				)

				slowLogCtx := helpers.InitSlowQueryDetails(ctx)
				// Seed both keys. ObjectSearch resolves through the batch, which
				// writes the with-view key from the request context, but
				// ObjectVectorSearch resolves through the per-key lookup, which
				// storage_object.go hands a context.TODO, so nothing it records
				// reaches this context. Seeded entries are what makes the gate
				// observable on both keys for both searches.
				for _, key := range secondaryLookupSlowLogKeys {
					helpers.AnnotateSlowQueryLogAppend(slowLogCtx, key,
						lsmkv.BucketSlowLogEntry{Total: time.Millisecond})
				}
				helpers.AnnotateSlowQueryLog(slowLogCtx, "unrelated", "kept")

				search(t, shard, slowLogCtx, tc.queryProfile)

				details := helpers.ExtractSlowQueryDetails(slowLogCtx)
				require.Equal(t, "kept", details["unrelated"], "the gate must only touch its own keys")
				for _, key := range secondaryLookupSlowLogKeys {
					if tc.wantReduced {
						require.IsTypef(t, lsmkv.BucketSlowLogEntryStats{}, details[key],
							"%s: per-lookup entries must be reduced to one stats summary", key)
						continue
					}
					require.NotContainsf(t, details, key,
						"%s: a query nothing reads must drop the entries rather than pay for the reduce", key)
				}
			})
		}
	}
}

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
		// earlierCalls is how many calls run on the same shard before the one checked.
		earlierCalls int
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
		{
			name: "an unreadable row seen by repeated calls is warned about once", matching: 10,
			unreadable: 1, earlierCalls: 2, wantCount: 9, wantWarnSkipped: 1,
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

			for range tc.earlierCalls {
				_, err := shard.FindUUIDs(findCtx, nameEquals(class, "match"), tc.limit)
				require.NoError(t, err)
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
			bucket := &stubDocIDBucket{objects: map[uint64][]byte{}, failAfter: tc.failAfter, err: readErr}
			ids := make([]uint64, 5)
			for i := range ids {
				ids[i] = uint64(i)
				bucket.objects[uint64(i)] = object(strfmt.UUID(uuid.NewString()))
			}

			uuids, _, err := resolveUUIDs(context.Background(), bucket, &sliceDocIDIterator{ids: ids}, len(ids), func(uint64) {})
			require.ErrorIs(t, err, readErr)
			require.ErrorContains(t, err, "resolve uuids")
			require.Nil(t, uuids)
		})
	}
}

// rowStateObject returns the stored bytes of an object with the given id.
func rowStateObject(t *testing.T, id strfmt.UUID) []byte {
	t.Helper()
	obj := &storobj.Object{
		MarshallerVersion: 1,
		Object:            models.Object{ID: id, Class: "RowStateTest"},
	}
	data, err := obj.MarshalBinary()
	require.NoError(t, err)
	return data
}

// TestResolveUUIDsTellsRowStatesApart covers the four states one doc id can land in. The
// caller has to tell them apart: a missing row means the object is gone and its doc id may
// be pruned, a row with no readable id is skipped and reported but never pruned, and a read
// error means the store could not answer and the resolve fails.
func TestResolveUUIDsTellsRowStatesApart(t *testing.T) {
	const docID = 42
	id := strfmt.UUID(uuid.NewString())
	row := rowStateObject(t, id)
	readErr := errors.New("segment read failed")

	cases := []struct {
		name string
		// row is what the bucket holds for the doc id; nil means the row is gone.
		row            []byte
		readErr        error
		wantUUIDs      []strfmt.UUID
		wantMissing    []uint64
		wantUnreadable int
	}{
		{name: "present", row: row, wantUUIDs: []strfmt.UUID{id}},
		{name: "absent", wantMissing: []uint64{docID}},
		{name: "corrupt row", row: row[:8], wantUnreadable: 1},
		{name: "read error", row: row, readErr: readErr},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			bucket := &stubDocIDBucket{objects: map[uint64][]byte{}, failAfter: 1}
			if tc.row != nil {
				bucket.objects[docID] = tc.row
			}
			if tc.readErr != nil {
				bucket.failAfter, bucket.err = 0, tc.readErr
			}

			var missing []uint64
			uuids, unreadable, err := resolveUUIDs(context.Background(), bucket,
				&sliceDocIDIterator{ids: []uint64{docID}}, 1, func(d uint64) { missing = append(missing, d) })
			if tc.readErr != nil {
				require.ErrorIs(t, err, tc.readErr, "the error a caller sees wraps the one the store returned")
				require.Nil(t, uuids)
				return
			}
			require.NoError(t, err)
			require.ElementsMatch(t, tc.wantUUIDs, uuids)
			require.Equal(t, tc.wantMissing, missing)
			require.Equal(t, tc.wantUnreadable, unreadable.count)
			if tc.wantUnreadable > 0 {
				require.ErrorContains(t, unreadable.first, fmt.Sprintf("doc id %d", docID))
			}
		})
	}
}

// TestResolveUUIDsLimitCountsUUIDsProduced pins that the limit counts UUIDs returned, not
// doc ids read: a missing row and an unreadable one in front of the live objects cost a
// read each and no slot, so the resolve walks past both to fill the limit.
func TestResolveUUIDsLimitCountsUUIDsProduced(t *testing.T) {
	first, second := strfmt.UUID(uuid.NewString()), strfmt.UUID(uuid.NewString())
	bucket := &stubDocIDBucket{failAfter: 100, objects: map[uint64][]byte{
		2: []byte("garbage"),
		3: rowStateObject(t, first),
		4: rowStateObject(t, second),
	}}

	var missing []uint64
	uuids, unreadable, err := resolveUUIDs(context.Background(), bucket,
		&sliceDocIDIterator{ids: []uint64{1, 2, 3, 4}}, 1, func(d uint64) { missing = append(missing, d) })
	require.NoError(t, err)
	require.Equal(t, []strfmt.UUID{first}, uuids)
	require.Equal(t, []uint64{1}, missing)
	require.Equal(t, 1, unreadable.count)
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
			const docID = 7
			bucket := &stubDocIDBucket{objects: map[uint64][]byte{docID: data}, failAfter: 1}

			ctx := context.Background()
			if tc.callerBudget > 0 {
				ctx = concurrency.CtxWithBudget(ctx, tc.callerBudget)
			}

			uuids, _, err := resolveUUIDs(ctx, bucket, &sliceDocIDIterator{ids: []uint64{docID}}, 1, func(uint64) {})
			require.NoError(t, err)
			require.Equal(t, []strfmt.UUID{object.ID()}, uuids)
			require.Equal(t, tc.wantBudget, bucket.gotBudget)
		})
	}
}

// TestShardObjectSearchSummarisesBatchedLookups pins the slow-query record a
// filtered search leaves when it resolves its doc ids through the batch: one
// summary under the with-view key that counts every lookup a segment served,
// across more than one chunk.
func TestShardObjectSearchSummarisesBatchedLookups(t *testing.T) {
	ctx := context.Background()
	class := textNameClass("SlowLogSummaryTest")
	const matches = 70

	shard, _ := testShardWithSettings(t, ctx, class, hnsw.UserConfig{Distance: common.DefaultDistanceMetric}, false, false, false)
	for range matches {
		putNamedObject(t, ctx, shard, class, "match")
	}
	putNamedObject(t, ctx, shard, class, "other")
	// Only a segment read records a lookup.
	require.NoError(t, shard.Store().Bucket(helpers.ObjectsBucketLSM).FlushAndSwitch())

	slowLogCtx := helpers.InitSlowQueryDetails(ctx)
	found, _, err := shard.ObjectSearch(slowLogCtx, matches, nameEquals(class, "match"), nil, nil, nil,
		additional.Properties{}, nil)
	require.NoError(t, err)
	require.Len(t, found, matches)

	stats, ok := helpers.ExtractSlowQueryDetails(slowLogCtx)[lsmkv.SlowLogKeyGetBySecondaryWithView].(lsmkv.BucketSlowLogEntryStats)
	require.True(t, ok, "the lookups must be summarised, not listed one per doc id")
	require.Equal(t, matches, stats.Count, "every batched lookup must reach the summary")
}

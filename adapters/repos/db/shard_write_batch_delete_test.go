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
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/additional"
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
		name                string
		matching            int
		other               int
		deletedMatching     int
		unreadableFirstLast bool
		limit               int
		cancelCtx           bool
		objectsBucketGone   bool
		wantCount           int
		wantErr             error
		wantErrText         string
	}
	cases := []testCase{
		{name: "no match", other: 5, wantCount: 0},
		{name: "one match", matching: 1, other: 5, wantCount: 1},
		{name: "many matches", matching: 25, other: 5, wantCount: 25},
		{name: "more matches than one bucket call", matching: 1203, wantCount: 1203},
		{name: "limit below matches", matching: 25, limit: 10, wantCount: 10},
		{name: "objects deleted after the allow list was built are skipped", matching: 25, deletedMatching: 5, wantCount: 20},
		{name: "every match deleted after the allow list was built", matching: 5, deletedMatching: 5, wantCount: 0},
		{name: "objects without a readable id are skipped", matching: 40, other: 5, unreadableFirstLast: true, wantCount: 38},
		{name: "cancelled context", matching: 5, cancelCtx: true, wantErr: context.Canceled},
		{name: "a missing objects bucket fails the call instead of skipping", matching: 5, objectsBucketGone: true, wantErr: lsmkv.ErrBucketNotFound, wantErrText: "objects bucket"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			shard, _ := testShardWithSettings(t, ctx, class, hnsw.UserConfig{Distance: common.DefaultDistanceMetric}, false, false)

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

			// Bytes shorter than the id header carry no readable id; putting it on
			// the first and last match lets two workers hit the skip branch concurrently.
			if tc.unreadableFirstLast {
				for _, obj := range []*storobj.Object{matching[0], matching[len(matching)-1]} {
					require.NoError(t, bucket.Put([]byte(obj.ID()), []byte("garbage"), lsmkv.WithSecondaryKey(0, docIDKeyOf(t, ctx, shard, obj.ID()))))
					skipped[obj.ID()] = true
				}
			}

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

func TestShardReducesSecondaryLookupSlowLogEntries(t *testing.T) {
	ctx := context.Background()
	class := textNameClass("SlowLogReduceTest")
	shard, _ := testShardWithSettings(t, ctx, class, hnsw.UserConfig{Distance: common.DefaultDistanceMetric}, false, false)
	for range 5 {
		putNamedObject(t, ctx, shard, class, "match")
	}
	// Only a segment read writes a per-lookup slow-log entry; a memtable hit does not.
	require.NoError(t, shard.Store().Bucket(helpers.ObjectsBucketLSM).FlushAndSwitch())

	cases := []struct {
		name string
		run  func(ctx context.Context) error
	}{
		{name: "ObjectSearch", run: func(ctx context.Context) error {
			_, _, err := shard.ObjectSearch(ctx, 10, nameEquals(class, "match"), nil, nil, nil, additional.Properties{}, nil)
			return err
		}},
		{name: "FindUUIDs", run: func(ctx context.Context) error {
			_, err := shard.FindUUIDs(ctx, nameEquals(class, "match"), 0)
			return err
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := helpers.InitSlowQueryDetails(ctx)
			require.NoError(t, tc.run(ctx))
			entry := helpers.ExtractSlowQueryDetails(ctx)["lsm_get_by_secondary_with_view"]
			require.IsType(t, lsmkv.BucketSlowLogEntryStats{}, entry, "per-lookup entries must be reduced to one stats summary")
		})
	}
}

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
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/usecases/objects"
)

func TestNameShardDeleteResults(t *testing.T) {
	const id1, id2, id3 = strfmt.UUID("id-1"), strfmt.UUID("id-2"), strfmt.UUID("id-3")
	errShard, errObject := errors.New("shard failed"), errors.New("object failed")

	tests := []struct {
		name  string
		uuids []strfmt.UUID
		objs  objects.BatchSimpleObjects
		want  objects.BatchSimpleObjects
	}{
		{
			name:  "named results are kept",
			uuids: []strfmt.UUID{id1, id2},
			objs:  objects.BatchSimpleObjects{{UUID: id1}, {UUID: id2, Err: errObject}},
			want:  objects.BatchSimpleObjects{{UUID: id1}, {UUID: id2, Err: errObject}},
		},
		{
			name:  "one unnamed failure for one object",
			uuids: []strfmt.UUID{id1},
			objs:  objects.BatchSimpleObjects{{Err: errShard}},
			want:  objects.BatchSimpleObjects{{UUID: id1, Err: errShard}},
		},
		{
			name:  "one unnamed failure for the whole shard",
			uuids: []strfmt.UUID{id1, id2, id3},
			objs:  objects.BatchSimpleObjects{{Err: errShard}},
			want: objects.BatchSimpleObjects{
				{UUID: id1, Err: errShard}, {UUID: id2, Err: errShard}, {UUID: id3, Err: errShard},
			},
		},
		{
			name:  "one unnamed failure per object",
			uuids: []strfmt.UUID{id1, id2},
			objs:  objects.BatchSimpleObjects{{Err: errShard}, {Err: errShard}},
			want:  objects.BatchSimpleObjects{{UUID: id1, Err: errShard}, {UUID: id2, Err: errShard}},
		},
		{
			name:  "unnamed results among named ones take their position",
			uuids: []strfmt.UUID{id1, id2, id3},
			objs:  objects.BatchSimpleObjects{{UUID: id1}, {Err: errObject}, {}},
			want:  objects.BatchSimpleObjects{{UUID: id1}, {UUID: id2, Err: errObject}, {UUID: id3}},
		},
		{
			name:  "no result at all",
			uuids: []strfmt.UUID{id1, id2},
			want: objects.BatchSimpleObjects{
				{UUID: id1, Err: errNoBatchDeleteResult}, {UUID: id2, Err: errNoBatchDeleteResult},
			},
		},
		{
			name:  "fewer results than objects",
			uuids: []strfmt.UUID{id1, id2, id3},
			objs:  objects.BatchSimpleObjects{{UUID: id3}, {UUID: id1, Err: errObject}},
			want: objects.BatchSimpleObjects{
				{UUID: id1, Err: errObject}, {UUID: id2, Err: errNoBatchDeleteResult}, {UUID: id3},
			},
		},
		{
			name:  "more results than objects",
			uuids: []strfmt.UUID{id1},
			objs:  objects.BatchSimpleObjects{{Err: errShard}, {Err: errShard}},
			want:  objects.BatchSimpleObjects{{UUID: id1, Err: errShard}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, nameShardDeleteResults(tt.uuids, tt.objs))
		})
	}
}

// TestBatchDeleteObjectsNamesEveryFailure asserts that a delete that fails for a
// whole shard still reports one failed entry per requested uuid. Callers render
// the ids, and a verbose gRPC reply cannot be built from an entry without one.
func TestBatchDeleteObjectsNamesEveryFailure(t *testing.T) {
	className := "BatchDeleteNamesFailures"
	const blockedShard = "blocked-shard"

	tests := []struct {
		name string
		// shardName overrides the shard the delete targets
		shardName string
		setup     func(t *testing.T, idx *Index, shard *Shard)
		ctx       func(t *testing.T) context.Context
	}{
		{
			name: "read-only shard",
			setup: func(t *testing.T, idx *Index, shard *Shard) {
				require.NoError(t, shard.SetStatusReadonly("test"))
			},
		},
		{
			name: "unreachable remote shard",
			setup: func(t *testing.T, idx *Index, shard *Shard) {
				forwardToRemote(t, idx, className, shard.name)
			},
		},
		{
			name:      "shard that cannot be used",
			shardName: blockedShard,
			setup: func(t *testing.T, idx *Index, shard *Shard) {
				idx.backupProtectedShards.Store(blockedShard, struct{}{})
			},
		},
		{
			name: "cancelled context",
			ctx: func(t *testing.T) context.Context {
				ctx, cancel := context.WithCancel(t.Context())
				cancel()
				return ctx
			},
		},
	}

	for _, tt := range tests {
		for _, count := range []int{1, 3} {
			t.Run(fmt.Sprintf("%s, %d objects", tt.name, count), func(t *testing.T) {
				idx, shard := refCountTestIndex(t, className)
				if tt.setup != nil {
					tt.setup(t, idx, shard)
				}
				ctx := t.Context()
				if tt.ctx != nil {
					ctx = tt.ctx(t)
				}
				shardName := shard.name
				if tt.shardName != "" {
					shardName = tt.shardName
				}

				ids := make([]strfmt.UUID, count)
				for i := range ids {
					ids[i] = strfmt.UUID(uuid.NewString())
				}

				objs, err := idx.batchDeleteObjects(ctx, map[string][]strfmt.UUID{shardName: ids},
					time.Now(), false, nil, 0, "")
				require.NoError(t, err)

				got := make([]strfmt.UUID, 0, len(objs))
				for _, obj := range objs {
					require.Error(t, obj.Err)
					got = append(got, obj.UUID)
				}
				require.ElementsMatch(t, ids, got)
			})
		}
	}
}

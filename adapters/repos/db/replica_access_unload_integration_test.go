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

package db

import (
	"context"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/usecases/objects"
	"github.com/weaviate/weaviate/usecases/replica"
)

func TestOnlyDataChangesAndUserReadsCountAsReplicaAccess(t *testing.T) {
	tests := []struct {
		name       string
		overwrite  func(v objects.VObject, cur int64) objects.VObject
		op         func(t *testing.T, index *Index, shard string)
		wantAccess bool
	}{
		{name: "overwrite applying a newer version", wantAccess: true, overwrite: func(v objects.VObject, cur int64) objects.VObject {
			v.LatestObject.LastUpdateTimeUnix = cur + 1000
			v.LastUpdateTimeUnixMilli, v.StaleUpdateTime = cur+1000, cur
			return v
		}},
		{name: "overwrite applying a delete", wantAccess: true, overwrite: func(v objects.VObject, cur int64) objects.VObject {
			return objects.VObject{ID: v.LatestObject.ID, Deleted: true, LastUpdateTimeUnixMilli: cur + 1000, StaleUpdateTime: cur}
		}},
		{name: "overwrite already up to date", overwrite: func(v objects.VObject, cur int64) objects.VObject {
			v.LastUpdateTimeUnixMilli, v.StaleUpdateTime = cur, cur-1
			return v
		}},
		{name: "overwrite in conflict", overwrite: func(v objects.VObject, cur int64) objects.VObject {
			v.LatestObject.LastUpdateTimeUnix = cur + 5
			v.LastUpdateTimeUnixMilli, v.StaleUpdateTime = cur+5, cur-1
			return v
		}},
		{name: "compare digests", op: func(t *testing.T, index *Index, shard string) {
			_, err := index.CompareDigests(context.Background(), shard, []types.RepairDigest{})
			require.NoError(t, err)
		}},
		{name: "digest objects in range", op: func(t *testing.T, index *Index, shard string) {
			_, err := index.DigestObjectsInRange(context.Background(), shard, batchDeleteObjectID(0), batchDeleteObjectID(9), 10)
			require.NoError(t, err)
		}},
		{name: "user read fetch objects", wantAccess: true, op: func(t *testing.T, index *Index, shard string) {
			_, err := index.FetchObjects(context.Background(), shard, []strfmt.UUID{batchDeleteObjectID(0)})
			require.NoError(t, err)
		}},
		{name: "user read digest objects", wantAccess: true, op: func(t *testing.T, index *Index, shard string) {
			_, err := index.DigestObjects(context.Background(), shard, []strfmt.UUID{batchDeleteObjectID(0)})
			require.NoError(t, err)
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			index, lazy := setupTTLUnloaderRepo(t, singleShardState(), batchDeleteTTLClass(), "", 0)
			if tt.overwrite != nil {
				rep, err := index.FetchObject(context.Background(), lazy.Name(), batchDeleteObjectID(0))
				require.NoError(t, err)
				require.NotNil(t, rep.Object)
				v := tt.overwrite(objects.VObject{LatestObject: &rep.Object.Object}, rep.LastUpdateTimeUnixMilli)
				lazy.lastAccess.Store(0)
				resp, err := index.OverwriteObjects(context.Background(), lazy.Name(), []*objects.VObject{&v})
				require.NoError(t, err)
				if tt.name == "overwrite in conflict" {
					require.Len(t, resp, 1)
				} else {
					require.Empty(t, resp)
				}
			} else {
				lazy.lastAccess.Store(0)
				tt.op(t, index, lazy.Name())
			}

			require.True(t, lazy.isLoaded())
			require.Equal(t, tt.wantAccess, lazy.LastAccess() != 0)
		})
	}
}

func TestBackgroundDeletesOnAReceiverCountAsAccessOnlyWhenTheyDelete(t *testing.T) {
	bg := withBackgroundShardAccess(context.Background())
	ids := []strfmt.UUID{batchDeleteObjectID(0)}
	absent := []strfmt.UUID{batchDeleteObjectID(999)}
	prepareIDs := func(ctx context.Context, index *Index, shard string, ids []strfmt.UUID, dryRun bool) replica.SimpleResponse {
		return index.ReplicateDeletions(ctx, shard, "r1", ids, time.Now(), dryRun, 0)
	}
	prepare := func(ctx context.Context, index *Index, shard string) replica.SimpleResponse {
		return prepareIDs(ctx, index, shard, ids, false)
	}
	commit := func(t *testing.T, index *Index, shard string) replica.DeleteBatchResponse {
		resp, ok := index.CommitReplication(bg, shard, "r1").(replica.DeleteBatchResponse)
		require.True(t, ok)
		require.NoError(t, resp.FirstError())
		return resp
	}
	tests := []struct {
		name       string
		readOnly   bool
		op         func(t *testing.T, index *Index, shard string)
		wantAccess bool
	}{
		{name: "user replicated prepare", wantAccess: true, op: func(t *testing.T, index *Index, shard string) {
			require.Empty(t, prepare(context.Background(), index, shard).Errors)
			index.AbortReplication(context.Background(), shard, "r1")
		}},
		{name: "background replicated prepare", op: func(t *testing.T, index *Index, shard string) {
			require.Empty(t, prepare(bg, index, shard).Errors)
			index.AbortReplication(bg, shard, "r1")
		}},
		{name: "background replicated delete committed", wantAccess: true, op: func(t *testing.T, index *Index, shard string) {
			require.Empty(t, prepare(bg, index, shard).Errors)
			commit(t, index, shard)
		}},
		{name: "background replicated dry run committed", op: func(t *testing.T, index *Index, shard string) {
			require.Empty(t, prepareIDs(bg, index, shard, ids, true).Errors)
			commit(t, index, shard)
		}},
		{name: "background replicated delete of an absent object committed", op: func(t *testing.T, index *Index, shard string) {
			require.Empty(t, prepareIDs(bg, index, shard, absent, false).Errors)
			commit(t, index, shard)
		}},
		{name: "background replicated prepare on a readonly shard", readOnly: true, op: func(t *testing.T, index *Index, shard string) {
			require.NotEmpty(t, prepare(bg, index, shard).Errors)
		}},
		{name: "background remote batch delete", wantAccess: true, op: func(t *testing.T, index *Index, shard string) {
			res := index.IncomingDeleteObjectBatch(bg, shard, ids, time.Now(), false, 0)
			require.Len(t, res, 1)
			require.NoError(t, res[0].Err)
		}},
		{name: "background remote batch delete of an absent object", op: func(t *testing.T, index *Index, shard string) {
			res := index.IncomingDeleteObjectBatch(bg, shard, absent, time.Now(), false, 0)
			require.Len(t, res, 1)
			require.NoError(t, res[0].Err)
		}},
		{name: "background remote batch delete dry run", op: func(t *testing.T, index *Index, shard string) {
			res := index.IncomingDeleteObjectBatch(bg, shard, ids, time.Now(), true, 0)
			require.Len(t, res, 1)
			require.NoError(t, res[0].Err)
		}},
		{name: "background remote batch delete on a readonly shard", readOnly: true, op: func(t *testing.T, index *Index, shard string) {
			res := index.IncomingDeleteObjectBatch(bg, shard, ids, time.Now(), false, 0)
			require.Len(t, res, 1)
			require.Error(t, res[0].Err)
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			index, lazy := setupTTLUnloaderRepo(t, singleShardState(), batchDeleteTTLClass(), "", 2)
			if tt.readOnly {
				require.NoError(t, lazy.SetStatusReadonly("test"))
			}
			lazy.lastAccess.Store(0)

			tt.op(t, index, lazy.Name())

			require.True(t, lazy.isLoaded())
			require.Equal(t, tt.wantAccess, lazy.LastAccess() != 0)
		})
	}
}

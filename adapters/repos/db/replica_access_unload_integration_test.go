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

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/usecases/objects"
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

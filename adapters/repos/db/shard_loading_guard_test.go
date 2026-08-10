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
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storagestate"
	"github.com/weaviate/weaviate/entities/storobj"
	esync "github.com/weaviate/weaviate/entities/sync"
)

const (
	loadingGuardClass = "C1"
	loadingGuardShard = "S1"
)

func newLoadingGuardIndex(shard ShardLike, replicationFactor int64) *Index {
	idx := &Index{
		Config: IndexConfig{
			ClassName:         schema.ClassName(loadingGuardClass),
			ReplicationFactor: replicationFactor,
		},
		shardCreateLocks: esync.NewKeyRWLocker(),
	}
	idx.shards.Store(loadingGuardShard, shard)
	return idx
}

func TestEnsureShardLocallyReady(t *testing.T) {
	tests := []struct {
		name              string
		status            storagestate.Status
		replicationFactor int64
		wantRejected      bool
	}{
		{
			name:              "loading shard is rejected so the read retries on a replica",
			status:            storagestate.StatusLoading,
			replicationFactor: 3,
			wantRejected:      true,
		},
		{
			name:              "loading shard is used as is when there is no replica to retry on",
			status:            storagestate.StatusLoading,
			replicationFactor: 1,
		},
		{
			name:              "ready shard is used, replicated",
			status:            storagestate.StatusReady,
			replicationFactor: 3,
		},
		{
			name:              "ready shard is used, unreplicated",
			status:            storagestate.StatusReady,
			replicationFactor: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			shard := NewMockShardLike(t)
			shard.EXPECT().GetStatus().Return(tt.status).Once()
			shard.EXPECT().Name().Return(loadingGuardShard).Maybe()

			err := newLoadingGuardIndex(shard, tt.replicationFactor).
				ensureShardLocallyReady(shard)

			if !tt.wantRejected {
				require.NoError(t, err)
				return
			}

			var unprocessable enterrors.ErrUnprocessable
			require.True(t, errors.As(err, &unprocessable),
				"a loading shard must be reported as unprocessable, got %T: %v", err, err)
			require.ErrorContains(t, err, "shard is not ready")
		})
	}
}

func TestFetchObjectsAppliesReadinessCheck(t *testing.T) {
	ids := []strfmt.UUID{
		"00000000-0000-0000-0000-00000000000a",
		"00000000-0000-0000-0000-00000000000b",
	}

	object := func(id strfmt.UUID, updateTime int64) *storobj.Object {
		return &storobj.Object{
			MarshallerVersion: 1,
			Object: models.Object{
				ID:                 id,
				Class:              loadingGuardClass,
				LastUpdateTimeUnix: updateTime,
			},
		}
	}

	t.Run("loading shard is refused while replicated", func(t *testing.T) {
		shard := NewMockShardLike(t)
		shard.EXPECT().GetStatus().Return(storagestate.StatusLoading).Once()
		shard.EXPECT().Name().Return(loadingGuardShard).Once()
		shard.EXPECT().preventShutdown().Return(func() {}, nil).Once()
		idx := newLoadingGuardIndex(shard, 3)

		_, err := idx.FetchObjects(context.Background(), loadingGuardShard, ids)

		var unprocessable enterrors.ErrUnprocessable
		require.True(t, errors.As(err, &unprocessable), "got %T: %v", err, err)
		shard.AssertNotCalled(t, "MultiObjectByID")
	})

	t.Run("loading shard is served when unreplicated", func(t *testing.T) {
		shard := NewMockShardLike(t)
		shard.EXPECT().GetStatus().Return(storagestate.StatusLoading).Once()
		shard.EXPECT().preventShutdown().Return(func() {}, nil).Once()
		shard.EXPECT().MultiObjectByID(mock.Anything, wrapIDsInMulti(ids)).
			Return([]*storobj.Object{object(ids[0], 100), object(ids[1], 200)}, nil).Once()
		idx := newLoadingGuardIndex(shard, 1)

		got, err := idx.FetchObjects(context.Background(), loadingGuardShard, ids)

		require.NoError(t, err)
		require.Len(t, got, len(ids))
		require.Equal(t, ids[0], got[0].ID)
		require.Equal(t, ids[1], got[1].ID)
		require.Equal(t, int64(100), got[0].LastUpdateTimeUnixMilli)
		require.Equal(t, int64(200), got[1].LastUpdateTimeUnixMilli)
		require.NotNil(t, got[0].Object)
		require.NotNil(t, got[1].Object)
	})

	t.Run("singular FetchObject agrees", func(t *testing.T) {
		shard := NewMockShardLike(t)
		shard.EXPECT().GetStatus().Return(storagestate.StatusLoading).Once()
		shard.EXPECT().preventShutdown().Return(func() {}, nil).Once()
		shard.EXPECT().ObjectByID(mock.Anything, ids[0], mock.Anything, mock.Anything).
			Return(object(ids[0], 100), nil).Once()
		idx := newLoadingGuardIndex(shard, 1)

		got, err := idx.FetchObject(context.Background(), loadingGuardShard, ids[0])

		require.NoError(t, err)
		require.Equal(t, ids[0], got.ID)
		require.Equal(t, int64(100), got.LastUpdateTimeUnixMilli)
		require.NotNil(t, got.Object)
	})
}

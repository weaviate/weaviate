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
	"testing"

	"github.com/stretchr/testify/require"

	localschema "github.com/weaviate/weaviate/cluster/schema/local"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/storagestate"
)

func requireStillRecovering(t *testing.T, idx *Index) {
	t.Helper()
	rec, ok := idx.shards.Load(recoveringSearchShard).(*RecoveringShard)
	require.True(t, ok)
	require.True(t, rec.IsRecovering(), "a status read loaded the recovering shard")
}

// A recovering shard reads RECOVERING, never an error, and stays unloaded.
func TestShardStatusOfARecoveringShardReadsRecovering(t *testing.T) {
	for _, tc := range []struct {
		name string
		read func(ctx context.Context, idx *Index) (string, error)
	}{
		{"local", func(ctx context.Context, idx *Index) (string, error) {
			return idx.localShardStatus(ctx, recoveringSearchShard)
		}},
		{"incoming", func(ctx context.Context, idx *Index) (string, error) {
			return idx.IncomingGetShardStatus(ctx, recoveringSearchShard)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			idx := recoveringSearchIndex(t, recoveringSearchLocal)

			got, err := tc.read(t.Context(), idx)

			require.NoError(t, err)
			require.Equal(t, storagestate.StatusRecovering.String(), got)
			requireStillRecovering(t, idx)
		})
	}
}

// A recovering replica's queue size stays a 422, so the caller asks another replica.
func TestShardQueueSizeOfARecoveringShardIsUnprocessable(t *testing.T) {
	idx := recoveringSearchIndex(t, recoveringSearchLocal)

	_, err := idx.IncomingGetShardQueueSize(t.Context(), recoveringSearchShard)

	require.ErrorAs(t, err, &enterrors.ErrUnprocessable{})
	require.True(t, enterrors.IsShardRecovering(err))
	requireStillRecovering(t, idx)
}

// The shards listing shows a recovering replica, local or remote, as RECOVERING and the others READY.
func TestShardsStorageStatusShowsARecoveringReplicaAsRecovering(t *testing.T) {
	for _, recovering := range recoveringSearchNodes {
		t.Run("recovering_"+recovering, func(t *testing.T) {
			idx := recoveringSearchIndex(t, recovering)
			reader := localschema.NewMockSchemaReader(t)
			reader.EXPECT().Shards(recoveringSearchClass).Return([]string{recoveringSearchShard}, nil)
			reader.EXPECT().ShardReplicas(recoveringSearchClass, recoveringSearchShard).Return(recoveringSearchNodes, nil)
			idx.schemaReader = reader
			wantPerNode := map[string]string{}
			for _, n := range recoveringSearchNodes {
				wantPerNode[n] = storagestate.StatusReady.String()
			}
			wantPerNode[recovering] = storagestate.StatusRecovering.String()

			perNode, legacy, err := idx.getShardsStorageStatus(t.Context(), "")

			require.NoError(t, err)
			require.Equal(t, map[string]map[string]string{recoveringSearchShard: wantPerNode}, perNode)
			require.Equal(t, map[string]string{recoveringSearchShard: wantPerNode[recoveringSearchLocal]}, legacy)
		})
	}
}

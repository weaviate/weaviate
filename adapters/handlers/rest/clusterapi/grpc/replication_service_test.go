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

package grpc

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	pb "github.com/weaviate/weaviate/adapters/handlers/rest/clusterapi/grpc/generated/protocol"
	"github.com/weaviate/weaviate/adapters/handlers/rest/clusterapi/shared"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/usecases/replica"
	"github.com/weaviate/weaviate/usecases/replica/hashtree"
	replicaTypes "github.com/weaviate/weaviate/usecases/replica/types"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestReplicationErrorToGRPC(t *testing.T) {
	t.Run("nil returns nil", func(t *testing.T) {
		assert.Nil(t, replicationErrorToGRPC(nil))
	})

	t.Run("ErrUnprocessable returns FailedPrecondition", func(t *testing.T) {
		err := enterrors.NewErrUnprocessable(errors.New("shard loading"))
		grpcErr := replicationErrorToGRPC(err)
		require.NotNil(t, grpcErr)
		st, ok := status.FromError(grpcErr)
		require.True(t, ok)
		assert.Equal(t, codes.FailedPrecondition, st.Code())
		assert.Contains(t, st.Message(), "shard loading")
	})

	t.Run("other error returns Internal", func(t *testing.T) {
		err := errors.New("something went wrong")
		grpcErr := replicationErrorToGRPC(err)
		require.NotNil(t, grpcErr)
		st, ok := status.FromError(grpcErr)
		require.True(t, ok)
		assert.Equal(t, codes.Internal, st.Code())
		assert.Contains(t, st.Message(), "something went wrong")
	})
}

func TestLocalIndexNotReady(t *testing.T) {
	t.Run("empty response", func(t *testing.T) {
		assert.False(t, shared.LocalIndexNotReady(replica.SimpleResponse{}))
	})

	t.Run("non-StatusNotReady error", func(t *testing.T) {
		resp := replica.SimpleResponse{
			Errors: []replica.Error{
				{Code: replica.StatusConflict, Msg: "conflict"},
			},
		}
		assert.False(t, shared.LocalIndexNotReady(resp))
	})

	t.Run("StatusNotReady returns true", func(t *testing.T) {
		resp := replica.SimpleResponse{
			Errors: []replica.Error{
				{Code: replica.StatusNotReady, Msg: "index loading"},
			},
		}
		assert.True(t, shared.LocalIndexNotReady(resp))
	})
}

func TestCompareHashTreeRootsRoundTrip(t *testing.T) {
	const index = "MyClass"

	t.Run("converts proto to map and returns diverging shards", func(t *testing.T) {
		mockReplicator := replicaTypes.NewMockReplicator(t)
		svc := &ReplicationService{server: mockReplicator}

		mockReplicator.EXPECT().
			CompareHashTreeRoots(mock.Anything, index, map[string]hashtree.Digest{
				"shard-a": {1, 2},
				"shard-b": {3, 4},
			}).
			Return([]string{"shard-b"}, nil)

		resp, err := svc.CompareHashTreeRoots(context.Background(), &pb.CompareHashTreeRootsRequest{
			Index: index,
			ShardRootDigests: []*pb.ShardRootDigest{
				{Shard: "shard-a", RootHashHigh: 1, RootHashLow: 2},
				{Shard: "shard-b", RootHashHigh: 3, RootHashLow: 4},
			},
		})
		require.NoError(t, err)
		assert.Equal(t, []string{"shard-b"}, resp.GetDivergingShards())
	})

	t.Run("propagates replicator error as gRPC error", func(t *testing.T) {
		mockReplicator := replicaTypes.NewMockReplicator(t)
		svc := &ReplicationService{server: mockReplicator}

		mockReplicator.EXPECT().
			CompareHashTreeRoots(mock.Anything, index, mock.Anything).
			Return(nil, errors.New("boom"))

		_, err := svc.CompareHashTreeRoots(context.Background(), &pb.CompareHashTreeRootsRequest{
			Index:            index,
			ShardRootDigests: []*pb.ShardRootDigest{{Shard: "shard-a", RootHashHigh: 1, RootHashLow: 2}},
		})
		require.Error(t, err)
		st, ok := status.FromError(err)
		require.True(t, ok)
		assert.Equal(t, codes.Internal, st.Code())
	})
}

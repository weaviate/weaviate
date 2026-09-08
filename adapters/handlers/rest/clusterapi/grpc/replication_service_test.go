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
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	pb "github.com/weaviate/weaviate/adapters/handlers/rest/clusterapi/grpc/generated/protocol"
	"github.com/weaviate/weaviate/adapters/handlers/rest/clusterapi/shared"
	routerTypes "github.com/weaviate/weaviate/cluster/router/types"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/usecases/replica"
	replicaerrors "github.com/weaviate/weaviate/usecases/replica/errors"
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

	t.Run("wrapped height mismatch returns FailedPrecondition", func(t *testing.T) {
		err := fmt.Errorf("%w: hashtree level 9 exceeds height 8 on shard %q", replica.ErrAsyncReplicationNotActive, "t")
		grpcErr := replicationErrorToGRPC(err)
		require.NotNil(t, grpcErr)
		st, ok := status.FromError(grpcErr)
		require.True(t, ok)
		assert.Equal(t, codes.FailedPrecondition, st.Code())
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
			Errors: []replicaerrors.Error{
				{Code: replicaerrors.StatusConflict, Msg: "conflict"},
			},
		}
		assert.False(t, shared.LocalIndexNotReady(resp))
	})

	t.Run("StatusNotReady returns true", func(t *testing.T) {
		resp := replica.SimpleResponse{
			Errors: []replicaerrors.Error{
				{Code: replicaerrors.StatusNotReady, Msg: "index loading"},
			},
		}
		assert.True(t, shared.LocalIndexNotReady(resp))
	})
}

func TestHashTreeLevelEncodingNegotiation(t *testing.T) {
	const index = "MyClass"
	digests := []hashtree.Digest{{1, 2}, {3, 4}, {^uint64(0), 5}}

	discriminant := hashtree.NewBitset(hashtree.LeavesCount(2))
	discriminant.Set(0)
	discData, err := discriminant.Marshal()
	require.NoError(t, err)

	t.Run("binary when requested", func(t *testing.T) {
		mockReplicator := replicaTypes.NewMockReplicator(t)
		svc := &ReplicationService{server: mockReplicator}

		mockReplicator.EXPECT().
			HashTreeLevel(mock.Anything, index, "S1", 2, mock.Anything).
			Return(digests, nil)

		resp, err := svc.HashTreeLevel(context.Background(), &pb.HashTreeLevelRequest{
			Index:          index,
			Shard:          "S1",
			Level:          2,
			Discriminant:   discData,
			AcceptEncoding: replica.DigestsEncodingBinary,
		})
		require.NoError(t, err)
		assert.Equal(t, replica.DigestsEncodingBinary, resp.GetEncoding())
		assert.Equal(t, hashtree.DigestsToBinary(digests), resp.GetDigestsData())

		decoded, err := hashtree.DigestsFromBinary(resp.GetDigestsData())
		require.NoError(t, err)
		assert.Equal(t, digests, decoded)
	})

	t.Run("JSON when accept_encoding unset (old client)", func(t *testing.T) {
		mockReplicator := replicaTypes.NewMockReplicator(t)
		svc := &ReplicationService{server: mockReplicator}

		mockReplicator.EXPECT().
			HashTreeLevel(mock.Anything, index, "S1", 2, mock.Anything).
			Return(digests, nil)

		resp, err := svc.HashTreeLevel(context.Background(), &pb.HashTreeLevelRequest{
			Index:        index,
			Shard:        "S1",
			Level:        2,
			Discriminant: discData,
		})
		require.NoError(t, err)
		assert.Equal(t, replica.DigestsEncodingJSON, resp.GetEncoding())

		var decoded []hashtree.Digest
		require.NoError(t, json.Unmarshal(resp.GetDigestsData(), &decoded))
		assert.Equal(t, digests, decoded)
	})

	t.Run("binary with empty result", func(t *testing.T) {
		mockReplicator := replicaTypes.NewMockReplicator(t)
		svc := &ReplicationService{server: mockReplicator}

		mockReplicator.EXPECT().
			HashTreeLevel(mock.Anything, index, "S1", 2, mock.Anything).
			Return(nil, nil)

		resp, err := svc.HashTreeLevel(context.Background(), &pb.HashTreeLevelRequest{
			Index:          index,
			Shard:          "S1",
			Level:          2,
			Discriminant:   discData,
			AcceptEncoding: replica.DigestsEncodingBinary,
		})
		require.NoError(t, err)
		assert.Equal(t, replica.DigestsEncodingBinary, resp.GetEncoding())
		assert.Empty(t, resp.GetDigestsData())
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

	t.Run("rejects a request exceeding the shard cap without calling the replicator", func(t *testing.T) {
		mockReplicator := replicaTypes.NewMockReplicator(t)
		svc := &ReplicationService{server: mockReplicator}

		shards := make([]*pb.ShardRootDigest, replica.CompareHashTreeRootsMaxShardsPerRequest+1)
		for i := range shards {
			shards[i] = &pb.ShardRootDigest{Shard: fmt.Sprintf("shard-%d", i)}
		}

		_, err := svc.CompareHashTreeRoots(context.Background(), &pb.CompareHashTreeRootsRequest{
			Index:            index,
			ShardRootDigests: shards,
		})
		require.Error(t, err)
		st, ok := status.FromError(err)
		require.True(t, ok)
		assert.Equal(t, codes.InvalidArgument, st.Code())
	})
}

func TestCompareHashTreeRootsMultiRoundTrip(t *testing.T) {
	t.Run("classifies per class and isolates per-class errors", func(t *testing.T) {
		mockReplicator := replicaTypes.NewMockReplicator(t)
		svc := &ReplicationService{server: mockReplicator}

		mockReplicator.EXPECT().
			CompareHashTreeRoots(mock.Anything, "ClassA", map[string]hashtree.Digest{"a1": {1, 2}, "a2": {3, 4}}).
			Return([]string{"a2"}, nil)
		mockReplicator.EXPECT().
			CompareHashTreeRoots(mock.Anything, "ClassB", map[string]hashtree.Digest{"b1": {5, 6}}).
			Return(nil, errors.New("index not loaded"))

		resp, err := svc.CompareHashTreeRootsMulti(context.Background(), &pb.CompareHashTreeRootsMultiRequest{
			Classes: []*pb.ClassShardRootDigests{
				{Index: "ClassA", ShardRootDigests: []*pb.ShardRootDigest{
					{Shard: "a1", RootHashHigh: 1, RootHashLow: 2},
					{Shard: "a2", RootHashHigh: 3, RootHashLow: 4},
				}},
				{Index: "ClassB", ShardRootDigests: []*pb.ShardRootDigest{
					{Shard: "b1", RootHashHigh: 5, RootHashLow: 6},
				}},
			},
		})
		require.NoError(t, err)
		require.Len(t, resp.GetClasses(), 2)
		byIndex := map[string]*pb.ClassDivergingShards{}
		for _, cls := range resp.GetClasses() {
			byIndex[cls.GetIndex()] = cls
		}
		assert.Equal(t, []string{"a2"}, byIndex["ClassA"].GetDivergingShards())
		assert.Empty(t, byIndex["ClassA"].GetError())
		assert.Contains(t, byIndex["ClassB"].GetError(), "index not loaded")
	})

	t.Run("rejects a request exceeding the total shard cap without calling the replicator", func(t *testing.T) {
		mockReplicator := replicaTypes.NewMockReplicator(t)
		svc := &ReplicationService{server: mockReplicator}

		perClass := replica.CompareHashTreeRootsMaxShardsPerRequest/2 + 1
		classes := make([]*pb.ClassShardRootDigests, 2)
		for c := range classes {
			shards := make([]*pb.ShardRootDigest, perClass)
			for i := range shards {
				shards[i] = &pb.ShardRootDigest{Shard: fmt.Sprintf("shard-%d", i)}
			}
			classes[c] = &pb.ClassShardRootDigests{Index: fmt.Sprintf("Class%d", c), ShardRootDigests: shards}
		}

		_, err := svc.CompareHashTreeRootsMulti(context.Background(), &pb.CompareHashTreeRootsMultiRequest{Classes: classes})
		require.Error(t, err)
		st, ok := status.FromError(err)
		require.True(t, ok)
		assert.Equal(t, codes.InvalidArgument, st.Code())
	})
}

func TestCompareDigestsEncoding(t *testing.T) {
	const index, shard = "MyClass", "myshard"
	source := []routerTypes.RepairDigest{
		{ID: uuid.MustParse("00000000-0000-0000-0000-000000000001"), UpdateTime: 1},
		{ID: uuid.MustParse("00000000-0000-0000-0000-000000000002"), UpdateTime: 2, Deleted: true},
	}
	stale := source[:1]

	tests := []struct {
		name         string
		req          *pb.CompareDigestsRequest
		expectSource []routerTypes.RepairDigest
		ret          []routerTypes.RepairDigest
		wantCode     codes.Code
		wantMsg      string
		wantEncoding uint32
		wantPacked   []byte
		wantProtoIDs []string
	}{
		{
			name: "packed request and packed reply when asked",
			req: &pb.CompareDigestsRequest{
				Index: index, Shard: shard,
				DigestsPacked:  replica.RepairDigestsToBinary(source),
				Encoding:       replica.RepairDigestsEncodingPacked,
				AcceptEncoding: replica.RepairDigestsEncodingPacked,
			},
			expectSource: source,
			ret:          stale,
			wantEncoding: replica.RepairDigestsEncodingPacked,
			wantPacked:   replica.RepairDigestsToBinary(stale),
		},
		{
			name: "proto request gets proto reply",
			req: &pb.CompareDigestsRequest{
				Index: index, Shard: shard,
				Digests: []*pb.RepairResponse{
					{Id: source[0].ID.String(), UpdateTime: 1},
					{Id: source[1].ID.String(), UpdateTime: 2, Deleted: true},
				},
			},
			expectSource: source,
			ret:          stale,
			wantEncoding: replica.RepairDigestsEncodingProto,
			wantProtoIDs: []string{source[0].ID.String()},
		},
		{
			name: "empty packed request",
			req: &pb.CompareDigestsRequest{
				Index: index, Shard: shard,
				Encoding:       replica.RepairDigestsEncodingPacked,
				AcceptEncoding: replica.RepairDigestsEncodingPacked,
			},
			expectSource: []routerTypes.RepairDigest{},
			wantEncoding: replica.RepairDigestsEncodingPacked,
			wantPacked:   []byte{},
		},
		{
			name: "truncated packed request rejected",
			req: &pb.CompareDigestsRequest{
				Index: index, Shard: shard,
				DigestsPacked: make([]byte, replica.CompareDigestsRecordLength-1),
				Encoding:      replica.RepairDigestsEncodingPacked,
			},
			wantCode: codes.InvalidArgument,
			wantMsg:  "not a multiple",
		},
		{
			name: "oversized packed request rejected",
			req: &pb.CompareDigestsRequest{
				Index: index, Shard: shard,
				DigestsPacked: make([]byte, replica.CompareDigestsMaxBodyBytes+replica.CompareDigestsRecordLength),
				Encoding:      replica.RepairDigestsEncodingPacked,
			},
			wantCode: codes.InvalidArgument,
			wantMsg:  "exceeds",
		},
		{
			name: "unknown encoding rejected",
			req: &pb.CompareDigestsRequest{
				Index: index, Shard: shard,
				DigestsPacked: replica.RepairDigestsToBinary(source),
				Encoding:      replica.RepairDigestsEncodingPacked + 1,
			},
			wantCode: codes.InvalidArgument,
			wantMsg:  "unsupported digests encoding",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockReplicator := replicaTypes.NewMockReplicator(t)
			svc := &ReplicationService{server: mockReplicator}
			if tt.expectSource != nil {
				mockReplicator.EXPECT().CompareDigests(mock.Anything, index, shard, tt.expectSource).Return(tt.ret, nil)
			}
			resp, err := svc.CompareDigests(context.Background(), tt.req)
			if tt.wantCode != codes.OK {
				st, ok := status.FromError(err)
				require.True(t, ok)
				assert.Equal(t, tt.wantCode, st.Code())
				assert.Contains(t, st.Message(), tt.wantMsg)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantEncoding, resp.GetEncoding())
			assert.Equal(t, tt.wantPacked, resp.GetDigestsPacked())
			var gotIDs []string
			for _, d := range resp.GetDigests() {
				gotIDs = append(gotIDs, d.GetId())
			}
			assert.Equal(t, tt.wantProtoIDs, gotIDs)
		})
	}
}

func TestDigestObjectsInRangeEncoding(t *testing.T) {
	const index, shard = "MyClass", "myshard"
	initial, final := strfmt.UUID("00000000-0000-0000-0000-000000000001"), strfmt.UUID("00000000-0000-0000-0000-0000000000ff")
	results := []routerTypes.RepairDigest{
		{ID: uuid.MustParse("00000000-0000-0000-0000-000000000005"), UpdateTime: 5},
	}

	tests := []struct {
		name           string
		acceptEncoding uint32
		wantEncoding   uint32
		wantPacked     []byte
		wantProtoIDs   []string
	}{
		{
			name:           "packed reply when asked",
			acceptEncoding: replica.RepairDigestsEncodingPacked,
			wantEncoding:   replica.RepairDigestsEncodingPacked,
			wantPacked:     replica.RepairDigestsToBinary(results),
		},
		{
			name:         "proto reply when accept_encoding unset",
			wantEncoding: replica.RepairDigestsEncodingProto,
			wantProtoIDs: []string{results[0].ID.String()},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockReplicator := replicaTypes.NewMockReplicator(t)
			svc := &ReplicationService{server: mockReplicator}
			mockReplicator.EXPECT().DigestObjectsInRange(mock.Anything, index, shard, initial, final, 10).
				Return(results, nil)
			resp, err := svc.DigestObjectsInRange(context.Background(), &pb.DigestObjectsInRangeRequest{
				Index: index, Shard: shard,
				InitialUuid: initial.String(), FinalUuid: final.String(), Limit: 10,
				AcceptEncoding: tt.acceptEncoding,
			})
			require.NoError(t, err)
			assert.Equal(t, tt.wantEncoding, resp.GetEncoding())
			assert.Equal(t, tt.wantPacked, resp.GetDigestsPacked())
			var gotIDs []string
			for _, d := range resp.GetDigests() {
				gotIDs = append(gotIDs, d.GetId())
			}
			assert.Equal(t, tt.wantProtoIDs, gotIDs)
		})
	}
}

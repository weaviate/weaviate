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

package clients

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pb "github.com/weaviate/weaviate/adapters/handlers/rest/clusterapi/grpc/generated/protocol"
	"github.com/weaviate/weaviate/usecases/replica"
	"github.com/weaviate/weaviate/usecases/replica/hashtree"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func rootsInput(classes, shardsPerClass int) map[string]map[string]hashtree.Digest {
	in := make(map[string]map[string]hashtree.Digest, classes)
	for c := 0; c < classes; c++ {
		roots := make(map[string]hashtree.Digest, shardsPerClass)
		for s := 0; s < shardsPerClass; s++ {
			roots[fmt.Sprintf("c%d_s%d", c, s)] = hashtree.Digest{uint64(c<<16 + s), uint64(s<<16 + c)}
		}
		in[fmt.Sprintf("C%d", c)] = roots
	}
	return in
}

func rootsMultiReqToMap(req *pb.CompareHashTreeRootsMultiRequest) map[string]map[string]hashtree.Digest {
	out := make(map[string]map[string]hashtree.Digest, len(req.GetClasses()))
	for _, cls := range req.GetClasses() {
		roots := make(map[string]hashtree.Digest, len(cls.GetShardRootDigests()))
		for _, d := range cls.GetShardRootDigests() {
			roots[d.GetShard()] = hashtree.Digest{d.GetRootHashHigh(), d.GetRootHashLow()}
		}
		out[cls.GetIndex()] = roots
	}
	return out
}

func assertReqMatches(t *testing.T, req *pb.CompareHashTreeRootsMultiRequest, want map[string]map[string]hashtree.Digest) {
	t.Helper()
	require.NotNil(t, req)
	require.Len(t, req.GetClasses(), len(want))
	for _, cls := range req.GetClasses() {
		require.Len(t, cls.GetShardRootDigests(), len(want[cls.GetIndex()]), cls.GetIndex())
	}
	assert.Equal(t, want, rootsMultiReqToMap(req))
}

func echoDivergingHandler(req *pb.CompareHashTreeRootsMultiRequest) (*pb.CompareHashTreeRootsMultiResponse, error) {
	resp := &pb.CompareHashTreeRootsMultiResponse{}
	for _, cls := range req.GetClasses() {
		shards := make([]string, 0, len(cls.GetShardRootDigests()))
		for _, d := range cls.GetShardRootDigests() {
			shards = append(shards, d.GetShard())
		}
		resp.Classes = append(resp.Classes, &pb.ClassDivergingShards{Index: cls.GetIndex(), DivergingShards: shards})
	}
	return resp, nil
}

func TestGRPCCompareHashTreeRootsMulti(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	fake := newFakeGRPCReplicationServer(t)
	client, cleanup := setupGRPCTestServer(t, fake)
	defer cleanup()
	session := client.NewCompareRootsSession()

	t.Run("translates per-class results", func(t *testing.T) {
		fake.compareRootsMultiHandler = func(req *pb.CompareHashTreeRootsMultiRequest) (*pb.CompareHashTreeRootsMultiResponse, error) {
			return &pb.CompareHashTreeRootsMultiResponse{Classes: []*pb.ClassDivergingShards{
				{Index: "C0", DivergingShards: []string{"c0_s1"}},
				{Index: "C1", Error: "index not loaded"},
				{Index: "C2"},
			}}, nil
		}
		defer func() { fake.compareRootsMultiHandler = nil }()

		in := rootsInput(3, 2)
		resp, err := session.CompareHashTreeRootsMulti(ctx, "passthrough:bufnet", in)
		require.NoError(t, err)
		assert.Equal(t, map[string]replica.CompareHashTreeRootsMultiClassResp{
			"C0": {DivergingShards: []string{"c0_s1"}},
			"C1": {Error: "index not loaded"},
			"C2": {},
		}, resp.Classes)
		assertReqMatches(t, fake.lastCompareRootsMultiReq.Load(), in)
	})

	t.Run("server error wraps and session stays usable", func(t *testing.T) {
		fake.compareRootsMultiErr = status.Error(codes.FailedPrecondition, "boom")
		_, err := session.CompareHashTreeRootsMulti(ctx, "passthrough:bufnet", rootsInput(2, 3))
		require.ErrorContains(t, err, "gRPC CompareHashTreeRootsMulti")
		fake.compareRootsMultiErr = nil

		in := rootsInput(1, 2)
		_, err = session.CompareHashTreeRootsMulti(ctx, "passthrough:bufnet", in)
		require.NoError(t, err)
		assertReqMatches(t, fake.lastCompareRootsMultiReq.Load(), in)
	})

	t.Run("unimplemented maps to sentinel and session stays usable", func(t *testing.T) {
		fake.compareRootsMultiErr = status.Error(codes.Unimplemented, "old peer")
		_, err := session.CompareHashTreeRootsMulti(ctx, "passthrough:bufnet", rootsInput(1, 1))
		require.ErrorIs(t, err, replica.ErrCompareHashTreeRootsUnsupported)
		fake.compareRootsMultiErr = nil

		in := rootsInput(2, 1)
		_, err = session.CompareHashTreeRootsMulti(ctx, "passthrough:bufnet", in)
		require.NoError(t, err)
		assertReqMatches(t, fake.lastCompareRootsMultiReq.Load(), in)
	})
}

func TestGRPCCompareRootsSessionReuse(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	fake := newFakeGRPCReplicationServer(t)
	client, cleanup := setupGRPCTestServer(t, fake)
	defer cleanup()
	session := client.NewCompareRootsSession()

	large := map[string]map[string]hashtree.Digest{
		"A": {"a1": {1, 2}, "a2": {3, 4}, "a3": {5, 6}, "a4": {7, 8}, "a5": {9, 10}},
		"B": {"b1": {11, 12}, "b2": {13, 14}, "b3": {15, 16}},
		"C": {"c1": {17, 18}},
	}
	_, err := session.CompareHashTreeRootsMulti(ctx, "passthrough:bufnet", large)
	require.NoError(t, err)
	assertReqMatches(t, fake.lastCompareRootsMultiReq.Load(), large)

	small := map[string]map[string]hashtree.Digest{"D": {"d1": {21, 22}}}
	_, err = session.CompareHashTreeRootsMulti(ctx, "passthrough:bufnet", small)
	require.NoError(t, err)
	assertReqMatches(t, fake.lastCompareRootsMultiReq.Load(), small)

	var remainingFailures atomic.Int32
	remainingFailures.Store(2)
	fake.compareRootsMultiHandler = func(*pb.CompareHashTreeRootsMultiRequest) (*pb.CompareHashTreeRootsMultiResponse, error) {
		if remainingFailures.Add(-1) >= 0 {
			return nil, status.Error(codes.Internal, "flaky")
		}
		return &pb.CompareHashTreeRootsMultiResponse{}, nil
	}
	callsBefore := fake.compareRootsMultiCalls.Load()
	retried := map[string]map[string]hashtree.Digest{"E": {"e1": {31, 32}, "e2": {33, 34}}}
	_, err = session.CompareHashTreeRootsMulti(ctx, "passthrough:bufnet", retried)
	require.NoError(t, err)
	assert.Equal(t, int32(3), fake.compareRootsMultiCalls.Load()-callsBefore)
	assertReqMatches(t, fake.lastCompareRootsMultiReq.Load(), retried)
}

func TestGRPCCompareRootsSessionFillReuse(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		steps []map[string]map[string]hashtree.Digest
	}{
		{"large then small", []map[string]map[string]hashtree.Digest{rootsInput(3, 4), rootsInput(1, 1)}},
		{"small then large", []map[string]map[string]hashtree.Digest{rootsInput(1, 1), rootsInput(4, 8)}},
		{"overlapping names changed digests", []map[string]map[string]hashtree.Digest{
			{"A": {"s1": {1, 1}, "s2": {2, 2}}},
			{"A": {"s1": {9, 9}}},
		}},
		{"zero classes", []map[string]map[string]hashtree.Digest{rootsInput(2, 2), {}}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := &grpcCompareRootsSession{}
			for _, in := range tc.steps {
				assertReqMatches(t, s.fill(in), in)
			}
		})
	}
}

func TestGRPCCompareRootsSessionConcurrent(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	fake := newFakeGRPCReplicationServer(t)
	client, cleanup := setupGRPCTestServer(t, fake)
	defer cleanup()
	fake.compareRootsMultiHandler = echoDivergingHandler

	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			session := client.NewCompareRootsSession()
			for i := 0; i < 25; i++ {
				class := fmt.Sprintf("G%d", g)
				in := map[string]map[string]hashtree.Digest{class: {}}
				for s := 0; s <= i%5; s++ {
					in[class][fmt.Sprintf("g%d_s%d", g, s)] = hashtree.Digest{uint64(g), uint64(s)}
				}
				resp, err := session.CompareHashTreeRootsMulti(ctx, "passthrough:bufnet", in)
				if !assert.NoError(t, err) {
					return
				}
				got := resp.Classes[class].DivergingShards
				if !assert.Len(t, got, len(in[class])) {
					return
				}
				for _, shard := range got {
					assert.Contains(t, in[class], shard)
				}
			}
		}(g)
	}
	wg.Wait()
}

func TestGRPCCompareRootsSessionZeroAlloc(t *testing.T) {
	s := &grpcCompareRootsSession{}
	large := rootsInput(16, 256)
	small := rootsInput(1, 1)
	s.fill(large)

	assert.Zero(t, testing.AllocsPerRun(100, func() {
		benchGRPCReqSink = s.fill(large)
	}))
	assert.Zero(t, testing.AllocsPerRun(100, func() {
		benchGRPCReqSink = s.fill(small)
	}))
}

func TestSwitchCompareRootsSessionDispatch(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	fake := newFakeGRPCReplicationServer(t)
	grpcClient, cleanup := setupGRPCTestServer(t, fake)
	defer cleanup()
	fake.compareRootsMultiHandler = echoDivergingHandler

	var restCalls atomic.Int32
	restServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		restCalls.Add(1)
		var req replica.CompareHashTreeRootsMultiReq
		require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
		resp := replica.CompareHashTreeRootsMultiResp{Classes: map[string]replica.CompareHashTreeRootsMultiClassResp{}}
		for cls := range req.Classes {
			resp.Classes[cls] = replica.CompareHashTreeRootsMultiClassResp{}
		}
		require.NoError(t, json.NewEncoder(w).Encode(resp))
	}))
	defer restServer.Close()
	restClient := newReplicationClient(t, restServer.Client())

	var useGRPC atomic.Bool
	sw := NewSwitchReplicationClient(grpcClient, restClient, useGRPC.Load)
	session := sw.NewCompareRootsSession()

	in := rootsInput(1, 2)
	useGRPC.Store(true)
	resp, err := session.CompareHashTreeRootsMulti(ctx, "passthrough:bufnet", in)
	require.NoError(t, err)
	assert.Len(t, resp.Classes["C0"].DivergingShards, 2)
	assert.Equal(t, int32(0), restCalls.Load())

	useGRPC.Store(false)
	resp, err = session.CompareHashTreeRootsMulti(ctx, restServer.URL[7:], in)
	require.NoError(t, err)
	assert.Empty(t, resp.Classes["C0"].DivergingShards)
	assert.Equal(t, int32(1), restCalls.Load())

	useGRPC.Store(true)
	grpcCallsBefore := fake.compareRootsMultiCalls.Load()
	_, err = session.CompareHashTreeRootsMulti(ctx, "passthrough:bufnet", in)
	require.NoError(t, err)
	assert.Equal(t, int32(1), fake.compareRootsMultiCalls.Load()-grpcCallsBefore)
	assert.Equal(t, int32(1), restCalls.Load())
}

var benchGRPCReqSink *pb.CompareHashTreeRootsMultiRequest

func perCallAssembleGRPC(classes map[string]map[string]hashtree.Digest) *pb.CompareHashTreeRootsMultiRequest {
	req := &pb.CompareHashTreeRootsMultiRequest{
		Classes: make([]*pb.ClassShardRootDigests, 0, len(classes)),
	}
	for class, roots := range classes {
		shards := make([]*pb.ShardRootDigest, 0, len(roots))
		for shard, root := range roots {
			shards = append(shards, &pb.ShardRootDigest{
				Shard:        shard,
				RootHashHigh: root[0],
				RootHashLow:  root[1],
			})
		}
		req.Classes = append(req.Classes, &pb.ClassShardRootDigests{Index: class, ShardRootDigests: shards})
	}
	return req
}

func BenchmarkCompareRootsMultiAssembly(b *testing.B) {
	shapes := []struct {
		name              string
		classes, perClass int
	}{
		{"1x128", 1, 128},
		{"16x256", 16, 256},
		{"100x1", 100, 1},
	}
	for _, shape := range shapes {
		in := rootsInput(shape.classes, shape.perClass)
		b.Run("session/"+shape.name, func(b *testing.B) {
			s := &grpcCompareRootsSession{}
			s.fill(in)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				benchGRPCReqSink = s.fill(in)
			}
		})
		b.Run("perCall/"+shape.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				benchGRPCReqSink = perCallAssembleGRPC(in)
			}
		})
	}
}

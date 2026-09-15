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

package replica_test

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/usecases/replica"
)

func checkpointShardNames(n int) []string {
	names := make([]string, n)
	for i := range names {
		names[i] = fmt.Sprintf("tenant-%05d", i)
	}
	return names
}

func newChunkingFinderClient(t *testing.T) (replica.FinderClient, *replica.MockRClient) {
	logger, _ := test.NewNullLogger()
	cl := replica.NewMockRClient(t)
	return replica.NewFinderClient(cl, logger), cl
}

func TestFinderClientAsyncCheckpointChunking(t *testing.T) {
	const (
		host  = "H1"
		index = "C1"
	)
	ctx := context.Background()
	cutoffMs := int64(12345)
	createdAt := time.Now().UTC()

	t.Run("CreateSingleChunkFastPath", func(t *testing.T) {
		fc, cl := newChunkingFinderClient(t)
		shards := checkpointShardNames(replica.AsyncCheckpointMaxShardsPerChunk)
		cl.EXPECT().CreateAsyncCheckpoint(mock.Anything, host, index, shards, cutoffMs, createdAt).Return(nil).Once()
		require.NoError(t, fc.CreateAsyncCheckpoint(ctx, host, index, shards, cutoffMs, createdAt))
	})

	t.Run("CreateChunksCoverEveryShardOnce", func(t *testing.T) {
		fc, cl := newChunkingFinderClient(t)
		shards := checkpointShardNames(2*replica.AsyncCheckpointMaxShardsPerChunk + 176)

		var mu sync.Mutex
		var calls [][]string
		cl.EXPECT().CreateAsyncCheckpoint(mock.Anything, host, index, mock.Anything, cutoffMs, createdAt).
			RunAndReturn(func(_ context.Context, _, _ string, chunk []string, _ int64, _ time.Time) error {
				mu.Lock()
				defer mu.Unlock()
				calls = append(calls, chunk)
				return nil
			})

		require.NoError(t, fc.CreateAsyncCheckpoint(ctx, host, index, shards, cutoffMs, createdAt))
		require.Len(t, calls, 3)
		var got []string
		for _, chunk := range calls {
			require.LessOrEqual(t, len(chunk), replica.AsyncCheckpointMaxShardsPerChunk)
			got = append(got, chunk...)
		}
		slices.Sort(got)
		require.Equal(t, shards, got)
	})

	t.Run("CreateFailedChunkFailsCall", func(t *testing.T) {
		fc, cl := newChunkingFinderClient(t)
		shards := checkpointShardNames(replica.AsyncCheckpointMaxShardsPerChunk + 1)
		cl.EXPECT().CreateAsyncCheckpoint(mock.Anything, host, index, mock.Anything, cutoffMs, createdAt).
			RunAndReturn(func(_ context.Context, _, _ string, chunk []string, _ int64, _ time.Time) error {
				if slices.Contains(chunk, shards[len(shards)-1]) {
					return fmt.Errorf("boom")
				}
				return nil
			})
		require.ErrorContains(t, fc.CreateAsyncCheckpoint(ctx, host, index, shards, cutoffMs, createdAt), "boom")
	})

	t.Run("DeleteChunksCoverEveryShardOnce", func(t *testing.T) {
		fc, cl := newChunkingFinderClient(t)
		shards := checkpointShardNames(replica.AsyncCheckpointMaxShardsPerChunk + 1)

		var mu sync.Mutex
		var calls [][]string
		cl.EXPECT().DeleteAsyncCheckpoint(mock.Anything, host, index, mock.Anything).
			RunAndReturn(func(_ context.Context, _, _ string, chunk []string) error {
				mu.Lock()
				defer mu.Unlock()
				calls = append(calls, chunk)
				return nil
			})

		require.NoError(t, fc.DeleteAsyncCheckpoint(ctx, host, index, shards))
		require.Len(t, calls, 2)
		var got []string
		for _, chunk := range calls {
			require.LessOrEqual(t, len(chunk), replica.AsyncCheckpointMaxShardsPerChunk)
			got = append(got, chunk...)
		}
		slices.Sort(got)
		require.Equal(t, shards, got)
	})

	t.Run("StatusMergesChunksWithoutClobbering", func(t *testing.T) {
		fc, cl := newChunkingFinderClient(t)
		shards := checkpointShardNames(2*replica.AsyncCheckpointMaxShardsPerChunk + 5)
		expected := make(map[string]replica.AsyncCheckpointShardStatus, len(shards))
		for i, s := range shards {
			expected[s] = replica.AsyncCheckpointShardStatus{CutoffMs: int64(i + 1), CreatedAt: createdAt}
		}

		cl.EXPECT().GetAsyncCheckpointStatus(mock.Anything, host, index, mock.Anything).
			RunAndReturn(func(_ context.Context, _, _ string, chunk []string) (map[string]replica.AsyncCheckpointShardStatus, error) {
				require.LessOrEqual(t, len(chunk), replica.AsyncCheckpointMaxShardsPerChunk)
				out := make(map[string]replica.AsyncCheckpointShardStatus, len(chunk))
				for _, s := range chunk {
					out[s] = expected[s]
				}
				return out, nil
			})

		got, err := fc.GetAsyncCheckpointStatus(ctx, host, index, shards)
		require.NoError(t, err)
		require.Equal(t, expected, got)
	})

	t.Run("StatusFailedChunkFailsCall", func(t *testing.T) {
		fc, cl := newChunkingFinderClient(t)
		shards := checkpointShardNames(replica.AsyncCheckpointMaxShardsPerChunk + 1)
		cl.EXPECT().GetAsyncCheckpointStatus(mock.Anything, host, index, mock.Anything).
			RunAndReturn(func(_ context.Context, _, _ string, chunk []string) (map[string]replica.AsyncCheckpointShardStatus, error) {
				if slices.Contains(chunk, shards[len(shards)-1]) {
					return nil, fmt.Errorf("boom")
				}
				return map[string]replica.AsyncCheckpointShardStatus{}, nil
			})
		got, err := fc.GetAsyncCheckpointStatus(ctx, host, index, shards)
		require.ErrorContains(t, err, "boom")
		require.Nil(t, got)
	})
}

// A broadcast over a class too large for one request must succeed via chunking; a mock refusing over-chunk calls pins the pre-chunking failure red.
func TestBroadcastCreateAsyncCheckpointChunksLargeClasses(t *testing.T) {
	const class = "C1"
	nodes := []string{"A", "B"}
	shards := checkpointShardNames(2*replica.AsyncCheckpointMaxShardsPerChunk + 176)

	f := newFakeFactory(t, class, shards[0], nodes, true)
	for _, s := range shards[1:] {
		f.AddShard(s, nodes)
	}
	finder := f.newFinder("A")

	cutoffMs := int64(12345)
	createdAt := time.Now().UTC()
	var mu sync.Mutex
	seen := map[string]int{}
	f.RClient.EXPECT().CreateAsyncCheckpoint(mock.Anything, "B", class, mock.Anything, cutoffMs, createdAt).
		RunAndReturn(func(_ context.Context, _, _ string, chunk []string, _ int64, _ time.Time) error {
			if len(chunk) > replica.AsyncCheckpointMaxShardsPerChunk {
				return fmt.Errorf("request too large: %d shards", len(chunk))
			}
			mu.Lock()
			defer mu.Unlock()
			for _, s := range chunk {
				seen[s]++
			}
			return nil
		})

	successes, failures := finder.BroadcastCreateAsyncCheckpoint(context.Background(), shards, cutoffMs, createdAt)
	require.Equal(t, 1, successes)
	require.Equal(t, 0, failures)
	require.Len(t, seen, len(shards))
	for s, n := range seen {
		require.Equalf(t, 1, n, "shard %s sent %d times", s, n)
	}
}

func TestAsyncCheckpointChunkSizingInvariants(t *testing.T) {
	require.LessOrEqual(t, replica.AsyncCheckpointMaxShardsPerChunk, replica.AsyncCheckpointMaxShardsPerRequest)

	worstCase := make([]string, replica.AsyncCheckpointMaxShardsPerChunk)
	for i := range worstCase {
		worstCase[i] = strings.Repeat("x", 64)
	}
	body, err := json.Marshal(struct {
		Shards      []string `json:"shards"`
		CutoffMs    int64    `json:"cutoff_ms"`
		CreatedAtMs int64    `json:"created_at_ms"`
	}{worstCase, math.MaxInt64, math.MaxInt64})
	require.NoError(t, err)
	require.LessOrEqual(t, len(body), replica.AsyncCheckpointMaxBodyBytes)
}

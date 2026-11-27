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
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/concurrency"
	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/searchparams"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/cluster"
	"github.com/weaviate/weaviate/usecases/sharding"
)

// TestIndexObjectSearchByShard_SplitsTheBudget pins that a search inside an
// admitted query, as a ref filter's is, searches no more shards at once than
// its grant allows and splits the grant between them, while a search without a
// grant searches every shard at once.
func TestIndexObjectSearchByShard_SplitsTheBudget(t *testing.T) {
	const className = "ObjectSearchBudget"
	shards := []string{"shard-a", "shard-b", "shard-c", "shard-d"}
	unbudgeted := min(len(shards), _NUMCPU*2+1)

	testCases := []struct {
		budget      int // 0 for none
		wantWorkers int
		wantBudget  int // per shard search; 0 for none
	}{
		{budget: 0, wantWorkers: unbudgeted},
		{budget: 1, wantWorkers: 1, wantBudget: 1},
		{budget: 2, wantWorkers: 2, wantBudget: 1},
		{budget: 3, wantWorkers: min(3, unbudgeted), wantBudget: 3 / min(3, unbudgeted)},
		{budget: 100, wantWorkers: unbudgeted, wantBudget: 100 / unbudgeted},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("budget %d", tc.budget), func(t *testing.T) {
			idx, _ := refCountTestIndex(t, className)
			plan := types.ReadRoutingPlan{}
			for _, shard := range shards {
				plan.ReplicaSet.Replicas = append(plan.ReplicaSet.Replicas,
					types.Replica{NodeName: "node2", ShardName: shard, HostAddr: "127.0.0.2"})
			}
			nodeResolver := cluster.NewMockNodeResolver(t)
			nodeResolver.EXPECT().NodeHostname(mock.Anything).Return("127.0.0.2", true).Maybe()
			client := &concurrentSearchClient{wait: tc.wantWorkers, all: make(chan struct{})}
			idx.remote = sharding.NewRemoteIndex(className, remoteShardsOnNode2{}, nil, nodeResolver, client)

			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			if tc.budget > 0 {
				ctx = concurrency.CtxWithBudget(ctx, tc.budget)
			}
			_, _, err := idx.objectSearchByShard(ctx, 10, nil, nil, nil, nil, additional.Properties{}, "", plan, nil)
			require.NoError(t, err)

			require.Equal(t, tc.wantWorkers, client.most)
			require.Len(t, client.budgets, len(shards))
			for _, b := range client.budgets {
				require.Equal(t, tc.wantBudget, b)
			}
		})
	}
}

// concurrentSearchClient holds the first wait searches until all of them have
// arrived, then records the most searches it ran at once and the budget of
// every search.
type concurrentSearchClient struct {
	FakeRemoteClient
	wait int
	all  chan struct{}

	mu      sync.Mutex
	arrived int
	running int
	most    int
	budgets []int
}

func (c *concurrentSearchClient) SearchShard(ctx context.Context, _, _, _ string,
	_ []models.Vector, _ []string, _ float32, _ int, _ *filters.LocalFilter,
	_ *searchparams.KeywordRanking, _ []filters.Sort, _ *filters.Cursor,
	_ *searchparams.GroupBy, _ additional.Properties, _ *dto.TargetCombination, _ []string, _ uint64,
) ([]*storobj.Object, []float32, []helpers.ShardQueryProfile, error) {
	c.mu.Lock()
	c.arrived++
	c.running++
	c.most = max(c.most, c.running)
	c.budgets = append(c.budgets, concurrency.BudgetFromCtx(ctx, 0))
	if c.arrived == c.wait {
		close(c.all)
	}
	c.mu.Unlock()

	select {
	case <-c.all:
	case <-ctx.Done():
		return nil, nil, nil, ctx.Err()
	}
	// lets a search started past the limit overlap the ones still running
	time.Sleep(20 * time.Millisecond)

	c.mu.Lock()
	c.running--
	c.mu.Unlock()
	return nil, nil, nil, nil
}

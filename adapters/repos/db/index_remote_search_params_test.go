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
	"sync"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/searchparams"
	"github.com/weaviate/weaviate/entities/storobj"
	esync "github.com/weaviate/weaviate/entities/sync"
	"github.com/weaviate/weaviate/usecases/cluster"
	"github.com/weaviate/weaviate/usecases/sharding/remote"
)

// capturingSearchClient records the vector index params every remote shard
// search carries, by host.
type capturingSearchClient struct {
	FakeRemoteClient
	mu     sync.Mutex
	params map[string]*searchparams.VectorIndexParams
}

func (c *capturingSearchClient) SearchShard(_ context.Context, host, _, _ string,
	_ []models.Vector, _ []string, _ float32, _ int, _ *filters.LocalFilter,
	_ *searchparams.KeywordRanking, _ []filters.Sort, _ *filters.Cursor,
	_ *searchparams.GroupBy, _ additional.Properties, _ *dto.TargetCombination,
	_ []string, params *searchparams.VectorIndexParams,
) ([]*storobj.Object, []float32, []helpers.ShardQueryProfile, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.params[host] = params
	return nil, nil, nil, nil
}

// The coordinator forwards the request's vector index params unresolved (the
// per-target map included) on both remote paths: the single SearchShard and,
// with ForceFullReplicasSearch, the SearchAllReplicas fan-out.
func TestRemoteShardSearchForwardsVectorIndexParams(t *testing.T) {
	const (
		className = "RemoteSearchParams"
		shardName = "S1"
	)
	nodes := shardReplicasOn{"node1", "node2", "node3"}
	params := &searchparams.VectorIndexParams{
		EF:        64,
		PerTarget: map[string]searchparams.VectorIndexParams{"title": {EF: 128}},
	}

	tests := []struct {
		name      string
		force     bool
		wantHosts []string
	}{
		{
			name:      "SearchShard reaches one replica",
			force:     false,
			wantHosts: nil, // any one of the three
		},
		{
			name:      "ForceFullReplicasSearch reaches every remote replica",
			force:     true,
			wantHosts: []string{"node2-host", "node3-host"}, // node1 is local
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, _ := test.NewNullLogger()
			idx := newTestIndex(t, logger, className, nil, nil)
			idx.shardCreateLocks = esync.NewKeyRWLocker()
			idx.Config.ReplicationFactor = int64(len(nodes))
			idx.Config.ForceFullReplicasSearch = tt.force

			nodeResolver := cluster.NewMockNodeResolver(t)
			for _, n := range nodes {
				nodeResolver.EXPECT().NodeHostname(n).Return(n+"-host", true).Maybe()
			}
			client := &capturingSearchClient{params: map[string]*searchparams.VectorIndexParams{}}
			idx.remote = remote.NewIndex(className, nodes, nodeResolver, client)

			_, _, err := idx.remoteShardSearch(context.Background(),
				[]models.Vector{[]float32{1, 2}, []float32{3, 4}}, []string{"title", "body"}, 0, 10,
				nil, nil, nil, additional.Properties{}, nil, nil, params, "", shardName)
			require.NoError(t, err)

			if tt.wantHosts == nil {
				require.Len(t, client.params, 1, "one replica searched")
			} else {
				hosts := make([]string, 0, len(client.params))
				for host := range client.params {
					hosts = append(hosts, host)
				}
				assert.ElementsMatch(t, tt.wantHosts, hosts)
			}
			for host, got := range client.params {
				assert.Same(t, params, got, "params sent to %s", host)
			}
		})
	}
}

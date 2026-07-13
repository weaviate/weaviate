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

package replica

import (
	"context"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/mock"

	"github.com/weaviate/weaviate/cluster/router/types"
	clusterMocks "github.com/weaviate/weaviate/usecases/cluster/mocks"
)

// TestAsyncReplicationResolutionIsLocalOnly pins that every async-replication node-resolution site builds its read routing plan with LocalOnly set, so resolution never queries the leader or implicitly activates a tenant.
func TestAsyncReplicationResolutionIsLocalOnly(t *testing.T) {
	const (
		class = "C"
		local = "A"
		shard = "s1"
	)

	mr := types.NewMockRouter(t)
	mr.EXPECT().BuildRoutingPlanOptions(shard, shard, types.ConsistencyLevelOne, "").
		Return(types.RoutingPlanBuildOptions{Shard: shard, Tenant: shard, ConsistencyLevel: types.ConsistencyLevelOne})
	mr.EXPECT().
		BuildReadRoutingPlan(mock.MatchedBy(func(o types.RoutingPlanBuildOptions) bool { return o.LocalOnly })).
		Return(types.ReadRoutingPlan{}, nil)

	logger, _ := test.NewNullLogger()
	f := &Finder{
		router:       mr,
		nodeResolver: clusterMocks.NewMockNodeSelector(local),
		nodeName:     local,
		finderStream: finderStream{repairer: repairer{class: class, logger: logger}, log: logger},
	}

	sites := map[string]func(){
		"targetHostAddrsForShard": func() { f.targetHostAddrsForShard(shard) },
		"remoteReplicaHosts":      func() { f.remoteReplicaHosts(shard) },
		"CollectShardDifferences": func() {
			f.CollectShardDifferences(context.Background(), shard, nil, time.Second, nil)
		},
	}
	for name, call := range sites {
		t.Run(name, func(t *testing.T) { call() })
	}
}

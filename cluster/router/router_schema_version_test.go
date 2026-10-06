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

package router_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	replicationTypes "github.com/weaviate/weaviate/cluster/replication/types"
	"github.com/weaviate/weaviate/cluster/router"
	"github.com/weaviate/weaviate/cluster/router/types"
	clusterSchema "github.com/weaviate/weaviate/cluster/schema"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/cluster/mocks"
	"github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
)

// TestBuildReadRoutingPlanStampsTheLocalSchemaVersion pins the version a read carries. It is
// the later of the class and sharding-state versions, because either can be the change that
// put the shard where the plan says it is.
func TestBuildReadRoutingPlanStampsTheLocalSchemaVersion(t *testing.T) {
	const collection = "TestClass"

	tests := []struct {
		name string
		info clusterSchema.ClassInfo
		want uint64
	}{
		{
			name: "the class version when it is the later change",
			info: clusterSchema.ClassInfo{ClassVersion: 120, ShardVersion: 90},
			want: 120,
		},
		{
			name: "the sharding-state version when a tenant moved last",
			info: clusterSchema.ClassInfo{ClassVersion: 90, ShardVersion: 120},
			want: 120,
		},
		{
			name: "zero for a class with no recorded version",
			info: clusterSchema.ClassInfo{},
			want: 0,
		},
	}

	for _, tt := range tests {
		for _, multiTenant := range []bool{false, true} {
			name := tt.name + "/single-tenant"
			if multiTenant {
				name = tt.name + "/multi-tenant"
			}
			t.Run(name, func(t *testing.T) {
				var (
					tenant       = ""
					shard        = "shard1"
					schemaState  = createShardingStateWithShards([]string{shard})
					nodeSelector = mocks.NewMockNodeSelector("node1", "node2")
					schemaGetter = schema.NewMockSchemaGetter(t)
					schemaReader = schema.NewMockSchemaReader(t)
					fsm          = replicationTypes.NewMockReplicationFSMReader(t)
				)
				if multiTenant {
					tenant = shard
					schemaGetter.EXPECT().OptimisticTenantStatus(mock.Anything, collection, tenant).
						Return(map[string]string{tenant: models.TenantActivityStatusHOT}, nil)
				}

				schemaReader.EXPECT().ClassInfo(collection).Return(tt.info)
				schemaReader.EXPECT().Shards(mock.Anything).Return(schemaState.AllPhysicalShards(), nil).Maybe()
				schemaReader.EXPECT().Read(mock.Anything, mock.Anything, mock.Anything).
					RunAndReturn(func(className string, _ bool, read func(*models.Class, *sharding.State) error) error {
						return read(&models.Class{Class: className}, schemaState)
					}).Maybe()
				schemaReader.EXPECT().ShardReplicas(collection, shard).Return([]string{"node1", "node2"}, nil)
				fsm.EXPECT().FilterOneShardReplicasRead(collection, shard, []string{"node1", "node2"}).
					Return([]string{"node1", "node2"})

				r := router.NewBuilder(collection, multiTenant, nodeSelector, schemaGetter, schemaReader, fsm).Build()

				plan, err := r.BuildReadRoutingPlan(types.RoutingPlanBuildOptions{
					Shard:            shard,
					Tenant:           tenant,
					ConsistencyLevel: types.ConsistencyLevelOne,
				})
				require.NoError(t, err)
				assert.Equal(t, tt.want, plan.SchemaVersion)
			})
		}
	}
}

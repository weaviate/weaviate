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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/sharding"
)

// TestNewShard_WiresBirthDesignation pins the production wiring of
// PreferredBirthLeader in NewShard: the designation handed to the raft store
// must derive from the replicated member order for single-tenant classes and
// from the class's tenant count for multi-tenant classes — the count read via
// SchemaReader.ClassInfo, whose Tenants field the harness mirrors from the
// sharding state exactly like production (len(Sharding.Physical)).
func TestNewShard_WiresBirthDesignation(t *testing.T) {
	members := []string{"node1", "node2", "node3"}

	tests := []struct {
		name  string
		class *models.Class
		// tenants in the sharding state; the shard under test is the one
		// named shardName.
		shardName     string
		wantPreferred string
	}{
		{
			name: "single-tenant designation is the first replica",
			class: &models.Class{
				Class:               "BirthPlacementST",
				InvertedIndexConfig: &models.InvertedIndexConfig{},
			},
			shardName:     "shard1",
			wantPreferred: "node1", // members[0]
		},
		{
			name: "multi-tenant designation strides by tenant count",
			class: &models.Class{
				Class:               "BirthPlacementMT",
				InvertedIndexConfig: &models.InvertedIndexConfig{},
				MultiTenancyConfig:  &models.MultiTenancyConfig{Enabled: true},
			},
			shardName:     "tenant1",
			wantPreferred: "node2", // count==1 (one tenant) -> members[1%3]
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			state := &sharding.State{
				PartitioningEnabled: tt.class.MultiTenancyConfig != nil && tt.class.MultiTenancyConfig.Enabled,
				Physical: map[string]sharding.Physical{
					tt.shardName: {
						Name:           tt.shardName,
						BelongsToNodes: members,
						Status:         models.TenantActivityStatusHOT,
					},
				},
			}
			state.SetLocalName("node1")

			_, reg, _ := newTestIndexWithShardRaft(t, tt.class, state)

			store := reg.GetStore(tt.class.Class, tt.shardName)
			require.NotNil(t, store, "raft store must exist after index creation")
			require.Equal(t, tt.wantPreferred, store.PreferredLeader())
		})
	}
}

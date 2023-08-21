//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package schema

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/usecases/sharding"
)

func TestShardOwner(t *testing.T) {
	cache := schemaCache{
		State: State{
			ShardingState: map[string]*sharding.State{},
		},
	}

	// class not found
	_, err := cache.ShardOwner("C", "S")
	assert.ErrorContains(t, err, "class not found")

	// shard not found
	ss := &sharding.State{Physical: make(map[string]sharding.Physical)}
	cache.State.ShardingState["C"] = ss
	_, err = cache.ShardOwner("C", "S")
	assert.ErrorContains(t, err, "shard not found")

	// empty node list
	ss.Physical["S"] = sharding.Physical{}
	_, err = cache.ShardOwner("C", "S")
	assert.ErrorContains(t, err, "empty node list")

	// node with an empty name
	nodes := []string{""}
	ss.Physical["S"] = sharding.Physical{BelongsToNodes: nodes}
	_, err = cache.ShardOwner("C", "S")
	assert.ErrorContains(t, err, "empty node")

	// one owner
	nodes[0] = "A"
	owner, err := cache.ShardOwner("C", "S")
	assert.Nil(t, err)
	assert.Equal(t, "A", owner)

	// many owner
	nodes = append(nodes, "B")
	ss.Physical["S"] = sharding.Physical{BelongsToNodes: nodes}
	got := map[string]struct{}{}
	for i := 0; i < 11; i++ {
		node, _ := cache.ShardOwner("C", "S")
		got[node] = struct{}{}
	}
	want := map[string]struct{}{"A": {}, "B": {}}
	assert.Equal(t, want, got)
}

func TestShardReplicas(t *testing.T) {
	cache := schemaCache{
		State: State{
			ShardingState: map[string]*sharding.State{},
		},
	}

	// class not found
	_, err := cache.ShardReplicas("C", "S")
	assert.ErrorContains(t, err, "class not found")

	// shard not found
	ss := &sharding.State{Physical: make(map[string]sharding.Physical)}
	cache.State.ShardingState["C"] = ss
	_, err = cache.ShardReplicas("C", "S")
	assert.ErrorContains(t, err, "shard not found")

	// node with an empty name
	nodes := []string{"A", "B"}
	ss.Physical["S"] = sharding.Physical{BelongsToNodes: nodes}
	res, err := cache.ShardReplicas("C", "S")
	assert.Nil(t, err)
	assert.Equal(t, nodes, res)
}

func TestTenantShard(t *testing.T) {
	cache := schemaCache{
		State: State{
			ShardingState: map[string]*sharding.State{},
		},
	}

	// class not found
	shard, status := cache.TenantShard("C", "S")
	assert.Empty(t, shard)
	assert.Empty(t, status)

	// partitioning is disabled
	ss := &sharding.State{
		Physical: map[string]sharding.Physical{
			"S": {},
		},
		PartitioningEnabled: false,
	}
	cache.State.ShardingState["C"] = ss
	shard, status = cache.TenantShard("C", "S")
	assert.Empty(t, shard)
	assert.Empty(t, status)

	// shard not found
	ss.PartitioningEnabled = true
	shard, status = cache.TenantShard("C", "U")
	assert.Empty(t, shard)
	assert.Empty(t, status)

	// success
	shard, status = cache.TenantShard("C", "S")
	assert.Equal(t, "S", shard)
	assert.NotEmpty(t, status)
}

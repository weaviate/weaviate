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
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
)

// ownershipReader implements only Read (the sole method ShardReplicaOwnershipActive
// uses); the embedded interface is nil, so any other call would panic — a guard that
// the function under test stays this narrow.
type ownershipReader struct {
	schemaUC.SchemaReader
	state *sharding.State
}

func (r ownershipReader) Read(_ string, _ bool, reader func(*models.Class, *sharding.State) error) error {
	return reader(nil, r.state)
}

func TestShardReplicaOwnershipActive_FiltersByTenantStatus(t *testing.T) {
	mtState := &sharding.State{
		PartitioningEnabled: true,
		Physical: map[string]sharding.Physical{
			"hot":    {Status: models.TenantActivityStatusHOT, BelongsToNodes: []string{"n1"}},
			"active": {Status: models.TenantActivityStatusACTIVE, BelongsToNodes: []string{"n2"}},
			"cold":   {Status: models.TenantActivityStatusCOLD, BelongsToNodes: []string{"n1"}},
			"frozen": {Status: "FROZEN", BelongsToNodes: []string{"n2"}},
		},
	}

	t.Run("multi-tenant returns only HOT/ACTIVE tenants", func(t *testing.T) {
		db := &DB{schemaReader: ownershipReader{state: mtState}}
		got, err := db.ShardReplicaOwnershipActive(context.Background(), "C")
		require.NoError(t, err)
		require.Equal(t, map[string][]string{"n1": {"hot"}, "n2": {"active"}}, got,
			"COLD/FROZEN tenants must be excluded")
	})

	t.Run("non-multi-tenant returns all shards regardless of status", func(t *testing.T) {
		nonMT := &sharding.State{
			PartitioningEnabled: false,
			Physical: map[string]sharding.Physical{
				"s1": {Status: "", BelongsToNodes: []string{"n1"}},
				"s2": {Status: "", BelongsToNodes: []string{"n1", "n2"}},
			},
		}
		db := &DB{schemaReader: ownershipReader{state: nonMT}}
		got, err := db.ShardReplicaOwnershipActive(context.Background(), "C")
		require.NoError(t, err)
		require.Equal(t, map[string][]string{"n1": {"s1", "s2"}, "n2": {"s2"}}, got)
	})
}

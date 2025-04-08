//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package schema

import (
	"context"
	"fmt"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/sharding"
)

func TestVersionedSchemaReaderShardReplicas(t *testing.T) {
	var (
		ctx = context.Background()
		sc  = NewSchema(t.Name(), nil, prometheus.NewPedanticRegistry())
		vsc = VersionedSchemaReader{
			schema:        sc,
			WaitForUpdate: func(ctx context.Context, version uint64) error { return nil },
		}
	)
	// class not found
	_, _, err := sc.ShardReplicas("C", "S")
	assert.ErrorIs(t, err, ErrClassNotFound)

	// shard not found
	ss := &sharding.State{Physical: make(map[string]sharding.Physical)}

	sc.addClass(&models.Class{Class: "C"}, ss, 1)

	_, err = vsc.ShardReplicas(ctx, "C", "S", 1)
	assert.ErrorIs(t, err, ErrShardNotFound)

	// two replicas found
	nodes := []string{"A", "B"}
	ss.Physical["S"] = sharding.Physical{BelongsToNodes: nodes}
	res, err := vsc.ShardReplicas(ctx, "C", "S", 1)
	assert.Nil(t, err)
	assert.Equal(t, nodes, res)
}

func TestVersionedSchemaReaderClass(t *testing.T) {
	var (
		ctx    = context.Background()
		retErr error
		f      = func(ctx context.Context, version uint64) error { return retErr }
		nodes  = []string{"N1", "N2"}
		s      = NewSchema(t.Name(), &MockShardReader{}, prometheus.NewPedanticRegistry())
		sc     = VersionedSchemaReader{s, f}
	)

	// class not found
	cls, err := sc.ReadOnlyClass(ctx, "C", 1)
	assert.Nil(t, cls)
	assert.Nil(t, err)
	ss, err := sc.CopyShardingState(ctx, "C", 1)
	assert.Nil(t, ss)
	assert.Nil(t, err)
	mt, err := sc.MultiTenancy(ctx, "C", 1)
	assert.Equal(t, mt, models.MultiTenancyConfig{})
	assert.Nil(t, err)

	info, err := sc.ClassInfo(ctx, "C", 1)
	assert.Equal(t, ClassInfo{}, info)
	assert.Nil(t, err)

	_, err = sc.ShardReplicas(ctx, "C", "S", 1)
	assert.ErrorIs(t, err, ErrClassNotFound)
	_, err = sc.ShardOwner(ctx, "C", "S", 1)
	assert.ErrorIs(t, err, ErrClassNotFound)
	err = sc.Read(ctx, "C", 1, func(c *models.Class, s *sharding.State) error { return nil })
	assert.ErrorIs(t, err, ErrClassNotFound)

	// Add Simple class
	cls1 := &models.Class{Class: "C"}
	ss1 := &sharding.State{Physical: map[string]sharding.Physical{
		"S1": {Status: "A"},
		"S2": {Status: "A", BelongsToNodes: nodes},
	}}

	assert.Nil(t, sc.schema.addClass(cls1, ss1, 1))
	info, err = sc.ClassInfo(ctx, "C", 1)
	assert.Equal(t, ClassInfo{
		ReplicationFactor: 1,
		ClassVersion:      1,
		ShardVersion:      1, Exists: true, Tenants: len(ss1.Physical),
	}, info)
	assert.Nil(t, err)

	cls, err = sc.ReadOnlyClass(ctx, "C", 1)
	assert.Equal(t, cls, cls1)
	assert.Nil(t, err)
	mt, err = sc.MultiTenancy(ctx, "D", 1)
	assert.Equal(t, models.MultiTenancyConfig{}, mt)
	assert.Nil(t, err)

	// Shards
	_, err = sc.ShardOwner(ctx, "C", "S1", 1)
	assert.ErrorContains(t, err, "node not found")
	_, err = sc.ShardOwner(ctx, "C", "Sx", 1)
	assert.ErrorIs(t, err, ErrShardNotFound)
	shards, _, err := sc.TenantsShards(ctx, 1, "C", "S2")
	assert.Empty(t, shards)
	assert.Nil(t, err)
	shard, err := sc.ShardFromUUID(ctx, "Cx", nil, 1)
	assert.Empty(t, shard)
	assert.Nil(t, err)

	// Add MT Class
	cls2 := &models.Class{Class: "D", MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true}}
	ss2 := &sharding.State{
		PartitioningEnabled: true,
		Physical:            map[string]sharding.Physical{"S1": {Status: "A", BelongsToNodes: nodes}},
	}
	sc.schema.addClass(cls2, ss2, 1)
	cls, err = sc.ReadOnlyClass(ctx, "D", 1)
	assert.Equal(t, cls, cls2, 1)
	assert.Nil(t, err)

	mt, err = sc.MultiTenancy(ctx, "D", 1)
	assert.Equal(t, models.MultiTenancyConfig{Enabled: true}, mt)
	assert.Nil(t, err)

	// ShardOwner
	owner, err := sc.ShardOwner(ctx, "D", "S1", 1)
	assert.Nil(t, err)
	assert.Contains(t, nodes, owner)

	// TenantShard
	shards, _, err = sc.TenantsShards(ctx, 1, "D", "S1")
	assert.Equal(t, shards, map[string]string{"S1": "A"})
	assert.Equal(t, shards["S1"], "A")
	assert.Nil(t, err)

	shards, _, err = sc.TenantsShards(ctx, 1, "D", "Sx")
	assert.Empty(t, shards)
	assert.Nil(t, err)

	reader := func(c *models.Class, s *sharding.State) error { return nil }
	assert.Nil(t, sc.Read(ctx, "C", 1, reader))
	retErr = fmt.Errorf("waiting error")
	assert.ErrorIs(t, sc.Read(ctx, "C", 1, reader), retErr)
	retErr = nil
}

func TestSchemaReaderShardReplicas(t *testing.T) {
	sc := NewSchema(t.Name(), nil, prometheus.NewPedanticRegistry())
	rsc := SchemaReader{sc, VersionedSchemaReader{}}
	// class not found
	_, _, err := sc.ShardReplicas("C", "S")
	assert.ErrorIs(t, err, ErrClassNotFound)

	// shard not found
	ss := &sharding.State{Physical: make(map[string]sharding.Physical)}

	sc.addClass(&models.Class{Class: "C"}, ss, 1)

	_, err = rsc.ShardReplicas("C", "S")
	assert.ErrorIs(t, err, ErrShardNotFound)

	// two replicas found
	nodes := []string{"A", "B"}
	ss.Physical["S"] = sharding.Physical{BelongsToNodes: nodes}
	res, err := rsc.ShardReplicas("C", "S")
	assert.Nil(t, err)
	assert.Equal(t, nodes, res)
}

func TestSchemaReaderClass(t *testing.T) {
	var (
		nodes = []string{"N1", "N2"}
		s     = NewSchema(t.Name(), &MockShardReader{}, prometheus.NewPedanticRegistry())
		sc    = SchemaReader{s, VersionedSchemaReader{}}
	)

	// class not found
	assert.Nil(t, sc.ReadOnlyClass("C"))
	cl := sc.ReadOnlyVersionedClass("C")
	assert.Nil(t, cl.Class)
	assert.Nil(t, sc.CopyShardingState("C"))
	assert.Equal(t, sc.ReadOnlySchema(), models.Schema{Classes: make([]*models.Class, 0)})
	assert.Equal(t, sc.MultiTenancy("C"), models.MultiTenancyConfig{})

	_, err := sc.ShardReplicas("C", "S")
	assert.ErrorIs(t, err, ErrClassNotFound)
	_, err = sc.ShardOwner("C", "S")
	assert.ErrorIs(t, err, ErrClassNotFound)
	err = sc.Read("C", func(c *models.Class, s *sharding.State) error { return nil })
	assert.ErrorIs(t, err, ErrClassNotFound)

	// Add Simple class
	cls1 := &models.Class{Class: "C"}
	ss1 := &sharding.State{Physical: map[string]sharding.Physical{
		"S1": {Status: "A"},
		"S2": {Status: "A", BelongsToNodes: nodes},
	}}

	sc.schema.addClass(cls1, ss1, 1)
	assert.Equal(t, sc.ReadOnlyClass("C"), cls1)
	versionedClass := sc.ReadOnlyVersionedClass("C")
	assert.Equal(t, versionedClass.Class, cls1)
	assert.Equal(t, sc.MultiTenancy("D"), models.MultiTenancyConfig{})
	assert.Nil(t, sc.Read("C", func(c *models.Class, s *sharding.State) error { return nil }))

	// Shards
	_, err = sc.ShardOwner("C", "S1")
	assert.ErrorContains(t, err, "node not found")
	_, err = sc.ShardOwner("C", "Sx")
	assert.ErrorIs(t, err, ErrShardNotFound)
	shard, _ := sc.TenantsShards("C", "S2")
	assert.Empty(t, shard)
	assert.Empty(t, sc.ShardFromUUID("Cx", nil))

	_, err = sc.GetShardsStatus("C", "")
	assert.Nil(t, err)

	// Add MT Class
	cls2 := &models.Class{Class: "D", MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true}}
	ss2 := &sharding.State{
		PartitioningEnabled: true,
		Physical:            map[string]sharding.Physical{"S1": {Status: "A", BelongsToNodes: nodes}},
	}
	sc.schema.addClass(cls2, ss2, 1)
	assert.Equal(t, sc.ReadOnlyClass("D"), cls2)
	versionedClass = sc.ReadOnlyVersionedClass("D")
	assert.Equal(t, versionedClass.Class, cls2)
	assert.Equal(t, sc.MultiTenancy("D"), models.MultiTenancyConfig{Enabled: true})

	assert.ElementsMatch(t, sc.ReadOnlySchema().Classes, []*models.Class{cls1, cls2})

	// ShardOwner
	owner, err := sc.ShardOwner("D", "S1")
	assert.Nil(t, err)
	assert.Contains(t, nodes, owner)

	// TenantShard
	shards, _ := sc.TenantsShards("D", "S1")
	assert.Equal(t, shards["S1"], "A")
	shards, _ = sc.TenantsShards("D", "Sx")
	assert.Empty(t, shards)
}

// TestPropertiesMigration ensures that our migration function sets proper default values
// The test verifies that we migrate top level properties and then at least one layer deep nested properties
func TestPropertiesMigration(t *testing.T) {
	class := &models.Class{
		Class: "C",
		Properties: []*models.Property{
			{
				NestedProperties: []*models.NestedProperty{
					{
						NestedProperties: []*models.NestedProperty{
							{},
						},
					},
				},
			},
		},
	}

	// Set the values to nil, which would be the case if we're upgrading a cluster with "old" classes in it
	class.Properties[0].IndexRangeFilters = nil
	class.Properties[0].NestedProperties[0].IndexRangeFilters = nil
	class.Properties[0].NestedProperties[0].NestedProperties[0].IndexRangeFilters = nil
	migratePropertiesIfNecessary(class)

	// Check
	require.NotNil(t, class.Properties[0].IndexRangeFilters)
	require.False(t, *(class.Properties[0].IndexRangeFilters))
	require.NotNil(t, class.Properties[0].NestedProperties[0].IndexRangeFilters)
	require.False(t, *(class.Properties[0].NestedProperties[0].IndexRangeFilters))
	require.NotNil(t, class.Properties[0].NestedProperties[0].NestedProperties[0].IndexRangeFilters)
	require.False(t, *(class.Properties[0].NestedProperties[0].NestedProperties[0].IndexRangeFilters))
}

type MockShardReader struct {
	lst models.ShardStatusList
	err error
}

func (m *MockShardReader) GetShardsStatus(class, tenant string) (models.ShardStatusList, error) {
	return m.lst, m.err
}

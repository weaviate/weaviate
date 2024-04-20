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

package store

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/sharding"
)

func TestVersionedSchemaReaderShardReplicas(t *testing.T) {
	var (
		ctx = context.Background()
		sc  = &schema{
			Classes: make(map[string]*metaClass),
		}
		vsc = versionedSchema{
			schema:        sc,
			WaitForUpdate: func(ctx context.Context, version uint64) error { return nil },
		}
	)
	// class not found
	_, _, err := sc.ShardReplicas("C", "S")
	assert.ErrorIs(t, err, errClassNotFound)

	// shard not found
	ss := &sharding.State{Physical: make(map[string]sharding.Physical)}

	sc.addClass(&models.Class{Class: "C"}, ss, 1)

	_, err = vsc.ShardReplicas(ctx, "C", "S", 1)
	assert.ErrorIs(t, err, errShardNotFound)

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
		s      = &schema{
			Classes:     make(map[string]*metaClass),
			shardReader: &MockShardReader{},
		}

		sc = versionedSchema{s, f}
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
	assert.ErrorIs(t, err, errClassNotFound)
	_, err = sc.ShardOwner(ctx, "C", "S", 1)
	assert.ErrorIs(t, err, errClassNotFound)
	err = sc.Read(ctx, "C", 1, func(c *models.Class, s *sharding.State) error { return nil })
	assert.ErrorIs(t, err, errClassNotFound)

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
	assert.ErrorIs(t, err, errShardNotFound)
	shards, err := sc.TenantsShards(ctx, 1, "C", "S2")
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
	assert.Equal(t, owner, "N1")

	// TenantShard
	shards, err = sc.TenantsShards(ctx, 1, "D", "S1")
	assert.Equal(t, shards, map[string]string{"S1": "A"})
	assert.Equal(t, shards["S1"], "A")
	assert.Nil(t, err)

	shards, err = sc.TenantsShards(ctx, 1, "D", "Sx")
	assert.Empty(t, shards)
	assert.Nil(t, err)

	reader := func(c *models.Class, s *sharding.State) error { return nil }
	assert.Nil(t, sc.Read(ctx, "C", 1, reader))
	retErr = ErrDeadlineExceeded
	assert.ErrorIs(t, sc.Read(ctx, "C", 1, reader), ErrDeadlineExceeded)
	retErr = nil
}

func TestSchemaReaderShardReplicas(t *testing.T) {
	sc := &schema{
		Classes: make(map[string]*metaClass),
	}
	rsc := retrySchema{sc, versionedSchema{}}
	// class not found
	_, _, err := sc.ShardReplicas("C", "S")
	assert.ErrorIs(t, err, errClassNotFound)

	// shard not found
	ss := &sharding.State{Physical: make(map[string]sharding.Physical)}

	sc.addClass(&models.Class{Class: "C"}, ss, 1)

	_, err = rsc.ShardReplicas("C", "S")
	assert.ErrorIs(t, err, errShardNotFound)

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
		s     = &schema{
			Classes:     make(map[string]*metaClass),
			shardReader: &MockShardReader{},
		}
		sc = retrySchema{s, versionedSchema{}}
	)

	// class not found
	assert.Nil(t, sc.ReadOnlyClass("C"))
	assert.Nil(t, sc.CopyShardingState("C"))
	assert.Equal(t, sc.ReadOnlySchema(), models.Schema{Classes: make([]*models.Class, 0)})
	assert.Equal(t, sc.MultiTenancy("C"), models.MultiTenancyConfig{})

	_, err := sc.ShardReplicas("C", "S")
	assert.ErrorIs(t, err, errClassNotFound)
	_, err = sc.ShardOwner("C", "S")
	assert.ErrorIs(t, err, errClassNotFound)
	err = sc.Read("C", func(c *models.Class, s *sharding.State) error { return nil })
	assert.ErrorIs(t, err, errClassNotFound)

	// Add Simple class
	cls1 := &models.Class{Class: "C"}
	ss1 := &sharding.State{Physical: map[string]sharding.Physical{
		"S1": {Status: "A"},
		"S2": {Status: "A", BelongsToNodes: nodes},
	}}

	sc.schema.addClass(cls1, ss1, 1)
	assert.Equal(t, sc.ReadOnlyClass("C"), cls1)
	assert.Equal(t, sc.MultiTenancy("D"), models.MultiTenancyConfig{})
	assert.Nil(t, sc.Read("C", func(c *models.Class, s *sharding.State) error { return nil }))

	// Shards
	_, err = sc.ShardOwner("C", "S1")
	assert.ErrorContains(t, err, "node not found")
	_, err = sc.ShardOwner("C", "Sx")
	assert.ErrorIs(t, err, errShardNotFound)
	shard, _ := sc.TenantsShards("C", "S2")
	assert.Empty(t, shard)
	assert.Empty(t, sc.ShardFromUUID("Cx", nil))

	_, err = sc.GetShardsStatus("C")
	assert.Nil(t, err)

	// Add MT Class
	cls2 := &models.Class{Class: "D", MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true}}
	ss2 := &sharding.State{
		PartitioningEnabled: true,
		Physical:            map[string]sharding.Physical{"S1": {Status: "A", BelongsToNodes: nodes}},
	}
	sc.schema.addClass(cls2, ss2, 1)
	assert.Equal(t, sc.ReadOnlyClass("D"), cls2)
	assert.Equal(t, sc.MultiTenancy("D"), models.MultiTenancyConfig{Enabled: true})

	assert.ElementsMatch(t, sc.ReadOnlySchema().Classes, []*models.Class{cls1, cls2})

	// ShardOwner
	owner, err := sc.ShardOwner("D", "S1")
	assert.Nil(t, err)
	assert.Equal(t, owner, "N1")

	// TenantShard
	shards, _ := sc.TenantsShards("D", "S1")
	assert.Equal(t, shards["S1"], "A")
	shards, _ = sc.TenantsShards("D", "Sx")
	assert.Empty(t, shards)
}

func TestSchemaSnapshot(t *testing.T) {
	var (
		node   = "N1"
		sc     = NewSchema(node, &MockIndexer{})
		parser = &MockParser{}

		cls = &models.Class{Class: "C"}
		ss  = &sharding.State{
			Physical: map[string]sharding.Physical{
				"S1": {Status: "A"},
				"S2": {Status: "A", BelongsToNodes: []string{"A", "B"}},
			},
		}
	)
	ss.SetLocalName(node)
	assert.Nil(t, sc.addClass(cls, ss, 1))
	parser.On("ParseClass", mock.Anything).Return(nil)

	// Create Snapshot
	sink := &MockSnapshotSink{}
	assert.Nil(t, sc.Persist(sink))

	// restore snapshot
	sc2 := NewSchema("N1", &MockIndexer{})
	assert.Nil(t, sc2.Restore(sink, parser))
	assert.Equal(t, sc.Classes, sc2.Classes)

	// Encoding error
	sink2 := &MockSnapshotSink{wErr: errAny, rErr: errAny}
	assert.ErrorContains(t, sc.Persist(sink2), "encode")

	// Decoding Error
	assert.ErrorContains(t, sc.Restore(sink2, parser), "decode")

	// Parsing Error
	parser2 := &MockParser{}
	parser2.On("ParseClass", mock.Anything).Return(errAny)

	sink3 := &MockSnapshotSink{}
	assert.Nil(t, sc.Persist(sink3))
	assert.ErrorContains(t, sc.Restore(sink3, parser2), "pars")
}

type MockShardReader struct {
	lst models.ShardStatusList
	err error
}

func (m *MockShardReader) GetShardsStatus(class string) (models.ShardStatusList, error) {
	return m.lst, m.err
}

type MockSnapshotSink struct {
	buf bytes.Buffer
	io.WriteCloser
	wErr error
	rErr error
}

func (MockSnapshotSink) ID() string    { return "ID" }
func (MockSnapshotSink) Cancel() error { return nil }

func (m *MockSnapshotSink) Write(p []byte) (n int, err error) {
	if m.wErr != nil {
		return 0, m.wErr
	}
	return m.buf.Write(p)
}

func (m *MockSnapshotSink) Close() error { return nil }

func (m *MockSnapshotSink) Read(p []byte) (n int, err error) {
	if m.rErr != nil {
		return 0, m.rErr
	}
	return m.buf.Read(p)
}

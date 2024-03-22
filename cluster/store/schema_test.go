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
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/sharding"
)

func TestSchemaReaderShardReplicas(t *testing.T) {
	sc := &schema{
		Classes: make(map[string]*metaClass),
	}
	rsc := retrySchema{sc}
	// class not found
	_, err := sc.ShardReplicas("C", "S")
	assert.ErrorIs(t, err, errClassNotFound)

	// shard not found
	ss := &sharding.State{Physical: make(map[string]sharding.Physical)}

	sc.addClass(&models.Class{Class: "C"}, ss)

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
		sc = retrySchema{s}
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

	sc.schema.addClass(cls1, ss1)
	assert.Equal(t, sc.ReadOnlyClass("C"), cls1)
	assert.Equal(t, sc.MultiTenancy("D"), models.MultiTenancyConfig{})
	assert.Nil(t, sc.Read("C", func(c *models.Class, s *sharding.State) error { return nil }))

	// Shards
	_, err = sc.ShardOwner("C", "S1")
	assert.ErrorContains(t, err, "node not found")
	_, err = sc.ShardOwner("C", "Sx")
	assert.ErrorIs(t, err, errShardNotFound)
	shard, _ := sc.TenantShard("C", "S2")
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
	sc.schema.addClass(cls2, ss2)
	assert.Equal(t, sc.ReadOnlyClass("D"), cls2)
	assert.Equal(t, sc.MultiTenancy("D"), models.MultiTenancyConfig{Enabled: true})

	assert.ElementsMatch(t, sc.ReadOnlySchema().Classes, []*models.Class{cls1, cls2})

	// ShardOwner
	owner, err := sc.ShardOwner("D", "S1")
	assert.Nil(t, err)
	assert.Equal(t, owner, "N1")

	// TenantShard
	shard, status := sc.TenantShard("D", "S1")
	assert.Equal(t, shard, "S1")
	assert.Equal(t, status, "A")
	shard, _ = sc.TenantShard("D", "Sx")
	assert.Empty(t, shard)
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
	assert.Nil(t, sc.addClass(cls, ss))
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

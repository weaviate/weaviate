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

package schema

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	gproto "google.golang.org/protobuf/proto"

	cmd "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/fakes"
	"github.com/weaviate/weaviate/usecases/sharding"
)

func TestResolveAlais(t *testing.T) {
	parser := fakes.NewMockParser()
	parser.On("ParseClass", mock.Anything).Return(nil)
	sm := NewSchemaManager("test-node", nil, parser, prometheus.NewPedanticRegistry(), logrus.New())
	areq := cmd.QueryResolveAliasRequest{
		Alias: "AliasNotExist",
	}

	subCommand, err := json.Marshal(&areq)
	require.NoError(t, err)

	req := &cmd.QueryRequest{
		Type:       cmd.QueryRequest_TYPE_RESOLVE_ALIAS,
		SubCommand: subCommand,
	}
	res, err := sm.ResolveAlias(req)
	// Make sure ResolveAlias api returns ErrAliasNotFound in the error chain
	// This is used to decide the final http status code on the http handlers
	require.ErrorIs(t, err, ErrAliasNotFound)
	require.Nil(t, res)
}

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
	assert.Equal(t, sc.ReadOnlySchema(), models.Schema{Classes: make([]*models.Class, 0)})
	assert.Equal(t, sc.MultiTenancy("C"), models.MultiTenancyConfig{})

	_, err := sc.ShardReplicas("C", "S")
	assert.ErrorIs(t, err, ErrClassNotFound)
	_, err = sc.ShardOwner("C", "S")
	assert.ErrorIs(t, err, ErrClassNotFound)
	err = sc.Read("C", true, func(c *models.Class, s *sharding.State) error { return nil })
	assert.ErrorIs(t, err, ErrClassNotFound)

	// Add Single Tenant Class (PartitioningEnabled: false (default))
	cls1 := &models.Class{Class: "C"}
	ss1 := &sharding.State{
		Physical: map[string]sharding.Physical{
			"S1": {Status: "A"},
			"S2": {Status: "A", BelongsToNodes: nodes},
		},
		Virtual: []sharding.Virtual{
			{
				Name:               "V1",
				Upper:              1000,
				OwnsPercentage:     1.0,
				AssignedToPhysical: "S1",
			},
			{
				Name:               "V2",
				Upper:              2000,
				OwnsPercentage:     1.0,
				AssignedToPhysical: "S2",
			},
		},
	}

	sc.schema.addClass(cls1, ss1, 1)
	assert.Equal(t, sc.ReadOnlyClass("C"), cls1)
	versionedClass := sc.ReadOnlyVersionedClass("C")
	assert.Equal(t, versionedClass.Class, cls1)
	assert.Equal(t, sc.MultiTenancy("D"), models.MultiTenancyConfig{})
	assert.Nil(t, sc.Read("C", true, func(c *models.Class, s *sharding.State) error { return nil }))

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

	// Add Multi Tenant Class (PartitioningEnabled: true)
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

// TestApplyPartialSchemaErr verifies that apply() respects the partialSchemaErr flag:
// when set, updateStore is called even when updateSchema returns an error, allowing
// ops like UpdateTenants to commit DB changes for the tenants that were successfully
// applied in the schema before the error is returned to the caller.
func TestApplyPartialSchemaErr(t *testing.T) {
	hardErr := fmt.Errorf("hard schema failure")
	storeErr := fmt.Errorf("store failure")

	tests := []struct {
		name            string
		op              applyOp
		wantStoreCalled bool
		wantErr         error
		wantErrAlso     error // optional: combined error must also wrap this sentinel
	}{
		{
			name: "validation fails when op name is empty",
			op: applyOp{
				updateSchema: func() error { return nil },
				updateStore:  func() error { return nil },
			},
			wantStoreCalled: false,
			wantErr:         fmt.Errorf("could not validate raft apply op"),
		},
		{
			name: "schema error without flag skips updateStore",
			op: applyOp{
				op:           cmd.ApplyRequest_TYPE_UPDATE_TENANT.String(),
				updateSchema: func() error { return hardErr },
				updateStore:  func() error { return nil },
			},
			wantStoreCalled: false,
			wantErr:         ErrSchema,
		},
		{
			name: "PartialUpdateError with allowPartialSchemaErr calls updateStore",
			op: applyOp{
				op: cmd.ApplyRequest_TYPE_UPDATE_TENANT.String(),
				updateSchema: func() error {
					return &PartialUpdateError{Errs: []error{ErrShardNotFound}}
				},
				updateStore:           func() error { return nil },
				allowPartialSchemaErr: true,
			},
			wantStoreCalled: true,
			wantErr:         ErrSchema,
		},
		{
			name: "PartialUpdateError without allowPartialSchemaErr skips updateStore",
			op: applyOp{
				op: cmd.ApplyRequest_TYPE_UPDATE_TENANT.String(),
				updateSchema: func() error {
					return &PartialUpdateError{Errs: []error{ErrShardNotFound}}
				},
				updateStore: func() error { return nil },
			},
			wantStoreCalled: false,
			wantErr:         ErrSchema,
		},
		{
			name: "PartialUpdateError with allowPartialSchemaErr and schemaOnly skips updateStore",
			op: applyOp{
				op: cmd.ApplyRequest_TYPE_UPDATE_TENANT.String(),
				updateSchema: func() error {
					return &PartialUpdateError{Errs: []error{ErrShardNotFound}}
				},
				updateStore:           func() error { return nil },
				schemaOnly:            true,
				allowPartialSchemaErr: true,
			},
			wantStoreCalled: false,
			wantErr:         ErrSchema,
		},
		{
			name: "PartialUpdateError with allowPartialSchemaErr and store failure returns both errors",
			op: applyOp{
				op: cmd.ApplyRequest_TYPE_UPDATE_TENANT.String(),
				updateSchema: func() error {
					return &PartialUpdateError{Errs: []error{ErrShardNotFound}}
				},
				updateStore:           func() error { return storeErr },
				allowPartialSchemaErr: true,
			},
			wantStoreCalled: true,
			wantErr:         ErrSchema,
			wantErrAlso:     errDB,
		},
		{
			name: "both succeed returns no error",
			op: applyOp{
				op:           cmd.ApplyRequest_TYPE_UPDATE_TENANT.String(),
				updateSchema: func() error { return nil },
				updateStore:  func() error { return nil },
			},
			wantStoreCalled: true,
			wantErr:         nil,
		},
		{
			name: "store failure returns errDB",
			op: applyOp{
				op:           cmd.ApplyRequest_TYPE_UPDATE_TENANT.String(),
				updateSchema: func() error { return nil },
				updateStore:  func() error { return storeErr },
			},
			wantStoreCalled: true,
			wantErr:         errDB,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			storeCalled := false
			origStore := tc.op.updateStore
			tc.op.updateStore = func() error {
				storeCalled = true
				return origStore()
			}

			sm := &SchemaManager{}
			err := sm.apply(tc.op)

			assert.Equal(t, tc.wantStoreCalled, storeCalled, "updateStore called mismatch")
			if tc.wantErr == nil {
				assert.NoError(t, err)
			} else {
				if errors.Is(tc.wantErr, ErrSchema) || errors.Is(tc.wantErr, errDB) {
					assert.ErrorIs(t, err, tc.wantErr)
				} else {
					assert.ErrorContains(t, err, tc.wantErr.Error())
				}
				if tc.wantErrAlso != nil {
					assert.ErrorIs(t, err, tc.wantErrAlso)
				}
			}
		})
	}
}

type MockShardReader struct {
	lst models.ShardStatusList
	err error
}

func (m *MockShardReader) GetShardsStatus(class, tenant string) (models.ShardStatusList, error) {
	return m.lst, m.err
}

// fakeReplicationFSM stands in for the real FSM here because importing
// cluster/replication would cycle through cluster/schema. Unused interface
// methods panic so an unexpected call surfaces immediately.
type fakeReplicationFSM struct {
	setUnCancellableCalls    []uint64
	setUnCancellableReturnFn func(id uint64) error
}

func (f *fakeReplicationFSM) SetUnCancellable(id uint64) error {
	f.setUnCancellableCalls = append(f.setUnCancellableCalls, id)
	if f.setUnCancellableReturnFn != nil {
		return f.setUnCancellableReturnFn(id)
	}
	return nil
}

func (f *fakeReplicationFSM) HasActiveReplicationForShard(string, string) bool {
	panic("unexpected HasActiveReplicationForShard call")
}

func (f *fakeReplicationFSM) HasActiveReplicationForCollection(string) bool {
	panic("unexpected HasActiveReplicationForCollection call")
}

func (f *fakeReplicationFSM) DeleteReplicationsByCollection(string) error {
	panic("unexpected DeleteReplicationsByCollection call")
}

func (f *fakeReplicationFSM) DeleteReplicationsByTenants(string, []string) error {
	panic("unexpected DeleteReplicationsByTenants call")
}

// TestSchemaManager_ReplicationAddReplicaToShard_AtomicallySetsUnCancellable
// firewalls the success-path co-occurrence invariant that the deleted
// conflicts acceptance tests depended on: a successful apply both flips
// UnCancellable on the FSM AND adds the replica to the schema, with
// SetUnCancellable running first so its failure short-circuits the schema add.
func TestSchemaManager_ReplicationAddReplicaToShard_AtomicallySetsUnCancellable(t *testing.T) {
	const (
		className  = "TestClass"
		shardName  = "shard1"
		sourceNode = "node1"
		targetNode = "node-target"
		opID       = uint64(42)
		schemaVer  = uint64(1)
		applyVer   = uint64(2)
	)

	buildManager := func(t *testing.T, fsm *fakeReplicationFSM) *SchemaManager {
		t.Helper()
		parser := fakes.NewMockParser()
		parser.On("ParseClass", mock.Anything).Return(nil)
		indexer := fakes.NewMockSchemaExecutor()
		indexer.On("ReconcileAsyncReplicationForShard", mock.Anything, mock.Anything).Return(nil).Maybe()
		sm := NewSchemaManager("local-node", indexer, parser, prometheus.NewPedanticRegistry(), logrus.New())
		sm.SetReplicationFSM(fsm)

		ss := &sharding.State{Physical: map[string]sharding.Physical{
			shardName: {Name: shardName, BelongsToNodes: []string{sourceNode}},
		}}
		require.NoError(t, sm.schema.addClass(&models.Class{Class: className}, ss, schemaVer))
		return sm
	}

	buildCmd := func(t *testing.T) *cmd.ApplyRequest {
		t.Helper()
		sub, err := json.Marshal(&cmd.ReplicationAddReplicaToShard{
			OpId: opID, Class: className, Shard: shardName,
			TargetNode: targetNode, SchemaVersion: schemaVer,
		})
		require.NoError(t, err)
		return &cmd.ApplyRequest{
			Type:       cmd.ApplyRequest_TYPE_REPLICATION_REPLICATE_ADD_REPLICA_TO_SHARD,
			Class:      className,
			Version:    applyVer,
			SubCommand: sub,
		}
	}

	t.Run("success path co-occurs UnCancellable flip and replica add", func(t *testing.T) {
		fsm := &fakeReplicationFSM{}
		sm := buildManager(t, fsm)

		require.NoError(t, sm.ReplicationAddReplicaToShard(buildCmd(t), false))

		require.Equal(t, []uint64{opID}, fsm.setUnCancellableCalls,
			"SetUnCancellable must be called exactly once with the op id")

		// Replica in the sharding state confirms addReplicaToShard committed.
		require.NoError(t, sm.schema.Read(className, false, func(_ *models.Class, ss *sharding.State) error {
			require.Contains(t, ss.Physical[shardName].BelongsToNodes, targetNode,
				"target node must be present in the shard's BelongsToNodes")
			return nil
		}))
	})

	t.Run("SetUnCancellable failure short-circuits addReplicaToShard", func(t *testing.T) {
		// Failure injection proves SetUnCancellable runs first — otherwise
		// the schema would already have mutated by the time we observe error.
		injectedErr := errors.New("synthetic SetUnCancellable failure")
		fsm := &fakeReplicationFSM{
			setUnCancellableReturnFn: func(uint64) error { return injectedErr },
		}
		sm := buildManager(t, fsm)

		err := sm.ReplicationAddReplicaToShard(buildCmd(t), false)
		require.Error(t, err)
		require.ErrorIs(t, err, ErrSchema)
		require.ErrorContains(t, err, injectedErr.Error())

		// Schema untouched ⇒ addReplicaToShard never ran.
		require.NoError(t, sm.schema.Read(className, false, func(_ *models.Class, ss *sharding.State) error {
			require.NotContains(t, ss.Physical[shardName].BelongsToNodes, targetNode,
				"target node must NOT be added when SetUnCancellable fails first")
			return nil
		}))
	})
}

// recordingMutationGuard captures every CheckPropertyUpdate call and
// optionally rejects with a configured error. Used to pin the
// SchemaManager.UpdateProperty guard call site (https://github.com/weaviate/0-weaviate-issues/issues/218).
type recordingMutationGuard struct {
	called      int
	lastClass   string
	lastProp    string
	lastRemoved []string
	rejectWith  error
}

func (g *recordingMutationGuard) CheckPropertyUpdate(class, prop string) error {
	g.called++
	g.lastClass = class
	g.lastProp = prop
	return g.rejectWith
}

func (g *recordingMutationGuard) CheckClassMutation(class string) error {
	g.called++
	g.lastClass = class
	return g.rejectWith
}

func (g *recordingMutationGuard) CheckTenantMutation(class string, tenants []string) error {
	g.called++
	g.lastClass = class
	return g.rejectWith
}

func (g *recordingMutationGuard) CheckVectorConfigRemoval(class string, removedVectors []string) error {
	g.called++
	g.lastClass = class
	g.lastRemoved = removedVectors
	return g.rejectWith
}

// TestSchemaManager_UpdateProperty_MutationGuard pins the cross-FSM
// MutationGuard wiring on the UpdateProperty apply path
// (https://github.com/weaviate/0-weaviate-issues/issues/218). The guard:
//
//   - MUST be consulted before the schema apply on any external
//     UpdateProperty.
//   - MUST be bypassed when FromInFlightMigration is true on the
//     request (the migration-completion path uses this to flip its
//     own scheduled schema update past the guard that would otherwise
//     reject it during FINALIZING).
//   - MUST short-circuit before the guard on malformed requests, so a
//     bad payload doesn't waste a detector dispatch.
//
// We construct a SchemaManager only enough to exercise the guard
// branch; the downstream apply intentionally errors because no class
// is registered. The point of the test is to pin the guard
// pre-condition, not the apply itself.
func TestSchemaManager_UpdateProperty_MutationGuard(t *testing.T) {
	mkRequest := func(class, prop string, fromMigration bool, fields ...string) *cmd.ApplyRequest {
		req := cmd.UpdatePropertyRequest{
			Property:              &models.Property{Name: prop},
			FieldsToUpdate:        fields,
			FromInFlightMigration: fromMigration,
		}
		sub, err := json.Marshal(&req)
		require.NoError(t, err)
		return &cmd.ApplyRequest{
			Type:       cmd.ApplyRequest_TYPE_UPDATE_PROPERTY,
			Class:      class,
			SubCommand: sub,
		}
	}

	newSM := func(guard MutationGuard) *SchemaManager {
		parser := fakes.NewMockParser()
		parser.On("ParseClass", mock.Anything).Return(nil)
		sm := NewSchemaManager("test-node", nil, parser, prometheus.NewPedanticRegistry(), logrus.New())
		if guard != nil {
			sm.SetMutationGuard(guard)
		}
		return sm
	}

	t.Run("guard rejects external UpdateProperty before apply", func(t *testing.T) {
		guard := &recordingMutationGuard{
			rejectWith: fmt.Errorf("reindex task X in flight on C.name"),
		}
		sm := newSM(guard)
		// schemaOnly=true skips the store (db) apply step, so the test
		// doesn't need a real Indexer. We still expect an error from
		// the guard.
		err := sm.UpdateProperty(mkRequest("C", "name", false), true, false)
		require.Error(t, err)
		require.Contains(t, err.Error(), "reindex task X in flight",
			"guard rejection must propagate to the caller")
		require.Equal(t, 1, guard.called,
			"guard MUST be consulted exactly once for an external request")
		require.Equal(t, "C", guard.lastClass)
		require.Equal(t, "name", guard.lastProp)
	})

	t.Run("guard bypassed when FromInFlightMigration=true", func(t *testing.T) {
		guard := &recordingMutationGuard{
			rejectWith: fmt.Errorf("guard rejection that MUST be bypassed"),
		}
		sm := newSM(guard)
		// FromInFlightMigration=true bypasses the guard. The downstream
		// apply still errors (no class set up), but the guard
		// rejection text must NOT appear in the returned error and the
		// guard call counter must stay at zero.
		err := sm.UpdateProperty(mkRequest("C", "name", true), true, false)
		require.Error(t, err, "downstream apply still errors on missing class, but the error must not be a guard rejection")
		require.NotContains(t, err.Error(), "MUST be bypassed",
			"FromInFlightMigration=true MUST bypass the guard rejection")
		require.Equal(t, 0, guard.called,
			"guard MUST NOT be consulted when FromInFlightMigration=true")
	})

	t.Run("no guard registered → no extra rejection", func(t *testing.T) {
		sm := newSM(nil) // no mutationGuard set
		err := sm.UpdateProperty(mkRequest("C", "name", false), true, false)
		// Downstream apply still fails (no class), but the error must
		// not be a guard rejection. We assert that no MutationGuard
		// rejection text shape appears.
		require.Error(t, err)
		require.NotContains(t, err.Error(), "in flight on",
			"with no guard registered, the in-flight error template must not appear")
	})

	t.Run("bad request body short-circuits before the guard", func(t *testing.T) {
		guard := &recordingMutationGuard{rejectWith: fmt.Errorf("should not be reached")}
		sm := newSM(guard)
		bad := &cmd.ApplyRequest{
			Type:       cmd.ApplyRequest_TYPE_UPDATE_PROPERTY,
			Class:      "C",
			SubCommand: []byte("not-json"),
		}
		err := sm.UpdateProperty(bad, true, false)
		require.Error(t, err)
		require.ErrorIs(t, err, ErrBadRequest)
		require.Equal(t, 0, guard.called,
			"unparseable request must not reach the guard")
	})

	t.Run("empty Property short-circuits before the guard", func(t *testing.T) {
		guard := &recordingMutationGuard{rejectWith: fmt.Errorf("should not be reached")}
		sm := newSM(guard)
		req := cmd.UpdatePropertyRequest{Property: nil}
		sub, _ := json.Marshal(&req)
		bad := &cmd.ApplyRequest{
			Type:       cmd.ApplyRequest_TYPE_UPDATE_PROPERTY,
			Class:      "C",
			SubCommand: sub,
		}
		err := sm.UpdateProperty(bad, true, false)
		require.Error(t, err)
		require.ErrorIs(t, err, ErrBadRequest)
		require.Equal(t, 0, guard.called,
			"empty property must not reach the guard")
	})
}

// TestSchemaManager_DeleteClass_MutationGuard pins the class-wide
// guard wiring on the DeleteClass apply path
// (https://github.com/weaviate/0-weaviate-issues/issues/218 / https://github.com/weaviate/0-weaviate-issues/issues/219). DeleteClass mid-reindex destroys
// every property's bucket state at once; the guard rejection must
// fire BEFORE any class-deletion side effect.
func TestSchemaManager_DeleteClass_MutationGuard(t *testing.T) {
	mkRequest := func(class string) *cmd.ApplyRequest {
		return &cmd.ApplyRequest{
			Type:  cmd.ApplyRequest_TYPE_DELETE_CLASS,
			Class: class,
		}
	}

	newSM := func(guard MutationGuard) *SchemaManager {
		parser := fakes.NewMockParser()
		parser.On("ParseClass", mock.Anything).Return(nil)
		sm := NewSchemaManager("test-node", nil, parser, prometheus.NewPedanticRegistry(), logrus.New())
		if guard != nil {
			sm.SetMutationGuard(guard)
		}
		return sm
	}

	t.Run("guard rejects DeleteClass before apply", func(t *testing.T) {
		guard := &recordingMutationGuard{
			rejectWith: fmt.Errorf("reindex task on class C is in flight"),
		}
		sm := newSM(guard)
		err := sm.DeleteClass(mkRequest("C"), true, false)
		require.Error(t, err)
		require.Contains(t, err.Error(), "reindex task on class C is in flight")
		require.Equal(t, 1, guard.called)
		require.Equal(t, "C", guard.lastClass)
	})

	t.Run("no guard registered → DeleteClass proceeds", func(t *testing.T) {
		sm := newSM(nil)
		// DeleteClass with schemaOnly=true short-circuits the db side; the
		// schema side will just delete a non-existent class, which is a no-op.
		err := sm.DeleteClass(mkRequest("C"), true, false)
		require.NoError(t, err,
			"with no guard registered and schemaOnly=true, DeleteClass on a non-existent class is a no-op")
	})
}

// TestSchemaManager_UpdateClass_VectorConfigRemovalGate pins the gate wiring
// on the UpdateClass apply path: when an update removes a dropped ("none")
// VectorConfig entry, the MutationGuard MUST be consulted with the removed names
// and its rejection MUST propagate; an update that removes no dropped entry must
// not consult the gate.
func TestSchemaManager_UpdateClass_VectorConfigRemovalGate(t *testing.T) {
	none := models.VectorConfig{VectorIndexType: "none"}
	hnsw := models.VectorConfig{VectorIndexType: "hnsw"}

	mkRequest := func(updated *models.Class) *cmd.ApplyRequest {
		sub, err := json.Marshal(&cmd.UpdateClassRequest{Class: updated, State: nil})
		require.NoError(t, err)
		return &cmd.ApplyRequest{
			Type:       cmd.ApplyRequest_TYPE_UPDATE_CLASS,
			Class:      "C",
			Version:    2,
			SubCommand: sub,
		}
	}

	// newSM registers class C with a live "keep" and a dropped "vec1", and mocks
	// ParseClassUpdate to return parsed (the post-update class the gate diffs against).
	newSM := func(guard MutationGuard, parsed *models.Class) *SchemaManager {
		parser := fakes.NewMockParser()
		parser.On("ParseClassUpdate", mock.Anything, mock.Anything).Return(parsed, nil)
		sm := NewSchemaManager("test-node", nil, parser, prometheus.NewPedanticRegistry(), logrus.New())
		if guard != nil {
			sm.SetMutationGuard(guard)
		}
		initial := &models.Class{
			Class:        "C",
			VectorConfig: map[string]models.VectorConfig{"keep": hnsw, "vec1": none},
		}
		ss := &sharding.State{Physical: map[string]sharding.Physical{}}
		require.NoError(t, sm.schema.addClass(initial, ss, 1))
		return sm
	}

	t.Run("removing a dropped entry consults the gate and propagates rejection", func(t *testing.T) {
		parsed := &models.Class{Class: "C", VectorConfig: map[string]models.VectorConfig{"keep": hnsw}}
		guard := &recordingMutationGuard{rejectWith: fmt.Errorf("cleanup task not FINISHED")}
		sm := newSM(guard, parsed)

		err := sm.UpdateClass(mkRequest(parsed), "test-node", true, false)
		require.Error(t, err)
		require.Contains(t, err.Error(), "cleanup task not FINISHED")
		require.Equal(t, 1, guard.called, "gate must be consulted once")
		require.Equal(t, []string{"vec1"}, guard.lastRemoved)
	})

	t.Run("removing a dropped entry succeeds when the gate allows", func(t *testing.T) {
		parsed := &models.Class{Class: "C", VectorConfig: map[string]models.VectorConfig{"keep": hnsw}}
		guard := &recordingMutationGuard{} // allows
		sm := newSM(guard, parsed)

		err := sm.UpdateClass(mkRequest(parsed), "test-node", true, false)
		require.NoError(t, err)
		require.Equal(t, 1, guard.called)
		require.Equal(t, []string{"vec1"}, guard.lastRemoved)
	})

	t.Run("update that removes no dropped entry does not consult the gate", func(t *testing.T) {
		// vec1 stays "none" (not removed); keep stays live.
		parsed := &models.Class{Class: "C", VectorConfig: map[string]models.VectorConfig{"keep": hnsw, "vec1": none}}
		guard := &recordingMutationGuard{rejectWith: fmt.Errorf("should not be reached")}
		sm := newSM(guard, parsed)

		err := sm.UpdateClass(mkRequest(parsed), "test-node", true, false)
		require.NoError(t, err)
		require.Equal(t, 0, guard.called, "no dropped entry removed → gate not consulted")
	})
}

// TestSchemaManager_DeleteTenants_MutationGuard pins the tenant-level
// guard wiring on the DeleteTenants apply path.
func TestSchemaManager_DeleteTenants_MutationGuard(t *testing.T) {
	mkRequest := func(class string, tenants []string) *cmd.ApplyRequest {
		req := &cmd.DeleteTenantsRequest{Tenants: tenants}
		sub, err := gproto.Marshal(req)
		require.NoError(t, err)
		return &cmd.ApplyRequest{
			Type:       cmd.ApplyRequest_TYPE_DELETE_TENANT,
			Class:      class,
			SubCommand: sub,
		}
	}

	newSM := func(guard MutationGuard) *SchemaManager {
		parser := fakes.NewMockParser()
		parser.On("ParseClass", mock.Anything).Return(nil)
		sm := NewSchemaManager("test-node", nil, parser, prometheus.NewPedanticRegistry(), logrus.New())
		if guard != nil {
			sm.SetMutationGuard(guard)
		}
		return sm
	}

	t.Run("guard rejects DeleteTenants before apply", func(t *testing.T) {
		guard := &recordingMutationGuard{
			rejectWith: fmt.Errorf("reindex task on class C is in flight, tenants blocked"),
		}
		sm := newSM(guard)
		err := sm.DeleteTenants(mkRequest("C", []string{"t1", "t2"}), true)
		require.Error(t, err)
		require.Contains(t, err.Error(), "tenants blocked")
		require.Equal(t, 1, guard.called)
		require.Equal(t, "C", guard.lastClass)
	})

	t.Run("empty tenants list does not consult the guard", func(t *testing.T) {
		guard := &recordingMutationGuard{rejectWith: fmt.Errorf("should not be reached")}
		sm := newSM(guard)
		// schemaOnly=true so we don't need a real schema/db.
		err := sm.DeleteTenants(mkRequest("C", nil), true)
		// schema may error on the empty list itself, but the guard MUST NOT
		// have been called — there's nothing to mutate.
		_ = err
		require.Equal(t, 0, guard.called,
			"empty tenants list means no mutation; guard MUST NOT be invoked")
	})
}

// TestTenantsTransitioningAwayFromActive pins the helper that
// narrows UpdateTenants guard invocation to tenants whose target
// status would make their shards locally unavailable.
func TestTenantsTransitioningAwayFromActive(t *testing.T) {
	tests := []struct {
		name    string
		tenants []*cmd.Tenant
		want    []string
	}{
		{
			name:    "empty list → empty result",
			tenants: nil,
			want:    nil,
		},
		{
			name: "all transitioning toward ACTIVE → empty result",
			tenants: []*cmd.Tenant{
				{Name: "t1", Status: models.TenantActivityStatusACTIVE},
				{Name: "t2", Status: models.TenantActivityStatusHOT},
				{Name: "t3", Status: models.TenantActivityStatusONLOADING},
				{Name: "t4", Status: models.TenantActivityStatusUNFREEZING},
			},
			want: nil,
		},
		{
			name: "all transitioning AWAY from ACTIVE → all returned",
			tenants: []*cmd.Tenant{
				{Name: "t1", Status: models.TenantActivityStatusINACTIVE},
				{Name: "t2", Status: models.TenantActivityStatusCOLD},
				{Name: "t3", Status: models.TenantActivityStatusOFFLOADED},
				{Name: "t4", Status: models.TenantActivityStatusOFFLOADING},
				{Name: "t5", Status: models.TenantActivityStatusFROZEN},
				{Name: "t6", Status: models.TenantActivityStatusFREEZING},
			},
			want: []string{"t1", "t2", "t3", "t4", "t5", "t6"},
		},
		{
			name: "mixed → only away-from-ACTIVE returned",
			tenants: []*cmd.Tenant{
				{Name: "active", Status: models.TenantActivityStatusACTIVE},
				{Name: "frozen", Status: models.TenantActivityStatusFROZEN},
				{Name: "hot", Status: models.TenantActivityStatusHOT},
				{Name: "cold", Status: models.TenantActivityStatusCOLD},
			},
			want: []string{"frozen", "cold"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := tenantsTransitioningAwayFromActive(tc.tenants)
			require.Equal(t, tc.want, got)
		})
	}
}

// TestSchemaManager_SetMutationGuard pins the setter contract — nil
// removes the guard, replacement overwrites, no panics.
func TestSchemaManager_SetMutationGuard(t *testing.T) {
	sm := &SchemaManager{}
	require.Nil(t, sm.mutationGuard)

	g1 := &recordingMutationGuard{}
	sm.SetMutationGuard(g1)
	require.Equal(t, MutationGuard(g1), sm.mutationGuard)

	g2 := &recordingMutationGuard{}
	sm.SetMutationGuard(g2)
	require.Equal(t, MutationGuard(g2), sm.mutationGuard, "subsequent calls overwrite")

	sm.SetMutationGuard(nil)
	require.Nil(t, sm.mutationGuard, "nil clears the guard")
}

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

// recordingMutationGuard captures every CheckPropertyUpdate call and
// optionally rejects with a configured error. Used to pin the
// SchemaManager.UpdateProperty guard call site (https://github.com/weaviate/0-weaviate-issues/issues/218).
type recordingMutationGuard struct {
	called     int
	lastClass  string
	lastProp   string
	rejectWith error
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

// TestSchemaManager_UpdateClass_FieldMask pins the FSM-side half of
// the UpdateClassRequest.FieldsToUpdate contract: only the listed
// class-level sub-configs are merged onto meta.Class. The other half
// (executor dispatch) is covered in usecases/schema executor tests;
// this test ensures a future refactor cannot regress the apply path
// silently while the executor-side tests still pass. See
// weaviate/0-weaviate-issues#240 for the read-only-cascade bug this
// mask was introduced to close.
func TestSchemaManager_UpdateClass_FieldMask(t *testing.T) {
	parser := fakes.NewMockParser()
	parser.On("ParseClass", mock.Anything).Return(nil)
	// MockParser.ParseClassUpdate (usecases/fakes) returns the
	// `update` argument untouched and `args.Error(1)` for the error.
	// Return(nil, nil) yields zero-error success while keeping the
	// parser behaviour neutral so the FSM merge is the only thing
	// under test.
	parser.On("ParseClassUpdate", mock.Anything).Return(nil, nil)

	sm := NewSchemaManager("test-node", nil, parser, prometheus.NewPedanticRegistry(), logrus.New())

	// Seed an initial class with non-nil sub-configs so we can
	// observe whether each one was replaced or preserved post-apply.
	initial := &models.Class{
		Class:               "C",
		Description:         "initial-desc",
		VectorIndexConfig:   "initial-vector-index-config",
		VectorConfig:        map[string]models.VectorConfig{"v": {VectorIndexType: "hnsw"}},
		InvertedIndexConfig: &models.InvertedIndexConfig{UsingBlockMaxWAND: false},
		ReplicationConfig:   &models.ReplicationConfig{Factor: 1},
		MultiTenancyConfig:  &models.MultiTenancyConfig{Enabled: false},
		ObjectTTLConfig:     &models.ObjectTTLConfig{Enabled: false},
		Properties:          []*models.Property{{Name: "p1"}},
	}
	require.NoError(t, sm.schema.addClass(initial, &sharding.State{}, 1))

	mkRequest := func(updated *models.Class, fields ...string) *cmd.ApplyRequest {
		req := cmd.UpdateClassRequest{Class: updated, FieldsToUpdate: fields}
		sub, err := json.Marshal(&req)
		require.NoError(t, err)
		return &cmd.ApplyRequest{
			Type:       cmd.ApplyRequest_TYPE_UPDATE_CLASS,
			Class:      updated.Class,
			SubCommand: sub,
			Version:    2,
		}
	}

	t.Run("masked to invertedIndexConfig only flips that field", func(t *testing.T) {
		updated := &models.Class{
			Class:               "C",
			Description:         "DIFFERENT-desc-MUST-NOT-LAND",
			VectorIndexConfig:   "DIFFERENT-vic-MUST-NOT-LAND",
			VectorConfig:        map[string]models.VectorConfig{"v": {VectorIndexType: "DIFFERENT"}},
			InvertedIndexConfig: &models.InvertedIndexConfig{UsingBlockMaxWAND: true},
			ReplicationConfig:   &models.ReplicationConfig{Factor: 99},
			MultiTenancyConfig:  &models.MultiTenancyConfig{Enabled: true},
			ObjectTTLConfig:     &models.ObjectTTLConfig{Enabled: true},
			Properties:          []*models.Property{{Name: "DIFFERENT-prop"}},
		}
		err := sm.UpdateClass(mkRequest(updated, cmd.ClassFieldInvertedIndexConfig), "n", true, false)
		require.NoError(t, err)

		got := sm.schema.classes["C"]
		require.True(t, got.Class.InvertedIndexConfig.UsingBlockMaxWAND,
			"invertedIndexConfig MUST be flipped — it's in the mask")
		require.Equal(t, "initial-desc", got.Class.Description,
			"Description MUST NOT land — not in the mask")
		require.Equal(t, "initial-vector-index-config", got.Class.VectorIndexConfig,
			"VectorIndexConfig MUST NOT land — not in the mask")
		require.Equal(t, "hnsw", got.Class.VectorConfig["v"].VectorIndexType,
			"VectorConfig MUST NOT land — not in the mask")
		require.Equal(t, int64(1), got.Class.ReplicationConfig.Factor,
			"ReplicationConfig MUST NOT land — not in the mask")
		require.False(t, got.Class.MultiTenancyConfig.Enabled,
			"MultiTenancyConfig MUST NOT land — not in the mask")
		require.False(t, got.Class.ObjectTTLConfig.Enabled,
			"ObjectTTLConfig MUST NOT land — not in the mask")
		require.Equal(t, "p1", got.Class.Properties[0].Name,
			"Properties MUST NOT land — not in the mask")
	})

	t.Run("empty mask falls back to legacy replace-everything", func(t *testing.T) {
		// Reset to a known state for this subtest.
		sm.schema.classes["C"].Class = *initial

		updated := &models.Class{
			Class:               "C",
			Description:         "post-update-desc",
			VectorIndexConfig:   "post-update-vic",
			VectorConfig:        map[string]models.VectorConfig{"v": {VectorIndexType: "flat"}},
			InvertedIndexConfig: &models.InvertedIndexConfig{UsingBlockMaxWAND: true},
			ReplicationConfig:   &models.ReplicationConfig{Factor: 1},
			MultiTenancyConfig:  &models.MultiTenancyConfig{Enabled: false},
			ObjectTTLConfig:     &models.ObjectTTLConfig{Enabled: true},
			Properties:          []*models.Property{{Name: "p2"}},
		}
		err := sm.UpdateClass(mkRequest(updated /* nil mask */), "n", true, false)
		require.NoError(t, err)

		got := sm.schema.classes["C"]
		require.Equal(t, "post-update-desc", got.Class.Description, "legacy unmasked merge MUST land Description")
		require.Equal(t, "post-update-vic", got.Class.VectorIndexConfig, "legacy unmasked merge MUST land VectorIndexConfig")
		require.Equal(t, "flat", got.Class.VectorConfig["v"].VectorIndexType, "legacy unmasked merge MUST land VectorConfig")
		require.True(t, got.Class.InvertedIndexConfig.UsingBlockMaxWAND, "legacy unmasked merge MUST land InvertedIndexConfig")
		require.True(t, got.Class.ObjectTTLConfig.Enabled, "legacy unmasked merge MUST land ObjectTTLConfig")
		require.Equal(t, "p2", got.Class.Properties[0].Name, "legacy unmasked merge MUST land Properties")
	})
}

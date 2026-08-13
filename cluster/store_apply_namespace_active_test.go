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

package cluster

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	gproto "google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/auth/authorization/conv"
	"github.com/weaviate/weaviate/usecases/namespaces"
	"github.com/weaviate/weaviate/usecases/sharding"
)

// nsCreateIndex and nsFlipIndex are the RAFT indexes the seeds in this file
// record at create and at the flip. Distinct because one log index can only
// belong to one command.
const (
	nsCreateIndex uint64 = 1
	nsFlipIndex   uint64 = 2
)

// This file tests the namespace checks on both sides of the RAFT log. Every
// ApplyRequest type belongs to exactly one of the four maps below. The namespace
// lifecycle commands stay ungated, so a suspended namespace can still be resumed
// or deleted.
//
// The maps record intent, not behaviour. The switch in store_apply.go is the
// authority, so a type listed here but wired to a different check still leaves
// this test green. The per-state cases in TestApplyGate_RejectsGatedSchemaApplyTypes,
// TestExecuteGate_RejectsCreateLikeApplyTypes and
// TestExecuteGate_DestructiveApplyTypes pin the behaviour.

// Commands gated by namespaces.RequireActive in the apply switch. They stay
// there because they materialize shards: a propose-time refusal cannot stop one
// that lands while the namespace keeps its shards closed from leaving a schema
// entry with nothing behind it.
var requireActiveApplyTypes = map[api.ApplyRequest_Type]struct{}{
	api.ApplyRequest_TYPE_ADD_CLASS:     {},
	api.ApplyRequest_TYPE_RESTORE_CLASS: {},
	api.ApplyRequest_TYPE_ADD_TENANT:    {},
	api.ApplyRequest_TYPE_UPDATE_TENANT: {},
}

// Commands gated by namespaces.RequireActive in store.admitCreateLike at propose
// time. Deliberately absent from the apply switch: see Store.admitPropose. They
// qualify because they materialize nothing, so a late one leaves no half-built
// entity.
var requireActiveProposeTypes = map[api.ApplyRequest_Type]struct{}{
	api.ApplyRequest_TYPE_CREATE_ALIAS:             {},
	api.ApplyRequest_TYPE_REPLACE_ALIAS:            {},
	api.ApplyRequest_TYPE_UPSERT_USER:              {},
	api.ApplyRequest_TYPE_UPSERT_ROLES_PERMISSIONS: {},
	api.ApplyRequest_TYPE_ADD_ROLES_FOR_USER:       {},
}

// Commands gated by namespaces.AdmitDestructiveApply, in store.admitDestructive
// at propose time. Deliberately absent from the apply switch: see
// Store.admitPropose.
var destructiveApplyTypes = map[api.ApplyRequest_Type]struct{}{
	api.ApplyRequest_TYPE_DELETE_CLASS:  {},
	api.ApplyRequest_TYPE_DELETE_TENANT: {},
	api.ApplyRequest_TYPE_DELETE_ALIAS:  {},
}

// Commands with no namespace check on either side of the log.
//
// The replication-record deletes remove in-memory operation records, not user
// data. The RBAC and user commands stay ungated so access can always be cut off:
// if a key leaks while a namespace is suspended, revoking it must not wait for a
// resume. Their create direction is still refused, before the entry is appended.
// DELETE_REPLICA_FROM_SHARD, TENANT_PROCESS and UPDATE_CLASS can destroy data
// and are ungated on purpose, tracked outside this change.
var ungatedApplyTypes = map[api.ApplyRequest_Type]struct{}{
	api.ApplyRequest_TYPE_UPDATE_CLASS:                                               {},
	api.ApplyRequest_TYPE_ADD_PROPERTY:                                               {},
	api.ApplyRequest_TYPE_UPDATE_PROPERTY:                                            {},
	api.ApplyRequest_TYPE_UPDATE_SHARD_STATUS:                                        {},
	api.ApplyRequest_TYPE_ADD_REPLICA_TO_SHARD:                                       {},
	api.ApplyRequest_TYPE_DELETE_REPLICA_FROM_SHARD:                                  {},
	api.ApplyRequest_TYPE_TENANT_PROCESS:                                             {},
	api.ApplyRequest_TYPE_DELETE_ROLES:                                               {},
	api.ApplyRequest_TYPE_REMOVE_PERMISSIONS:                                         {},
	api.ApplyRequest_TYPE_REVOKE_ROLES_FOR_USER:                                      {},
	api.ApplyRequest_TYPE_DELETE_USER:                                                {},
	api.ApplyRequest_TYPE_ROTATE_USER_API_KEY:                                        {},
	api.ApplyRequest_TYPE_SUSPEND_USER:                                               {},
	api.ApplyRequest_TYPE_ACTIVATE_USER:                                              {},
	api.ApplyRequest_TYPE_CREATE_USER_WITH_KEY:                                       {},
	api.ApplyRequest_TYPE_DELETE_USERS_IN_NAMESPACE:                                  {},
	api.ApplyRequest_TYPE_ADD_NAMESPACE:                                              {},
	api.ApplyRequest_TYPE_UPDATE_NAMESPACE:                                           {},
	api.ApplyRequest_TYPE_CHANGE_NAMESPACE_STATE:                                     {},
	api.ApplyRequest_TYPE_REMOVE_NAMESPACE_ENTITY:                                    {},
	api.ApplyRequest_TYPE_REPLICATION_REPLICATE:                                      {},
	api.ApplyRequest_TYPE_REPLICATION_REPLICATE_UPDATE_STATE:                         {},
	api.ApplyRequest_TYPE_REPLICATION_REPLICATE_REGISTER_ERROR:                       {},
	api.ApplyRequest_TYPE_REPLICATION_REPLICATE_CANCEL:                               {},
	api.ApplyRequest_TYPE_REPLICATION_REPLICATE_DELETE:                               {},
	api.ApplyRequest_TYPE_REPLICATION_REPLICATE_REMOVE:                               {},
	api.ApplyRequest_TYPE_REPLICATION_REPLICATE_CANCELLATION_COMPLETE:                {},
	api.ApplyRequest_TYPE_REPLICATION_REPLICATE_DELETE_ALL:                           {},
	api.ApplyRequest_TYPE_REPLICATION_REPLICATE_DELETE_BY_COLLECTION:                 {},
	api.ApplyRequest_TYPE_REPLICATION_REPLICATE_DELETE_BY_TENANTS:                    {},
	api.ApplyRequest_TYPE_REPLICATION_REGISTER_SCHEMA_VERSION:                        {},
	api.ApplyRequest_TYPE_REPLICATION_REPLICATE_SYNC_SHARD:                           {}, //nolint:staticcheck // deprecated but must stay classified for the drift check
	api.ApplyRequest_TYPE_REPLICATION_REPLICATE_ADD_REPLICA_TO_SHARD:                 {},
	api.ApplyRequest_TYPE_REPLICATION_NODE_REACHED_STATE:                             {},
	api.ApplyRequest_TYPE_REPLICATION_REPLICATE_FORCE_DELETE_ALL:                     {},
	api.ApplyRequest_TYPE_REPLICATION_REPLICATE_FORCE_DELETE_BY_COLLECTION:           {},
	api.ApplyRequest_TYPE_REPLICATION_REPLICATE_FORCE_DELETE_BY_COLLECTION_AND_SHARD: {},
	api.ApplyRequest_TYPE_REPLICATION_REPLICATE_FORCE_DELETE_BY_TARGET_NODE:          {},
	api.ApplyRequest_TYPE_REPLICATION_REPLICATE_FORCE_DELETE_BY_UUID:                 {},
	api.ApplyRequest_TYPE_DISTRIBUTED_TASK_ADD:                                       {},
	api.ApplyRequest_TYPE_DISTRIBUTED_TASK_CANCEL:                                    {},
	api.ApplyRequest_TYPE_DISTRIBUTED_TASK_RECORD_NODE_COMPLETED:                     {}, //nolint:staticcheck // deprecated but must stay classified for the drift check
	api.ApplyRequest_TYPE_DISTRIBUTED_TASK_CLEAN_UP:                                  {},
	api.ApplyRequest_TYPE_DISTRIBUTED_TASK_RECORD_UNIT_COMPLETED:                     {},
	api.ApplyRequest_TYPE_DISTRIBUTED_TASK_UPDATE_UNIT_PROGRESS:                      {},
	api.ApplyRequest_TYPE_DISTRIBUTED_TASK_MARK_FINALIZED:                            {},
	api.ApplyRequest_TYPE_DISTRIBUTED_TASK_MARK_FAILED:                               {},
	api.ApplyRequest_TYPE_DISTRIBUTED_TASK_RECORD_POST_COMPLETION_ACK:                {},
	api.ApplyRequest_TYPE_DISTRIBUTED_TASK_RECORD_PREPARATION_COMPLETE_ACK:           {},
	api.ApplyRequest_TYPE_CLUSTER_ID_SET:                                             {},
}

// applyTypeBuckets pairs each classification map with its name, so the drift
// check can report which ones a misfiled type appears in.
var applyTypeBuckets = []struct {
	name  string
	types map[api.ApplyRequest_Type]struct{}
}{
	{"requireActiveApplyTypes", requireActiveApplyTypes},
	{"requireActiveProposeTypes", requireActiveProposeTypes},
	{"destructiveApplyTypes", destructiveApplyTypes},
	{"ungatedApplyTypes", ungatedApplyTypes},
}

// TestApplyTypeNamespaceGateClassification fails when an ApplyRequest_Type is
// classified in none of the buckets or in more than one.
func TestApplyTypeNamespaceGateClassification(t *testing.T) {
	for value := range api.ApplyRequest_Type_name {
		applyType := api.ApplyRequest_Type(value)
		if applyType == api.ApplyRequest_TYPE_UNSPECIFIED {
			continue
		}
		var found []string
		for _, b := range applyTypeBuckets {
			if _, ok := b.types[applyType]; ok {
				found = append(found, b.name)
			}
		}
		assert.Len(t, found, 1,
			"apply type %s must appear in exactly one bucket in store_apply_namespace_active_test.go, found %v",
			applyType, found)
	}
}

// seedNamespaceInState creates name and flips it to state. Resuming is only
// reachable from suspended, so it takes two flips.
func seedNamespaceInState(t *testing.T, c *namespaces.Controller, name string, state api.NamespaceState) {
	t.Helper()
	require.NoError(t, c.Create(api.Namespace{Name: name, HomeNodes: []string{"node-1"}}, nsCreateIndex))
	if state == api.NamespaceStateActive {
		return
	}
	if state == api.NamespaceStateResuming {
		require.NoError(t, c.ChangeState(name, api.NamespaceStateSuspended, namespaces.StateChange{AppliedIndex: nsFlipIndex}))
	}
	require.NoError(t, c.ChangeState(name, state, namespaces.StateChange{AppliedIndex: nsFlipIndex}))
}

// seededTenant is the one tenant seedClass gives every class it writes.
const seededTenant = "T1"

// aliasBackingClass is what a seeded alias points at. Distinct from the alias
// itself, which createAlias requires.
const aliasBackingClass = "backing:Target"

// seedClass writes a class straight into the schema instead of applying an
// ADD_CLASS, which RequireActive would refuse once the namespace has been
// flipped to the state the case under test needs. schemaOnly keeps the write
// out of the store, so it adds no expectation a later assertion has to allow.
func seedClass(t *testing.T, ms *MockStore, class string) {
	t.Helper()
	sub, err := json.Marshal(api.AddClassRequest{
		Class: &models.Class{Class: class, MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true}},
		State: &sharding.State{
			// A multi-tenant class partitions by tenant, and SchemaReader.Read
			// rejects a state that says otherwise with an empty Virtual map.
			PartitioningEnabled: true,
			Physical: map[string]sharding.Physical{
				seededTenant: {Name: seededTenant, BelongsToNodes: []string{"Node-1"}, Status: "HOT"},
			},
		},
	})
	require.NoError(t, err)
	ms.parser.On("ParseClass", mock.Anything).Return(nil)
	require.NoError(t, ms.store.schemaManager.AddClass(
		&api.ApplyRequest{Type: api.ApplyRequest_TYPE_ADD_CLASS, Class: class, SubCommand: sub},
		"Node-1", true, false))
}

// seedAlias writes alias into the schema pointing at a backing class in a
// different namespace, which is the case the alias gate has to key on the alias
// name rather than the target. CreateAlias fires the schema callback, so the
// expectation it records is the one a later DELETE_ALIAS reuses.
func seedAlias(t *testing.T, ms *MockStore, alias string) {
	t.Helper()
	seedClass(t, ms, aliasBackingClass)
	sub, err := gproto.Marshal(&api.CreateAliasRequest{Collection: aliasBackingClass, Alias: alias})
	require.NoError(t, err)
	ms.indexer.On("TriggerSchemaUpdateCallbacks").Return()
	require.NoError(t, ms.store.schemaManager.CreateAlias(
		&api.ApplyRequest{Type: api.ApplyRequest_TYPE_CREATE_ALIAS, SubCommand: sub}))
}

// destructiveGateCase varies which namespace a destructive command names and
// what state that namespace is in. The cases that keep alpha suspended while
// naming something else are what fail a gate that refuses whenever any
// namespace is suspended.
type destructiveGateCase struct {
	name    string
	seed    func(*testing.T, *namespaces.Controller)
	target  string
	wantErr error // nil means the check must admit the command
}

func destructiveGateCases() []destructiveGateCase {
	alphaAt := func(state api.NamespaceState) func(*testing.T, *namespaces.Controller) {
		return func(t *testing.T, c *namespaces.Controller) { seedNamespaceInState(t, c, "alpha", state) }
	}
	return []destructiveGateCase{
		{name: "active is applied", seed: alphaAt(api.NamespaceStateActive), target: "alpha:Foo"},
		{
			name: "suspended is refused", seed: alphaAt(api.NamespaceStateSuspended),
			target: "alpha:Foo", wantErr: namespaces.ErrNamespaceSuspended,
		},
		{
			name: "resuming is refused", seed: alphaAt(api.NamespaceStateResuming),
			target: "alpha:Foo", wantErr: namespaces.ErrNamespaceResuming,
		},
		// Refusing this one would stall the cleanup cascade, which issues these
		// same commands while the namespace sits in deleting.
		{name: "deleting is applied", seed: alphaAt(api.NamespaceStateDeleting), target: "alpha:Foo"},
		{name: "missing namespace is applied", seed: func(*testing.T, *namespaces.Controller) {}, target: "alpha:Foo"},
		{
			name: "a suspension elsewhere does not refuse",
			seed: func(t *testing.T, c *namespaces.Controller) {
				seedNamespaceInState(t, c, "alpha", api.NamespaceStateSuspended)
				seedNamespaceInState(t, c, "beta", api.NamespaceStateActive)
			},
			target: "beta:Foo",
		},
		{
			name:   "an unqualified name is not gated",
			seed:   alphaAt(api.NamespaceStateSuspended),
			target: "Foo",
		},
	}
}

// destructiveCommand is one of the three destructive commands, with everything
// needed to drive it through either side of the log and read the outcome back.
type destructiveCommand struct {
	name string
	// build returns the command naming target.
	build func(target string) *api.ApplyRequest
	// seedEntity puts the entity the command destroys into the schema, so both
	// outcomes can be read back off it.
	seedEntity func(t *testing.T, ms *MockStore, target string)
	// expectApplied records the store calls a pass-through makes.
	expectApplied func(t *testing.T, ms *MockStore, target string)
	// stillThere reports whether the entity survived.
	stillThere func(ms *MockStore, target string) bool
}

func destructiveCommands() []destructiveCommand {
	return []destructiveCommand{
		{
			name: "TYPE_DELETE_CLASS",
			build: func(target string) *api.ApplyRequest {
				return &api.ApplyRequest{Type: api.ApplyRequest_TYPE_DELETE_CLASS, Class: target}
			},
			seedEntity: seedClass,
			expectApplied: func(t *testing.T, ms *MockStore, target string) {
				ms.indexer.On("TriggerSchemaUpdateCallbacks").Return()
				ms.indexer.On("DeleteClass", target).Return(nil)
				ms.replicationFSM.On("DeleteReplicationsByCollection", target).Return(nil)
			},
			stillThere: func(ms *MockStore, target string) bool {
				return ms.store.SchemaReader().ClassEqual(target) == target
			},
		},
		{
			name: "TYPE_DELETE_TENANT",
			build: func(target string) *api.ApplyRequest {
				sub, err := gproto.Marshal(&api.DeleteTenantsRequest{Tenants: []string{seededTenant}})
				if err != nil {
					panic(err)
				}
				return &api.ApplyRequest{Type: api.ApplyRequest_TYPE_DELETE_TENANT, Class: target, SubCommand: sub}
			},
			seedEntity: seedClass,
			expectApplied: func(t *testing.T, ms *MockStore, target string) {
				ms.indexer.On("DeleteTenants", target, mock.Anything).Return(nil)
				ms.replicationFSM.On("DeleteReplicationsByTenants", target, []string{seededTenant}).Return(nil)
			},
			stillThere: func(ms *MockStore, target string) bool {
				// Read the sharding state rather than TenantsShards, which
				// retries and filters by activity status.
				var found bool
				_ = ms.store.SchemaReader().Read(target, false,
					func(_ *models.Class, ss *sharding.State) error {
						_, found = ss.Physical[seededTenant]
						return nil
					})
				return found
			},
		},
		{
			name: "TYPE_DELETE_ALIAS",
			build: func(target string) *api.ApplyRequest {
				sub, err := gproto.Marshal(&api.DeleteAliasRequest{Alias: target})
				if err != nil {
					panic(err)
				}
				return &api.ApplyRequest{Type: api.ApplyRequest_TYPE_DELETE_ALIAS, SubCommand: sub}
			},
			seedEntity: seedAlias,
			// seedAlias already records the callback the delete makes.
			expectApplied: func(t *testing.T, ms *MockStore, target string) {},
			stillThere: func(ms *MockStore, target string) bool {
				return ms.store.SchemaReader().ResolveAlias(target) != ""
			},
		},
	}
}

// admitProposeBytes runs the propose-time check on a marshalled command, so a
// test can build commands the same way the apply-side tests do.
func admitProposeBytes(t *testing.T, ms *MockStore, data []byte) error {
	t.Helper()
	req := &api.ApplyRequest{}
	require.NoError(t, gproto.Unmarshal(data, req))
	return ms.store.admitPropose(req)
}

// TestExecuteGate_DestructiveApplyTypes drives the three destructive commands
// through the propose-time check against every namespace state, plus two cases
// that name a namespace other than the suspended one.
func TestExecuteGate_DestructiveApplyTypes(t *testing.T) {
	for _, tt := range destructiveCommands() {
		for _, c := range destructiveGateCases() {
			t.Run(tt.name+"/"+c.name, func(t *testing.T) {
				ms, _ := setupApplyTest(t)
				c.seed(t, ms.cfg.NamespacesController)

				err := ms.store.admitDestructive(tt.build(c.target))
				if c.wantErr != nil {
					require.ErrorIs(t, err, c.wantErr)
					return
				}
				require.NoError(t, err)
			})
		}
	}
}

// TestExecuteGate_RefusesBeforeTheAppend pins that the check runs ahead of
// st.raft.Apply. The mock store has no raft, so a refusal that returns the
// sentinel proves nothing was appended: reaching the append would panic.
func TestExecuteGate_RefusesBeforeTheAppend(t *testing.T) {
	ms, _ := setupApplyTest(t)
	seedNamespaceInState(t, ms.cfg.NamespacesController, "alpha", api.NamespaceStateSuspended)

	_, err := ms.store.Execute(&api.ApplyRequest{
		Type:  api.ApplyRequest_TYPE_DELETE_CLASS,
		Class: "alpha:Foo",
	})
	require.ErrorIs(t, err, namespaces.ErrNamespaceSuspended)
}

// TestApplyGate_DestructiveTypesIgnoreNamespaceState is the guard that keeps the
// destructive checks out of the FSM. Apply must be a pure function of the log,
// so a committed delete has to apply in every namespace state, live and on
// replay. Re-adding a gate to any of these three arms fails this test, and would
// otherwise have an older binary destroy the data an upgraded one keeps.
func TestApplyGate_DestructiveTypesIgnoreNamespaceState(t *testing.T) {
	states := []struct {
		name string
		seed func(*testing.T, *namespaces.Controller)
	}{
		{"active", func(t *testing.T, c *namespaces.Controller) {
			seedNamespaceInState(t, c, "alpha", api.NamespaceStateActive)
		}},
		{"suspended", func(t *testing.T, c *namespaces.Controller) {
			seedNamespaceInState(t, c, "alpha", api.NamespaceStateSuspended)
		}},
		{"resuming", func(t *testing.T, c *namespaces.Controller) {
			seedNamespaceInState(t, c, "alpha", api.NamespaceStateResuming)
		}},
		{"deleting", func(t *testing.T, c *namespaces.Controller) {
			seedNamespaceInState(t, c, "alpha", api.NamespaceStateDeleting)
		}},
		{"missing", func(*testing.T, *namespaces.Controller) {}},
	}

	const target = "alpha:Foo"
	for _, tt := range destructiveCommands() {
		for _, s := range states {
			t.Run(tt.name+"/"+s.name, func(t *testing.T) {
				ms, log := setupApplyTest(t)
				s.seed(t, ms.cfg.NamespacesController)
				tt.seedEntity(t, &ms, target)
				tt.expectApplied(t, &ms, target)

				data, err := gproto.Marshal(tt.build(target))
				require.NoError(t, err)
				log.Data = data

				resp, ok := ms.store.Apply(log).(Response)
				require.True(t, ok)
				require.NoError(t, resp.Error)
				require.False(t, tt.stillThere(&ms, target),
					"a committed delete must apply regardless of namespace state")
			})
		}
	}
}

// TestApplyGate_DestructiveTypesApplyDuringReplay is the same guard on the
// catch-up path, which is where a gate in the FSM did its damage: the entry was
// committed and executed by a binary without the gate, so refusing it on replay
// returns a class whose data is already gone.
func TestApplyGate_DestructiveTypesApplyDuringReplay(t *testing.T) {
	ms, log := setupApplyTest(t)
	seedNamespaceInState(t, ms.cfg.NamespacesController, "alpha", api.NamespaceStateSuspended)
	seedClass(t, &ms, "alpha:Foo")

	// schemaOnly, so the delete touches the schema and leaves the store alone.
	ms.store.lastAppliedIndexToDB.Store(10)
	log.Index = 5
	log.Data = cmdAsBytes("alpha:Foo", api.ApplyRequest_TYPE_DELETE_CLASS, nil, nil)

	resp, ok := ms.store.Apply(log).(Response)
	require.True(t, ok)
	require.NoError(t, resp.Error)
	require.Empty(t, ms.store.SchemaReader().ClassEqual("alpha:Foo"),
		"replay must not resurrect a class the committed entry deleted")
}

func TestSubjectNamespace(t *testing.T) {
	tests := []struct {
		name    string
		subject string
		want    string
		wantErr bool
	}{
		{name: "namespaced db subject", subject: "db:customer1:bob", want: "customer1"},
		// Group subjects are global: no namespace to gate on.
		{name: "group subject", subject: conv.PrefixGroupName("some-group"), want: ""},
		// Unparseable subjects must not be reported as namespace-less, which
		// would skip the gate.
		{name: "no separator", subject: "bob", wantErr: true},
		{name: "empty prefix", subject: ":bob", wantErr: true},
		{name: "empty user", subject: "db:", wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := subjectNamespace(tc.subject)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

// inactiveNamespaceCases seeds alpha in each state that refuses a create-like
// command, with the sentinel that state answers with.
func inactiveNamespaceCases(t *testing.T) []struct {
	name    string
	seed    func(*namespaces.Controller)
	wantErr error
} {
	return []struct {
		name    string
		seed    func(*namespaces.Controller)
		wantErr error
	}{
		{
			name: "deleting namespace rejected with ErrNamespaceDeleting",
			seed: func(c *namespaces.Controller) {
				seedNamespaceInState(t, c, "alpha", api.NamespaceStateDeleting)
			},
			wantErr: namespaces.ErrNamespaceDeleting,
		},
		{
			name: "suspended namespace rejected with ErrNamespaceSuspended",
			seed: func(c *namespaces.Controller) {
				seedNamespaceInState(t, c, "alpha", api.NamespaceStateSuspended)
			},
			wantErr: namespaces.ErrNamespaceSuspended,
		},
		{
			name: "resuming namespace rejected with ErrNamespaceResuming",
			seed: func(c *namespaces.Controller) {
				seedNamespaceInState(t, c, "alpha", api.NamespaceStateResuming)
			},
			wantErr: namespaces.ErrNamespaceResuming,
		},
		{
			name:    "missing namespace rejected with ErrNamespaceGone",
			seed:    func(*namespaces.Controller) {},
			wantErr: namespaces.ErrNamespaceGone,
		},
	}
}

// TestExecuteGate_RejectsCreateLikeApplyTypes drives the create-like commands
// with nothing to materialize through the propose-time check. They are gated
// there rather than in the apply switch, so a committed one applies on every
// binary; see Store.admitPropose.
func TestExecuteGate_RejectsCreateLikeApplyTypes(t *testing.T) {
	tests := []struct {
		name    string
		cmdType api.ApplyRequest_Type
		jsonSub any
		rpcSub  protoreflect.ProtoMessage
	}{
		{
			name:    "TYPE_CREATE_ALIAS",
			cmdType: api.ApplyRequest_TYPE_CREATE_ALIAS,
			rpcSub:  &api.CreateAliasRequest{Collection: "alpha:Foo", Alias: "alpha:Bar"},
		},
		{
			name:    "TYPE_REPLACE_ALIAS",
			cmdType: api.ApplyRequest_TYPE_REPLACE_ALIAS,
			rpcSub:  &api.ReplaceAliasRequest{Collection: "alpha:Foo", Alias: "alpha:Bar"},
		},
		{
			name:    "TYPE_UPSERT_USER",
			cmdType: api.ApplyRequest_TYPE_UPSERT_USER,
			jsonSub: api.CreateUsersRequest{UserId: "bob", Namespace: "alpha"},
		},
	}

	for _, tt := range tests {
		for _, c := range inactiveNamespaceCases(t) {
			t.Run(tt.name+"/"+c.name, func(t *testing.T) {
				ms, _ := setupApplyTest(t)
				c.seed(ms.cfg.NamespacesController)

				err := admitProposeBytes(t, &ms, cmdAsBytes("", tt.cmdType, tt.jsonSub, tt.rpcSub))
				require.ErrorIs(t, err, c.wantErr)
			})
		}
	}
}

// TestApplyGate_CreateLikeTypesIgnoreNamespaceState is the guard that keeps the
// create-like checks with nothing to materialize out of the FSM, matching
// TestApplyGate_DestructiveTypesIgnoreNamespaceState. UPSERT_USER's manager is
// not wired in the mock store, so it is covered by the dynusers tests.
func TestApplyGate_CreateLikeTypesIgnoreNamespaceState(t *testing.T) {
	for _, state := range []api.NamespaceState{
		api.NamespaceStateActive,
		api.NamespaceStateSuspended,
		api.NamespaceStateResuming,
		api.NamespaceStateDeleting,
	} {
		t.Run("TYPE_CREATE_ALIAS/"+string(state), func(t *testing.T) {
			ms, log := setupApplyTest(t)
			seedClass(t, &ms, "alpha:Foo")
			seedNamespaceInState(t, ms.cfg.NamespacesController, "alpha", state)
			ms.indexer.On("TriggerSchemaUpdateCallbacks").Return()

			log.Data = cmdAsBytes("", api.ApplyRequest_TYPE_CREATE_ALIAS, nil,
				&api.CreateAliasRequest{Collection: "alpha:Foo", Alias: "alpha:Bar"})

			resp, ok := ms.store.Apply(log).(Response)
			require.True(t, ok)
			require.NoError(t, resp.Error)
			require.Equal(t, "alpha:Foo", ms.store.SchemaReader().ResolveAlias("alpha:Bar"),
				"a committed create must apply regardless of namespace state")
		})
	}
}

// TestApplyGate_RejectsGatedSchemaApplyTypes drives each gate still in the apply
// switch and asserts deleting/missing namespaces are rejected. These four stay
// there because they materialize shards: a propose-time refusal cannot stop one
// that lands while the namespace keeps its shards closed from leaving a schema
// entry with nothing behind it.
func TestApplyGate_RejectsGatedSchemaApplyTypes(t *testing.T) {
	cls := func(name string) *models.Class {
		return &models.Class{
			Class:              name,
			MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true},
		}
	}
	ss := &sharding.State{Physical: map[string]sharding.Physical{
		"T1": {Name: "T1", BelongsToNodes: []string{"Node-1"}, Status: "HOT"},
	}}

	tests := []struct {
		name    string
		cmdType api.ApplyRequest_Type
		jsonSub any
		rpcSub  protoreflect.ProtoMessage
	}{
		{
			name:    "TYPE_ADD_CLASS",
			cmdType: api.ApplyRequest_TYPE_ADD_CLASS,
			jsonSub: api.AddClassRequest{Class: cls("alpha:Foo"), State: ss},
		},
		{
			name:    "TYPE_RESTORE_CLASS",
			cmdType: api.ApplyRequest_TYPE_RESTORE_CLASS,
			jsonSub: api.AddClassRequest{Class: cls("alpha:Foo"), State: ss},
		},
		{
			// The schema commits before the DB refuses the shard, so an ungated
			// create leaves the tenant listed with nothing behind it.
			name:    "TYPE_ADD_TENANT",
			cmdType: api.ApplyRequest_TYPE_ADD_TENANT,
			rpcSub: &api.AddTenantsRequest{
				Tenants:      []*api.Tenant{{Name: "T2", Status: models.TenantActivityStatusHOT}},
				ClusterNodes: []string{"Node-1"},
			},
		},
		{
			// A freeze started here would abort against a status no node can
			// read back, silently activating or deactivating the tenant.
			name:    "TYPE_UPDATE_TENANT",
			cmdType: api.ApplyRequest_TYPE_UPDATE_TENANT,
			rpcSub: &api.UpdateTenantsRequest{
				Tenants:      []*api.Tenant{{Name: "T1", Status: models.TenantActivityStatusFROZEN}},
				ClusterNodes: []string{"Node-1"},
			},
		},
	}

	cases := []struct {
		name      string
		seed      func(*namespaces.Controller)
		wantErr   error
		className string
	}{
		{
			name:      "deleting namespace rejected with ErrNamespaceDeleting",
			className: "alpha:Foo",
			seed: func(c *namespaces.Controller) {
				require.NoError(t, c.Create(api.Namespace{Name: "alpha", HomeNodes: []string{"node-1"}}, nsCreateIndex))
				require.NoError(t, c.ChangeState("alpha", api.NamespaceStateDeleting, namespaces.StateChange{AppliedIndex: nsFlipIndex}))
			},
			wantErr: namespaces.ErrNamespaceDeleting,
		},
		{
			name:      "suspended namespace rejected with ErrNamespaceSuspended",
			className: "alpha:Foo",
			seed: func(c *namespaces.Controller) {
				require.NoError(t, c.Create(api.Namespace{Name: "alpha", HomeNodes: []string{"node-1"}}, nsCreateIndex))
				require.NoError(t, c.ChangeState("alpha", api.NamespaceStateSuspended, namespaces.StateChange{AppliedIndex: nsFlipIndex}))
			},
			wantErr: namespaces.ErrNamespaceSuspended,
		},
		{
			name:      "resuming namespace rejected with ErrNamespaceResuming",
			className: "alpha:Foo",
			seed: func(c *namespaces.Controller) {
				require.NoError(t, c.Create(api.Namespace{Name: "alpha", HomeNodes: []string{"node-1"}}, nsCreateIndex))
				require.NoError(t, c.ChangeState("alpha", api.NamespaceStateSuspended, namespaces.StateChange{AppliedIndex: nsFlipIndex}))
				require.NoError(t, c.ChangeState("alpha", api.NamespaceStateResuming, namespaces.StateChange{AppliedIndex: 2}))
			},
			wantErr: namespaces.ErrNamespaceResuming,
		},
		{
			name:      "missing namespace rejected with ErrNamespaceGone",
			className: "alpha:Foo",
			seed:      func(c *namespaces.Controller) {},
			wantErr:   namespaces.ErrNamespaceGone,
		},
	}

	for _, tt := range tests {
		for _, c := range cases {
			t.Run(tt.name+"/"+c.name, func(t *testing.T) {
				ms, log := setupApplyTest(t)
				c.seed(ms.cfg.NamespacesController)

				log.Data = cmdAsBytes(c.className, tt.cmdType, tt.jsonSub, tt.rpcSub)

				result := ms.store.Apply(log)
				resp, ok := result.(Response)
				require.True(t, ok)
				require.ErrorIs(t, resp.Error, c.wantErr)
			})
		}
	}
}

// TestApplyGate_PassesActiveNamespace asserts the gate doesn't reject when
// the namespace is active.
func TestApplyGate_PassesActiveNamespace(t *testing.T) {
	cls := &models.Class{
		Class:              "alpha:Foo",
		MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true},
	}
	ss := &sharding.State{Physical: map[string]sharding.Physical{
		"T1": {Name: "T1", BelongsToNodes: []string{"Node-1"}, Status: "HOT"},
	}}

	tests := []struct {
		name    string
		cmdType api.ApplyRequest_Type
		jsonSub any
		rpcSub  protoreflect.ProtoMessage
	}{
		{
			name:    "TYPE_ADD_CLASS",
			cmdType: api.ApplyRequest_TYPE_ADD_CLASS,
			jsonSub: api.AddClassRequest{Class: cls, State: ss},
		},
		{
			name:    "TYPE_ADD_TENANT",
			cmdType: api.ApplyRequest_TYPE_ADD_TENANT,
			rpcSub: &api.AddTenantsRequest{
				Tenants:      []*api.Tenant{{Name: "T2", Status: models.TenantActivityStatusHOT}},
				ClusterNodes: []string{"Node-1"},
			},
		},
		{
			name:    "TYPE_UPDATE_TENANT",
			cmdType: api.ApplyRequest_TYPE_UPDATE_TENANT,
			rpcSub: &api.UpdateTenantsRequest{
				Tenants:      []*api.Tenant{{Name: "T1", Status: models.TenantActivityStatusFROZEN}},
				ClusterNodes: []string{"Node-1"},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ms, log := setupApplyTest(t)
			require.NoError(t, ms.cfg.NamespacesController.Create(api.Namespace{Name: "alpha", HomeNodes: []string{"node-1"}}, nsCreateIndex))

			log.Data = cmdAsBytes("alpha:Foo", tc.cmdType, tc.jsonSub, tc.rpcSub)

			result := ms.store.Apply(log)
			resp, ok := result.(Response)
			require.True(t, ok)
			require.NotErrorIs(t, resp.Error, namespaces.ErrNamespaceDeleting)
			require.NotErrorIs(t, resp.Error, namespaces.ErrNamespaceGone)
			require.NotErrorIs(t, resp.Error, namespaces.ErrNamespaceSuspended)
			require.NotErrorIs(t, resp.Error, namespaces.ErrNamespaceResuming)
		})
	}
}

// TestExecuteGate_RejectsRoleCreationIntoInactiveNamespace drives the role-
// upsert gate through the propose-time check: a namespaced role can't be minted
// into a deleting or missing namespace. Permission-only upserts
// (RoleCreation=false) re-mint the role row too, so they're gated as well;
// global (unqualified) roles always pass.
func TestExecuteGate_RejectsRoleCreationIntoInactiveNamespace(t *testing.T) {
	roleCmd := func(name string, creation bool) []byte {
		return cmdAsBytes("", api.ApplyRequest_TYPE_UPSERT_ROLES_PERMISSIONS,
			api.CreateRolesRequest{
				Roles:        map[string][]authorization.Policy{name: nil},
				RoleCreation: creation,
			}, nil)
	}

	tests := []struct {
		name     string
		seed     func(*namespaces.Controller)
		role     string
		creation bool
		wantErr  error
	}{
		{
			name: "create into deleting namespace rejected",
			seed: func(c *namespaces.Controller) {
				require.NoError(t, c.Create(api.Namespace{Name: "alpha", HomeNodes: []string{"node-1"}}, nsCreateIndex))
				require.NoError(t, c.ChangeState("alpha", api.NamespaceStateDeleting, namespaces.StateChange{AppliedIndex: nsFlipIndex}))
			},
			role:     "alpha:editor",
			creation: true,
			wantErr:  namespaces.ErrNamespaceDeleting,
		},
		{
			name:     "create into missing namespace rejected",
			seed:     func(c *namespaces.Controller) {},
			role:     "alpha:editor",
			creation: true,
			wantErr:  namespaces.ErrNamespaceGone,
		},
		{
			name: "create into active namespace passes gate",
			seed: func(c *namespaces.Controller) {
				require.NoError(t, c.Create(api.Namespace{Name: "alpha", HomeNodes: []string{"node-1"}}, nsCreateIndex))
			},
			role:     "alpha:editor",
			creation: true,
		},
		{
			name:     "global role create passes gate",
			seed:     func(c *namespaces.Controller) {},
			role:     "editor",
			creation: true,
		},
		{
			name: "non-creation upsert into deleting namespace rejected",
			seed: func(c *namespaces.Controller) {
				require.NoError(t, c.Create(api.Namespace{Name: "alpha", HomeNodes: []string{"node-1"}}, nsCreateIndex))
				require.NoError(t, c.ChangeState("alpha", api.NamespaceStateDeleting, namespaces.StateChange{AppliedIndex: nsFlipIndex}))
			},
			role:     "alpha:editor",
			creation: false,
			wantErr:  namespaces.ErrNamespaceDeleting,
		},
		{
			name:     "non-creation upsert into missing namespace rejected",
			seed:     func(c *namespaces.Controller) {},
			role:     "alpha:editor",
			creation: false,
			wantErr:  namespaces.ErrNamespaceGone,
		},
		{
			name: "non-creation upsert into active namespace passes gate",
			seed: func(c *namespaces.Controller) {
				require.NoError(t, c.Create(api.Namespace{Name: "alpha", HomeNodes: []string{"node-1"}}, nsCreateIndex))
			},
			role:     "alpha:editor",
			creation: false,
		},
		{
			name:     "global non-creation upsert passes gate",
			seed:     func(c *namespaces.Controller) {},
			role:     "editor",
			creation: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ms, _ := setupApplyTest(t)
			tc.seed(ms.cfg.NamespacesController)

			err := admitProposeBytes(t, &ms, roleCmd(tc.role, tc.creation))
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
		})
	}
}

// TestExecuteGate_RejectsMixedRoleBatchWithInactiveNamespace verifies the check
// loops over every name in a multi-role upsert: a single namespaced-inactive
// name rejects the whole batch even when other names are global or active.
func TestExecuteGate_RejectsMixedRoleBatchWithInactiveNamespace(t *testing.T) {
	ms, _ := setupApplyTest(t)
	require.NoError(t, ms.cfg.NamespacesController.Create(api.Namespace{Name: "alpha", HomeNodes: []string{"node-1"}}, nsCreateIndex))
	require.NoError(t, ms.cfg.NamespacesController.ChangeState("alpha", api.NamespaceStateDeleting, namespaces.StateChange{AppliedIndex: nsFlipIndex}))

	err := admitProposeBytes(t, &ms, cmdAsBytes("", api.ApplyRequest_TYPE_UPSERT_ROLES_PERMISSIONS,
		api.CreateRolesRequest{
			Roles: map[string][]authorization.Policy{
				"editor":       nil,
				"alpha:editor": nil,
			},
			RoleCreation: false,
		}, nil))
	require.ErrorIs(t, err, namespaces.ErrNamespaceDeleting)
}

// TestExecuteGate_RejectsRoleAssignmentIntoInactiveNamespace drives the role-
// assignment gate through the propose-time check: a role can't be assigned to a
// subject in a deleting or missing namespace, otherwise a late assignment would
// leave a grouping row behind after the cleanup cascade emptied the namespace.
// OIDC subjects are gated too (their handler-side existence check is a no-op),
// and global (unqualified) subjects are not gated.
func TestExecuteGate_RejectsRoleAssignmentIntoInactiveNamespace(t *testing.T) {
	assignCmd := func(user string) []byte {
		return cmdAsBytes("", api.ApplyRequest_TYPE_ADD_ROLES_FOR_USER,
			api.AddRolesForUsersRequest{User: user, Roles: []string{"editor"}}, nil)
	}

	tests := []struct {
		name    string
		seed    func(*namespaces.Controller)
		user    string
		wantErr error
	}{
		{
			name: "assign into deleting namespace rejected",
			seed: func(c *namespaces.Controller) {
				require.NoError(t, c.Create(api.Namespace{Name: "alpha", HomeNodes: []string{"node-1"}}, nsCreateIndex))
				require.NoError(t, c.ChangeState("alpha", api.NamespaceStateDeleting, namespaces.StateChange{AppliedIndex: nsFlipIndex}))
			},
			user:    "db:alpha:bob",
			wantErr: namespaces.ErrNamespaceDeleting,
		},
		{
			name:    "assign into missing namespace rejected",
			seed:    func(c *namespaces.Controller) {},
			user:    "db:alpha:bob",
			wantErr: namespaces.ErrNamespaceGone,
		},
		{
			name: "assign to oidc subject in deleting namespace rejected",
			seed: func(c *namespaces.Controller) {
				require.NoError(t, c.Create(api.Namespace{Name: "alpha", HomeNodes: []string{"node-1"}}, nsCreateIndex))
				require.NoError(t, c.ChangeState("alpha", api.NamespaceStateDeleting, namespaces.StateChange{AppliedIndex: nsFlipIndex}))
			},
			user:    "oidc:alpha:carol",
			wantErr: namespaces.ErrNamespaceDeleting,
		},
		{
			name: "assign into active namespace passes gate",
			seed: func(c *namespaces.Controller) {
				require.NoError(t, c.Create(api.Namespace{Name: "alpha", HomeNodes: []string{"node-1"}}, nsCreateIndex))
			},
			user: "db:alpha:bob",
		},
		{
			name: "global subject passes gate",
			seed: func(c *namespaces.Controller) {},
			user: "db:bob",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ms, _ := setupApplyTest(t)
			tc.seed(ms.cfg.NamespacesController)

			err := admitProposeBytes(t, &ms, assignCmd(tc.user))
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
		})
	}
}

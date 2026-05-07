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
//

package cluster

import (
	"testing"

	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/reflect/protoreflect"

	clusternamespaces "github.com/weaviate/weaviate/cluster/namespaces"
	"github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/namespaces"
	"github.com/weaviate/weaviate/usecases/sharding"
)

// namespaceTouchingCreateLikeApplyTypes lists apply types gated by
// requireNamespaceActive. New create-like types that own namespace-bound
// state must be added here; the drift test fails otherwise.
var namespaceTouchingCreateLikeApplyTypes = map[api.ApplyRequest_Type]struct{}{
	api.ApplyRequest_TYPE_ADD_CLASS:     {},
	api.ApplyRequest_TYPE_RESTORE_CLASS: {},
	api.ApplyRequest_TYPE_CREATE_ALIAS:  {},
	api.ApplyRequest_TYPE_REPLACE_ALIAS: {},
	api.ApplyRequest_TYPE_UPSERT_USER:   {},
}

// nonNamespaceTouchingApplyTypes lists apply types the gate must not
// reject — either they don't mint namespace-owned state or they are the
// namespace lifecycle commands themselves.
var nonNamespaceTouchingApplyTypes = map[api.ApplyRequest_Type]struct{}{
	api.ApplyRequest_TYPE_UPDATE_CLASS:                                               {},
	api.ApplyRequest_TYPE_DELETE_CLASS:                                               {},
	api.ApplyRequest_TYPE_ADD_PROPERTY:                                               {},
	api.ApplyRequest_TYPE_UPDATE_PROPERTY:                                            {},
	api.ApplyRequest_TYPE_UPDATE_SHARD_STATUS:                                        {},
	api.ApplyRequest_TYPE_ADD_REPLICA_TO_SHARD:                                       {},
	api.ApplyRequest_TYPE_DELETE_REPLICA_FROM_SHARD:                                  {},
	api.ApplyRequest_TYPE_ADD_TENANT:                                                 {},
	api.ApplyRequest_TYPE_UPDATE_TENANT:                                              {},
	api.ApplyRequest_TYPE_DELETE_TENANT:                                              {},
	api.ApplyRequest_TYPE_TENANT_PROCESS:                                             {},
	api.ApplyRequest_TYPE_DELETE_ALIAS:                                               {},
	api.ApplyRequest_TYPE_UPSERT_ROLES_PERMISSIONS:                                   {},
	api.ApplyRequest_TYPE_DELETE_ROLES:                                               {},
	api.ApplyRequest_TYPE_REMOVE_PERMISSIONS:                                         {},
	api.ApplyRequest_TYPE_ADD_ROLES_FOR_USER:                                         {},
	api.ApplyRequest_TYPE_REVOKE_ROLES_FOR_USER:                                      {},
	api.ApplyRequest_TYPE_DELETE_USER:                                                {},
	api.ApplyRequest_TYPE_ROTATE_USER_API_KEY:                                        {},
	api.ApplyRequest_TYPE_SUSPEND_USER:                                               {},
	api.ApplyRequest_TYPE_ACTIVATE_USER:                                              {},
	api.ApplyRequest_TYPE_CREATE_USER_WITH_KEY:                                       {},
	api.ApplyRequest_TYPE_DELETE_USERS_IN_NAMESPACE:                                  {},
	api.ApplyRequest_TYPE_ADD_NAMESPACE:                                              {},
	api.ApplyRequest_TYPE_DELETE_NAMESPACE:                                           {},
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
	api.ApplyRequest_TYPE_REPLICATION_REPLICATE_SYNC_SHARD:                           {},
	api.ApplyRequest_TYPE_REPLICATION_REGISTER_SCHEMA_VERSION:                        {},
	api.ApplyRequest_TYPE_REPLICATION_REPLICATE_ADD_REPLICA_TO_SHARD:                 {},
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
}

// TestApplyTypeNamespaceGateClassification fails when a new
// ApplyRequest_Type value isn't classified in exactly one of the two lists.
func TestApplyTypeNamespaceGateClassification(t *testing.T) {
	for value := range api.ApplyRequest_Type_name {
		applyType := api.ApplyRequest_Type(value)
		if applyType == api.ApplyRequest_TYPE_UNSPECIFIED {
			continue
		}
		_, gated := namespaceTouchingCreateLikeApplyTypes[applyType]
		_, nonGated := nonNamespaceTouchingApplyTypes[applyType]
		assert.True(t, gated != nonGated,
			"unclassified or double-classified apply type %s: must appear in exactly one of the two lists in store_apply_namespace_active_test.go",
			applyType)
	}
}

func TestRequireNamespaceActive(t *testing.T) {
	logger, _ := logrustest.NewNullLogger()
	controller := namespaces.NewController(logger)
	require.NoError(t, controller.Create(api.Namespace{Name: "active1"}))
	require.NoError(t, controller.Create(api.Namespace{Name: "deleting1"}))
	require.NoError(t, controller.ChangeState("deleting1", api.NamespaceStateDeleting))
	exister := clusternamespaces.NewManager(controller, emptySchemaLister{}, nil, logger)

	tests := []struct {
		name      string
		namespace string
		wantErr   error
	}{
		{name: "empty namespace passes", namespace: ""},
		{name: "active namespace passes", namespace: "active1"},
		{name: "deleting namespace returns ErrNamespaceDeleting", namespace: "deleting1", wantErr: namespaces.ErrNamespaceDeleting},
		{name: "missing namespace returns ErrNamespaceGone", namespace: "never-existed", wantErr: namespaces.ErrNamespaceGone},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := requireNamespaceActive(exister, tc.namespace)
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestNamespaceFromQualified(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{name: "no separator returns empty", in: "MyClass", want: ""},
		{name: "qualified name returns namespace", in: "alpha:MyClass", want: "alpha"},
		{name: "empty input returns empty", in: "", want: ""},
		{name: "leading separator returns empty namespace prefix", in: ":MyClass", want: ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, namespaceFromQualified(tc.in))
		})
	}
}

// TestApplyGate_RejectsCreateLikeApplyTypes drives each schema/alias gate
// through the live apply switch and asserts deleting/missing namespaces
// are rejected. TYPE_UPSERT_USER is covered by the dynusers manager tests.
func TestApplyGate_RejectsCreateLikeApplyTypes(t *testing.T) {
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
			name:    "TYPE_CREATE_ALIAS",
			cmdType: api.ApplyRequest_TYPE_CREATE_ALIAS,
			rpcSub:  &api.CreateAliasRequest{Collection: "alpha:Foo", Alias: "alpha:Bar"},
		},
		{
			name:    "TYPE_REPLACE_ALIAS",
			cmdType: api.ApplyRequest_TYPE_REPLACE_ALIAS,
			rpcSub:  &api.ReplaceAliasRequest{Collection: "alpha:Foo", Alias: "alpha:Bar"},
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
				require.NoError(t, c.Create(api.Namespace{Name: "alpha"}))
				require.NoError(t, c.ChangeState("alpha", api.NamespaceStateDeleting))
			},
			wantErr: namespaces.ErrNamespaceDeleting,
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
	ms, log := setupApplyTest(t)
	require.NoError(t, ms.cfg.NamespacesController.Create(api.Namespace{Name: "alpha"}))

	cls := &models.Class{
		Class:              "alpha:Foo",
		MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true},
	}
	ss := &sharding.State{Physical: map[string]sharding.Physical{
		"T1": {Name: "T1", BelongsToNodes: []string{"Node-1"}, Status: "HOT"},
	}}
	log.Data = cmdAsBytes("alpha:Foo", api.ApplyRequest_TYPE_ADD_CLASS,
		api.AddClassRequest{Class: cls, State: ss}, nil)

	result := ms.store.Apply(log)
	resp, ok := result.(Response)
	require.True(t, ok)
	require.NotErrorIs(t, resp.Error, namespaces.ErrNamespaceDeleting)
	require.NotErrorIs(t, resp.Error, namespaces.ErrNamespaceGone)
}

type emptySchemaLister struct{}

func (emptySchemaLister) ClassesInNamespace(string) []string { return nil }
func (emptySchemaLister) AliasesInNamespace(string) []string { return nil }

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
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/auth/authorization/mocks"
)

// A component-test like test suite that makes sure that every available UC is
// potentially protected with the Authorization plugin
func Test_Schema_Authorization(t *testing.T) {
	type testCase struct {
		methodName        string
		additionalArgs    []any
		expectedVerb      string
		expectedResources []string
	}

	tests := []testCase{
		{
			methodName:        "GetClass",
			additionalArgs:    []any{"classname"},
			expectedVerb:      authorization.READ,
			expectedResources: authorization.CollectionsMetadata("classname"),
		},
		{
			methodName:        "GetConsistentClass",
			additionalArgs:    []any{"classname", false},
			expectedVerb:      authorization.READ,
			expectedResources: authorization.CollectionsMetadata("classname"),
		},
		{
			methodName:        "GetCachedClass",
			additionalArgs:    []any{"classname"},
			expectedVerb:      authorization.READ,
			expectedResources: authorization.CollectionsMetadata("classname"),
		},
		{
			methodName:        "AddClass",
			additionalArgs:    []any{&models.Class{Class: "classname"}},
			expectedVerb:      authorization.CREATE,
			expectedResources: authorization.CollectionsMetadata("Classname"),
		},
		{
			methodName:        "UpdateClass",
			additionalArgs:    []any{"class", &models.Class{Class: "class"}},
			expectedVerb:      authorization.UPDATE,
			expectedResources: authorization.CollectionsMetadata("class"),
		},
		{
			methodName:        "DeleteClass",
			additionalArgs:    []any{"somename"},
			expectedVerb:      authorization.DELETE,
			expectedResources: authorization.CollectionsMetadata("somename"),
		},
		{
			methodName:        "AddClassProperty",
			additionalArgs:    []any{&models.Class{Class: "classname"}, "classname", false, &models.Property{}},
			expectedVerb:      authorization.UPDATE,
			expectedResources: authorization.CollectionsMetadata("classname"),
		},
		{
			methodName:        "DeleteClassProperty",
			additionalArgs:    []any{"somename", "someprop"},
			expectedVerb:      authorization.UPDATE,
			expectedResources: authorization.CollectionsMetadata("somename"),
		},
		{
			methodName:        "DeleteClassPropertyIndex",
			additionalArgs:    []any{&models.Class{Class: "classname"}, "classname", "someprop", &models.DeletePropertyIndexRequest{}},
			expectedVerb:      authorization.UPDATE,
			expectedResources: authorization.CollectionsMetadata("classname"),
		},
		{
			methodName:        "UpdateShardStatus",
			additionalArgs:    []any{"className", "shardName", "targetStatus"},
			expectedVerb:      authorization.UPDATE,
			expectedResources: authorization.ShardsMetadata("className", "shardName"),
		},
		{
			methodName:        "ShardsStatus",
			additionalArgs:    []any{"className", "tenant"},
			expectedVerb:      authorization.READ,
			expectedResources: authorization.ShardsMetadata("className", "tenant"),
		},
		{
			methodName:        "AddTenants",
			additionalArgs:    []any{"className", []*models.Tenant{{Name: "P1"}}},
			expectedVerb:      authorization.CREATE,
			expectedResources: authorization.ShardsMetadata("className", "P1"),
		},
		{
			methodName: "UpdateTenants",
			additionalArgs: []any{"className", []*models.Tenant{
				{Name: "P1", ActivityStatus: models.TenantActivityStatusHOT},
			}},
			expectedVerb:      authorization.UPDATE,
			expectedResources: authorization.ShardsMetadata("className", "P1"),
		},
		{
			methodName:        "DeleteTenants",
			additionalArgs:    []any{"className", []string{"P1"}},
			expectedVerb:      authorization.DELETE,
			expectedResources: authorization.ShardsMetadata("className", "P1"),
		},
		{
			methodName:        "ConsistentTenantExists",
			additionalArgs:    []any{"className", false, "P1"},
			expectedVerb:      authorization.READ,
			expectedResources: authorization.ShardsMetadata("className", "P1"),
		},
		{
			methodName:        "AddAlias",
			additionalArgs:    []any{&models.Alias{Class: "classname", Alias: "aliasName"}},
			expectedVerb:      authorization.CREATE,
			expectedResources: authorization.Aliases("Classname", "AliasName"),
		},
		{
			methodName:        "UpdateAlias",
			additionalArgs:    []any{"aliasName", "class"},
			expectedVerb:      authorization.UPDATE,
			expectedResources: authorization.Aliases("class", "aliasName"),
		},
		{
			methodName:        "DeleteAlias",
			additionalArgs:    []any{"aliasName"},
			expectedVerb:      authorization.DELETE,
			expectedResources: authorization.Aliases("class", "aliasName"),
		},
		{
			methodName:        "GetAlias",
			additionalArgs:    []any{"aliasName"},
			expectedVerb:      authorization.READ,
			expectedResources: authorization.Aliases("class", "aliasName"),
		},
	}

	t.Run("verify that a test for every public method exists", func(t *testing.T) {
		testedMethods := make([]string, len(tests))
		for i, test := range tests {
			testedMethods[i] = test.methodName
		}

		for _, method := range allExportedMethods(&Handler{classGetter: nil}) {
			switch method {
			case "RegisterSchemaUpdateCallback",
				// introduced by sync.Mutex in go 1.18
				"UpdateMeta", "GetSchemaSkipAuth", "IndexedInverted", "RLock", "RUnlock", "Lock", "Unlock",
				"TryLock", "RLocker", "TryRLock", "TxManager", "RestoreClass",
				"ShardOwner", "TenantShard", "ShardFromUUID", "LockGuard", "RLockGuard", "ShardReplicas",
				"GetCachedClassNoAuth",
				// internal methods to indicate readiness state
				"StartServing", "Shutdown", "Statistics",
				// Cluster/nodes related endpoint
				"JoinNode", "RemoveNode", "Nodes", "NodeName", "ClusterHealthScore", "ClusterStatus", "ResolveParentNodes",
				// revert to schema v0 (non raft),
				"GetConsistentSchema", "GetConsistentTenants", "GetConsistentTenant", "GetAliases":
				// don't require auth on methods which are exported because other
				// packages need to call them for maintenance and other regular jobs,
				// but aren't user facing
				continue
			}
			assert.Contains(t, testedMethods, method)
		}
	})

	t.Run("verify the tested methods require correct permissions from the Authorizer", func(t *testing.T) {
		principal := &models.Principal{}
		for _, test := range tests {
			t.Run(test.methodName, func(t *testing.T) {
				authorizer := mocks.NewMockAuthorizer()
				authorizer.SetErr(errors.New("just a test fake"))
				handler, fakeSchemaManager := newTestHandlerWithCustomAuthorizer(t, &fakeDB{}, authorizer)
				fakeSchemaManager.On("ReadOnlySchema").Return(models.Schema{})
				fakeSchemaManager.On("ReadOnlyClass", mock.Anything).Return(models.Class{})
				fakeSchemaManager.On("GetAliases", mock.Anything, mock.Anything, mock.Anything).Return([]*models.Alias{{}}, nil)
				// NOTE: When user invoking GetAlias by name, the collection is unknown.
				// So we get the right alias (if exists) and use the collection that alias belongs to
				// to verify the permission
				fakeSchemaManager.On("GetAlias", mock.Anything, mock.Anything).Return(&models.Alias{Alias: "aliasName", Class: "class"}, nil)

				var args []any
				if test.methodName == "GetSchema" || test.methodName == "GetConsistentSchema" {
					// no context on this method
					args = append([]any{principal}, test.additionalArgs...)
				} else {
					args = append([]any{context.Background(), principal}, test.additionalArgs...)
				}
				out, _ := callFuncByName(handler, test.methodName, args...)

				require.Len(t, authorizer.Calls(), 1, "Authorizer must be called")
				assert.Equal(t, errors.New("just a test fake"), out[len(out)-1].Interface(),
					"execution must abort with Authorizer error")
				assert.Equal(t, mocks.AuthZReq{Principal: principal, Verb: test.expectedVerb, Resources: test.expectedResources},
					authorizer.Calls()[0], "correct parameters must have been used on Authorizer")
			})
		}
	})
}

// inspired by https://stackoverflow.com/a/33008200
func callFuncByName(manager any, funcName string, params ...any) (out []reflect.Value, err error) {
	managerValue := reflect.ValueOf(manager)
	m := managerValue.MethodByName(funcName)
	if !m.IsValid() {
		return make([]reflect.Value, 0), fmt.Errorf("Method not found \"%s\"", funcName)
	}
	in := make([]reflect.Value, len(params))
	for i, param := range params {
		in[i] = reflect.ValueOf(param)
	}
	out = m.Call(in)
	return out, err
}

func allExportedMethods(subject any) []string {
	var methods []string
	subjectType := reflect.TypeOf(subject)
	for i := 0; i < subjectType.NumMethod(); i++ {
		name := subjectType.Method(i).Name
		if name[0] >= 'A' && name[0] <= 'Z' {
			methods = append(methods, name)
		}
	}

	return methods
}

// Test_Schema_Authorization_AliasResolution tests the specific bug fix where
// authorization should happen on resolved collection names, not alias names
func Test_Schema_Authorization_AliasResolution(t *testing.T) {
	principal := &models.Principal{}
	aliasName := "MyAlias"
	resolvedClassName := "ResolvedClass"
	shardName := "shard1"

	t.Run("GetConsistentClass - authorization uses resolved class name, not alias", func(t *testing.T) {
		authorizer := mocks.NewMockAuthorizer()
		handler, fakeSchemaManager := newTestHandlerWithCustomAuthorizer(t, &fakeDB{}, authorizer)

		// Mock class retrieval
		fakeSchemaManager.On("ReadOnlyClassWithVersion", mock.Anything, resolvedClassName, mock.Anything).Return(&models.Class{Class: resolvedClassName}, nil)

		// Create custom schema manager with alias resolution
		fakeSchemaManagerWithAlias := &fakeSchemaManagerWithAlias{
			fakeSchemaManager: fakeSchemaManager,
			aliasMap:          map[string]string{aliasName: resolvedClassName},
		}
		handler.schemaReader = fakeSchemaManagerWithAlias

		_, _, err := handler.GetConsistentClass(context.Background(), principal, aliasName, false)
		require.NoError(t, err)

		// Verify authorization was called with resolved class name, not alias
		require.Len(t, authorizer.Calls(), 1, "Authorizer must be called exactly once")
		authCall := authorizer.Calls()[0]
		assert.Equal(t, principal, authCall.Principal)
		assert.Equal(t, authorization.READ, authCall.Verb)
		assert.Equal(t, authorization.CollectionsMetadata(resolvedClassName), authCall.Resources,
			"Authorization should use resolved class name '%s', not alias '%s'", resolvedClassName, aliasName)

		fakeSchemaManager.AssertExpectations(t)
	})

	t.Run("ShardsStatus - authorization uses resolved class name, not alias", func(t *testing.T) {
		authorizer := mocks.NewMockAuthorizer()
		handler, fakeSchemaManager := newTestHandlerWithCustomAuthorizer(t, &fakeDB{}, authorizer)

		// Mock shard status retrieval
		expectedStatus := models.ShardStatusList{
			&models.ShardStatusGetResponse{
				Name:   shardName,
				Status: "READY",
			},
		}
		fakeSchemaManager.On("GetShardsStatus", resolvedClassName, shardName).Return(expectedStatus, nil)

		// Create custom schema manager with alias resolution
		fakeSchemaManagerWithAlias := &fakeSchemaManagerWithAlias{
			fakeSchemaManager: fakeSchemaManager,
			aliasMap:          map[string]string{aliasName: resolvedClassName},
		}
		handler.schemaReader = fakeSchemaManagerWithAlias

		_, err := handler.ShardsStatus(context.Background(), principal, aliasName, shardName)
		require.NoError(t, err)

		// Verify authorization was called with resolved class name, not alias
		require.Len(t, authorizer.Calls(), 1, "Authorizer must be called exactly once")
		authCall := authorizer.Calls()[0]
		assert.Equal(t, principal, authCall.Principal)
		assert.Equal(t, authorization.READ, authCall.Verb)
		assert.Equal(t, authorization.ShardsMetadata(resolvedClassName, shardName), authCall.Resources,
			"Authorization should use resolved class name '%s', not alias '%s'", resolvedClassName, aliasName)

		fakeSchemaManager.AssertExpectations(t)
	})

	t.Run("GetConsistentClass - authorization uses original class name when no alias resolution", func(t *testing.T) {
		authorizer := mocks.NewMockAuthorizer()
		handler, fakeSchemaManager := newTestHandlerWithCustomAuthorizer(t, &fakeDB{}, authorizer)

		className := "DirectClassName"

		// Mock class retrieval
		fakeSchemaManager.On("ReadOnlyClassWithVersion", mock.Anything, className, mock.Anything).Return(&models.Class{Class: className}, nil)

		// Create custom schema manager without alias resolution
		fakeSchemaManagerWithAlias := &fakeSchemaManagerWithAlias{
			fakeSchemaManager: fakeSchemaManager,
			aliasMap:          map[string]string{}, // empty - no aliases
		}
		handler.schemaReader = fakeSchemaManagerWithAlias

		_, _, err := handler.GetConsistentClass(context.Background(), principal, className, false)
		require.NoError(t, err)

		// Verify authorization was called with the original class name
		require.Len(t, authorizer.Calls(), 1, "Authorizer must be called exactly once")
		authCall := authorizer.Calls()[0]
		assert.Equal(t, principal, authCall.Principal)
		assert.Equal(t, authorization.READ, authCall.Verb)
		assert.Equal(t, authorization.CollectionsMetadata(className), authCall.Resources,
			"Authorization should use class name '%s' when no alias resolution", className)

		fakeSchemaManager.AssertExpectations(t)
	})

	t.Run("ShardsStatus - authorization uses original class name when no alias resolution", func(t *testing.T) {
		authorizer := mocks.NewMockAuthorizer()
		handler, fakeSchemaManager := newTestHandlerWithCustomAuthorizer(t, &fakeDB{}, authorizer)

		className := "DirectClassName"

		// Mock shard status retrieval
		expectedStatus := models.ShardStatusList{
			&models.ShardStatusGetResponse{
				Name:   shardName,
				Status: "READY",
			},
		}
		fakeSchemaManager.On("GetShardsStatus", className, shardName).Return(expectedStatus, nil)

		// Create custom schema manager without alias resolution
		fakeSchemaManagerWithAlias := &fakeSchemaManagerWithAlias{
			fakeSchemaManager: fakeSchemaManager,
			aliasMap:          map[string]string{}, // empty - no aliases
		}
		handler.schemaReader = fakeSchemaManagerWithAlias

		_, err := handler.ShardsStatus(context.Background(), principal, className, shardName)
		require.NoError(t, err)

		// Verify authorization was called with the original class name
		require.Len(t, authorizer.Calls(), 1, "Authorizer must be called exactly once")
		authCall := authorizer.Calls()[0]
		assert.Equal(t, principal, authCall.Principal)
		assert.Equal(t, authorization.READ, authCall.Verb)
		assert.Equal(t, authorization.ShardsMetadata(className, shardName), authCall.Resources,
			"Authorization should use class name '%s' when no alias resolution", className)

		fakeSchemaManager.AssertExpectations(t)
	})
}

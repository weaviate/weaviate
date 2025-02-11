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
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/auth/authorization/mocks"
)

// A component-test like test suite that makes sure that every available UC is
// potentially protected with the Authorization plugin
func Test_Schema_Authorization(t *testing.T) {
	type testCase struct {
		methodName        string
		additionalArgs    []interface{}
		expectedVerb      string
		expectedResources string
	}

	tests := []testCase{
		{
			methodName:        "GetSchema",
			expectedVerb:      authorization.READ,
			expectedResources: authorization.CollectionsMetadata()[0],
		},
		{
			methodName:        "GetConsistentSchema",
			expectedVerb:      authorization.READ,
			additionalArgs:    []interface{}{false},
			expectedResources: authorization.CollectionsMetadata()[0],
		},
		{
			methodName:        "GetClass",
			additionalArgs:    []interface{}{"classname"},
			expectedVerb:      authorization.READ,
			expectedResources: authorization.CollectionsMetadata("classname")[0],
		},
		{
			methodName:        "GetConsistentClass",
			additionalArgs:    []interface{}{"classname", false},
			expectedVerb:      authorization.READ,
			expectedResources: authorization.CollectionsMetadata("classname")[0],
		},
		{
			methodName:        "GetCachedClass",
			additionalArgs:    []interface{}{"classname"},
			expectedVerb:      authorization.READ,
			expectedResources: authorization.CollectionsMetadata("classname")[0],
		},
		{
			methodName:        "AddClass",
			additionalArgs:    []interface{}{&models.Class{Class: "classname"}},
			expectedVerb:      authorization.CREATE,
			expectedResources: authorization.CollectionsMetadata("Classname")[0],
		},
		{
			methodName:        "UpdateClass",
			additionalArgs:    []interface{}{"class", &models.Class{Class: "class"}},
			expectedVerb:      authorization.UPDATE,
			expectedResources: authorization.CollectionsMetadata("class")[0],
		},
		{
			methodName:        "DeleteClass",
			additionalArgs:    []interface{}{"somename"},
			expectedVerb:      authorization.DELETE,
			expectedResources: authorization.CollectionsMetadata("somename")[0],
		},
		{
			methodName:        "AddClassProperty",
			additionalArgs:    []interface{}{&models.Class{Class: "classname"}, "classname", false, &models.Property{}},
			expectedVerb:      authorization.UPDATE,
			expectedResources: authorization.CollectionsMetadata("classname")[0],
		},
		{
			methodName:        "DeleteClassProperty",
			additionalArgs:    []interface{}{"somename", "someprop"},
			expectedVerb:      authorization.UPDATE,
			expectedResources: authorization.CollectionsMetadata("somename")[0],
		},
		{
			methodName:        "UpdateShardStatus",
			additionalArgs:    []interface{}{"className", "shardName", "targetStatus"},
			expectedVerb:      authorization.UPDATE,
			expectedResources: authorization.ShardsMetadata("className", "shardName")[0],
		},
		{
			methodName:        "ShardsStatus",
			additionalArgs:    []interface{}{"className", "tenant"},
			expectedVerb:      authorization.READ,
			expectedResources: authorization.ShardsMetadata("className", "tenant")[0],
		},
		{
			methodName:        "AddTenants",
			additionalArgs:    []interface{}{"className", []*models.Tenant{{Name: "P1"}}},
			expectedVerb:      authorization.CREATE,
			expectedResources: authorization.ShardsMetadata("className", "P1")[0],
		},
		{
			methodName: "UpdateTenants",
			additionalArgs: []interface{}{"className", []*models.Tenant{
				{Name: "P1", ActivityStatus: models.TenantActivityStatusHOT},
			}},
			expectedVerb:      authorization.UPDATE,
			expectedResources: authorization.ShardsMetadata("className", "P1")[0],
		},
		{
			methodName:        "DeleteTenants",
			additionalArgs:    []interface{}{"className", []string{"P1"}},
			expectedVerb:      authorization.DELETE,
			expectedResources: authorization.ShardsMetadata("className", "P1")[0],
		},
		{
			methodName:        "GetConsistentTenants",
			additionalArgs:    []interface{}{"className", false, []string{}},
			expectedVerb:      authorization.READ,
			expectedResources: authorization.ShardsMetadata("className")[0],
		},
		{
			methodName:        "ConsistentTenantExists",
			additionalArgs:    []interface{}{"className", false, "P1"},
			expectedVerb:      authorization.READ,
			expectedResources: authorization.ShardsMetadata("className", "P1")[0],
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
				"TryLock", "RLocker", "TryRLock", "CopyShardingState", "TxManager", "RestoreClass",
				"ShardOwner", "TenantShard", "ShardFromUUID", "LockGuard", "RLockGuard", "ShardReplicas",
				// internal methods to indicate readiness state
				"StartServing", "Shutdown", "Statistics",
				// Cluster/nodes related endpoint
				"JoinNode", "RemoveNode", "Nodes", "NodeName", "ClusterHealthScore", "ClusterStatus", "ResolveParentNodes",
				// revert to schema v0 (non raft)
				"StoreSchemaV1":
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
				authorizer := mocks.NewAuthorizer(t)
				authorizer.On("Authorize", principal, test.expectedVerb, test.expectedResources).Return(errors.New("just a test fake")).Once()
				handler, fakeSchemaManager := newTestHandlerWithCustomAuthorizer(t, &fakeDB{}, authorizer)
				fakeSchemaManager.On("ReadOnlySchema").Return(models.Schema{})
				fakeSchemaManager.On("ReadOnlyClass", mock.Anything).Return(models.Class{})

				var args []interface{}
				if test.methodName == "GetSchema" || test.methodName == "GetConsistentSchema" {
					// no context on this method
					args = append([]interface{}{principal}, test.additionalArgs...)
				} else {
					args = append([]interface{}{context.Background(), principal}, test.additionalArgs...)
				}
				out, _ := callFuncByName(handler, test.methodName, args...)
				assert.Equal(t, errors.New("just a test fake"), out[len(out)-1].Interface(),
					"execution must abort with Authorizer error")
			})
		}
	})
}

// inspired by https://stackoverflow.com/a/33008200
func callFuncByName(manager interface{}, funcName string, params ...interface{}) (out []reflect.Value, err error) {
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
	return
}

func allExportedMethods(subject interface{}) []string {
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

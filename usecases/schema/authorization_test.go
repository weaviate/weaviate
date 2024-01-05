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

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/config"
)

// A component-test like test suite that makes sure that every available UC is
// potentially protected with the Authorization plugin

func Test_Schema_Authorization(t *testing.T) {
	type testCase struct {
		methodName       string
		additionalArgs   []interface{}
		expectedVerb     string
		expectedResource string
	}

	tests := []testCase{
		{
			methodName:       "GetSchema",
			expectedVerb:     "list",
			expectedResource: "schema/*",
		},
		{
			methodName:       "GetClass",
			additionalArgs:   []interface{}{"classname"},
			expectedVerb:     "list",
			expectedResource: "schema/*",
		},
		{
			methodName:       "GetShardsStatus",
			additionalArgs:   []interface{}{"className", "tenant"},
			expectedVerb:     "list",
			expectedResource: "schema/className/shards",
		},
		{
			methodName:       "AddClass",
			additionalArgs:   []interface{}{&models.Class{}},
			expectedVerb:     "create",
			expectedResource: "schema/objects",
		},
		{
			methodName:       "UpdateClass",
			additionalArgs:   []interface{}{"somename", &models.Class{}},
			expectedVerb:     "update",
			expectedResource: "schema/objects",
		},
		{
			methodName:       "DeleteClass",
			additionalArgs:   []interface{}{"somename"},
			expectedVerb:     "delete",
			expectedResource: "schema/objects",
		},
		{
			methodName:       "AddClassProperty",
			additionalArgs:   []interface{}{"somename", &models.Property{}},
			expectedVerb:     "update",
			expectedResource: "schema/objects",
		},
		{
			methodName:       "MergeClassObjectProperty",
			additionalArgs:   []interface{}{"somename", &models.Property{}},
			expectedVerb:     "update",
			expectedResource: "schema/objects",
		},
		{
			methodName:       "DeleteClassProperty",
			additionalArgs:   []interface{}{"somename", "someprop"},
			expectedVerb:     "update",
			expectedResource: "schema/objects",
		},
		{
			methodName:       "UpdateShardStatus",
			additionalArgs:   []interface{}{"className", "shardName", "targetStatus"},
			expectedVerb:     "update",
			expectedResource: "schema/className/shards/shardName",
		},
		{
			methodName:       "AddTenants",
			additionalArgs:   []interface{}{"className", []*models.Tenant{{Name: "P1"}}},
			expectedVerb:     "update",
			expectedResource: tenantsPath,
		},
		{
			methodName: "UpdateTenants",
			additionalArgs: []interface{}{"className", []*models.Tenant{
				{Name: "P1", ActivityStatus: models.TenantActivityStatusHOT},
			}},
			expectedVerb:     "update",
			expectedResource: tenantsPath,
		},
		{
			methodName:       "DeleteTenants",
			additionalArgs:   []interface{}{"className", []string{"P1"}},
			expectedVerb:     "delete",
			expectedResource: tenantsPath,
		},
		{
			methodName:       "GetTenants",
			additionalArgs:   []interface{}{"className"},
			expectedVerb:     "get",
			expectedResource: tenantsPath,
		},
	}

	t.Run("verify that a test for every public method exists", func(t *testing.T) {
		testedMethods := make([]string, len(tests))
		for i, test := range tests {
			testedMethods[i] = test.methodName
		}

		for _, method := range allExportedMethods(&Manager{}) {
			switch method {
			case "RegisterSchemaUpdateCallback",
				"UpdateMeta", "GetSchemaSkipAuth", "IndexedInverted", "RLock", "RUnlock", "Lock", "Unlock",
				"TryLock", "RLocker", "TryRLock", // introduced by sync.Mutex in go 1.18
				"Nodes", "NodeName", "ClusterHealthScore", "ClusterStatus", "ResolveParentNodes",
				"CopyShardingState", "TxManager", "RestoreClass",
				"ShardOwner", "TenantShard", "ShardFromUUID", "LockGuard", "RLockGuard", "ShardReplicas",
				"StartServing", "Shutdown": // internal methods to indicate readiness state
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
		logger, _ := test.NewNullLogger()
		for _, test := range tests {
			t.Run(test.methodName, func(t *testing.T) {
				authorizer := &authDenier{}
				manager, err := NewManager(&NilMigrator{}, newFakeRepo(),
					logger, authorizer, config.Config{},
					dummyParseVectorConfig, &fakeVectorizerValidator{},
					dummyValidateInvertedConfig, &fakeModuleConfig{},
					&fakeClusterState{hosts: []string{"node1"}}, &fakeTxClient{},
					&fakeTxPersistence{}, &fakeScaleOutManager{})
				require.Nil(t, err)

				var args []interface{}
				if test.methodName == "GetSchema" {
					// no context on this method
					args = append([]interface{}{principal}, test.additionalArgs...)
				} else {
					args = append([]interface{}{context.Background(), principal}, test.additionalArgs...)
				}
				out, _ := callFuncByName(manager, test.methodName, args...)

				require.Len(t, authorizer.calls, 1, "Authorizer must be called")
				assert.Equal(t, errors.New("just a test fake"), out[len(out)-1].Interface(),
					"execution must abort with Authorizer error")
				assert.Equal(t, authorizeCall{principal, test.expectedVerb, test.expectedResource},
					authorizer.calls[0], "correct parameters must have been used on Authorizer")
			})
		}
	})
}

type authorizeCall struct {
	principal *models.Principal
	verb      string
	resource  string
}

type authDenier struct {
	calls []authorizeCall
}

func (a *authDenier) Authorize(principal *models.Principal, verb, resource string) error {
	a.calls = append(a.calls, authorizeCall{principal, verb, resource})
	return errors.New("just a test fake")
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

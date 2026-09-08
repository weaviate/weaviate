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

package objects

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/auth/authorization/mocks"
	"github.com/weaviate/weaviate/usecases/config"
)

// A component-test like test suite that makes sure that every available UC is
// potentially protected with the Authorization plugin

var errAuthzFake = errors.New("just a test fake")

func Test_Kinds_Authorization(t *testing.T) {
	type testCase struct {
		methodName        string
		additionalArgs    []interface{}
		expectedVerb      string
		expectedResources []string
		// authorizedBySchemaManager marks a method that authorizes through
		// schema.Handler.GetClass instead of calling the authorizer itself.
		authorizedBySchemaManager bool
		// precedingCalls are the checks the authorizer allows before it denies
		// the one this row pins.
		precedingCalls []mocks.AuthZReq
	}

	queryTenant := "tenant"
	principal := &models.Principal{}

	tests := []testCase{
		// single kind
		{
			methodName: "AddObject",
			additionalArgs: []interface{}{
				&models.Object{Class: "class", ID: "foo", Tenant: "tenant"},
				(*additional.ReplicationProperties)(nil),
			},
			expectedVerb:      authorization.CREATE,
			expectedResources: authorization.ShardsData("class", "tenant"),
		},
		{
			methodName: "ValidateObject",
			additionalArgs: []interface{}{
				&models.Object{Class: "class", ID: "foo", Tenant: "tenant"},
				(*additional.ReplicationProperties)(nil),
			},
			expectedVerb:      authorization.READ,
			expectedResources: []string{authorization.Objects("class", "tenant", "foo")},
		},
		{
			methodName: "GetObject",
			additionalArgs: []interface{}{
				"class", strfmt.UUID("foo"),
				additional.Properties{},
				(*additional.ReplicationProperties)(nil), "tenant",
			},
			expectedVerb:      authorization.READ,
			expectedResources: []string{authorization.Objects("class", "tenant", "foo")},
		},
		{
			methodName: "DeleteObject",
			additionalArgs: []interface{}{
				"class", strfmt.UUID("foo"),
				(*additional.ReplicationProperties)(nil), "tenant",
			},
			expectedVerb:      authorization.DELETE,
			expectedResources: []string{authorization.Objects("class", "tenant", "foo")},
		},
		{
			// the path class and id differ from the body, so the row pins which
			// source update.go builds the authz resource from
			methodName: "UpdateObject",
			additionalArgs: []interface{}{
				"pathClass", strfmt.UUID("11111111-1111-1111-1111-111111111111"),
				&models.Object{Class: "class", ID: "foo", Tenant: "tenant"},
				(*additional.ReplicationProperties)(nil),
			},
			expectedVerb:      authorization.UPDATE,
			expectedResources: []string{authorization.Objects("class", "tenant", "foo")},
		},
		{
			methodName: "MergeObject",
			additionalArgs: []interface{}{
				&models.Object{Class: "class", ID: "foo", Tenant: "tenant"},
				(*additional.ReplicationProperties)(nil),
			},
			expectedVerb:      authorization.UPDATE,
			expectedResources: []string{authorization.Objects("class", "tenant", "foo")},
		},
		{
			methodName:        "HeadObject",
			additionalArgs:    []interface{}{"class", strfmt.UUID("foo"), (*additional.ReplicationProperties)(nil), "tenant"},
			expectedVerb:      authorization.READ,
			expectedResources: []string{authorization.Objects("class", "tenant", "foo")},
		},
		{ // the deprecated route carries no class, which widens the resource to every collection
			methodName:        "HeadObject",
			additionalArgs:    []interface{}{"", strfmt.UUID("foo"), (*additional.ReplicationProperties)(nil), ""},
			expectedVerb:      authorization.READ,
			expectedResources: []string{authorization.Objects("", "", "foo")},
		},

		// class lookups
		{
			methodName:        "GetObjectsClass",
			additionalArgs:    []interface{}{strfmt.UUID("foo")},
			expectedVerb:      authorization.READ,
			expectedResources: []string{authorization.Objects("", "", "foo")},
		},
		{
			methodName:                "GetObjectClassFromName",
			additionalArgs:            []interface{}{"class"},
			authorizedBySchemaManager: true,
		},

		// query objects
		{
			// Query authorizes the whole collection, so the tenant never reaches the resource
			methodName:        "Query",
			additionalArgs:    []interface{}{&QueryParams{Class: "class", Tenant: &queryTenant}},
			expectedVerb:      authorization.READ,
			expectedResources: authorization.CollectionsData("class"),
		},
		{ // list objects is deprecated by query
			methodName: "GetObjects",
			additionalArgs: []interface{}{
				(*int64)(nil), (*int64)(nil), (*string)(nil), (*string)(nil), (*string)(nil),
				additional.Properties{},
				"tenant",
			},
			expectedVerb:      authorization.READ,
			expectedResources: []string{authorization.Objects("", "tenant", "")},
		},

		// reference on objects
		{
			methodName: "AddObjectReference",
			additionalArgs: []interface{}{
				&AddReferenceInput{Class: "class", ID: strfmt.UUID("foo"), Property: "someProp"},
				(*additional.ReplicationProperties)(nil), "tenant",
			},
			expectedVerb:      authorization.UPDATE,
			expectedResources: authorization.ShardsData("class", "tenant"),
		},
		{
			methodName: "DeleteObjectReference",
			additionalArgs: []interface{}{
				&DeleteReferenceInput{Class: "class", ID: strfmt.UUID("foo"), Property: "someProp"},
				(*additional.ReplicationProperties)(nil), "tenant",
			},
			expectedVerb:      authorization.READ,
			expectedResources: authorization.ShardsData("class", "tenant"),
		},
		{ // DeleteObjectReference authorizes READ then UPDATE, so this row pins the write gate
			methodName: "DeleteObjectReference",
			additionalArgs: []interface{}{
				&DeleteReferenceInput{Class: "class", ID: strfmt.UUID("foo"), Property: "someProp"},
				(*additional.ReplicationProperties)(nil), "tenant",
			},
			precedingCalls: []mocks.AuthZReq{{
				Principal: principal, Verb: authorization.READ,
				Resources: authorization.ShardsData("class", "tenant"),
			}},
			expectedVerb:      authorization.UPDATE,
			expectedResources: authorization.ShardsData("class", "tenant"),
		},
		{
			methodName: "UpdateObjectReferences",
			additionalArgs: []interface{}{
				&PutReferenceInput{Class: "class", ID: strfmt.UUID("foo"), Property: "someProp"},
				(*additional.ReplicationProperties)(nil), "tenant",
			},
			expectedVerb:      authorization.UPDATE,
			expectedResources: authorization.ShardsData("class", "tenant"),
		},
	}

	t.Run("verify that a test for every public method exists", func(t *testing.T) {
		testedMethods := make([]string, len(tests))
		for i, test := range tests {
			testedMethods[i] = test.methodName
		}

		for _, method := range allExportedMethods(&Manager{}, "") {
			assert.Contains(t, testedMethods, method)
		}
	})

	t.Run("verify the tested methods require correct permissions from the authorizer", func(t *testing.T) {
		logger, _ := test.NewNullLogger()
		for _, test := range tests {
			t.Run(test.methodName, func(t *testing.T) {
				schemaManager := &fakeSchemaManager{}
				if test.authorizedBySchemaManager {
					schemaManager.GetschemaErr = errAuthzFake
				}
				cfg := &config.WeaviateConfig{}
				authorizer := mocks.NewMockAuthorizer()
				authorizer.SetErrAfter(len(test.precedingCalls), errAuthzFake)
				vectorRepo := &fakeVectorRepo{}
				manager := NewManager(schemaManager,
					cfg, logger, authorizer,
					vectorRepo, getFakeModulesProvider(), &fakeMetrics{}, nil,
					NewAutoSchemaManager(schemaManager, vectorRepo, cfg, logger, prometheus.NewPedanticRegistry()))

				args := append([]interface{}{context.Background(), principal}, test.additionalArgs...)
				out, err := callFuncByName(manager, test.methodName, args...)
				require.NoError(t, err)

				if test.authorizedBySchemaManager {
					require.Empty(t, authorizer.Calls(), "authorizer must not be called directly")
					require.Equal(t, []string{"Class"}, schemaManager.GetClassCalls,
						"the schema manager must run the check")
				} else {
					require.Equal(t, expectedAuthZReqs(principal, test.precedingCalls, test.expectedVerb, test.expectedResources),
						authorizer.Calls(), "correct parameters must have been used on authorizer")
				}

				returned := out[len(out)-1]
				require.False(t, returned.IsNil(), "execution must abort with an error")
				require.ErrorIs(t, returned.Interface().(error), errAuthzFake,
					"execution must abort with the denial")
			})
		}
	})
}

// expectedAuthZReqs returns the full call sequence a denied row must produce,
// with the checks that pass first and the denied one last.
func expectedAuthZReqs(principal *models.Principal, preceding []mocks.AuthZReq,
	verb string, resources []string,
) []mocks.AuthZReq {
	reqs := make([]mocks.AuthZReq, 0, len(preceding)+1)
	reqs = append(reqs, preceding...)
	return append(reqs, mocks.AuthZReq{Principal: principal, Verb: verb, Resources: resources})
}

func Test_BatchKinds_Authorization(t *testing.T) {
	type testCase struct {
		methodName        string
		additionalArgs    []interface{}
		expectedVerb      string
		expectedResources []string
		// precedingCalls are the checks the authorizer allows before it denies
		// the one this row pins.
		precedingCalls []mocks.AuthZReq
	}

	uri := strfmt.URI("weaviate://localhost/Class/" + uuid.New().String())
	principal := &models.Principal{}

	tests := []testCase{
		{
			methodName: "AddObjects",
			additionalArgs: []interface{}{
				[]*models.Object{{Class: "class", Tenant: "tenant"}},
				[]*string{},
				&additional.ReplicationProperties{},
			},
			expectedVerb:      authorization.UPDATE,
			expectedResources: authorization.ShardsData("class", "tenant"),
		},
		{ // AddObjects authorizes UPDATE then CREATE, so this row pins the second check
			methodName: "AddObjects",
			additionalArgs: []interface{}{
				[]*models.Object{{Class: "class", Tenant: "tenant"}},
				[]*string{},
				&additional.ReplicationProperties{},
			},
			precedingCalls: []mocks.AuthZReq{{
				Principal: principal, Verb: authorization.UPDATE,
				Resources: authorization.ShardsData("class", "tenant"),
			}},
			expectedVerb:      authorization.CREATE,
			expectedResources: authorization.ShardsData("class", "tenant"),
		},
		{
			methodName: "AddReferences",
			additionalArgs: []interface{}{
				[]*models.BatchReference{{From: uri + "/ref", To: uri, Tenant: "tenant"}},
				&additional.ReplicationProperties{},
			},
			expectedVerb:      authorization.UPDATE,
			expectedResources: authorization.ShardsData("Class", "tenant"),
		},
		{
			methodName: "DeleteObjects",
			additionalArgs: []interface{}{
				&models.BatchDeleteMatch{Class: "class"},
				(*int64)(nil),
				(*bool)(nil),
				(*string)(nil),
				&additional.ReplicationProperties{},
				"tenant",
			},
			expectedVerb:      authorization.DELETE,
			expectedResources: authorization.ShardsData("class", "tenant"),
		},
	}

	t.Run("verify that a test for every public method exists", func(t *testing.T) {
		testedMethods := make([]string, len(tests))
		for i, test := range tests {
			testedMethods[i] = test.methodName
		}

		// exception is public method for GRPC which has its own authorization check
		for _, method := range allExportedMethods(&BatchManager{}, "DeleteObjectsFromGRPCAfterAuth", "AddObjectsGRPCAfterAuth") {
			assert.Contains(t, testedMethods, method)
		}
	})

	t.Run("verify the tested methods require correct permissions from the authorizer", func(t *testing.T) {
		logger, _ := test.NewNullLogger()
		for _, test := range tests {
			t.Run(test.methodName, func(t *testing.T) {
				schemaManager := &fakeSchemaManager{}
				cfg := &config.WeaviateConfig{}
				authorizer := mocks.NewMockAuthorizer()
				authorizer.SetErrAfter(len(test.precedingCalls), errAuthzFake)
				vectorRepo := &fakeVectorRepo{}
				modulesProvider := getFakeModulesProvider()
				manager := NewBatchManager(vectorRepo, modulesProvider, schemaManager, cfg, logger, authorizer, nil,
					NewAutoSchemaManager(schemaManager, vectorRepo, cfg, logger, prometheus.NewPedanticRegistry()))

				args := append([]interface{}{context.Background(), principal}, test.additionalArgs...)
				out, err := callFuncByName(manager, test.methodName, args...)
				require.NoError(t, err)

				require.Equal(t, expectedAuthZReqs(principal, test.precedingCalls, test.expectedVerb, test.expectedResources),
					authorizer.Calls(), "correct parameters must have been used on authorizer")

				returned := out[len(out)-1]
				require.False(t, returned.IsNil(), "execution must abort with an error")
				require.ErrorIs(t, returned.Interface().(error), errAuthzFake,
					"execution must abort with the denial")
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
	return out, err
}

func allExportedMethods(subject interface{}, exceptions ...string) []string {
	var methods []string
	subjectType := reflect.TypeOf(subject)
methodLoop:
	for i := 0; i < subjectType.NumMethod(); i++ {
		name := subjectType.Method(i).Name
		for j := range exceptions {
			if name == exceptions[j] {
				continue methodLoop
			}
		}
		if name[0] >= 'A' && name[0] <= 'Z' {
			methods = append(methods, name)
		}
	}

	return methods
}

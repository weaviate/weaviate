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

func Test_Kinds_Authorization(t *testing.T) {
	type testCase struct {
		methodName        string
		additionalArgs    []interface{}
		expectedVerb      string
		expectedResources []string
	}

	tests := []testCase{
		// single kind
		{
			methodName:        "AddObject",
			additionalArgs:    []interface{}{(*models.Object)(nil)},
			expectedVerb:      authorization.UPDATE,
			expectedResources: authorization.ShardsMetadata("", ""),
		},
		{
			methodName:        "ValidateObject",
			additionalArgs:    []interface{}{(*models.Object)(nil)},
			expectedVerb:      authorization.READ,
			expectedResources: []string{authorization.Objects("", "", "")},
		},
		{
			methodName:        "GetObject",
			additionalArgs:    []interface{}{"", strfmt.UUID("foo"), additional.Properties{}},
			expectedVerb:      authorization.READ,
			expectedResources: []string{authorization.Objects("", "", "foo")},
		},
		{
			methodName:        "DeleteObject",
			additionalArgs:    []interface{}{"class", strfmt.UUID("foo")},
			expectedVerb:      authorization.DELETE,
			expectedResources: []string{authorization.Objects("class", "", "foo")},
		},
		{ // deprecated by the one above
			methodName:        "DeleteObject",
			additionalArgs:    []interface{}{"class", strfmt.UUID("foo")},
			expectedVerb:      authorization.DELETE,
			expectedResources: []string{authorization.Objects("class", "", "foo")},
		},
		{
			methodName:        "UpdateObject",
			additionalArgs:    []interface{}{"class", strfmt.UUID("foo"), (*models.Object)(nil)},
			expectedVerb:      authorization.UPDATE,
			expectedResources: []string{authorization.Objects("class", "", "foo")},
		},
		{ // deprecated by the one above
			methodName:        "UpdateObject",
			additionalArgs:    []interface{}{"class", strfmt.UUID("foo"), (*models.Object)(nil)},
			expectedVerb:      authorization.UPDATE,
			expectedResources: []string{authorization.Objects("class", "", "foo")},
		},
		{
			methodName: "MergeObject",
			additionalArgs: []interface{}{
				&models.Object{Class: "class", ID: "foo"},
				(*additional.ReplicationProperties)(nil),
			},
			expectedVerb:      authorization.UPDATE,
			expectedResources: []string{authorization.Objects("class", "", "foo")},
		},
		{
			methodName:        "GetObjectsClass",
			additionalArgs:    []interface{}{strfmt.UUID("foo")},
			expectedVerb:      authorization.READ,
			expectedResources: []string{authorization.Objects("", "", "foo")},
		},
		{
			methodName:        "GetObjectClassFromName",
			additionalArgs:    []interface{}{strfmt.UUID("foo")},
			expectedVerb:      authorization.READ,
			expectedResources: []string{authorization.Objects("", "", "foo")},
		},
		{
			methodName:        "HeadObject",
			additionalArgs:    []interface{}{"class", strfmt.UUID("foo")},
			expectedVerb:      authorization.READ,
			expectedResources: []string{authorization.Objects("class", "", "foo")},
		},
		{ // deprecated by the one above
			methodName:        "HeadObject",
			additionalArgs:    []interface{}{"", strfmt.UUID("foo")},
			expectedVerb:      authorization.READ,
			expectedResources: []string{authorization.Objects("", "", "foo")},
		},

		// query objects
		{
			methodName:        "Query",
			additionalArgs:    []interface{}{new(QueryParams)},
			expectedVerb:      authorization.READ,
			expectedResources: []string{authorization.ShardsMetadata("", "")[0]},
		},

		{ // list objects is deprecated by query
			methodName:        "GetObjects",
			additionalArgs:    []interface{}{(*int64)(nil), (*int64)(nil), (*string)(nil), (*string)(nil), additional.Properties{}},
			expectedVerb:      authorization.READ,
			expectedResources: []string{authorization.Objects("", "", "")},
		},

		// reference on objects
		{
			methodName:        "AddObjectReference",
			additionalArgs:    []interface{}{AddReferenceInput{Class: "class", ID: strfmt.UUID("foo"), Property: "some prop"}, (*models.SingleRef)(nil)},
			expectedVerb:      authorization.UPDATE,
			expectedResources: []string{authorization.Objects("class", "", "foo")},
		},
		{
			methodName:        "DeleteObjectReference",
			additionalArgs:    []interface{}{strfmt.UUID("foo"), "some prop", (*models.SingleRef)(nil)},
			expectedVerb:      authorization.UPDATE,
			expectedResources: []string{authorization.Objects("", "", "foo")},
		},
		{
			methodName:        "UpdateObjectReferences",
			additionalArgs:    []interface{}{&PutReferenceInput{Class: "class", ID: strfmt.UUID("foo"), Property: "some prop"}},
			expectedVerb:      authorization.UPDATE,
			expectedResources: []string{authorization.Objects("class", "", "foo")},
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
		principal := &models.Principal{}
		logger, _ := test.NewNullLogger()
		for _, test := range tests {
			if test.methodName != "MergeObject" {
				continue
			}
			t.Run(test.methodName, func(t *testing.T) {
				schemaManager := &fakeSchemaManager{}
				cfg := &config.WeaviateConfig{}
				authorizer := mocks.NewMockAuthorizer()
				authorizer.SetErr(errors.New("just a test fake"))
				vectorRepo := &fakeVectorRepo{}
				manager := NewManager(schemaManager,
					cfg, logger, authorizer,
					vectorRepo, getFakeModulesProvider(), nil, nil,
					NewAutoSchemaManager(schemaManager, vectorRepo, cfg, authorizer, logger, prometheus.NewPedanticRegistry()))

				args := append([]interface{}{context.Background(), principal}, test.additionalArgs...)
				out, _ := callFuncByName(manager, test.methodName, args...)

				require.Len(t, authorizer.Calls(), 1, "authorizer must be called")
				aerr := out[len(out)-1].Interface().(error)
				var customErr *Error
				if !errors.As(aerr, &customErr) || !customErr.Forbidden() {
					assert.Equal(t, errors.New("just a test fake"), aerr,
						"execution must abort with authorizer error")
				}

				assert.Equal(t, mocks.AuthZReq{Principal: principal, Verb: test.expectedVerb, Resources: test.expectedResources},
					authorizer.Calls()[0], "correct parameters must have been used on authorizer")
			})
		}
	})
}

func Test_BatchKinds_Authorization(t *testing.T) {
	type testCase struct {
		methodName        string
		additionalArgs    []interface{}
		expectedVerb      string
		expectedResources []string
	}

	uri := strfmt.URI("weaviate://localhost/Class/" + uuid.New().String())

	tests := []testCase{
		{
			methodName: "AddObjects",
			additionalArgs: []interface{}{
				[]*models.Object{{}},
				[]*string{},
				&additional.ReplicationProperties{},
			},
			expectedVerb:      authorization.UPDATE,
			expectedResources: authorization.ShardsData("", ""),
		},
		{
			methodName: "AddReferences",
			additionalArgs: []interface{}{
				[]*models.BatchReference{{From: uri + "/ref", To: uri, Tenant: ""}},
				&additional.ReplicationProperties{},
			},
			expectedVerb:      authorization.UPDATE,
			expectedResources: authorization.ShardsData("Class", ""),
		},
		{
			methodName: "DeleteObjects",
			additionalArgs: []interface{}{
				&models.BatchDeleteMatch{},
				(*int64)(nil),
				(*bool)(nil),
				(*string)(nil),
				&additional.ReplicationProperties{},
				"",
			},
			expectedVerb:      authorization.DELETE,
			expectedResources: authorization.ShardsData("", ""),
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
		principal := &models.Principal{}
		logger, _ := test.NewNullLogger()
		for _, test := range tests {
			schemaManager := &fakeSchemaManager{}
			cfg := &config.WeaviateConfig{}
			authorizer := mocks.NewMockAuthorizer()
			authorizer.SetErr(errors.New("just a test fake"))
			vectorRepo := &fakeVectorRepo{}
			modulesProvider := getFakeModulesProvider()
			manager := NewBatchManager(vectorRepo, modulesProvider, schemaManager, cfg, logger, authorizer, nil,
				NewAutoSchemaManager(schemaManager, vectorRepo, cfg, authorizer, logger, prometheus.NewPedanticRegistry()))

			args := append([]interface{}{context.Background(), principal}, test.additionalArgs...)
			out, _ := callFuncByName(manager, test.methodName, args...)

			require.Len(t, authorizer.Calls(), 1, "authorizer must be called")
			assert.Equal(t, errors.New("just a test fake"), out[len(out)-1].Interface(),
				"execution must abort with authorizer error")
			assert.Equal(t, mocks.AuthZReq{Principal: principal, Verb: test.expectedVerb, Resources: test.expectedResources},
				authorizer.Calls()[0], "correct parameters must have been used on authorizer")
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

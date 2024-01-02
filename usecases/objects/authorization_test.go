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
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/config"
)

// A component-test like test suite that makes sure that every available UC is
// potentially protected with the Authorization plugin

func Test_Kinds_Authorization(t *testing.T) {
	type testCase struct {
		methodName       string
		additionalArgs   []interface{}
		expectedVerb     string
		expectedResource string
	}

	tests := []testCase{
		// single kind
		{
			methodName:       "AddObject",
			additionalArgs:   []interface{}{(*models.Object)(nil)},
			expectedVerb:     "create",
			expectedResource: "objects",
		},
		{
			methodName:       "ValidateObject",
			additionalArgs:   []interface{}{(*models.Object)(nil)},
			expectedVerb:     "validate",
			expectedResource: "objects",
		},
		{
			methodName:       "GetObject",
			additionalArgs:   []interface{}{"", strfmt.UUID("foo"), additional.Properties{}},
			expectedVerb:     "get",
			expectedResource: "objects/foo",
		},
		{
			methodName:       "DeleteObject",
			additionalArgs:   []interface{}{"class", strfmt.UUID("foo")},
			expectedVerb:     "delete",
			expectedResource: "objects/class/foo",
		},
		{ // deprecated by the one above
			methodName:       "DeleteObject",
			additionalArgs:   []interface{}{"", strfmt.UUID("foo")},
			expectedVerb:     "delete",
			expectedResource: "objects/foo",
		},
		{
			methodName:       "UpdateObject",
			additionalArgs:   []interface{}{"class", strfmt.UUID("foo"), (*models.Object)(nil)},
			expectedVerb:     "update",
			expectedResource: "objects/class/foo",
		},
		{ // deprecated by the one above
			methodName:       "UpdateObject",
			additionalArgs:   []interface{}{"", strfmt.UUID("foo"), (*models.Object)(nil)},
			expectedVerb:     "update",
			expectedResource: "objects/foo",
		},
		{
			methodName: "MergeObject",
			additionalArgs: []interface{}{
				&models.Object{Class: "class", ID: "foo"},
				(*additional.ReplicationProperties)(nil),
			},
			expectedVerb:     "update",
			expectedResource: "objects/class/foo",
		},
		{
			methodName:       "GetObjectsClass",
			additionalArgs:   []interface{}{strfmt.UUID("foo")},
			expectedVerb:     "get",
			expectedResource: "objects/foo",
		},
		{
			methodName:       "GetObjectClassFromName",
			additionalArgs:   []interface{}{strfmt.UUID("foo")},
			expectedVerb:     "get",
			expectedResource: "objects/foo",
		},
		{
			methodName:       "HeadObject",
			additionalArgs:   []interface{}{"class", strfmt.UUID("foo")},
			expectedVerb:     "head",
			expectedResource: "objects/class/foo",
		},
		{ // deprecated by the one above
			methodName:       "HeadObject",
			additionalArgs:   []interface{}{"", strfmt.UUID("foo")},
			expectedVerb:     "head",
			expectedResource: "objects/foo",
		},

		// query objects
		{
			methodName:       "Query",
			additionalArgs:   []interface{}{new(QueryParams)},
			expectedVerb:     "list",
			expectedResource: "objects",
		},

		{ // list objects is deprecated by query
			methodName:       "GetObjects",
			additionalArgs:   []interface{}{(*int64)(nil), (*int64)(nil), (*string)(nil), (*string)(nil), additional.Properties{}},
			expectedVerb:     "list",
			expectedResource: "objects",
		},

		// reference on objects
		{
			methodName:       "AddObjectReference",
			additionalArgs:   []interface{}{AddReferenceInput{Class: "class", ID: strfmt.UUID("foo"), Property: "some prop"}, (*models.SingleRef)(nil)},
			expectedVerb:     "update",
			expectedResource: "objects/class/foo",
		},
		{
			methodName:       "DeleteObjectReference",
			additionalArgs:   []interface{}{strfmt.UUID("foo"), "some prop", (*models.SingleRef)(nil)},
			expectedVerb:     "update",
			expectedResource: "objects/foo",
		},
		{
			methodName:       "UpdateObjectReferences",
			additionalArgs:   []interface{}{&PutReferenceInput{Class: "class", ID: strfmt.UUID("foo"), Property: "some prop"}},
			expectedVerb:     "update",
			expectedResource: "objects/class/foo",
		},
	}

	t.Run("verify that a test for every public method exists", func(t *testing.T) {
		testedMethods := make([]string, len(tests))
		for i, test := range tests {
			testedMethods[i] = test.methodName
		}

		for _, method := range allExportedMethods(&Manager{}) {
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
				locks := &fakeLocks{}
				cfg := &config.WeaviateConfig{}
				authorizer := &authDenier{}
				vectorRepo := &fakeVectorRepo{}
				manager := NewManager(locks, schemaManager,
					cfg, logger, authorizer,
					vectorRepo, getFakeModulesProvider(), nil)

				args := append([]interface{}{context.Background(), principal}, test.additionalArgs...)
				out, _ := callFuncByName(manager, test.methodName, args...)

				require.Len(t, authorizer.calls, 1, "authorizer must be called")
				aerr := out[len(out)-1].Interface().(error)
				if err, ok := aerr.(*Error); !ok || !err.Forbidden() {
					assert.Equal(t, errors.New("just a test fake"), aerr,
						"execution must abort with authorizer error")
				}

				assert.Equal(t, authorizeCall{principal, test.expectedVerb, test.expectedResource},
					authorizer.calls[0], "correct parameters must have been used on authorizer")
			})
		}
	})
}

func Test_BatchKinds_Authorization(t *testing.T) {
	type testCase struct {
		methodName       string
		additionalArgs   []interface{}
		expectedVerb     string
		expectedResource string
	}

	tests := []testCase{
		{
			methodName: "AddObjects",
			additionalArgs: []interface{}{
				[]*models.Object{},
				[]*string{},
				&additional.ReplicationProperties{},
			},
			expectedVerb:     "create",
			expectedResource: "batch/objects",
		},

		{
			methodName: "AddReferences",
			additionalArgs: []interface{}{
				[]*models.BatchReference{},
				&additional.ReplicationProperties{},
			},
			expectedVerb:     "update",
			expectedResource: "batch/*",
		},

		{
			methodName: "DeleteObjects",
			additionalArgs: []interface{}{
				&models.BatchDeleteMatch{},
				(*bool)(nil),
				(*string)(nil),
				&additional.ReplicationProperties{},
				"",
			},
			expectedVerb:     "delete",
			expectedResource: "batch/objects",
		},
	}

	t.Run("verify that a test for every public method exists", func(t *testing.T) {
		testedMethods := make([]string, len(tests))
		for i, test := range tests {
			testedMethods[i] = test.methodName
		}

		for _, method := range allExportedMethods(&BatchManager{}) {
			assert.Contains(t, testedMethods, method)
		}
	})

	t.Run("verify the tested methods require correct permissions from the authorizer", func(t *testing.T) {
		principal := &models.Principal{}
		logger, _ := test.NewNullLogger()
		for _, test := range tests {
			schemaManager := &fakeSchemaManager{}
			locks := &fakeLocks{}
			cfg := &config.WeaviateConfig{}
			authorizer := &authDenier{}
			vectorRepo := &fakeVectorRepo{}
			modulesProvider := getFakeModulesProvider()
			manager := NewBatchManager(vectorRepo, modulesProvider, locks, schemaManager, cfg, logger, authorizer, nil)

			args := append([]interface{}{context.Background(), principal}, test.additionalArgs...)
			out, _ := callFuncByName(manager, test.methodName, args...)

			require.Len(t, authorizer.calls, 1, "authorizer must be called")
			assert.Equal(t, errors.New("just a test fake"), out[len(out)-1].Interface(),
				"execution must abort with authorizer error")
			assert.Equal(t, authorizeCall{principal, test.expectedVerb, test.expectedResource},
				authorizer.calls[0], "correct parameters must have been used on authorizer")
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

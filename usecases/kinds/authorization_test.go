/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
 * LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
 * CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@semi.technology
 */package kinds

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/usecases/config"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
		testCase{
			methodName:       "AddThing",
			additionalArgs:   []interface{}{(*models.Thing)(nil)},
			expectedVerb:     "create",
			expectedResource: "things",
		},
		testCase{
			methodName:       "AddAction",
			additionalArgs:   []interface{}{(*models.Action)(nil)},
			expectedVerb:     "create",
			expectedResource: "actions",
		},
		testCase{
			methodName:       "ValidateThing",
			additionalArgs:   []interface{}{(*models.Thing)(nil)},
			expectedVerb:     "validate",
			expectedResource: "things",
		},
		testCase{
			methodName:       "ValidateAction",
			additionalArgs:   []interface{}{(*models.Action)(nil)},
			expectedVerb:     "validate",
			expectedResource: "actions",
		},
		testCase{
			methodName:       "GetThing",
			additionalArgs:   []interface{}{strfmt.UUID("foo")},
			expectedVerb:     "get",
			expectedResource: "things/foo",
		},
		testCase{
			methodName:       "GetAction",
			additionalArgs:   []interface{}{strfmt.UUID("foo")},
			expectedVerb:     "get",
			expectedResource: "actions/foo",
		},
		testCase{
			methodName:       "DeleteThing",
			additionalArgs:   []interface{}{strfmt.UUID("foo")},
			expectedVerb:     "delete",
			expectedResource: "things/foo",
		},
		testCase{
			methodName:       "DeleteAction",
			additionalArgs:   []interface{}{strfmt.UUID("foo")},
			expectedVerb:     "delete",
			expectedResource: "actions/foo",
		},
		testCase{
			methodName:       "UpdateThing",
			additionalArgs:   []interface{}{strfmt.UUID("foo"), (*models.Thing)(nil)},
			expectedVerb:     "update",
			expectedResource: "things/foo",
		},
		testCase{
			methodName:       "UpdateAction",
			additionalArgs:   []interface{}{strfmt.UUID("foo"), (*models.Action)(nil)},
			expectedVerb:     "update",
			expectedResource: "actions/foo",
		},

		// list kinds
		testCase{
			methodName:       "GetThings",
			additionalArgs:   []interface{}{(*int64)(nil)},
			expectedVerb:     "list",
			expectedResource: "things",
		},
		testCase{
			methodName:       "GetActions",
			additionalArgs:   []interface{}{(*int64)(nil)},
			expectedVerb:     "list",
			expectedResource: "actions",
		},

		// reference on kinds
		testCase{
			methodName:       "AddThingReference",
			additionalArgs:   []interface{}{strfmt.UUID("foo"), "some prop", (*models.SingleRef)(nil)},
			expectedVerb:     "update",
			expectedResource: "things/foo",
		},
		testCase{
			methodName:       "AddActionReference",
			additionalArgs:   []interface{}{strfmt.UUID("foo"), "some prop", (*models.SingleRef)(nil)},
			expectedVerb:     "update",
			expectedResource: "actions/foo",
		},
		testCase{
			methodName:       "DeleteThingReference",
			additionalArgs:   []interface{}{strfmt.UUID("foo"), "some prop", (*models.SingleRef)(nil)},
			expectedVerb:     "update",
			expectedResource: "things/foo",
		},
		testCase{
			methodName:       "DeleteActionReference",
			additionalArgs:   []interface{}{strfmt.UUID("foo"), "some prop", (*models.SingleRef)(nil)},
			expectedVerb:     "update",
			expectedResource: "actions/foo",
		},
		testCase{
			methodName:       "UpdateThingReferences",
			additionalArgs:   []interface{}{strfmt.UUID("foo"), "some prop", (models.MultipleRef)(nil)},
			expectedVerb:     "update",
			expectedResource: "things/foo",
		},
		testCase{
			methodName:       "UpdateActionReferences",
			additionalArgs:   []interface{}{strfmt.UUID("foo"), "some prop", (models.MultipleRef)(nil)},
			expectedVerb:     "update",
			expectedResource: "actions/foo",
		},
	}

	t.Run("verify that a test for every public method exists", func(t *testing.T) {
		testedMethods := make([]string, len(tests), len(tests))
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
			repo := &fakeRepo{}
			schemaManager := &fakeSchemaManager{}
			locks := &fakeLocks{}
			network := &fakeNetwork{}
			cfg := &config.WeaviateConfig{}
			authorizer := &authDenier{}
			manager := NewManager(repo, locks, schemaManager, network, cfg, logger, authorizer)

			args := append([]interface{}{context.Background(), principal}, test.additionalArgs...)
			out, _ := callFuncByName(manager, test.methodName, args...)

			require.Len(t, authorizer.calls, 1, "authorizer must be called")
			assert.Equal(t, errors.New("just a test fake"), out[len(out)-1].Interface(),
				"execution must abort with authorizer error")
			assert.Equal(t, authorizeCall{principal, test.expectedVerb, test.expectedResource},
				authorizer.calls[0], "correct paramteres must have been used on authorizer")
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
		testCase{
			methodName:       "AddActions",
			additionalArgs:   []interface{}{[]*models.Action{}, []*string{}},
			expectedVerb:     "create",
			expectedResource: "batch/actions",
		},

		testCase{
			methodName:       "AddThings",
			additionalArgs:   []interface{}{[]*models.Thing{}, []*string{}},
			expectedVerb:     "create",
			expectedResource: "batch/things",
		},

		testCase{
			methodName:       "AddReferences",
			additionalArgs:   []interface{}{[]*models.BatchReference{}},
			expectedVerb:     "update",
			expectedResource: "batch/*",
		},
	}

	t.Run("verify that a test for every public method exists", func(t *testing.T) {
		testedMethods := make([]string, len(tests), len(tests))
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
			repo := &fakeRepo{}
			schemaManager := &fakeSchemaManager{}
			locks := &fakeLocks{}
			network := &fakeNetwork{}
			cfg := &config.WeaviateConfig{}
			authorizer := &authDenier{}
			manager := NewBatchManager(repo, locks, schemaManager, network, cfg, logger, authorizer)

			args := append([]interface{}{context.Background(), principal}, test.additionalArgs...)
			out, _ := callFuncByName(manager, test.methodName, args...)

			require.Len(t, authorizer.calls, 1, "authorizer must be called")
			assert.Equal(t, errors.New("just a test fake"), out[len(out)-1].Interface(),
				"execution must abort with authorizer error")
			assert.Equal(t, authorizeCall{principal, test.expectedVerb, test.expectedResource},
				authorizer.calls[0], "correct paramteres must have been used on authorizer")
		}
	})
}

func Test_Traverser_Authorization(t *testing.T) {

	type testCase struct {
		methodName       string
		additionalArgs   []interface{}
		expectedVerb     string
		expectedResource string
	}

	tests := []testCase{
		testCase{
			methodName:       "LocalGetClass",
			additionalArgs:   []interface{}{&LocalGetParams{}},
			expectedVerb:     "get",
			expectedResource: "traversal/*",
		},

		testCase{
			methodName:       "LocalGetMeta",
			additionalArgs:   []interface{}{&GetMetaParams{}},
			expectedVerb:     "get",
			expectedResource: "traversal/*",
		},

		testCase{
			methodName:       "LocalAggregate",
			additionalArgs:   []interface{}{&AggregateParams{}},
			expectedVerb:     "get",
			expectedResource: "traversal/*",
		},

		testCase{
			methodName:       "LocalFetchFuzzy",
			additionalArgs:   []interface{}{FetchFuzzySearch{}},
			expectedVerb:     "get",
			expectedResource: "traversal/*",
		},

		testCase{
			methodName:       "LocalFetchKindClass",
			additionalArgs:   []interface{}{&FetchSearch{}},
			expectedVerb:     "get",
			expectedResource: "traversal/*",
		},
	}

	t.Run("verify that a test for every public method exists", func(t *testing.T) {
		testedMethods := make([]string, len(tests), len(tests))
		for i, test := range tests {
			testedMethods[i] = test.methodName
		}

		for _, method := range allExportedMethods(&Traverser{}) {
			assert.Contains(t, testedMethods, method)
		}
	})

	t.Run("verify the tested methods require correct permissions from the authorizer", func(t *testing.T) {
		principal := &models.Principal{}
		logger, _ := test.NewNullLogger()
		for _, test := range tests {
			repo := &fakeRepo{}
			locks := &fakeLocks{}
			authorizer := &authDenier{}
			c11y := &fakeC11y{}
			manager := NewTraverser(locks, repo, c11y, logger, authorizer)

			args := append([]interface{}{context.Background(), principal}, test.additionalArgs...)
			out, _ := callFuncByName(manager, test.methodName, args...)

			require.Len(t, authorizer.calls, 1, "authorizer must be called")
			assert.Equal(t, errors.New("just a test fake"), out[len(out)-1].Interface(),
				"execution must abort with authorizer error")
			assert.Equal(t, authorizeCall{principal, test.expectedVerb, test.expectedResource},
				authorizer.calls[0], "correct paramteres must have been used on authorizer")
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

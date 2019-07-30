//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
// 
//  Copyright © 2016 - 2019 Weaviate. All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

package schema

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
		testCase{
			methodName:       "GetSchema",
			expectedVerb:     "list",
			expectedResource: "schema/*",
		},

		testCase{
			methodName:       "AddThing",
			additionalArgs:   []interface{}{&models.Class{}},
			expectedVerb:     "create",
			expectedResource: "schema/things",
		},
		testCase{
			methodName:       "AddAction",
			additionalArgs:   []interface{}{&models.Class{}},
			expectedVerb:     "create",
			expectedResource: "schema/actions",
		},

		testCase{
			methodName:       "UpdateThing",
			additionalArgs:   []interface{}{"somename", &models.Class{}},
			expectedVerb:     "update",
			expectedResource: "schema/things",
		},
		testCase{
			methodName:       "UpdateAction",
			additionalArgs:   []interface{}{"somename", &models.Class{}},
			expectedVerb:     "update",
			expectedResource: "schema/actions",
		},

		testCase{
			methodName:       "DeleteThing",
			additionalArgs:   []interface{}{"somename"},
			expectedVerb:     "delete",
			expectedResource: "schema/things",
		},
		testCase{
			methodName:       "DeleteAction",
			additionalArgs:   []interface{}{"somename"},
			expectedVerb:     "delete",
			expectedResource: "schema/actions",
		},

		testCase{
			methodName:       "AddThingProperty",
			additionalArgs:   []interface{}{"somename", &models.Property{}},
			expectedVerb:     "update",
			expectedResource: "schema/things",
		},
		testCase{
			methodName:       "AddActionProperty",
			additionalArgs:   []interface{}{"somename", &models.Property{}},
			expectedVerb:     "update",
			expectedResource: "schema/actions",
		},

		testCase{
			methodName:       "UpdateThingProperty",
			additionalArgs:   []interface{}{"somename", "someprop", &models.Property{}},
			expectedVerb:     "update",
			expectedResource: "schema/things",
		},
		testCase{
			methodName:       "UpdateActionProperty",
			additionalArgs:   []interface{}{"somename", "someprop", &models.Property{}},
			expectedVerb:     "update",
			expectedResource: "schema/actions",
		},
		testCase{
			methodName:       "UpdatePropertyAddDataType",
			additionalArgs:   []interface{}{kind.Thing, "somename", "someprop", "datatype"},
			expectedVerb:     "update",
			expectedResource: "schema/things",
		},

		testCase{
			methodName:       "DeleteThingProperty",
			additionalArgs:   []interface{}{"somename", "someprop"},
			expectedVerb:     "update",
			expectedResource: "schema/things",
		},
		testCase{
			methodName:       "DeleteActionProperty",
			additionalArgs:   []interface{}{"somename", "someprop"},
			expectedVerb:     "update",
			expectedResource: "schema/actions",
		},
	}

	t.Run("verify that a test for every public method exists", func(t *testing.T) {
		// t.Skip()
		testedMethods := make([]string, len(tests), len(tests))
		for i, test := range tests {
			testedMethods[i] = test.methodName
		}

		for _, method := range allExportedMethods(&Manager{}) {
			switch method {
			case "TriggerSchemaUpdateCallbacks", "RegisterSchemaUpdateCallback", "UpdateMeta", "GetSchemaSkipAuth":
				// don't require auth on methods which are exported because other
				// packages need to call them for maintenance and other regular jobs,
				// but aren't user facing
				continue
			}
			assert.Contains(t, testedMethods, method)
		}
	})

	t.Run("verify the tested methods require correct permissions from the authorizer", func(t *testing.T) {
		principal := &models.Principal{}
		logger, _ := test.NewNullLogger()
		for _, test := range tests {
			t.Run(test.methodName, func(t *testing.T) {
				authorizer := &authDenier{}
				manager, err := NewManager(&NilMigrator{}, newFakeRepo(), newFakeLocks(),
					nil, logger, &fakeC11y{}, authorizer, nil)
				require.Nil(t, err)

				var args []interface{}
				if test.methodName == "GetSchema" {
					// no context on this method
					args = append([]interface{}{principal}, test.additionalArgs...)
				} else {
					args = append([]interface{}{context.Background(), principal}, test.additionalArgs...)

				}
				out, _ := callFuncByName(manager, test.methodName, args...)

				require.Len(t, authorizer.calls, 1, "authorizer must be called")
				assert.Equal(t, errors.New("just a test fake"), out[len(out)-1].Interface(),
					"execution must abort with authorizer error")
				assert.Equal(t, authorizeCall{principal, test.expectedVerb, test.expectedResource},
					authorizer.calls[0], "correct paramteres must have been used on authorizer")
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

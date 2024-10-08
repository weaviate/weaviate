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

package backup

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
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/auth/authorization/mocks"
)

// A component-test like test suite that makes sure that every available UC is
// potentially protected with the Authorization plugin

func Test_Authorization(t *testing.T) {
	req := &BackupRequest{ID: "123", Backend: "s3"}
	type testCase struct {
		methodName       string
		additionalArgs   []interface{}
		expectedVerb     string
		expectedResource string
	}

	tests := []testCase{
		{
			methodName:       "Backup",
			additionalArgs:   []interface{}{req},
			expectedVerb:     authorization.ADD,
			expectedResource: "backups/s3/123",
		},
		{
			methodName:       "BackupStatus",
			additionalArgs:   []interface{}{"s3", "123"},
			expectedVerb:     authorization.GET,
			expectedResource: "backups/s3/123",
		},
		{
			methodName:       "Restore",
			additionalArgs:   []interface{}{req},
			expectedVerb:     authorization.RESTORE,
			expectedResource: "backups/s3/123/restore",
		},
		{
			methodName:       "RestorationStatus",
			additionalArgs:   []interface{}{"s3", "123"},
			expectedVerb:     authorization.GET,
			expectedResource: "backups/s3/123/restore",
		},
		{
			methodName:       "Cancel",
			additionalArgs:   []interface{}{"s3", "123"},
			expectedVerb:     authorization.DELETE,
			expectedResource: "backups/s3/123",
		},
		{
			methodName:       "List",
			additionalArgs:   []interface{}{"s3"},
			expectedVerb:     authorization.GET,
			expectedResource: "backups/s3",
		},
	}

	t.Run("verify that a test for every public method exists", func(t *testing.T) {
		testedMethods := make([]string, len(tests))
		for i, test := range tests {
			testedMethods[i] = test.methodName
		}

		for _, method := range allExportedMethods(&Scheduler{}) {
			switch method {
			case "OnCommit", "OnAbort", "OnCanCommit", "OnStatus":
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
				authorizer := mocks.NewMockAuthorizer()
				authorizer.SetErr(errors.New("just a test fake"))
				s := NewScheduler(authorizer, nil, nil, nil, nil, &fakeSchemaManger{}, logger)
				require.NotNil(t, s)

				args := append([]interface{}{context.Background(), principal}, test.additionalArgs...)
				out, _ := callFuncByName(s, test.methodName, args...)

				require.Len(t, authorizer.Calls(), 1, "authorizer must be called")
				assert.Equal(t, errors.New("just a test fake"), out[len(out)-1].Interface(),
					"execution must abort with authorizer error")
				assert.Equal(t, mocks.AuthZReq{Principal: principal, Verb: test.expectedVerb, Resource: test.expectedResource},
					authorizer.Calls()[0], "correct parameters must have been used on authorizer")
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

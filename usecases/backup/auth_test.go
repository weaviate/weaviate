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
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

// A component-test like test suite that makes sure that every available UC is
// potentially protected with the Authorization plugin

func Test_Authorization(t *testing.T) {
	req := &BackupRequest{ID: "123", Backend: "filesystem"}
	type testCase struct {
		methodName       string
		additionalArgs   []interface{}
		classes          []string
		expectedVerb     string
		expectedResource string
		ignoreAuthZ      bool
	}

	tests := []testCase{
		{
			methodName:       "Backup",
			additionalArgs:   []interface{}{req},
			expectedVerb:     authorization.CREATE,
			expectedResource: authorization.Backups("ABC")[0],
			classes:          []string{"ABC"},
		},
		{
			methodName:       "BackupStatus",
			additionalArgs:   []interface{}{"filesystem", "123", "", ""},
			expectedVerb:     authorization.READ,
			expectedResource: authorization.Backups("ABC")[0],
			classes:          []string{"ABC"},
			ignoreAuthZ:      true,
		},
		{
			methodName:       "Restore",
			additionalArgs:   []interface{}{req},
			expectedVerb:     authorization.READ,
			expectedResource: authorization.Backups("ABC")[0],
			classes:          []string{"ABC"},
		},
		{
			methodName:       "RestorationStatus",
			additionalArgs:   []interface{}{"filesystem", "123", "", ""},
			expectedVerb:     authorization.READ,
			expectedResource: authorization.Backups("ABC")[0],
			classes:          []string{"ABC"},
			ignoreAuthZ:      true,
		},
		{
			methodName:       "Cancel",
			additionalArgs:   []interface{}{"filesystem", "123", "", ""},
			expectedVerb:     authorization.DELETE,
			expectedResource: authorization.Backups("ABC")[0],
			classes:          []string{"ABC"},
		},
		{
			methodName:       "List",
			additionalArgs:   []interface{}{"filesystem"},
			expectedVerb:     authorization.READ,
			expectedResource: authorization.Backups("ABC")[0],
			classes:          []string{"ABC"},
			ignoreAuthZ:      true,
		},
	}

	t.Run("verify that a test for every public method exists", func(t *testing.T) {
		testedMethods := make([]string, len(tests))
		for i, test := range tests {
			testedMethods[i] = test.methodName
		}

		for _, method := range allExportedMethods(&Scheduler{}) {
			switch method {
			case "OnCommit", "OnAbort", "OnCanCommit",
				"OnStatus", "CleanupUnfinishedBackups":
				continue
			}
			assert.Contains(t, testedMethods, method)
		}
	})

	t.Run("verify the tested methods require correct permissions from the authorizer", func(t *testing.T) {
		// TODO: mooga come back
		// logger, _ := test.NewNullLogger()
		// // for _, test := range tests {
		// 	t.Run(test.methodName, func(t *testing.T) {
		// 		authorizer := authZMocks.NewAuthorizer(t)
		// 		selector := bmocks.NewSelector(t)
		// 		backupProvider := bmocks.NewBackupBackendProvider(t)
		// 		nodeResolver := bmocks.NewNodeResolver(t)
		// 		modulecapabilities := cmocks.NewBackupBackend(t)

		// 		backupProvider.On("BackupBackend", mock.Anything).Return(modulecapabilities, nil).Maybe()

		// 		modulecapabilities.On("IsExternal").Return(false).Maybe()
		// 		modulecapabilities.On("HomeDir", mock.Anything, mock.Anything, mock.Anything).Return("/").Maybe()

		// 		d, err := json.Marshal(backup.DistributedBackupDescriptor{
		// 			Status: backup.Cancelled,
		// 		})
		// 		require.Nil(t, err)
		// 		var dd backup.DistributedBackupDescriptor
		// 		err = json.Unmarshal(d, &dd)
		// 		require.Nil(t, err)

		// 		modulecapabilities.On("GetObject", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(d, nil).Maybe()
		// 		modulecapabilities.On("Initialize", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

		// 		nodeResolver.On("NodeCount").Return(1).Maybe()

		// 		selector.On("ListClasses", mock.Anything).Return(test.classes).Maybe()
		// 		selector.On("Backupable", mock.Anything, mock.Anything).Return(nil).Maybe()

		// 		s := NewScheduler(authorizer, nil, selector, backupProvider, nodeResolver, &fakeSchemaManger{}, logger)
		// 		require.NotNil(t, s)

		// 		args := append([]interface{}{context.Background(), &models.Principal{}}, test.additionalArgs...)
		// 		callFuncByName(s, test.methodName, args...)
		// 		if !test.ignoreAuthZ {
		// 			authorizer.On("Authorize", mock.Anything, test.expectedVerb, test.expectedResource).Return(nil)
		// 			// require.Len(t, authorizer.Calls(), 1, "authorizer must be called")
		// 			// assert.Equal(t, errors.New("just a test fake"), out[len(out)-1].Interface(),
		// 			// 	"execution must abort with authorizer error")
		// 			// assert.Equal(t, mocks.AuthZReq{Principal: principal, Verb: test.expectedVerb, Resources: []string{test.expectedResource}},
		// 			// 	authorizer.Calls()[0], "correct parameters must have been used on authorizer")
		// 		}
		// 	})
		// }
	})
}

// inspired by https://stackoverflow.com/a/33008200
// func callFuncByName(manager interface{}, funcName string, params ...interface{}) (out []reflect.Value, err error) {
// 	managerValue := reflect.ValueOf(manager)
// 	m := managerValue.MethodByName(funcName)
// 	if !m.IsValid() {
// 		return make([]reflect.Value, 0), fmt.Errorf("Method not found \"%s\"", funcName)
// 	}
// 	in := make([]reflect.Value, len(params))
// 	for i, param := range params {
// 		in[i] = reflect.ValueOf(param)
// 	}
// 	out = m.Call(in)
// 	return
// }

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

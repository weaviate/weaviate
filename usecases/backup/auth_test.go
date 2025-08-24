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
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
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
			methodName:     "BackupStatus",
			additionalArgs: []interface{}{"filesystem", "123", "", ""},
			classes:        []string{"ABC"},
			ignoreAuthZ:    true,
		},
		{
			methodName:       "Restore",
			additionalArgs:   []interface{}{req},
			expectedVerb:     authorization.CREATE,
			expectedResource: authorization.Backups("ABC")[0],
			classes:          []string{"ABC"},
		},
		{
			methodName:     "RestorationStatus",
			additionalArgs: []interface{}{"filesystem", "123", "", ""},
			classes:        []string{"ABC"},
			ignoreAuthZ:    true,
		},
		{
			methodName:       "Cancel",
			additionalArgs:   []interface{}{"filesystem", "123", "", ""},
			expectedVerb:     authorization.DELETE,
			expectedResource: authorization.Backups("ABC")[0],
			classes:          []string{"ABC"},
		},
		{
			methodName:     "List",
			additionalArgs: []interface{}{"filesystem"},
			classes:        []string{"ABC"},
			ignoreAuthZ:    true,
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
		logger, _ := test.NewNullLogger()
		for _, test := range tests {
			t.Run(test.methodName, func(t *testing.T) {
				authorizer := authorization.NewMockAuthorizer(t)
				selector := NewMockSelector(t)
				backupProvider := NewMockBackupBackendProvider(t)
				nodeResolver := NewMockNodeResolver(t)
				modcapabilities := modulecapabilities.NewMockBackupBackend(t)

				backupProvider.On("BackupBackend", mock.Anything).Return(modcapabilities, nil).Maybe()

				modcapabilities.On("IsExternal").Return(false).Maybe()
				modcapabilities.On("HomeDir", mock.Anything, mock.Anything, mock.Anything).Return("/").Maybe()

				d, err := json.Marshal(backup.DistributedBackupDescriptor{
					StartedAt: time.Now(),
					Nodes: map[string]*backup.NodeDescriptor{
						"node-0": {
							Classes: test.classes,
							Status:  backup.Success,
						},
					},
					Status:        backup.Success,
					ID:            "123",
					Version:       "2.1",
					ServerVersion: "x.x.x",
					Error:         "",
				})
				require.Nil(t, err)
				var dd backup.DistributedBackupDescriptor
				err = json.Unmarshal(d, &dd)
				require.Nil(t, err)

				var notFound interface{}
				if test.methodName == "Backup" {
					notFound = backup.ErrNotFound{}
				} else {
					notFound = nil
				}

				modcapabilities.On("GetObject", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(d, notFound).Maybe()
				modcapabilities.On("PutObject", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

				modcapabilities.On("Initialize", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

				// AllBackups mock expectation for List method
				if test.methodName == "List" {
					modcapabilities.On("AllBackups", mock.Anything).Return([]*backup.DistributedBackupDescriptor{&dd}, nil)
				}

				nodeResolver.On("NodeCount").Return(1).Maybe()
				nodeResolver.On("LeaderID").Return("node-0").Maybe()
				nodeResolver.On("AllNames").Return([]string{"node-0"}).Maybe()
				nodeResolver.On("NodeHostname", mock.Anything).Return("localhost", false).Maybe()

				selector.On("Shards", mock.Anything, test.classes[0]).Return([]string{"node-0"}, nil).Maybe()
				selector.On("ListClasses", mock.Anything).Return(test.classes).Maybe()
				selector.On("Backupable", mock.Anything, mock.Anything).Return(nil).Maybe()

				s := NewScheduler(authorizer, nil, selector, backupProvider, nodeResolver, &fakeSchemaManger{}, logger)
				require.NotNil(t, s)

				if !test.ignoreAuthZ {
					authorizer.On("Authorize", mock.Anything, mock.Anything, test.expectedVerb, test.expectedResource).Return(nil)
				}

				args := append([]interface{}{context.Background(), &models.Principal{}}, test.additionalArgs...)
				callFuncByName(s, test.methodName, args...)
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

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
	authzerrors "github.com/weaviate/weaviate/usecases/auth/authorization/errors"
	"github.com/weaviate/weaviate/usecases/auth/authorization/mocks"
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

	// The expected verb/resource below is the *first* authz call made by the
	// scheduler method; any subsequent, more fine-grained checks fall through
	// to the catch-all registered in the test body.
	tests := []testCase{
		{
			// req has an empty Include, so the first authz call is the
			// per-class filter over the resolved class list.
			methodName:       "Backup",
			additionalArgs:   []interface{}{req},
			expectedVerb:     authorization.CREATE,
			expectedResource: authorization.Backups("ABC")[0],
			classes:          []string{"ABC"},
		},
		{
			// BackupStatus authorizes against the backup's actual classes
			// (resolved from the meta).
			methodName:       "BackupStatus",
			additionalArgs:   []interface{}{"filesystem", "123", "", ""},
			expectedVerb:     authorization.READ,
			expectedResource: authorization.Backups("ABC")[0],
			classes:          []string{"ABC"},
		},
		{
			// req has an empty Include, so the first authz call is the
			// per-class filter over the backup's meta classes.
			methodName:       "Restore",
			additionalArgs:   []interface{}{req, false},
			expectedVerb:     authorization.CREATE,
			expectedResource: authorization.Backups("ABC")[0],
			classes:          []string{"ABC"},
		},
		{
			// RestorationStatus authorizes against the backup's actual classes
			// (resolved from the meta).
			methodName:       "RestorationStatus",
			additionalArgs:   []interface{}{"filesystem", "123", "", ""},
			expectedVerb:     authorization.READ,
			expectedResource: authorization.Backups("ABC")[0],
			classes:          []string{"ABC"},
		},
		{
			// Cancel authorizes DELETE against the backup's actual classes
			// (resolved from the meta).
			methodName:       "Cancel",
			additionalArgs:   []interface{}{"filesystem", "123", "", ""},
			expectedVerb:     authorization.DELETE,
			expectedResource: authorization.Backups("ABC")[0],
			classes:          []string{"ABC"},
		},
		{
			methodName:       "CancelRestore",
			additionalArgs:   []interface{}{"filesystem", "123", "", ""},
			expectedVerb:     authorization.DELETE,
			expectedResource: authorization.Backups("ABC")[0],
			classes:          []string{"ABC"},
		},
		{
			// List authorizes per-backup READ against its resolved classes
			// ("backups/collections/ABC") and filters the response to what
			// the caller is permitted to see. Args: backend, sortingOrder,
			// includeBaseBackupID.
			methodName:       "List",
			additionalArgs:   []interface{}{"filesystem", func(s string) *string { return &s }("desc"), false},
			expectedVerb:     authorization.READ,
			expectedResource: authorization.Backups("ABC")[0],
			classes:          []string{"ABC"},
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
		for _, test := range tests {
			t.Run(test.methodName, func(t *testing.T) {
				authorizer := authorization.NewMockAuthorizer(t)
				s := newAuthzTestScheduler(t, test.methodName, test.classes, authorizer)

				if !test.ignoreAuthZ {
					authorizer.On("Authorize", mock.Anything, mock.Anything, test.expectedVerb, test.expectedResource).Return(nil).Once()
					// Subsequent fine-grained authz calls (e.g. Backup/Restore
					// re-authorizing on resolved classes, Cancel re-authorizing
					// on meta classes) are allowed but not required.
					authorizer.On("Authorize", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
				}

				args := append([]interface{}{context.Background(), &models.Principal{}}, test.additionalArgs...)
				callFuncByName(s, test.methodName, args...)
			})
		}
	})

	// The classes come from the cluster or the backup, not from the request.
	t.Run("a denial names none of the classes it was checked against", func(t *testing.T) {
		for _, test := range tests {
			for _, pr := range []*models.Principal{{}, nil} {
				name := test.methodName
				if pr == nil {
					name += "/anonymous"
				}
				t.Run(name, func(t *testing.T) {
					s := newAuthzTestScheduler(t, test.methodName, test.classes, permitBackupsOf(t))

					args := append([]interface{}{context.Background(), pr}, test.additionalArgs...)
					out, _ := callFuncByName(s, test.methodName, args...)
					err, _ := out[len(out)-1].Interface().(error)
					if test.methodName == "List" {
						// List drops a denied backup from the response instead.
						require.NoError(t, err)
						assert.Empty(t, *out[0].Interface().(*models.BackupListResponse))
						return
					}
					require.ErrorAs(t, err, &authzerrors.Forbidden{})
					assert.NotContains(t, err.Error(), test.classes[0])
					assert.Contains(t, err.Error(), authorization.Backups()[0])
				})
			}
		}
	})

	t.Run("an authorizer failure is not reported as a denial", func(t *testing.T) {
		for _, test := range tests {
			switch test.methodName {
			case "BackupStatus", "RestorationStatus", "Cancel", "CancelRestore":
			default:
				continue
			}
			t.Run(test.methodName, func(t *testing.T) {
				failing := mocks.NewMockAuthorizer()
				failing.SetErr(ErrAny)
				s := newAuthzTestScheduler(t, test.methodName, test.classes, failing)

				args := append([]interface{}{context.Background(), &models.Principal{}}, test.additionalArgs...)
				out, _ := callFuncByName(s, test.methodName, args...)
				err, _ := out[len(out)-1].Interface().(error)
				require.ErrorIs(t, err, ErrAny)
			})
		}
	})
}

// newAuthzTestScheduler returns a scheduler whose cluster holds classes and
// whose backend holds one successful backup of them. For methodName "Backup"
// the backend reports that backup as not found, so a new one can be created.
func newAuthzTestScheduler(t *testing.T, methodName string, classes []string, authorizer authorization.Authorizer) *Scheduler {
	logger, _ := test.NewNullLogger()
	selector := NewMockSelector(t)
	backupProvider := NewMockBackupBackendProvider(t)
	nodeResolver := NewMockNodeResolver(t)
	modcapabilities := modulecapabilities.NewMockBackupBackend(t)

	backupProvider.On("BackupBackend", mock.Anything, mock.Anything).Return(modcapabilities, nil).Maybe()

	modcapabilities.On("IsExternal").Return(false).Maybe()
	modcapabilities.On("HomeDir", mock.Anything, mock.Anything, mock.Anything).Return("/").Maybe()

	d, err := json.Marshal(backup.DistributedBackupDescriptor{
		StartedAt: time.Now(),
		Nodes: map[string]*backup.NodeDescriptor{
			"node-0": {
				Classes: classes,
				Status:  backup.Success,
			},
		},
		Status:          backup.Success,
		ID:              "123",
		Version:         "2.1",
		ServerVersion:   "x.x.x",
		Error:           "",
		CompressionType: backup.CompressionZSTD,
	})
	require.NoError(t, err)
	var dd backup.DistributedBackupDescriptor
	err = json.Unmarshal(d, &dd)
	require.NoError(t, err)

	var notFound interface{}
	if methodName == "Backup" {
		notFound = backup.ErrNotFound{}
	} else {
		notFound = nil
	}

	modcapabilities.On("GetObject", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(d, notFound).Maybe()
	modcapabilities.On("PutObject", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

	modcapabilities.On("Initialize", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

	// List reads its backups from AllBackups.
	if methodName == "List" {
		modcapabilities.On("AllBackups", mock.Anything).Return([]*backup.DistributedBackupDescriptor{&dd}, nil)
	}

	nodeResolver.On("NodeCount").Return(1).Maybe()
	nodeResolver.On("LeaderID").Return("node-0").Maybe()
	nodeResolver.On("AllNames").Return([]string{"node-0"}).Maybe()
	nodeResolver.On("NodeHostname", mock.Anything).Return("localhost", false).Maybe()

	selector.On("Shards", mock.Anything, classes[0]).Return([]string{"node-0"}, nil).Maybe()
	selector.On("ListClasses", mock.Anything).Return(classes).Maybe()
	selector.On("Backupable", mock.Anything, mock.Anything).Return(nil).Maybe()

	s := NewScheduler(authorizer, nil, selector, nil, nil, backupProvider, nodeResolver, &fakeSchemaManger{}, nil, nil, nil, logger)
	require.NotNil(t, s)
	return s
}

// permitBackupsOf returns an authorizer that permits backups of the permitted
// classes and denies any other resource, wrapping Forbidden as RBAC does.
func permitBackupsOf(t *testing.T, permitted ...string) *authorization.MockAuthorizer {
	allowed := make(map[string]bool, len(permitted))
	for _, c := range permitted {
		allowed[authorization.Backups(c)[0]] = true
	}
	a := authorization.NewMockAuthorizer(t)
	a.EXPECT().Authorize(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, pr *models.Principal, verb string, resources ...string) error {
			for _, r := range resources {
				if !allowed[r] {
					return fmt.Errorf("rbac: %w", authzerrors.NewForbidden(pr, verb, r))
				}
			}
			return nil
		}).Maybe()
	return a
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

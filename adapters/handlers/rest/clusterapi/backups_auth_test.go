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

package clusterapi_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/handlers/rest/clusterapi"
	"github.com/weaviate/weaviate/usecases/backup"
	"github.com/weaviate/weaviate/usecases/cluster"
)

// backupsAPI names the methods under test; clusterapi.NewBackups returns an
// unexported type.
type backupsAPI interface {
	Commit() http.Handler
	Abort() http.Handler
	Status() http.Handler
}

func TestBackupsBasicAuth(t *testing.T) {
	const (
		user = "alice"
		pass = "s3cret"
	)

	endpoints := []struct {
		name        string
		handler     func(b backupsAPI) http.Handler
		expect      func(m *fakeBackupManager)
		successCode int
	}{
		{
			name:        "commit",
			handler:     backupsAPI.Commit,
			expect:      func(m *fakeBackupManager) { m.On("OnCommit", &backup.StatusRequest{}).Return(nil) },
			successCode: http.StatusCreated,
		},
		{
			name:        "abort",
			handler:     backupsAPI.Abort,
			expect:      func(m *fakeBackupManager) { m.On("OnAbort", &backup.AbortRequest{}).Return(nil) },
			successCode: http.StatusNoContent,
		},
		{
			name:    "status",
			handler: backupsAPI.Status,
			expect: func(m *fakeBackupManager) {
				m.On("OnStatus", &backup.StatusRequest{}).Return(&backup.StatusResponse{})
			},
			successCode: http.StatusOK,
		},
	}

	for _, ep := range endpoints {
		for _, withCreds := range []bool{false, true} {
			name := ep.name + " without credentials is denied"
			if withCreds {
				name = ep.name + " with correct credentials is allowed"
			}
			t.Run(name, func(t *testing.T) {
				// The deny case registers no expectations, so any manager call
				// fails the test.
				manager := &fakeBackupManager{}
				if withCreds {
					ep.expect(manager)
				}
				b := clusterapi.NewBackups(manager, clusterapi.NewBasicAuthHandler(cluster.AuthConfig{
					BasicAuth: cluster.BasicAuth{Username: user, Password: pass},
				}))

				req, err := http.NewRequest(http.MethodPost, "/", strings.NewReader("{}"))
				require.NoError(t, err)
				if withCreds {
					req.SetBasicAuth(user, pass)
				}
				rec := httptest.NewRecorder()

				ep.handler(b).ServeHTTP(rec, req)

				if withCreds {
					assert.Equal(t, ep.successCode, rec.Code)
					manager.AssertExpectations(t)
				} else {
					assert.Equal(t, http.StatusUnauthorized, rec.Code)
				}
			})
		}
	}
}

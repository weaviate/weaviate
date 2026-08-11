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
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/handlers/rest/clusterapi"
	entitiesbackup "github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/usecases/auth/authorization/mocks"
	"github.com/weaviate/weaviate/usecases/backup"
	"github.com/weaviate/weaviate/usecases/cluster"
	"github.com/weaviate/weaviate/usecases/config"
)

// The backup Handler's dependencies, stubbed down to what taking a participant
// backup slot touches. Only the backend has a generated mock.
type fakeBackupSourcer struct{}

func (fakeBackupSourcer) ReleaseBackup(context.Context, string, string) error { return nil }
func (fakeBackupSourcer) Backupable(context.Context, []string) error          { return nil }

func (fakeBackupSourcer) BackupDescriptors(context.Context, string, []string,
	[]*entitiesbackup.BackupDescriptor,
) <-chan entitiesbackup.ClassDescriptor {
	return nil
}

type fakeBackupSchema struct{}

func (fakeBackupSchema) RestoreClass(context.Context, *entitiesbackup.ClassDescriptor,
	map[string]string, bool, bool,
) error {
	return nil
}
func (fakeBackupSchema) NodeName() string              { return "node1" }
func (fakeBackupSchema) NamespacesEnabled() bool       { return false }
func (fakeBackupSchema) ClassEqual(name string) string { return name }

// fakeSnapshotter serves both the RBAC and the dynamic-user snapshotter slot;
// their method sets are identical.
type fakeSnapshotter struct{}

func (fakeSnapshotter) Snapshot(...string) ([]byte, error) { return nil, nil }
func (fakeSnapshotter) Restore([]byte, bool) error         { return nil }

// busyBackupProbe returns the production probe with a participant backup slot
// genuinely held, taken via OnCanCommit since the probe has no setter.
func busyBackupProbe(t *testing.T, backupID string) *backup.NodeActivityProbe {
	t.Helper()

	logger, _ := logrustest.NewNullLogger()

	store := modulecapabilities.NewMockBackupBackend(t)
	store.EXPECT().HomeDir(mock.Anything, mock.Anything, mock.Anything).
		Return("bucket/backups/" + backupID).Maybe()
	store.EXPECT().GetObject(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(nil, entitiesbackup.ErrNotFound{}).Maybe()
	store.EXPECT().Initialize(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(nil).Maybe()

	backends := backup.NewMockBackupBackendProvider(t)
	backends.EXPECT().BackupBackend(mock.Anything, mock.Anything).Return(store, nil).Maybe()

	participant := backup.NewHandler(logger, config.Backup{}, mocks.NewMockAuthorizer(),
		fakeBackupSchema{}, fakeBackupSourcer{}, backends, fakeSnapshotter{}, fakeSnapshotter{})

	probe := backup.NewNodeActivityProbe(participant)
	resp := participant.OnCanCommit(context.Background(), &backup.Request{
		Method:  backup.OpCreate,
		ID:      backupID,
		Classes: []string{"Movies"},
		Backend: "filesystem",
		// Long enough that the slot cannot lapse mid-request.
		Duration: time.Minute,
	})
	require.Empty(t, resp.Err)
	return probe
}

func TestInternalBackupsNodeActivity(t *testing.T) {
	tests := []struct {
		name       string
		probe      func(t *testing.T) *backup.NodeActivityProbe
		wantStatus int
		wantBody   string
	}{
		{
			name:       "idle probe",
			probe:      func(*testing.T) *backup.NodeActivityProbe { return backup.NewNodeActivityProbe(nil) },
			wantStatus: http.StatusOK,
			wantBody:   `{"probe":"weaviate/backup-node-activity","busy":false}`,
		},
		{
			// kind/id are pinned: the caller formats them into the 409 it raises.
			name:       "busy with a backup",
			probe:      func(t *testing.T) *backup.NodeActivityProbe { return busyBackupProbe(t, "backup-1") },
			wantStatus: http.StatusOK,
			wantBody:   `{"probe":"weaviate/backup-node-activity","busy":true,"kind":"backup","id":"backup-1"}`,
		},
		{
			name:       "probe unavailable",
			probe:      func(*testing.T) *backup.NodeActivityProbe { return nil },
			wantStatus: http.StatusServiceUnavailable,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler := clusterapi.NewBackups(nil, tt.probe(t), clusterapi.NewNoopAuthHandler(), nullLogger())
			server := httptest.NewServer(handler.NodeActivity())
			defer server.Close()

			res, err := server.Client().Get(server.URL + "/backups/node-activity")
			require.NoError(t, err)
			defer res.Body.Close()

			require.Equal(t, tt.wantStatus, res.StatusCode)
			if tt.wantBody == "" {
				return
			}

			assert.Equal(t, "application/json", res.Header.Get("Content-Type"))
			body, err := io.ReadAll(res.Body)
			require.NoError(t, err)
			assert.JSONEq(t, tt.wantBody, string(body))
		})
	}
}

// An operator tracing a stalled backup gate has to see what each node
// answered, including the node that could not answer at all.
func TestInternalBackupsNodeActivityLogsEveryAnswer(t *testing.T) {
	tests := []struct {
		name     string
		probe    func(t *testing.T) *backup.NodeActivityProbe
		wantBusy bool
		wantKind string
		wantID   string
		wantLog  string
	}{
		{
			name:    "idle",
			probe:   func(*testing.T) *backup.NodeActivityProbe { return backup.NewNodeActivityProbe(nil) },
			wantID:  `""`,
			wantLog: "backup node activity probe answered",
		},
		{
			name:     "busy with a backup",
			probe:    func(t *testing.T) *backup.NodeActivityProbe { return busyBackupProbe(t, "backup-1") },
			wantBusy: true,
			wantKind: "backup",
			wantID:   `"backup-1"`,
			wantLog:  "backup node activity probe answered",
		},
		{
			name:    "cannot answer",
			probe:   func(*testing.T) *backup.NodeActivityProbe { return nil },
			wantLog: "backup node activity probe answered: unavailable on this node",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, hook := logrustest.NewNullLogger()
			logger.SetLevel(logrus.DebugLevel)

			handler := clusterapi.NewBackups(nil, tt.probe(t), clusterapi.NewNoopAuthHandler(), logger)
			server := httptest.NewServer(handler.NodeActivity())
			defer server.Close()

			res, err := server.Client().Get(server.URL + "/backups/node-activity")
			require.NoError(t, err)
			defer res.Body.Close()

			entry := hook.LastEntry()
			require.NotNil(t, entry)
			assert.Equal(t, "backup_node_activity_probe", entry.Data["action"])
			assert.Equal(t, tt.wantLog, entry.Message)
			if tt.wantID == "" {
				assert.NotContains(t, entry.Data, "busy",
					"a node that cannot tell must not log an answer either way")
				return
			}
			assert.Equal(t, tt.wantBusy, entry.Data["busy"])
			assert.Equal(t, tt.wantKind, entry.Data["kind"])
			assert.Equal(t, tt.wantID, entry.Data["id"])
		})
	}
}

// Cluster-internal state: an unauthenticated caller must be refused.
func TestInternalBackupsNodeActivityRequiresAuth(t *testing.T) {
	const (
		user = "alice"
		pass = "s3cret"
	)
	auth := clusterapi.NewBasicAuthHandler(cluster.AuthConfig{
		BasicAuth: cluster.BasicAuth{Username: user, Password: pass},
	})

	tests := []struct {
		name       string
		setAuth    bool
		user, pass string
		wantStatus int
	}{
		{name: "no credentials", wantStatus: http.StatusUnauthorized},
		{name: "wrong user", setAuth: true, user: "mallory", pass: pass, wantStatus: http.StatusUnauthorized},
		{name: "wrong password", setAuth: true, user: user, pass: "guess", wantStatus: http.StatusUnauthorized},
		{name: "correct credentials", setAuth: true, user: user, pass: pass, wantStatus: http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler := clusterapi.NewBackups(nil, backup.NewNodeActivityProbe(nil), auth, nullLogger())
			server := httptest.NewServer(handler.NodeActivity())
			defer server.Close()

			req, err := http.NewRequest(http.MethodGet, server.URL+"/backups/node-activity", nil)
			require.NoError(t, err)
			if tt.setAuth {
				req.SetBasicAuth(tt.user, tt.pass)
			}

			res, err := server.Client().Do(req)
			require.NoError(t, err)
			defer res.Body.Close()

			require.Equal(t, tt.wantStatus, res.StatusCode)
			if tt.wantStatus != http.StatusOK {
				body, err := io.ReadAll(res.Body)
				require.NoError(t, err)
				assert.Empty(t, body, "a refused caller must not be told what this node is doing")
			}
		})
	}
}

func TestInternalBackupsNodeActivityRejectsNonGET(t *testing.T) {
	handler := clusterapi.NewBackups(nil, backup.NewNodeActivityProbe(nil), clusterapi.NewNoopAuthHandler(), nullLogger())
	server := httptest.NewServer(handler.NodeActivity())
	defer server.Close()

	assertRejectsNonGET(t, server, "/backups/node-activity")
}

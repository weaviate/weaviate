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

func (fakeBackupSourcer) ReleaseBackup(context.Context, string, string) error        { return nil }
func (fakeBackupSourcer) Backupable(context.Context, []string) error                 { return nil }
func (fakeBackupSourcer) RefuseIfAnyReindexInFlight(context.Context, []string) error { return nil }
func (fakeBackupSourcer) RefuseIfReindexOverlapped(context.Context, []string, time.Time) error {
	return nil
}

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

type fakeSnapshotter struct{}

func (fakeSnapshotter) Snapshot(...string) ([]byte, error) { return nil, nil }
func (fakeSnapshotter) Restore([]byte, bool) error         { return nil }

type fakeDynUserSnapshotter struct{}

func (fakeDynUserSnapshotter) Snapshot(...string) ([]byte, error) { return nil, nil }
func (fakeDynUserSnapshotter) Restore([]byte, bool) error         { return nil }

// busyBackupProbe returns the production probe with a participant backup slot
// genuinely held. NodeActivityProbe owns no state and has no setter — it reads
// slots the backup subsystem manages — so taking one through OnCanCommit is the
// only way it ever answers busy.
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
		fakeBackupSchema{}, fakeBackupSourcer{}, backends, fakeSnapshotter{}, fakeDynUserSnapshotter{})

	probe := backup.NewNodeActivityProbe(participant)
	resp := participant.OnCanCommit(context.Background(), &backup.Request{
		Method:  backup.OpCreate,
		ID:      backupID,
		Classes: []string{"Movies"},
		Backend: "filesystem",
		// Long enough that the slot cannot lapse mid-request; the pre-commit
		// window self-clears it, which usecases/backup pins separately.
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
			// The answer a reindex gate must refuse to start on. Every field is
			// pinned: the caller formats kind and id into the 409 it raises, and
			// a node that always answers idle turns the whole gate into a no-op.
			name:       "busy with a backup",
			probe:      func(t *testing.T) *backup.NodeActivityProbe { return busyBackupProbe(t, "backup-1") },
			wantStatus: http.StatusOK,
			wantBody:   `{"probe":"weaviate/backup-node-activity","busy":true,"kind":"backup","id":"backup-1"}`,
		},
		{
			name:       "probe not wired",
			probe:      func(*testing.T) *backup.NodeActivityProbe { return nil },
			wantStatus: http.StatusServiceUnavailable,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler := clusterapi.NewBackups(nil, tt.probe(t), clusterapi.NewNoopAuthHandler())
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

// The route reports whether this node is part of a backup, which is
// cluster-internal state: an unauthenticated caller must be refused rather
// than answered.
func TestInternalBackupsNodeActivityRequiresAuth(t *testing.T) {
	auth := clusterapi.NewBasicAuthHandler(cluster.AuthConfig{
		BasicAuth: cluster.BasicAuth{Username: probeUser, Password: probePass},
	})

	assertRequiresBasicAuth(t, "/backups/node-activity",
		func(*testing.T) *httptest.Server {
			return httptest.NewServer(
				clusterapi.NewBackups(nil, backup.NewNodeActivityProbe(nil), auth).NodeActivity())
		},
		func(t *testing.T, res *http.Response, authorized bool) {
			if authorized {
				return
			}
			body, err := io.ReadAll(res.Body)
			require.NoError(t, err)
			assert.Empty(t, body, "a refused caller must not be told what this node is doing")
		})
}

func TestInternalBackupsNodeActivityRejectsNonGET(t *testing.T) {
	handler := clusterapi.NewBackups(nil, backup.NewNodeActivityProbe(nil), clusterapi.NewNoopAuthHandler())
	server := httptest.NewServer(handler.NodeActivity())
	defer server.Close()

	assertRejectsNonGET(t, server, "/backups/node-activity")
}

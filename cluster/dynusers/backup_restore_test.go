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

package dynusers

import (
	"os"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	cmd "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/usecases/auth/authentication/apikey"
	"github.com/weaviate/weaviate/usecases/auth/authentication/apikey/keys"
)

// seedUser creates one dynamic user through the manager's own apply path.
func seedUser(t *testing.T, m *Manager, id, namespace string) {
	t.Helper()
	_, hash, identifier, err := keys.CreateApiKeyAndHash()
	require.NoError(t, err)
	require.NoError(t, m.CreateUser(&cmd.ApplyRequest{SubCommand: mustMarshalJSON(t, cmd.CreateUsersRequest{
		UserId:         id,
		SecureHash:     hash,
		UserIdentifier: identifier,
		Namespace:      namespace,
		CreatedAt:      time.Now(),
	})}))
}

// userSnapshot builds a backup blob holding exactly the given users.
func userSnapshot(t *testing.T, known []string, users map[string]string) []byte {
	t.Helper()
	source, dyn, _ := newTestManager(t, newNamespacesMock(t, known...))
	for id, namespace := range users {
		seedUser(t, source, id, namespace)
	}
	blob, err := dyn.Snapshot()
	require.NoError(t, err)
	return blob
}

// TestRestoreFromBackupReplacesAndPersistsUsers pins both halves of the user
// restore: the in-memory state is replaced, and the file a restarting node reads
// before RAFT replays anything is written. Nothing writes that file on any other
// restore path.
func TestRestoreFromBackupReplacesAndPersistsUsers(t *testing.T) {
	blob := userSnapshot(t, nil, map[string]string{"restored": ""})

	m, dyn, dir := newTestManager(t, newNamespacesMock(t))
	seedUser(t, m, "pre-existing", "")

	require.NoError(t, m.RestoreFromBackup(&cmd.RestoreRolesAndUsersRequest{Users: blob}))

	users, err := dyn.GetUsers()
	require.NoError(t, err)
	assert.Contains(t, users, "restored")
	assert.NotContains(t, users, "pre-existing", "restore is a whole-store replace")

	logger, _ := test.NewNullLogger()
	reopened, err := apikey.NewDBUser(dir, false, logger, newNamespacesMock(t))
	require.NoError(t, err)
	fromDisk, err := reopened.GetUsers()
	require.NoError(t, err)
	assert.Contains(t, fromDisk, "restored", "restored users must reach the boot cache on disk")
}

// TestRestoreFromBackupSurvivesUnwritablePath pins that the file write cannot
// fail an apply the rest of the cluster completed: the RAFT log and snapshot are
// the durable copy, the file is only a boot cache.
func TestRestoreFromBackupSurvivesUnwritablePath(t *testing.T) {
	blob := userSnapshot(t, nil, map[string]string{"restored": ""})

	m, dyn, dir := newTestManager(t, newNamespacesMock(t))
	require.NoError(t, os.RemoveAll(dir))

	require.NoError(t, m.RestoreFromBackup(&cmd.RestoreRolesAndUsersRequest{Users: blob}))

	users, err := dyn.GetUsers()
	require.NoError(t, err)
	assert.Contains(t, users, "restored")
}

// TestSnapshotRestoreDoesNotWriteUserFile pins that boot-time RAFT snapshot
// install stays independent of the user file being writable. hashicorp/raft
// treats a failed FSM restore as fatal at boot, so this path must do no IO.
func TestSnapshotRestoreDoesNotWriteUserFile(t *testing.T) {
	blob := userSnapshot(t, nil, map[string]string{"restored": ""})

	m, dyn, dir := newTestManager(t, newNamespacesMock(t))
	require.NoError(t, os.RemoveAll(dir))

	require.NoError(t, m.Restore(blob))

	users, err := dyn.GetUsers()
	require.NoError(t, err)
	assert.Contains(t, users, "restored")
	_, err = os.Stat(dir)
	assert.True(t, os.IsNotExist(err), "snapshot install must not recreate the user file")
}

// TestValidateBackupSnapshotNamespaceStates pins the fail-closed namespace
// check for the user blob, across both places a dynamic user carries its
// namespace: the explicit field and the qualifier on its id.
func TestValidateBackupSnapshotNamespaceStates(t *testing.T) {
	tests := []struct {
		name    string
		users   map[string]string
		states  map[string]cmd.NamespaceState
		strip   bool
		wantErr string
	}{
		{
			name:   "active namespace passes",
			users:  map[string]string{"ns1:alice": "ns1"},
			states: map[string]cmd.NamespaceState{"ns1": cmd.NamespaceStateActive},
		},
		{
			name:    "missing namespace is named",
			users:   map[string]string{"ns1:alice": "ns1"},
			states:  map[string]cmd.NamespaceState{},
			wantErr: "ns1",
		},
		{
			name:    "suspended namespace is named",
			users:   map[string]string{"ns1:alice": "ns1"},
			states:  map[string]cmd.NamespaceState{"ns1": cmd.NamespaceStateSuspended},
			wantErr: "ns1",
		},
		{
			name:    "deleting namespace is named",
			users:   map[string]string{"ns1:alice": "ns1"},
			states:  map[string]cmd.NamespaceState{"ns1": cmd.NamespaceStateDeleting},
			wantErr: "ns1",
		},
		{
			name:    "resuming namespace is named",
			users:   map[string]string{"ns1:alice": "ns1"},
			states:  map[string]cmd.NamespaceState{"ns1": cmd.NamespaceStateResuming},
			wantErr: "ns1",
		},
		{
			// The id qualifier is the fallback when the field is empty, which is
			// what a user written by an older server carries.
			name:    "qualified id alone is enough to be checked",
			users:   map[string]string{"ns2:bob": ""},
			states:  map[string]cmd.NamespaceState{},
			wantErr: "ns2",
		},
		{
			name:   "strip skips the check entirely",
			users:  map[string]string{"ns1:alice": "ns1"},
			states: map[string]cmd.NamespaceState{},
			strip:  true,
		},
		{
			name:   "unqualified users reference no namespace",
			users:  map[string]string{"alice": ""},
			states: map[string]cmd.NamespaceState{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var known []string
			for _, ns := range tt.users {
				if ns != "" {
					known = append(known, ns)
				}
			}
			req := &cmd.RestoreRolesAndUsersRequest{
				Users:           userSnapshot(t, known, tt.users),
				StripNamespaces: tt.strip,
			}

			m, _, _ := newTestManager(t, newNamespacesMock(t))
			err := m.ValidateBackupSnapshot(req, newNamespacesMockInState(t, tt.states))
			if tt.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

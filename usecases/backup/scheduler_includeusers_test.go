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
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/auth/authorization/mocks"
)

// resolveUserSelectors is the pure core of includeUsers resolution: it must
// behave like the class include-list (dedup + wildcard expansion) while
// rejecting selectors that name nothing.
func TestResolveUserSelectors(t *testing.T) {
	t.Parallel()

	// stored ids are the qualified "namespace:userId" form; "dave" is a
	// pre-namespace unqualified user.
	allUsers := []string{"ns1:alice", "ns1:bob", "ns2:carol", "dave"}

	tests := []struct {
		name        string
		include     []string
		want        []string
		wantErrPart string
	}{
		{
			name:    "exact match",
			include: []string{"ns1:alice"},
			want:    []string{"ns1:alice"},
		},
		{
			name:    "namespace wildcard",
			include: []string{"ns1:*"},
			want:    []string{"ns1:alice", "ns1:bob"},
		},
		{
			name:    "question-mark wildcard",
			include: []string{"dav?"},
			want:    []string{"dave"},
		},
		{
			name:    "bare star matches every user",
			include: []string{"*"},
			want:    []string{"ns1:alice", "ns1:bob", "ns2:carol", "dave"},
		},
		{
			name:    "exact selector plus wildcard, deduplicated",
			include: []string{"ns2:carol", "ns2:*"},
			want:    []string{"ns2:carol"},
		},
		{
			name:        "duplicate selector",
			include:     []string{"ns1:*", "ns1:*"},
			wantErrPart: "duplicate",
		},
		{
			name:        "exact selector for a missing user",
			include:     []string{"ns1:zoe"},
			wantErrPart: `user "ns1:zoe" in 'includeUsers' does not exist`,
		},
		{
			name:        "wildcard matches nothing",
			include:     []string{"ns9:*"},
			wantErrPart: "no dynamic users match",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := resolveUserSelectors(tt.include, allUsers)
			if tt.wantErrPart != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErrPart)
				assert.Nil(t, got)
				return
			}
			require.NoError(t, err)
			assert.ElementsMatch(t, tt.want, got)
		})
	}
}

// resolveUsers wraps resolveUserSelectors with the empty-input and
// users-disabled handling that callers depend on.
func TestResolveUsers(t *testing.T) {
	t.Parallel()

	t.Run("absent includeUsers yields no users", func(t *testing.T) {
		s := &Scheduler{} // no userLister: must not be consulted at all
		users, err := s.resolveUsers(nil)
		require.NoError(t, err)
		assert.Empty(t, users)
	})

	t.Run("includeUsers without a user lister is rejected", func(t *testing.T) {
		s := &Scheduler{} // userLister nil => dynamic DB users disabled
		users, err := s.resolveUsers([]string{"ns1:*"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "dynamic DB users are not enabled")
		assert.Nil(t, users)
	})

	t.Run("resolves against the user lister", func(t *testing.T) {
		s := &Scheduler{userLister: &fakeUserLister{users: []string{"ns1:alice", "ns2:bob"}}}
		users, err := s.resolveUsers([]string{"ns1:*"})
		require.NoError(t, err)
		assert.Equal(t, []string{"ns1:alice"}, users)
	})
}

// userBackupAuthz returns the Authorize call the scheduler made for the
// user-scoped backup permission (backups/users/<id>), failing if none was
// recorded.
func userBackupAuthz(t *testing.T, fs *fakeScheduler) mocks.AuthZReq {
	t.Helper()
	fa, ok := fs.auth.(*mocks.FakeAuthorizer)
	require.True(t, ok, "fakeScheduler.auth must be a *mocks.FakeAuthorizer")
	for _, c := range fa.Calls() {
		if len(c.Resources) > 0 && strings.HasPrefix(c.Resources[0], authorization.BackupsDomain+"/users/") {
			return c
		}
	}
	t.Fatalf("no user-backup authorization call recorded; calls=%v", fa.Calls())
	return mocks.AuthZReq{}
}

// Scheduler.Backup must resolve includeUsers, gate the selected users behind a
// backup-create authorization on backups/users/<id>, and surface them in the
// create-backup response.
func TestSchedulerCreateBackupIncludeUsers(t *testing.T) {
	t.Parallel()

	var (
		cls         = "Class-A"
		node        = "Node-A"
		backendName = "gcs"
		backupID    = "1"
		any         = mock.Anything
		ctx         = context.Background()
		path        = "dst/path"
		cresp       = &CanCommitResponse{Method: OpCreate, ID: backupID, Timeout: 1}
		sReq        = &StatusRequest{OpCreate, backupID, backendName, "", "", ""}
		sresp       = &StatusResponse{Status: backup.Success, ID: backupID, Method: OpCreate}
	)

	t.Run("resolves includeUsers and returns them in the response", func(t *testing.T) {
		req := BackupRequest{
			ID:           backupID,
			Include:      []string{cls},
			Backend:      backendName,
			IncludeUsers: []string{"ns1:*"},
		}
		fs := newFakeScheduler(newFakeNodeResolver([]string{node}))
		fs.userLister.users = []string{"ns1:alice", "ns1:bob", "ns2:carol"}
		fs.selector.On("ListClasses", ctx).Return([]string{cls})
		fs.selector.On("Backupable", ctx, req.Include).Return(nil)
		fs.selector.On("Shards", ctx, cls).Return([]string{node}, nil)
		fs.backend.On("GetObject", ctx, backupID, GlobalBackupFile).Return(nil, backup.ErrNotFound{})
		fs.backend.On("GetObject", ctx, backupID, BackupFile).Return(nil, backup.ErrNotFound{})
		fs.backend.On("HomeDir", mock.Anything, mock.Anything, mock.Anything).Return(path)
		fs.backend.On("Initialize", ctx, mock.Anything).Return(nil)
		fs.client.On("CanCommit", any, node, any).Return(cresp, nil)
		fs.client.On("Commit", any, node, sReq).Return(nil)
		fs.client.On("Status", any, node, sReq).Return(sresp, nil)
		fs.backend.On("PutObject", any, backupID, GlobalBackupFile, any).Return(nil).Twice()
		fs.backend.On("GetObject", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return(marshalMeta(backup.BackupDescriptor{Status: backup.Success}), nil)

		resp, err := fs.scheduler().Backup(ctx, &models.Principal{}, &req)
		require.Nil(t, err)
		// ns2:carol must not leak in: only ns1:* was requested.
		assert.ElementsMatch(t, []string{"ns1:alice", "ns1:bob"}, resp.Users)
		assert.Equal(t, []string{cls}, resp.Classes)

		// Exporting credential material requires backup-create permission on
		// each selected user — mirroring the collection backup check.
		authz := userBackupAuthz(t, fs)
		assert.Equal(t, authorization.CREATE, authz.Verb)
		assert.ElementsMatch(t, authorization.BackupUsers("ns1:alice", "ns1:bob"), authz.Resources)
	})

	t.Run("duplicate includeUsers selector is rejected", func(t *testing.T) {
		fs := newFakeScheduler(nil)
		fs.userLister.users = []string{"ns1:alice"}
		fs.selector.On("ListClasses", ctx).Return([]string{cls})
		fs.selector.On("Backupable", ctx, mock.Anything).Return(nil)

		resp, err := fs.scheduler().Backup(ctx, &models.Principal{}, &BackupRequest{
			ID:           backupID,
			Backend:      backendName,
			Include:      []string{cls},
			IncludeUsers: []string{"ns1:*", "ns1:*"},
		})
		assert.Nil(t, resp)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "duplicate")
		assert.IsType(t, backup.ErrUnprocessable{}, err)
	})

	t.Run("includeUsers naming a missing user is rejected", func(t *testing.T) {
		fs := newFakeScheduler(nil)
		fs.userLister.users = []string{"ns1:alice"}
		fs.selector.On("ListClasses", ctx).Return([]string{cls})
		fs.selector.On("Backupable", ctx, mock.Anything).Return(nil)

		resp, err := fs.scheduler().Backup(ctx, &models.Principal{}, &BackupRequest{
			ID:           backupID,
			Backend:      backendName,
			Include:      []string{cls},
			IncludeUsers: []string{"ns1:ghost"},
		})
		assert.Nil(t, resp)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "does not exist")
		assert.IsType(t, backup.ErrUnprocessable{}, err)
	})

	t.Run("includeUsers matching no users is rejected", func(t *testing.T) {
		fs := newFakeScheduler(nil)
		// userLister.users left empty: nothing for the selector to match.
		fs.selector.On("ListClasses", ctx).Return([]string{cls})
		fs.selector.On("Backupable", ctx, mock.Anything).Return(nil)

		resp, err := fs.scheduler().Backup(ctx, &models.Principal{}, &BackupRequest{
			ID:           backupID,
			Backend:      backendName,
			Include:      []string{cls},
			IncludeUsers: []string{"ns1:*"},
		})
		assert.Nil(t, resp)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "no dynamic users match")
		assert.IsType(t, backup.ErrUnprocessable{}, err)
	})
}

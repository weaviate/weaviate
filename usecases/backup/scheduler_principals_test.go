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
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	cmd "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	authzerrors "github.com/weaviate/weaviate/usecases/auth/authorization/errors"
	"github.com/weaviate/weaviate/usecases/auth/authorization/mocks"
	"github.com/weaviate/weaviate/usecases/namespaces"
)

// Five IDs listed out of order: a descriptor list comes from map iteration, so
// fewer IDs could pass an unsorted check by chance.
var (
	unsortedUsers = []string{"ns1:erin", "ns1:carol", "ns1:alice", "ns1:dave", "ns1:bob"}
	unsortedRoles = []string{"ns1:writer", "ns1:auditor", "ns1:editor", "ns1:reader", "ns1:owner"}
	qualifiedIDs  = []string{
		"backups/users/ns1:alice", "backups/users/ns1:bob", "backups/users/ns1:carol", "backups/users/ns1:dave", "backups/users/ns1:erin",
		"backups/roles/ns1:auditor", "backups/roles/ns1:editor", "backups/roles/ns1:owner", "backups/roles/ns1:reader", "backups/roles/ns1:writer",
	}
	strippedIDs = []string{
		"backups/users/alice", "backups/users/bob", "backups/users/carol", "backups/users/dave", "backups/users/erin",
		"backups/roles/auditor", "backups/roles/editor", "backups/roles/owner", "backups/roles/reader", "backups/roles/writer",
	}
)

func TestSchedulerBackupPrincipals(t *testing.T) {
	t.Parallel()
	const (
		cls         = "Movies"
		node        = "Node-A"
		backendName = "gcs"
		backupID    = "principals"
	)
	ctx := context.Background()
	caller := &models.Principal{Username: "operator"}
	call := func(resources ...string) mocks.AuthZReq {
		return mocks.AuthZReq{Principal: caller, Verb: authorization.CREATE, Resources: resources}
	}

	// setup wires a create that runs to completion and returns the request the
	// participant receives.
	setup := func(fs *fakeScheduler) *Request {
		nodeReq := new(Request)
		fs.userLister.users = []string{"alice", "bob"}
		fs.roleLister.roles = []string{"editor", "writer"}
		fs.selector.On("ListClasses", ctx).Return([]string{cls})
		fs.selector.On("Backupable", ctx, mock.Anything).Return(nil)
		fs.selector.On("Shards", ctx, cls).Return([]string{node}, nil)
		fs.backend.On("GetObject", ctx, backupID, GlobalBackupFile).Return(nil, backup.ErrNotFound{})
		fs.backend.On("GetObject", ctx, backupID, BackupFile).Return(nil, backup.ErrNotFound{})
		fs.backend.On("HomeDir", mock.Anything, mock.Anything, mock.Anything).Return("dst/path")
		fs.backend.On("Initialize", ctx, mock.Anything).Return(nil)
		fs.backend.On("PutObject", mock.Anything, backupID, GlobalBackupFile, mock.Anything).Return(nil)
		sReq := &StatusRequest{OpCreate, backupID, backendName, "", "", ""}
		fs.client.On("CanCommit", mock.Anything, node, mock.Anything).
			Return(&CanCommitResponse{Method: OpCreate, ID: backupID, Timeout: 1}, nil).
			Run(func(a mock.Arguments) { *nodeReq = *a.Get(2).(*Request) })
		fs.client.On("Commit", mock.Anything, node, sReq).Return(nil)
		fs.client.On("Status", mock.Anything, node, sReq).
			Return(&StatusResponse{Status: backup.Success, ID: backupID, Method: OpCreate}, nil)
		return nodeReq
	}
	run := func(t *testing.T, fs *fakeScheduler, req *BackupRequest) error {
		t.Helper()
		s := fs.scheduler()
		_, err := s.Backup(ctx, caller, req)
		if err == nil {
			require.Eventually(t, func() bool { return s.backupper.lastOp.get().Status == "" },
				10*time.Second, 10*time.Millisecond, "backup did not finish")
		}
		return err
	}

	t.Run("an omitted selector checks its kind wildcard after the class check", func(t *testing.T) {
		fs := newFakeScheduler(newFakeNodeResolver([]string{node}))
		nodeReq := setup(fs)

		require.NoError(t, run(t, fs, &BackupRequest{ID: backupID, Backend: backendName}))
		assert.Equal(t, []mocks.AuthZReq{
			call("backups/collections/Movies"),
			call("backups/users/*"),
			call("backups/roles/*"),
		}, fs.auth.(*mocks.FakeAuthorizer).Calls())
		assert.False(t, nodeReq.SkipUsers)
		assert.False(t, nodeReq.SkipRoles)
	})

	t.Run("a denied whole store narrows only its own subsystem", func(t *testing.T) {
		tests := []struct {
			name                 string
			deny                 []string
			skipUsers, skipRoles bool
			warning              string
		}{
			{name: "users", deny: []string{"backups/users/*"}, skipUsers: true, warning: "dynamic users"},
			{name: "roles", deny: []string{"backups/roles/*"}, skipRoles: true, warning: "roles"},
			{name: "both", deny: []string{"backups/users/*", "backups/roles/*"}, skipUsers: true, skipRoles: true},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				fs := newFakeScheduler(newFakeNodeResolver([]string{node}))
				logger, hook := test.NewNullLogger()
				fs.log = logger
				fs.auth.(*mocks.FakeAuthorizer).Deny(tt.deny...)
				nodeReq := setup(fs)

				require.NoError(t, run(t, fs, &BackupRequest{ID: backupID, Backend: backendName}))
				assert.Equal(t, []string{cls}, nodeReq.Classes, "the collections are still backed up")
				assert.Equal(t, tt.skipUsers, nodeReq.SkipUsers)
				assert.Equal(t, tt.skipRoles, nodeReq.SkipRoles)
				assert.Equal(t, tt.skipUsers, fs.backend.glMeta.SkipUsers)
				assert.Equal(t, tt.skipRoles, fs.backend.glMeta.SkipRoles)

				warned := 0
				for _, e := range hook.AllEntries() {
					if e.Level == logrus.WarnLevel && e.Data["backup_id"] == backupID {
						warned++
					}
				}
				want := 0
				for _, skipped := range []bool{tt.skipUsers, tt.skipRoles} {
					if skipped {
						want++
					}
				}
				assert.Equal(t, want, warned, "each narrowing leaves a warning")
			})
		}
	})

	t.Run("a class-less request narrowed on both stores is refused", func(t *testing.T) {
		fs := newFakeScheduler(newFakeNodeResolver([]string{node}))
		fs.userLister.users = []string{"alice"}
		fs.roleLister.roles = []string{"editor"}
		fs.auth.(*mocks.FakeAuthorizer).Deny("backups/users/*", "backups/roles/*")
		fs.selector.On("ListClasses", ctx).Return([]string(nil))
		fs.backend.On("GetObject", ctx, backupID, GlobalBackupFile).Return(nil, backup.ErrNotFound{})
		fs.backend.On("GetObject", ctx, backupID, BackupFile).Return(nil, backup.ErrNotFound{})
		fs.backend.On("HomeDir", mock.Anything, mock.Anything, mock.Anything).Return("dst/path")

		_, err := fs.scheduler().Backup(ctx, caller, &BackupRequest{ID: backupID, Backend: backendName})
		require.IsType(t, backup.ErrUnprocessable{}, err)
		// The validation's class-less wording, so the refusal does not say
		// whether any user or role exists.
		assert.ErrorContains(t, err, "backup selects no collections, users, or roles: available collections: []")
		assert.Equal(t, []mocks.AuthZReq{
			call("backups/users/*"),
			call("backups/roles/*"),
		}, fs.auth.(*mocks.FakeAuthorizer).Calls())
		fs.client.AssertNotCalled(t, "CanCommit", mock.Anything, mock.Anything, mock.Anything)
		fs.backend.AssertNotCalled(t, "Initialize", mock.Anything, mock.Anything)
	})

	t.Run("a class-less request narrowed on one store proceeds without it", func(t *testing.T) {
		fs := newFakeScheduler(newFakeNodeResolver([]string{node}))
		fs.userLister.users = []string{"alice"}
		fs.roleLister.roles = []string{"editor"}
		fs.auth.(*mocks.FakeAuthorizer).Deny("backups/roles/*")
		nodeReq := new(Request)
		fs.selector.On("ListClasses", ctx).Return([]string(nil))
		fs.backend.On("GetObject", ctx, backupID, GlobalBackupFile).Return(nil, backup.ErrNotFound{})
		fs.backend.On("GetObject", ctx, backupID, BackupFile).Return(nil, backup.ErrNotFound{})
		fs.backend.On("HomeDir", mock.Anything, mock.Anything, mock.Anything).Return("dst/path")
		fs.backend.On("Initialize", ctx, mock.Anything).Return(nil)
		fs.backend.On("PutObject", mock.Anything, backupID, GlobalBackupFile, mock.Anything).Return(nil)
		sReq := &StatusRequest{OpCreate, backupID, backendName, "", "", ""}
		fs.client.On("CanCommit", mock.Anything, node, mock.Anything).
			Return(&CanCommitResponse{Method: OpCreate, ID: backupID, Timeout: 1}, nil).
			Run(func(a mock.Arguments) { *nodeReq = *a.Get(2).(*Request) })
		fs.client.On("Commit", mock.Anything, node, sReq).Return(nil)
		fs.client.On("Status", mock.Anything, node, sReq).
			Return(&StatusResponse{Status: backup.Success, ID: backupID, Method: OpCreate}, nil)

		require.NoError(t, run(t, fs, &BackupRequest{ID: backupID, Backend: backendName}))
		assert.Empty(t, nodeReq.Classes)
		assert.False(t, nodeReq.SkipUsers)
		assert.True(t, nodeReq.SkipRoles)
	})

	t.Run("a non-Forbidden authorizer error fails as Unprocessable", func(t *testing.T) {
		fs := newFakeScheduler(newFakeNodeResolver([]string{node}))
		fs.auth.(*mocks.FakeAuthorizer).SetErrAfter(1, ErrAny)
		setup(fs)

		err := run(t, fs, &BackupRequest{ID: backupID, Backend: backendName})
		assert.IsType(t, backup.ErrUnprocessable{}, err)
		assert.Contains(t, err.Error(), ErrAny.Error())
		fs.client.AssertNotCalled(t, "CanCommit", mock.Anything, mock.Anything, mock.Anything)
	})

	t.Run("literal selectors are checked per ID, sorted, before the store is read", func(t *testing.T) {
		fs := newFakeScheduler(newFakeNodeResolver([]string{node}))
		nodeReq := setup(fs)

		require.NoError(t, run(t, fs, &BackupRequest{
			ID: backupID, Backend: backendName, Include: []string{cls},
			IncludeUsers: []string{"bob", "alice"}, IncludeRoles: []string{"writer", "editor"},
		}))
		assert.Equal(t, []mocks.AuthZReq{
			call("backups/collections/Movies", "backups/users/alice", "backups/users/bob", "backups/roles/editor", "backups/roles/writer"),
		}, fs.auth.(*mocks.FakeAuthorizer).Calls())
		assert.ElementsMatch(t, []string{"alice", "bob"}, nodeReq.Users)
		assert.ElementsMatch(t, []string{"editor", "writer"}, nodeReq.Roles)
	})

	t.Run("a pattern selector checks its kind wildcard once", func(t *testing.T) {
		fs := newFakeScheduler(newFakeNodeResolver([]string{node}))
		setup(fs)

		require.NoError(t, run(t, fs, &BackupRequest{
			ID: backupID, Backend: backendName, Include: []string{cls},
			IncludeUsers: []string{"bob", "a*"},
		}))
		assert.Equal(t, []mocks.AuthZReq{
			call("backups/collections/Movies", "backups/users/*"),
			call("backups/roles/*"),
		}, fs.auth.(*mocks.FakeAuthorizer).Calls())
	})

	// The caller may back up bob and writer by name and nothing else. Whether a
	// selector names an existing ID, a missing one, or a pattern matching a
	// denied ID, an allowed ID or nothing, the answer is the same 403 and the
	// store is never read.
	t.Run("a denied selector gets the same 403 whatever exists", func(t *testing.T) {
		denied := []string{
			"backups/users/*", "backups/users/alice", "backups/users/ghost",
			"backups/roles/*", "backups/roles/editor", "backups/roles/ghost",
		}
		tests := []struct {
			name         string
			users, roles []string
			want         string
		}{
			{name: "literal user that exists", users: []string{"alice"}, want: "backups/users/alice"},
			{name: "literal user that does not exist", users: []string{"ghost"}, want: "backups/users/ghost"},
			{name: "user pattern matching a denied user", users: []string{"al*"}, want: "backups/users/*"},
			{name: "user pattern matching an allowed user", users: []string{"b?b"}, want: "backups/users/*"},
			{name: "user pattern matching nothing", users: []string{"zz*"}, want: "backups/users/*"},
			{name: "literal role that exists", roles: []string{"editor"}, want: "backups/roles/editor"},
			{name: "literal role that does not exist", roles: []string{"ghost"}, want: "backups/roles/ghost"},
			{name: "role pattern matching a denied role", roles: []string{"ed*"}, want: "backups/roles/*"},
			{name: "role pattern matching an allowed role", roles: []string{"wr?ter"}, want: "backups/roles/*"},
			{name: "role pattern matching nothing", roles: []string{"zz*"}, want: "backups/roles/*"},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				fs := newFakeScheduler(newFakeNodeResolver([]string{node}))
				fs.auth.(*mocks.FakeAuthorizer).Deny(denied...)
				setup(fs)
				req := &BackupRequest{ID: backupID, Backend: backendName, IncludeUsers: tt.users, IncludeRoles: tt.roles}

				err := run(t, fs, req)
				require.ErrorAs(t, err, &authzerrors.Forbidden{})
				assert.EqualError(t, err, authzerrors.NewForbidden(caller, authorization.CREATE, tt.want).Error())
				assert.Equal(t, []mocks.AuthZReq{call(tt.want)}, fs.auth.(*mocks.FakeAuthorizer).Calls())
				fs.selector.AssertNotCalled(t, "ListClasses", mock.Anything)
				fs.client.AssertNotCalled(t, "CanCommit", mock.Anything, mock.Anything, mock.Anything)
			})
		}

		t.Run("a collection shares the selector's one call and the same 403", func(t *testing.T) {
			fs := newFakeScheduler(newFakeNodeResolver([]string{node}))
			fs.auth.(*mocks.FakeAuthorizer).Deny(denied...)
			setup(fs)

			err := run(t, fs, &BackupRequest{
				ID: backupID, Backend: backendName, Include: []string{cls}, IncludeUsers: []string{"alice"},
			})
			require.ErrorAs(t, err, &authzerrors.Forbidden{})
			assert.EqualError(t, err, authzerrors.NewForbidden(caller, authorization.CREATE, "backups/users/alice").Error())
			assert.Equal(t, []mocks.AuthZReq{
				call("backups/collections/Movies", "backups/users/alice"),
			}, fs.auth.(*mocks.FakeAuthorizer).Calls())
			fs.client.AssertNotCalled(t, "CanCommit", mock.Anything, mock.Anything, mock.Anything)
		})
	})
}

// restorePrincipalsCase describes one restore: what the backup descriptor
// records, the version of the node that uploaded the blobs, and what the
// cluster and request look like.
type restorePrincipalsCase struct {
	users, roles []string
	// blobUsers and blobRoles are the IDs inside the snapshots; unset, they
	// follow users and roles.
	blobUsers, blobRoles []string
	// nilListers turns dynamic users and RBAC off on the restoring cluster.
	nilListers         bool
	namespacesEnabled  bool
	userOpt, rbacOpt   string
	reqUsers, reqRoles []string
	deny               []string
}

type restorePrincipalsOutcome struct {
	err     error
	calls   []mocks.AuthZReq
	applied []rolesAndUsersCall
	// restoreMeta is the restore descriptor the restore persisted.
	restoreMeta backup.DistributedBackupDescriptor
	fs          *fakeScheduler
}

func runRestorePrincipals(t *testing.T, c restorePrincipalsCase) restorePrincipalsOutcome {
	t.Helper()
	const (
		backupID = "restore-principals"
		node     = "Node-A"
	)
	ctx := context.Background()
	blobIDs := func(ids ...[]string) []string {
		for _, candidate := range ids {
			if len(candidate) > 0 {
				return candidate
			}
		}
		return nil
	}
	nodeMeta, err := json.Marshal(backup.BackupDescriptor{
		ID: backupID, Status: backup.Success, ServerVersion: "1.39.6",
		Classes:     []backup.ClassDescriptor{{Name: "Movies"}},
		UserBackups: makeUserSnapshot(t, blobIDs(c.blobUsers, c.users, []string{"ns1:alice"})...),
		RbacBackups: makeRbacSnapshot(t, blobIDs(c.blobRoles, c.roles, []string{"ns1:editor"})...),
	})
	require.NoError(t, err)
	meta := backup.DistributedBackupDescriptor{
		ID: backupID, StartedAt: time.Now().UTC(), Version: Version,
		ServerVersion: "1.39.6", Status: backup.Success, Leader: node,
		Nodes: map[string]*backup.NodeDescriptor{node: {Classes: []string{"Movies"}}},
		Users: c.users, Roles: c.roles,
	}

	fs := newFakeScheduler(newFakeNodeResolver([]string{node}))
	fs.nilListers = c.nilListers
	fs.schema.namespacesEnabled = c.namespacesEnabled
	if c.namespacesEnabled {
		fs.namespaces = namespaces.NewMockExisterInState(t, map[string]cmd.NamespaceState{"ns1": cmd.NamespaceStateActive})
	}
	fs.auth.(*mocks.FakeAuthorizer).Deny(c.deny...)
	rec := &recordingRolesAndUsersRestorer{}
	fs.rolesAndUsers = rec
	fs.backend.On("Initialize", mock.Anything, mock.Anything).Return(nil)
	fs.backend.On("GetObject", ctx, backupID, GlobalBackupFile).Return(marshalCoordinatorMeta(meta), nil)
	fs.backend.On("GetObject", ctx, backupID, GlobalRestoreFile).Return(nil, backup.ErrNotFound{})
	fs.backend.On("GetObject", ctx, backupID+"/"+node, BackupFile).Return(nodeMeta, nil)
	fs.backend.On("HomeDir", mock.Anything, mock.Anything, mock.Anything).Return("bucket/" + backupID)
	fs.backend.On("PutObject", mock.Anything, mock.Anything, GlobalRestoreFile, mock.Anything).Return(nil)
	fs.client.On("CanCommit", mock.Anything, node, mock.Anything).
		Return(&CanCommitResponse{Method: OpRestore, ID: backupID, Timeout: 1}, nil)
	fs.client.On("Commit", mock.Anything, node, mock.Anything).Return(nil)
	fs.client.On("Status", mock.Anything, node, mock.Anything).
		Return(&StatusResponse{Status: backup.Success, ID: backupID, Method: OpRestore}, nil)

	opt := func(o, def string) string {
		if o == "" {
			return def
		}
		return o
	}
	s := fs.scheduler()
	_, err = s.Restore(ctx, &models.Principal{Username: "operator"}, &BackupRequest{
		ID: backupID, Backend: "gcs", Include: []string{"Movies"},
		UserRestoreOption: opt(c.userOpt, models.RestoreConfigUsersOptionsAll),
		RbacRestoreOption: opt(c.rbacOpt, models.RestoreConfigRolesOptionsAll),
		IncludeUsers:      c.reqUsers, IncludeRoles: c.reqRoles,
	}, false)
	if err == nil {
		require.Eventually(t, func() bool { return s.restorer.lastOp.get().Status == "" },
			10*time.Second, 10*time.Millisecond, "restore did not finish")
	}
	fs.backend.RLock()
	restoreMeta := fs.backend.glMeta
	fs.backend.RUnlock()
	return restorePrincipalsOutcome{
		err: err, calls: fs.auth.(*mocks.FakeAuthorizer).Calls(),
		applied: rec.recorded(), restoreMeta: restoreMeta, fs: fs,
	}
}

func TestSchedulerRestorePrincipals(t *testing.T) {
	t.Parallel()
	caller := &models.Principal{Username: "operator"}
	collections := mocks.AuthZReq{Principal: caller, Verb: authorization.CREATE, Resources: []string{"backups/collections/Movies"}}

	allowed := []struct {
		name string
		c    restorePrincipalsCase
		want []string
	}{
		{
			name: "an applied restore checks the kind wildcards whatever IDs the descriptor names",
			c:    restorePrincipalsCase{users: unsortedUsers, roles: unsortedRoles, namespacesEnabled: true},
			want: wholeStores,
		},
		{
			name: "request includeUsers and includeRoles are ignored",
			c: restorePrincipalsCase{
				users: []string{"ns1:alice"}, roles: []string{"ns1:editor"}, namespacesEnabled: true,
				reqUsers: []string{"mallory"}, reqRoles: []string{"intruder"},
			},
			want: wholeStores,
		},
		{
			name: "a users-only restore checks only the users wildcard",
			c: restorePrincipalsCase{
				users: []string{"ns1:alice"}, roles: []string{"ns1:editor"}, namespacesEnabled: true,
				rbacOpt: models.RestoreConfigRolesOptionsNoRestore,
			},
			want: []string{"backups/users/*"},
		},
		{
			name: "a roles-only restore checks only the roles wildcard",
			c: restorePrincipalsCase{
				users: []string{"ns1:alice"}, roles: []string{"ns1:editor"}, namespacesEnabled: true,
				userOpt: models.RestoreConfigUsersOptionsNoRestore,
			},
			want: []string{"backups/roles/*"},
		},
	}
	for _, tt := range allowed {
		t.Run(tt.name, func(t *testing.T) {
			out := runRestorePrincipals(t, tt.c)
			require.NoError(t, out.err)
			assert.Equal(t, []mocks.AuthZReq{
				collections,
				{Principal: caller, Verb: authorization.CREATE, Resources: tt.want},
			}, out.calls)
			require.Len(t, out.applied, 1)
			assert.Equal(t, tt.c.userOpt == models.RestoreConfigUsersOptionsNoRestore, out.restoreMeta.SkipUsers)
			assert.Equal(t, tt.c.rbacOpt == models.RestoreConfigRolesOptionsNoRestore, out.restoreMeta.SkipRoles)
		})
	}

	denied := []struct {
		name string
		c    restorePrincipalsCase
	}{
		{
			name: "a denied users wildcard fails before any restore work, even with named IDs",
			c: restorePrincipalsCase{
				users: unsortedUsers, roles: unsortedRoles, namespacesEnabled: true,
				deny: []string{"backups/users/*"},
			},
		},
		{
			name: "a denied roles wildcard fails before any restore work",
			c: restorePrincipalsCase{
				users: unsortedUsers, roles: unsortedRoles,
				deny: []string{"backups/roles/*"},
			},
		},
	}
	for _, tt := range denied {
		t.Run(tt.name, func(t *testing.T) {
			out := runRestorePrincipals(t, tt.c)
			require.ErrorAs(t, out.err, &authzerrors.Forbidden{})
			assert.EqualError(t, out.err, authzerrors.NewForbidden(caller, authorization.CREATE, tt.c.deny[0]).Error())
			out.fs.client.AssertNotCalled(t, "CanCommit", mock.Anything, mock.Anything, mock.Anything)
			out.fs.backend.AssertNotCalled(t, "PutObject", mock.Anything, mock.Anything, GlobalRestoreFile, mock.Anything)
			assert.Empty(t, out.applied)
		})
	}

	// Each snapshot fails a content check the restore runs, and that check's 422
	// names what the snapshot holds. Only a caller who may restore it gets there.
	contentChecks := []struct {
		name  string
		c     restorePrincipalsCase
		names []string
	}{
		{
			name: "strip collisions",
			c: restorePrincipalsCase{
				blobUsers: []string{"ns1:alice", "ns2:alice"}, blobRoles: []string{"ns1:editor", "ns2:editor"},
			},
			names: []string{"alice", "editor"},
		},
		{
			name:  "a namespace missing on this cluster",
			c:     restorePrincipalsCase{blobUsers: []string{"ns9:alice"}, namespacesEnabled: true},
			names: []string{"ns9"},
		},
	}
	for _, tt := range contentChecks {
		t.Run(tt.name+" reach an authorized caller as a 422", func(t *testing.T) {
			out := runRestorePrincipals(t, tt.c)
			require.IsType(t, backup.ErrUnprocessable{}, out.err)
			for _, name := range tt.names {
				assert.Contains(t, out.err.Error(), name)
			}
		})
		t.Run(tt.name+" stay behind the 403 for a denied caller", func(t *testing.T) {
			c := tt.c
			c.deny = wholeStores
			out := runRestorePrincipals(t, c)
			require.ErrorAs(t, out.err, &authzerrors.Forbidden{})
			assert.EqualError(t, out.err, authzerrors.NewForbidden(caller, authorization.CREATE, "backups/users/*").Error())
			for _, name := range tt.names {
				assert.NotContains(t, out.err.Error(), name)
			}
			assert.Equal(t, []mocks.AuthZReq{collections, {Principal: caller, Verb: authorization.CREATE, Resources: wholeStores}}, out.calls)
			out.fs.client.AssertNotCalled(t, "CanCommit", mock.Anything, mock.Anything, mock.Anything)
		})
	}

	t.Run("snapshots a cluster without dynamic users and RBAC does not apply are neither authorized nor read", func(t *testing.T) {
		out := runRestorePrincipals(t, restorePrincipalsCase{
			blobUsers: []string{"ns1:alice", "ns2:alice"}, blobRoles: []string{"ns1:editor", "ns2:editor"},
			nilListers: true, deny: wholeStores,
		})
		require.NoError(t, out.err)
		assert.Equal(t, []mocks.AuthZReq{collections}, out.calls)
		assert.Empty(t, out.applied)
	})

	t.Run("a default restore makes no users or roles call and is pollable and cancellable without their grants", func(t *testing.T) {
		out := runRestorePrincipals(t, restorePrincipalsCase{
			users: []string{"ns1:alice"}, roles: []string{"ns1:editor"}, namespacesEnabled: true,
			userOpt: models.RestoreConfigUsersOptionsNoRestore, rbacOpt: models.RestoreConfigRolesOptionsNoRestore,
		})
		require.NoError(t, out.err)
		assert.Equal(t, []mocks.AuthZReq{collections}, out.calls)
		assert.True(t, out.restoreMeta.SkipUsers)
		assert.True(t, out.restoreMeta.SkipRoles)
		assert.Empty(t, out.applied)

		// A collection-only caller polls and cancels that restore.
		restoreMeta := out.restoreMeta
		restoreMeta.Status = backup.Cancelled
		fs := newFakeScheduler(nil)
		fs.auth.(*mocks.FakeAuthorizer).Deny(wholeStores...)
		fs.backend.On("GetObject", mock.Anything, restoreMeta.ID, GlobalRestoreFile).Return(marshalCoordinatorMeta(restoreMeta), nil)
		fs.backend.On("HomeDir", mock.Anything, mock.Anything, mock.Anything).Return("bucket/" + restoreMeta.ID)
		fs.backend.On("Initialize", mock.Anything, mock.Anything).Return(nil)
		s := fs.scheduler()

		_, err := s.RestorationStatus(context.Background(), caller, "gcs", restoreMeta.ID, "", "")
		require.NoError(t, err)
		require.NoError(t, s.CancelRestore(context.Background(), caller, "gcs", restoreMeta.ID, "", ""))
		assert.Equal(t, []mocks.AuthZReq{
			{Principal: caller, Verb: authorization.READ, Resources: []string{"backups/collections/Movies"}},
			{Principal: caller, Verb: authorization.DELETE, Resources: []string{"backups/collections/Movies"}},
		}, fs.auth.(*mocks.FakeAuthorizer).Calls())
	})
}

func TestSchedulerDescriptorPrincipals(t *testing.T) {
	t.Parallel()
	const backupID = "descriptor-principals"
	ctx := context.Background()
	caller := &models.Principal{Username: "operator"}

	type operation struct {
		name string
		verb string
		run  func(s *Scheduler) error
	}
	type descriptor struct {
		name                 string
		users, roles         []string
		skipUsers, skipRoles bool
		namespacesEnabled    bool
		// want is the users and roles call; nil means none is made.
		want []string
	}
	operations := []operation{
		{name: "backup status", verb: authorization.READ, run: func(s *Scheduler) error {
			_, err := s.BackupStatus(ctx, caller, "gcs", backupID, "", "")
			return err
		}},
		{name: "restore status", verb: authorization.READ, run: func(s *Scheduler) error {
			_, err := s.RestorationStatus(ctx, caller, "gcs", backupID, "", "")
			return err
		}},
		{name: "cancel", verb: authorization.DELETE, run: func(s *Scheduler) error {
			return s.Cancel(ctx, caller, "gcs", backupID, "", "")
		}},
		{name: "cancel restore", verb: authorization.DELETE, run: func(s *Scheduler) error {
			return s.CancelRestore(ctx, caller, "gcs", backupID, "", "")
		}},
	}
	descriptors := []descriptor{
		{name: "named IDs are checked per ID, sorted", users: unsortedUsers, roles: unsortedRoles, namespacesEnabled: true, want: qualifiedIDs},
		{name: "namespaces off checks the stripped IDs", users: unsortedUsers, roles: unsortedRoles, want: strippedIDs},
		{name: "empty lists check the kind wildcards", namespacesEnabled: true, want: wholeStores},
		{name: "a skipped subsystem requires nothing for it", roles: []string{"ns1:editor"}, skipUsers: true, namespacesEnabled: true, want: []string{"backups/roles/ns1:editor"}},
		{name: "both skipped make no users or roles call", users: []string{"ns1:alice"}, skipUsers: true, skipRoles: true, namespacesEnabled: true},
	}

	serve := func(fs *fakeScheduler, meta backup.DistributedBackupDescriptor) {
		b := marshalCoordinatorMeta(meta)
		fs.backend.On("GetObject", mock.Anything, backupID, GlobalBackupFile).Return(b, nil)
		fs.backend.On("GetObject", mock.Anything, backupID, GlobalRestoreFile).Return(b, nil)
		fs.backend.On("HomeDir", mock.Anything, mock.Anything, mock.Anything).Return("bucket/" + backupID)
		fs.backend.On("Initialize", mock.Anything, mock.Anything).Return(nil)
	}

	// All four operations authorize through the one descriptor-derivation
	// helper, so the descriptor cases run on one operation and each remaining
	// operation pins its wiring and verb on the richest descriptor.
	statusOp := operations[0]
	runCase := func(op operation, d descriptor) {
		t.Run(op.name+": "+d.name, func(t *testing.T) {
			fs := newFakeScheduler(nil)
			fs.schema.namespacesEnabled = d.namespacesEnabled
			serve(fs, backup.DistributedBackupDescriptor{
				ID: backupID, Status: backup.Cancelled,
				Nodes: map[string]*backup.NodeDescriptor{"node1": {Classes: []string{"Movies"}}},
				Users: d.users, Roles: d.roles, SkipUsers: d.skipUsers, SkipRoles: d.skipRoles,
			})

			require.NoError(t, op.run(fs.scheduler()))
			want := []mocks.AuthZReq{{Principal: caller, Verb: op.verb, Resources: []string{"backups/collections/Movies"}}}
			if d.want != nil {
				want = append(want, mocks.AuthZReq{Principal: caller, Verb: op.verb, Resources: d.want})
			}
			assert.Equal(t, want, fs.auth.(*mocks.FakeAuthorizer).Calls())
		})
	}
	for _, d := range descriptors {
		runCase(statusOp, d)
	}
	for _, op := range operations[1:] {
		runCase(op, descriptors[0])
	}

	t.Run("a denied per-ID resource fails", func(t *testing.T) {
		fs := newFakeScheduler(nil)
		fs.schema.namespacesEnabled = true
		fs.auth.(*mocks.FakeAuthorizer).Deny("backups/users/ns1:carol")
		serve(fs, backup.DistributedBackupDescriptor{
			ID: backupID, Status: backup.Cancelled,
			Nodes: map[string]*backup.NodeDescriptor{"node1": {Classes: []string{"Movies"}}},
			Users: unsortedUsers, SkipRoles: true,
		})

		err := statusOp.run(fs.scheduler())
		require.ErrorAs(t, err, &authzerrors.Forbidden{})
		assert.EqualError(t, err, authzerrors.NewForbidden(caller, statusOp.verb, "backups/users/ns1:carol").Error())
	})

	// Until the restore writes its own descriptor, cancel-restore judges the
	// backup descriptor. Its snapshots hold the whole stores, so cancelling
	// takes both wildcards even for a restore that applies neither.
	t.Run("cancel restore before the restore descriptor exists requires what the backup holds", func(t *testing.T) {
		for _, tt := range []struct {
			name string
			deny []string
		}{
			{name: "a collection-only caller is denied", deny: wholeStores},
			{name: "a caller holding both wildcards cancels"},
		} {
			t.Run(tt.name, func(t *testing.T) {
				fs := newFakeScheduler(newFakeNodeResolver([]string{"node1"}))
				fs.auth.(*mocks.FakeAuthorizer).Deny(tt.deny...)
				fs.backend.On("GetObject", mock.Anything, backupID, GlobalRestoreFile).Return(nil, backup.ErrNotFound{})
				fs.backend.On("GetObject", mock.Anything, backupID, BackupFile).Return(nil, backup.ErrNotFound{}).Maybe()
				fs.backend.On("GetObject", mock.Anything, backupID, GlobalBackupFile).Return(marshalCoordinatorMeta(backup.DistributedBackupDescriptor{
					ID: backupID, Status: backup.Success,
					Nodes: map[string]*backup.NodeDescriptor{"node1": {Classes: []string{"Movies"}}},
				}), nil)
				fs.backend.On("Initialize", mock.Anything, mock.Anything).Return(nil)
				fs.selector.On("ListClasses", ctx).Return([]string{"Movies"})
				fs.selector.On("Shards", ctx, "Movies").Return([]string{"node1"}, nil)
				fs.client.On("Abort", mock.Anything, mock.Anything, mock.Anything).Return(nil)

				err := fs.scheduler().CancelRestore(ctx, caller, "gcs", backupID, "", "")
				assert.Equal(t, []mocks.AuthZReq{
					{Principal: caller, Verb: authorization.DELETE, Resources: []string{"backups/collections/Movies"}},
					{Principal: caller, Verb: authorization.DELETE, Resources: wholeStores},
				}, fs.auth.(*mocks.FakeAuthorizer).Calls())
				if len(tt.deny) > 0 {
					require.ErrorAs(t, err, &authzerrors.Forbidden{})
					fs.client.AssertNotCalled(t, "Abort", mock.Anything, mock.Anything, mock.Anything)
					return
				}
				require.NoError(t, err)
				fs.client.AssertCalled(t, "Abort", mock.Anything, mock.Anything, mock.Anything)
			})
		}
	})

	t.Run("cancel without a readable descriptor requires both wildcards", func(t *testing.T) {
		for _, op := range operations[2:] {
			fs := newFakeScheduler(nil)
			fs.backend.On("GetObject", mock.Anything, backupID, mock.Anything).Return(nil, ErrAny)
			fs.backend.On("Initialize", mock.Anything, mock.Anything).Return(ErrAny)

			require.ErrorIs(t, op.run(fs.scheduler()), ErrAny, op.name)
			assert.Equal(t, []mocks.AuthZReq{
				{Principal: caller, Verb: authorization.DELETE, Resources: []string{"backups/collections/*"}},
				{Principal: caller, Verb: authorization.DELETE, Resources: wholeStores},
			}, fs.auth.(*mocks.FakeAuthorizer).Calls(), op.name)
		}
	})
}

func TestSchedulerListPrincipals(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	caller := &models.Principal{Username: "operator"}
	order := "desc"
	backups := []*backup.DistributedBackupDescriptor{
		{
			ID: "named", Status: backup.Success, StartedAt: time.Now(),
			Nodes: map[string]*backup.NodeDescriptor{"node1": {Classes: []string{"Movies"}}},
			Users: []string{"ns1:bob", "ns1:alice"}, SkipRoles: true,
		},
		{
			ID: "whole-store", Status: backup.Success, StartedAt: time.Now().Add(-time.Minute),
			Nodes: map[string]*backup.NodeDescriptor{"node1": {Classes: []string{"Movies"}}},
		},
		{
			ID: "skips-both", Status: backup.Success, StartedAt: time.Now().Add(-2 * time.Minute),
			Nodes:     map[string]*backup.NodeDescriptor{"node1": {Classes: []string{"Movies"}}},
			SkipUsers: true, SkipRoles: true,
		},
	}
	tests := []struct {
		name    string
		deny    []string
		wantIDs []string
	}{
		{
			name:    "a collections-only reader skips the blanket path and sees only backups without users or roles",
			deny:    append([]string{"backups/users/ns1:alice", "backups/users/ns1:bob"}, wholeStores...),
			wantIDs: []string{"skips-both"},
		},
		{
			name:    "a denied named user hides only the backup naming it",
			deny:    []string{"backups/collections/*", "backups/users/ns1:bob"},
			wantIDs: []string{"whole-store", "skips-both"},
		},
		{
			name:    "per-ID grants show the backup naming those IDs",
			deny:    append([]string{"backups/collections/*"}, wholeStores...),
			wantIDs: []string{"named", "skips-both"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fs := newFakeScheduler(nil)
			fs.schema.namespacesEnabled = true
			authorizer := fs.auth.(*mocks.FakeAuthorizer)
			authorizer.Deny(tt.deny...)
			fs.backend.On("AllBackups", mock.Anything).Return(backups, nil)

			resp, err := fs.scheduler().List(ctx, caller, "gcs", &order, false)
			require.NoError(t, err)
			gotIDs := make([]string, 0, len(*resp))
			for _, item := range *resp {
				gotIDs = append(gotIDs, item.ID)
			}
			assert.Equal(t, tt.wantIDs, gotIDs)
			assert.Equal(t, []mocks.AuthZReq{
				{Principal: caller, Verb: authorization.READ, Resources: listWildcards, Silent: true},
				{Principal: caller, Verb: authorization.READ, Resources: []string{
					"backups/collections/Movies", "backups/roles/*", "backups/users/*",
					"backups/users/ns1:alice", "backups/users/ns1:bob",
				}},
			}, authorizer.Calls())
		})
	}
}

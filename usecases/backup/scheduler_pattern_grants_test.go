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
	"fmt"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/auth/authorization/conv"
	authzerrors "github.com/weaviate/weaviate/usecases/auth/authorization/errors"
	"github.com/weaviate/weaviate/usecases/auth/authorization/rbac"
	"github.com/weaviate/weaviate/usecases/auth/authorization/rbac/rbacconf"
	"github.com/weaviate/weaviate/usecases/config"
)

// patternGrantAuthorizer returns a real rbac.Manager on which restoreCaller
// holds manage_backups on every collection, on the users pattern and on the
// roles pattern. An empty pattern grants nothing on that kind. The grants go
// through conv.RolesToPolicies, the conversion a REST role create runs, because
// the bug these tests pin lives in how casbin matches what that conversion stores.
func patternGrantAuthorizer(t *testing.T, users, roles string) *rbac.Manager {
	t.Helper()
	m, _ := auditedPatternGrantAuthorizer(t, users, roles)
	return m
}

// auditedPatternGrantAuthorizer is a patternGrantAuthorizer that also returns
// a hook recording the Manager's audit log.
func auditedPatternGrantAuthorizer(t *testing.T, users, roles string) (*rbac.Manager, *test.Hook) {
	t.Helper()
	logger, hook := test.NewNullLogger()
	m, err := rbac.New(t.TempDir(), rbacconf.Config{Enabled: true}, config.Authentication{}, false, nil, logger)
	require.NoError(t, err)
	role, action, all := "backup-operator", authorization.ManageBackups, "*"
	perms := []*models.Permission{{Action: &action, Backups: &models.PermissionBackups{Collection: &all}}}
	if users != "" {
		perms = append(perms, &models.Permission{Action: &action, Backups: &models.PermissionBackups{User: &users}})
	}
	if roles != "" {
		perms = append(perms, &models.Permission{Action: &action, Backups: &models.PermissionBackups{Role: &roles}})
	}
	policies, err := conv.RolesToPolicies(&models.Role{Name: &role, Permissions: perms})
	require.NoError(t, err)
	require.NoError(t, m.CreateRolesPermissions(policies))
	require.NoError(t, m.AddRolesForUser(conv.UserNameWithTypeFromPrincipal(restoreCaller), []string{role}))
	return m, hook
}

func nilIfEmpty(ids []string) []string {
	if len(ids) == 0 {
		return nil
	}
	return ids
}

// roleOnlySnapshot builds an RBAC snapshot holding only the named roles' rows,
// as a backup filtered to named roles carries, so no built-in role is in it.
func roleOnlySnapshot(t *testing.T, roleNames ...string) []byte {
	t.Helper()
	logger, _ := test.NewNullLogger()
	m, err := rbac.New(filepath.Join(t.TempDir(), "policy.csv"), rbacconf.Config{Enabled: true}, config.Authentication{}, false, nil, logger)
	require.NoError(t, err)
	perms := make(map[string][]authorization.Policy, len(roleNames))
	for _, r := range roleNames {
		perms[r] = []authorization.Policy{{Resource: "data/collections/Movies/shards/*/objects/*", Verb: authorization.READ, Domain: authorization.DataDomain}}
	}
	require.NoError(t, m.CreateRolesPermissions(perms))
	snap, err := m.Snapshot(roleNames...)
	require.NoError(t, err)
	return snap
}

func TestSchedulerPatternGrants(t *testing.T) {
	t.Parallel()
	const (
		cls         = "Movies"
		node        = "Node-A"
		backendName = "gcs"
		backupID    = "pattern-grants"
	)
	ctx := context.Background()

	type createOutcome struct {
		desc backup.DistributedBackupDescriptor
		resp *models.BackupCreateResponse
		fs   *fakeScheduler
		err  error
	}
	// backUp runs a create as restoreCaller under auth against a cluster holding
	// users and roles, and returns the response and the descriptor it wrote.
	backUp := func(t *testing.T, auth authorization.Authorizer, users, roles []string, req BackupRequest) createOutcome {
		t.Helper()
		fs := newFakeScheduler(newFakeNodeResolver([]string{node}))
		fs.auth = auth
		fs.userLister.users = users
		fs.roleLister.roles = roles
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
			Return(&CanCommitResponse{Method: OpCreate, ID: backupID, Timeout: 1}, nil)
		fs.client.On("Commit", mock.Anything, node, sReq).Return(nil)
		fs.client.On("Status", mock.Anything, node, sReq).
			Return(&StatusResponse{Status: backup.Success, ID: backupID, Method: OpCreate}, nil)

		req.ID, req.Backend = backupID, backendName
		s := fs.scheduler()
		resp, err := s.Backup(ctx, restoreCaller, &req)
		if err == nil {
			require.Eventually(t, func() bool { return s.backupper.lastOp.get().Status == "" },
				10*time.Second, 10*time.Millisecond, "backup did not finish")
		}
		fs.backend.RLock()
		defer fs.backend.RUnlock()
		return createOutcome{desc: fs.backend.glMeta, resp: resp, fs: fs, err: err}
	}

	var (
		users       = []string{"alice", "bob", "xavier"}
		roles       = []string{authorization.Admin, "editor", authorization.Viewer, "xavier"}
		shortUsers  = []string{"a", "b"}
		shortRoles  = []string{authorization.Admin, "e", "w"}
		permitAll   = []string(nil)
		permitNoIDs = []string{}
	)
	// Each grant is installed on users and roles alike. A nil want is the whole
	// store; an empty one is a skipped snapshot.
	creates := []struct {
		name                 string
		grant                string
		users, roles         []string
		wantUsers, wantRoles []string
	}{
		{
			name: "[^x]* leaves out the IDs it does not match", grant: "[^x]*", users: users, roles: roles,
			wantUsers: []string{"alice", "bob"}, wantRoles: []string{"editor"},
		},
		{name: "* keeps today's whole stores", grant: "*", users: users, roles: roles, wantUsers: permitAll, wantRoles: permitAll},
		{name: ".+ matches every ID and keeps the whole stores", grant: ".+", users: users, roles: roles, wantUsers: permitAll, wantRoles: permitAll},
		{name: ". matches every single-character ID and keeps the whole stores", grant: ".", users: shortUsers, roles: shortRoles, wantUsers: permitAll, wantRoles: permitAll},
		{name: ". matches no longer ID and backs up none", grant: ".", users: users, roles: roles, wantUsers: permitNoIDs, wantRoles: permitNoIDs},
	}

	t.Run("an omitted selector backs up only what the grant matches", func(t *testing.T) {
		for _, tt := range creates {
			t.Run(tt.name, func(t *testing.T) {
				out := backUp(t, patternGrantAuthorizer(t, tt.grant, tt.grant), tt.users, tt.roles, BackupRequest{})
				require.NoError(t, out.err)
				// The descriptor drops an empty list; the skip flag tells it from a whole store.
				assert.Equal(t, nilIfEmpty(tt.wantUsers), out.desc.Users, "users backed up from %v under %q; a whole store would carry every one", tt.users, tt.grant)
				assert.Equal(t, nilIfEmpty(tt.wantRoles), out.desc.Roles, "roles backed up from %v under %q; a whole store would carry every one", tt.roles, tt.grant)
				assert.Equal(t, tt.wantUsers != nil && len(tt.wantUsers) == 0, out.desc.SkipUsers, "users skipped")
				assert.Equal(t, tt.wantRoles != nil && len(tt.wantRoles) == 0, out.desc.SkipRoles, "roles skipped")
			})
		}
	})

	t.Run("an omitted selector on an empty store keeps the whole store only for the kind wildcard", func(t *testing.T) {
		// Zero IDs leave nothing for the per-ID check to refuse, so only the kind
		// wildcard keeps a grantless caller from the full RBAC snapshot.
		builtIns := []string{authorization.Admin, authorization.Viewer}
		for _, tt := range []struct {
			name      string
			grant     string
			wantSkips bool
		}{
			{name: "a caller without a grant backs up neither store", grant: "", wantSkips: true},
			{name: "* keeps both whole stores", grant: "*", wantSkips: false},
		} {
			t.Run(tt.name, func(t *testing.T) {
				out := backUp(t, patternGrantAuthorizer(t, tt.grant, tt.grant), []string{}, builtIns, BackupRequest{})
				require.NoError(t, out.err)
				assert.Equal(t, tt.wantSkips, out.desc.SkipUsers, "users skipped under %q", tt.grant)
				assert.Equal(t, tt.wantSkips, out.desc.SkipRoles, "roles skipped under %q", tt.grant)
				assert.Nil(t, out.desc.Users)
				assert.Nil(t, out.desc.Roles)
			})
		}
	})

	t.Run("a create answers the same whether a refused ID exists", func(t *testing.T) {
		// [^x]* passes the kind wildcard but refuses xavier. A store holding
		// only xavier must answer as an empty store does, or the answer reveals
		// that xavier exists. Excluding cls leaves the request with no collection.
		builtIns := []string{authorization.Admin, authorization.Viewer}
		classless := []string{cls}
		for _, tt := range []struct {
			name string
			// The grant goes on users or on roles; that kind's store holds
			// xavier in the second of the two stores compared.
			onRoles bool
			req     BackupRequest
			// wantErr is empty for a backup that starts.
			wantErr string
		}{
			{name: "users, with a collection", req: BackupRequest{}},
			{
				name:    "users, class-less with roles the caller may not back up",
				req:     BackupRequest{Exclude: classless},
				wantErr: "backup selects no collections, users, or roles",
			},
			{
				name:    "users, class-less with roles explicitly empty",
				req:     BackupRequest{Exclude: classless, IncludeRoles: []string{}},
				wantErr: "backup selects no collections, users, or roles",
			},
			{
				// The built-in roles count for a caller who passes the roles
				// wildcard, so the backup proceeds with either store.
				name: "roles, class-less with users explicitly empty", onRoles: true,
				req: BackupRequest{Exclude: classless, IncludeUsers: []string{}},
			},
		} {
			t.Run(tt.name, func(t *testing.T) {
				usersGrant, rolesGrant := "[^x]*", ""
				users, roles := []string{"xavier"}, builtIns
				if tt.onRoles {
					usersGrant, rolesGrant = "", "[^x]*"
					users, roles = []string{}, append(slices.Clone(builtIns), "xavier")
				}
				empty := backUp(t, patternGrantAuthorizer(t, usersGrant, rolesGrant), []string{}, builtIns, tt.req)
				withXavier := backUp(t, patternGrantAuthorizer(t, usersGrant, rolesGrant), users, roles, tt.req)
				if tt.wantErr == "" {
					require.NoError(t, empty.err, "empty store")
				} else {
					require.IsType(t, backup.ErrUnprocessable{}, empty.err)
					require.ErrorContains(t, empty.err, tt.wantErr)
				}
				assert.Equal(t, fmt.Sprint(empty.err), fmt.Sprint(withXavier.err), "store with xavier must answer as the empty store does")
				assert.IsType(t, empty.err, withXavier.err)
				assert.Equal(t, empty.resp, withXavier.resp)
			})
		}
	})

	t.Run("a pattern selector expands only over what the grant matches", func(t *testing.T) {
		// A pattern selector names its IDs whatever the grant, so "whole store"
		// becomes every custom ID.
		custom := func(ids, store []string) []string {
			if ids != nil {
				return ids
			}
			out := []string{}
			for _, id := range store {
				if id != authorization.Admin && id != authorization.Viewer {
					out = append(out, id)
				}
			}
			return out
		}
		for _, tt := range creates {
			t.Run(tt.name, func(t *testing.T) {
				out := backUp(t, patternGrantAuthorizer(t, tt.grant, tt.grant), tt.users, tt.roles,
					BackupRequest{IncludeUsers: []string{"*"}, IncludeRoles: []string{"*"}})
				require.NoError(t, out.err)
				wantUsers, wantRoles := nilIfEmpty(custom(tt.wantUsers, tt.users)), nilIfEmpty(custom(tt.wantRoles, tt.roles))
				assert.Equal(t, wantUsers, out.desc.Users, "what users '*' expands to under %q from %v", tt.grant, tt.users)
				assert.Equal(t, wantRoles, out.desc.Roles, "what roles '*' expands to under %q from %v", tt.grant, tt.roles)
				assert.Equal(t, wantUsers == nil, out.desc.SkipUsers, "users skipped")
				assert.Equal(t, wantRoles == nil, out.desc.SkipRoles, "roles skipped")
			})
		}

		// A caller with no users or roles grant gets the same 403 whether the
		// pattern matches an ID that exists or nothing.
		for _, kind := range []struct {
			name string
			req  func(selector string) BackupRequest
		}{
			{name: "users", req: func(sel string) BackupRequest { return BackupRequest{IncludeUsers: []string{sel}} }},
			{name: "roles", req: func(sel string) BackupRequest { return BackupRequest{IncludeRoles: []string{sel}} }},
		} {
			t.Run("a caller without a "+kind.name+" grant is refused the same whatever exists", func(t *testing.T) {
				var denials []string
				for _, sel := range []string{"x*", "zz*"} {
					out := backUp(t, patternGrantAuthorizer(t, "", ""), users, roles, kind.req(sel))
					require.ErrorAs(t, out.err, &authzerrors.Forbidden{}, sel)
					out.fs.client.AssertNotCalled(t, "CanCommit", mock.Anything, mock.Anything, mock.Anything)
					denials = append(denials, out.err.Error())
				}
				assert.Equal(t, denials[0], denials[1])
				assert.NotContains(t, denials[0], "xavier")
			})
		}
	})

	t.Run("a restore is refused when the grant misses an ID the apply touches", func(t *testing.T) {
		usersDenial := authzerrors.NewForbidden(restoreCaller, "create_backups", "backups/users/*").Error()
		rolesDenial := authzerrors.NewForbidden(restoreCaller, "create_backups", "backups/roles/*").Error()
		// Each case puts xavier on exactly one leg of one subsystem. The
		// descriptors name no IDs, as a backup of the whole store records
		// none, so only the snapshot blobs carry the snapshot leg.
		legs := []struct {
			name string
			c    restorePrincipalsCase
			want string
		}{
			{
				name: "a current user the snapshot lacks, which the apply deletes",
				c: restorePrincipalsCase{
					currentUsers: []string{"alice", "xavier"}, blobUsers: []string{"alice"},
					currentRoles: []string{"editor"}, blobRoles: []string{"editor"},
				},
				want: usersDenial,
			},
			{
				name: "a snapshot user the cluster lacks, which the apply creates",
				c: restorePrincipalsCase{
					currentUsers: []string{"alice"}, blobUsers: []string{"alice", "xavier"},
					currentRoles: []string{"editor"}, blobRoles: []string{"editor"},
				},
				want: usersDenial,
			},
			{
				name: "a user on both sides, which the apply overwrites",
				c: restorePrincipalsCase{
					currentUsers: []string{"alice", "xavier"}, blobUsers: []string{"alice", "xavier"},
					currentRoles: []string{"editor"}, blobRoles: []string{"editor"},
				},
				want: usersDenial,
			},
			{
				name: "a current role the snapshot lacks, which the apply deletes",
				c: restorePrincipalsCase{
					currentUsers: []string{"alice"}, blobUsers: []string{"alice"},
					currentRoles: []string{"editor", "xavier", authorization.Admin}, blobRoles: []string{"editor"},
				},
				want: rolesDenial,
			},
			{
				name: "a snapshot role the cluster lacks, which the apply creates",
				c: restorePrincipalsCase{
					currentUsers: []string{"alice"}, blobUsers: []string{"alice"},
					currentRoles: []string{"editor"}, blobRoles: []string{"editor", "xavier"},
				},
				want: rolesDenial,
			},
			{
				name: "a role on both sides, which the apply overwrites",
				c: restorePrincipalsCase{
					currentUsers: []string{"alice"}, blobUsers: []string{"alice"},
					currentRoles: []string{"editor", "xavier"}, blobRoles: []string{"editor", "xavier"},
				},
				want: rolesDenial,
			},
		}
		// How the audit log writes the kind wildcard that each denial names.
		audited := map[string]string{usersDenial: "[Domain: backups, User: *]", rolesDenial: "[Domain: backups, Role: *]"}
		for _, leg := range legs {
			t.Run(leg.name, func(t *testing.T) {
				c := leg.c
				auth, audit := auditedPatternGrantAuthorizer(t, "[^x]*", "[^x]*")
				c.auth = auth
				out := runRestorePrincipals(t, c)
				require.ErrorAs(t, out.err, &authzerrors.Forbidden{}, "a [^x]* grant restored over xavier")
				assert.EqualError(t, out.err, leg.want)
				out.fs.client.AssertNotCalled(t, "CanCommit", mock.Anything, mock.Anything, mock.Anything)
				assert.Empty(t, out.applied)

				var denials []*logrus.Entry
				for _, e := range audit.AllEntries() {
					if e.Message == "authorization denied" {
						denials = append(denials, e)
					}
				}
				require.Len(t, denials, 1, "audit denials for a restore refused over xavier")
				assert.Equal(t, audited[leg.want], denials[0].Data["resource"])
				line, err := denials[0].String()
				require.NoError(t, err)
				assert.NotContains(t, line, "xavier")

				for _, grant := range []string{"*", ".+"} {
					c.auth = patternGrantAuthorizer(t, grant, grant)
					out := runRestorePrincipals(t, c)
					require.NoError(t, out.err, "a %q grant covers xavier", grant)
					assert.Len(t, out.applied, 1, grant)
				}
			})
		}

		t.Run(". restores stores of single-character IDs", func(t *testing.T) {
			out := runRestorePrincipals(t, restorePrincipalsCase{
				auth:         patternGrantAuthorizer(t, ".", "."),
				currentUsers: []string{"a", "b"}, blobUsers: []string{"a", "c"},
				currentRoles: []string{"e", "w", authorization.Admin}, rbacBlob: roleOnlySnapshot(t, "e", "r"),
			})
			require.NoError(t, out.err)
			assert.Len(t, out.applied, 1)
		})

		t.Run("built-in role rows in the snapshot are checked", func(t *testing.T) {
			// A full RBAC snapshot carries the built-in roles' rows; "." matches
			// none of their names.
			out := runRestorePrincipals(t, restorePrincipalsCase{
				auth:         patternGrantAuthorizer(t, ".", "."),
				currentUsers: []string{"a"}, blobUsers: []string{"a"},
				currentRoles: []string{"e"}, blobRoles: []string{"e"},
			})
			require.ErrorAs(t, out.err, &authzerrors.Forbidden{})
			assert.EqualError(t, out.err, rolesDenial)
			assert.Empty(t, out.applied)
		})

		t.Run("an unreadable snapshot fails without naming what it holds", func(t *testing.T) {
			for _, tt := range []struct {
				name string
				c    restorePrincipalsCase
				want string
			}{
				{
					// encoding/json names the map key of a mistyped value.
					name: "users",
					c: restorePrincipalsCase{
						userBlob: []byte(`{"Data":{"Users":{"xavier":"not a user"}},"Version":0}`),
						rbacOpt:  models.RestoreConfigRolesOptionsNoRestore,
					},
					want: "the backup's users snapshot cannot be read",
				},
				{
					name: "roles",
					c: restorePrincipalsCase{
						rbacBlob: []byte(`{"roles_policies":[["role:xavier",1]],"version":1}`),
						userOpt:  models.RestoreConfigUsersOptionsNoRestore,
					},
					want: "the backup's roles snapshot cannot be read",
				},
			} {
				t.Run(tt.name, func(t *testing.T) {
					c := tt.c
					c.auth = patternGrantAuthorizer(t, "[^x]*", "[^x]*")
					out := runRestorePrincipals(t, c)
					require.IsType(t, backup.ErrUnprocessable{}, out.err)
					assert.EqualError(t, out.err, tt.want)
					out.fs.client.AssertNotCalled(t, "CanCommit", mock.Anything, mock.Anything, mock.Anything)
				})
			}
		})

		t.Run("an empty store and an empty snapshot still need the kind wildcard", func(t *testing.T) {
			out := runRestorePrincipals(t, restorePrincipalsCase{
				auth:     patternGrantAuthorizer(t, "", ""),
				userBlob: makeUserSnapshot(t), rbacOpt: models.RestoreConfigRolesOptionsNoRestore,
			})
			require.ErrorAs(t, out.err, &authzerrors.Forbidden{})
			out.fs.client.AssertNotCalled(t, "CanCommit", mock.Anything, mock.Anything, mock.Anything)
			assert.Empty(t, out.applied)
		})
	})
}

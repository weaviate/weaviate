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

package acceptance_with_go_client

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	wv5 "github.com/weaviate/weaviate-go-client/v5/weaviate"
	authv5 "github.com/weaviate/weaviate-go-client/v5/weaviate/auth"
	"github.com/weaviate/weaviate-go-client/v5/weaviate/fault"
	rbacv5 "github.com/weaviate/weaviate-go-client/v5/weaviate/rbac"
	wv6 "github.com/weaviate/weaviate-go-client/v6"
	rbacv6 "github.com/weaviate/weaviate-go-client/v6/rbac"
	"github.com/weaviate/weaviate/test/docker"
)

const (
	backupsRootUser  = "backups-root"
	backupsRootKey   = "backups-root-key"
	backupsUser      = "backups-user"
	backupsUserKey   = "backups-user-key"
	backupsGroup     = "backups-group"
	manageBackups    = "manage_backups"
	readBackups      = "read_backups"
	backupsWildUser  = manageBackups + " user=*"
	backupsWildRole  = manageBackups + " role=*"
	backupsGrantOnA  = manageBackups + " collection=A"
	backupsGrantOnB  = manageBackups + " collection=B"
	collectionMarker = " collection="
)

// rbacJourneyClient runs every role write and read of the journey through one
// client. Each read returns the role's backups grants as strings of the form
// "<action> <kind>=<name>", such as "manage_backups collection=A".
type rbacJourneyClient interface {
	createRole(ctx context.Context, role, collection string) error
	assignRole(ctx context.Context, role string) error
	addBackups(ctx context.Context, role, collection string) error
	removeBackups(ctx context.Context, role, collection string) error
	deleteRole(ctx context.Context, role string) error
	roleExists(ctx context.Context, role string) (bool, error)
	reads() []backupsRead
}

type backupsRead struct {
	name string
	read func(ctx context.Context, role string) ([]string, error)
}

func TestBackupPrincipalsHeader(t *testing.T) {
	ctx := context.Background()

	compose, err := docker.New().
		WithWeaviateWithGRPC().
		WithApiKey().WithRBAC().WithDbUsers().
		WithUserApiKey(backupsRootUser, backupsRootKey).
		WithUserApiKey(backupsUser, backupsUserKey).
		WithRbacRoots(backupsRootUser).
		Start(ctx)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, compose.Terminate(ctx))
	}()

	httpHost := compose.GetWeaviate().GetEndpoint(docker.HTTP)
	grpcHost := compose.GetWeaviate().GetEndpoint(docker.GRPC)
	v5 := v5JourneyClient{
		root: newV5Client(t, httpHost, backupsRootKey),
		user: newV5Client(t, httpHost, backupsUserKey),
	}
	v6 := v6JourneyClient{
		root: newV6Client(t, httpHost, grpcHost, backupsRootKey),
		user: newV6Client(t, httpHost, grpcHost, backupsUserKey),
	}
	readers := []journeyReader{
		{name: "v5", client: v5, legacy: true},
		{name: "v6", client: v6},
	}

	tests := []struct {
		name   string
		writer rbacJourneyClient
		role   string
		// stored holds the role's backups grants after steps 1, 3, 4 and 5.
		stored [4][]string
	}{
		{
			name:   "v5 writer",
			writer: v5,
			role:   "backupPrincipalsV5",
			stored: [4][]string{
				{backupsGrantOnA, backupsWildUser, backupsWildRole},
				{backupsGrantOnA, backupsGrantOnB, backupsWildUser, backupsWildRole},
				{backupsGrantOnB, backupsWildUser, backupsWildRole},
				nil,
			},
		},
		{
			name:   "v6 writer",
			writer: v6,
			role:   "backupPrincipalsV6",
			stored: [4][]string{
				{backupsGrantOnA},
				{backupsGrantOnA, backupsGrantOnB},
				{backupsGrantOnB},
				nil,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			runBackupsJourney(t, tt.writer, readers, tt.role, tt.stored)
		})
	}
}

type journeyReader struct {
	name   string
	client rbacJourneyClient
	// legacy readers send no backup principals header, so they see only
	// collections grants.
	legacy bool
}

func runBackupsJourney(t *testing.T, writer rbacJourneyClient, readers []journeyReader, role string, stored [4][]string) {
	ctx := context.Background()

	assertReads := func(step string, stored []string) {
		t.Run(step, func(t *testing.T) {
			for _, r := range readers {
				want := stored
				if r.legacy {
					want = collectionsOnly(stored)
				}
				t.Run(r.name, func(t *testing.T) {
					for _, read := range r.client.reads() {
						t.Run(read.name, func(t *testing.T) {
							got, err := read.read(ctx, role)
							require.NoError(t, err)
							assert.ElementsMatch(t, want, got)
						})
					}
				})
			}
		})
	}

	require.NoError(t, writer.createRole(ctx, role, "A"))
	// Step 2 changes no permission, so these reads see what step 1 stored.
	require.NoError(t, writer.assignRole(ctx, role))
	assertReads("after step 1 create and step 2 assign", stored[0])

	require.NoError(t, writer.addBackups(ctx, role, "B"))
	assertReads("after step 3 add B", stored[1])

	require.NoError(t, writer.removeBackups(ctx, role, "A"))
	assertReads("after step 4 remove A", stored[2])

	require.NoError(t, writer.removeBackups(ctx, role, "B"))
	assertReads("after step 5 remove B", stored[3])

	require.NoError(t, writer.deleteRole(ctx, role))
	t.Run("after step 6 delete", func(t *testing.T) {
		for _, r := range readers {
			t.Run(r.name, func(t *testing.T) {
				exists, err := r.client.roleExists(ctx, role)
				require.NoError(t, err)
				assert.False(t, exists)
			})
		}
	})
}

func collectionsOnly(grants []string) []string {
	var out []string
	for _, g := range grants {
		if strings.Contains(g, collectionMarker) {
			out = append(out, g)
		}
	}
	return out
}

type v5JourneyClient struct {
	root *wv5.Client
	// user authenticates as backupsUser. Only the own info read uses it.
	user *wv5.Client
}

func newV5Client(t *testing.T, host, key string) *wv5.Client {
	t.Helper()
	c, err := wv5.NewClient(wv5.Config{
		Scheme:     "http",
		Host:       host,
		AuthConfig: authv5.ApiKey{Value: key},
	})
	require.NoError(t, err)
	return c
}

func (c v5JourneyClient) createRole(ctx context.Context, role, collection string) error {
	return c.root.Roles().Creator().WithRole(rbacv5.NewRole(role,
		rbacv5.BackupsPermission{Actions: []string{manageBackups}, Collection: collection},
		rbacv5.ClusterPermission{Actions: []string{"read_cluster"}},
	)).Do(ctx)
}

func (c v5JourneyClient) assignRole(ctx context.Context, role string) error {
	if err := c.root.Users().DB().RolesAssigner().WithUserID(backupsUser).WithRoles(role).Do(ctx); err != nil {
		return err
	}
	return c.root.Groups().OIDC().RolesAssigner().WithGroupId(backupsGroup).WithRoles(role).Do(ctx)
}

func (c v5JourneyClient) addBackups(ctx context.Context, role, collection string) error {
	return c.root.Roles().PermissionAdder().WithRole(role).WithPermissions(
		rbacv5.BackupsPermission{Actions: []string{manageBackups}, Collection: collection},
	).Do(ctx)
}

func (c v5JourneyClient) removeBackups(ctx context.Context, role, collection string) error {
	return c.root.Roles().PermissionRemover().WithRole(role).WithPermissions(
		rbacv5.BackupsPermission{Actions: []string{manageBackups}, Collection: collection},
	).Do(ctx)
}

func (c v5JourneyClient) deleteRole(ctx context.Context, role string) error {
	return c.root.Roles().Deleter().WithName(role).Do(ctx)
}

// v5's exists call returns a 404 error for a missing role. roleExists reports
// that error as false.
func (c v5JourneyClient) roleExists(ctx context.Context, role string) (bool, error) {
	exists, err := c.root.Roles().Exists().WithName(role).Do(ctx)
	var clientErr *fault.WeaviateClientError
	if errors.As(err, &clientErr) && clientErr.StatusCode == http.StatusNotFound {
		return false, nil
	}
	return exists, err
}

func (c v5JourneyClient) reads() []backupsRead {
	reads := []backupsRead{
		{name: "get role", read: func(ctx context.Context, role string) ([]string, error) {
			r, err := c.root.Roles().Getter().WithName(role).Do(ctx)
			if err != nil {
				return nil, err
			}
			return v5Grants(&r), nil
		}},
		{name: "list roles", read: func(ctx context.Context, role string) ([]string, error) {
			all, err := c.root.Roles().AllGetter().Do(ctx)
			if err != nil {
				return nil, err
			}
			roles := make([]*rbacv5.Role, len(all))
			for i := range all {
				roles[i] = &all[i]
			}
			return v5RoleGrants(roles, role)
		}},
		{name: "roles of db user", read: func(ctx context.Context, role string) ([]string, error) {
			roles, err := c.root.Users().DB().RolesGetter().WithUserID(backupsUser).WithIncludeFullRoles(true).Do(ctx)
			if err != nil {
				return nil, err
			}
			return v5RoleGrants(roles, role)
		}},
		{name: "roles of oidc group", read: func(ctx context.Context, role string) ([]string, error) {
			roles, err := c.root.Groups().OIDC().RolesGetter().WithGroupID(backupsGroup).WithIncludeFullRoles(true).Do(ctx)
			if err != nil {
				return nil, err
			}
			return v5RoleGrants(roles, role)
		}},
		{name: "own info", read: func(ctx context.Context, role string) ([]string, error) {
			info, err := c.user.Users().MyUserGetter().Do(ctx)
			if err != nil {
				return nil, err
			}
			return v5RoleGrants(info.Roles, role)
		}},
		{name: "roles of user deprecated", read: func(ctx context.Context, role string) ([]string, error) {
			roles, err := c.root.Users().UserRolesGetter().WithUserID(backupsUser).Do(ctx) //nolint:staticcheck // the deprecated endpoint is under test
			if err != nil {
				return nil, err
			}
			return v5RoleGrants(roles, role)
		}},
	}
	for i := range reads {
		reads[i].read = recoverPanic(reads[i].read)
	}
	return reads
}

// The v5 role parser panics on a backups grant with no collection, because it
// dereferences the nil collection. recoverPanic turns that panic into an error
// returned by the read that hit it.
func recoverPanic(read func(context.Context, string) ([]string, error)) func(context.Context, string) ([]string, error) {
	return func(ctx context.Context, role string) (grants []string, err error) {
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("v5 client panicked: %v", r)
			}
		}()
		return read(ctx, role)
	}
}

func v5RoleGrants(roles []*rbacv5.Role, role string) ([]string, error) {
	for _, r := range roles {
		if r.Name == role {
			return v5Grants(r), nil
		}
	}
	return nil, fmt.Errorf("role %q not in response", role)
}

func v5Grants(r *rbacv5.Role) []string {
	var out []string
	for _, p := range r.Backups {
		for _, action := range p.Actions {
			out = append(out, action+collectionMarker+p.Collection)
		}
	}
	return out
}

type v6JourneyClient struct {
	root *wv6.Client
	// user authenticates as backupsUser. Only the own info read uses it.
	user *wv6.Client
}

func newV6Client(t *testing.T, httpHost, grpcHost, key string) *wv6.Client {
	t.Helper()
	host, httpPort, err := net.SplitHostPort(httpHost)
	require.NoError(t, err)
	gHost, grpcPort, err := net.SplitHostPort(grpcHost)
	require.NoError(t, err)

	c, err := wv6.NewClient(context.Background(),
		wv6.WithScheme("http"),
		wv6.WithHTTPHost(host),
		wv6.WithHTTPPort(httpPort),
		wv6.WithGRPCHost(gHost),
		wv6.WithGRPCPort(grpcPort),
		wv6.WithAPIKey(key),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		if err := c.Close(); err != nil {
			t.Logf("WARN: close weaviate client: %v", err)
		}
	})
	return c
}

func v6BackupsOn(collection string) rbacv6.Permissions {
	return rbacv6.Permissions{
		Backups: []rbacv6.BackupsPermission{{Collection: collection, Manage: true}},
	}
}

func (c v6JourneyClient) createRole(ctx context.Context, role, collection string) error {
	perms := v6BackupsOn(collection)
	perms.Cluster = []rbacv6.ClusterPermission{{Read: true}}
	return c.root.Roles.Create(ctx, rbacv6.Role{ID: role, Permissions: perms})
}

func (c v6JourneyClient) assignRole(ctx context.Context, role string) error {
	if err := c.root.Users.DB.AssignRoles(ctx, rbacv6.AssignRolesOptions{ID: backupsUser, Roles: []string{role}}); err != nil {
		return err
	}
	return c.root.Groups.AssignRoles(ctx, rbacv6.AssignRolesOptions{ID: backupsGroup, Roles: []string{role}})
}

func (c v6JourneyClient) addBackups(ctx context.Context, role, collection string) error {
	return c.root.Roles.AddPermissions(ctx, rbacv6.AddPermissions{RoleID: role, Permissions: v6BackupsOn(collection)})
}

func (c v6JourneyClient) removeBackups(ctx context.Context, role, collection string) error {
	return c.root.Roles.RemovePermissions(ctx, rbacv6.RemovePermissions{RoleID: role, Permissions: v6BackupsOn(collection)})
}

func (c v6JourneyClient) deleteRole(ctx context.Context, role string) error {
	return c.root.Roles.Delete(ctx, role)
}

func (c v6JourneyClient) roleExists(ctx context.Context, role string) (bool, error) {
	return c.root.Roles.Exists(ctx, role)
}

func (c v6JourneyClient) reads() []backupsRead {
	return []backupsRead{
		{name: "get role", read: func(ctx context.Context, role string) ([]string, error) {
			r, err := c.root.Roles.Get(ctx, role)
			if err != nil {
				return nil, err
			}
			return v6Grants(r), nil
		}},
		{name: "list roles", read: func(ctx context.Context, role string) ([]string, error) {
			roles, err := c.root.Roles.List(ctx)
			if err != nil {
				return nil, err
			}
			return v6RoleGrants(roles, role)
		}},
		{name: "roles of db user", read: func(ctx context.Context, role string) ([]string, error) {
			roles, err := c.root.Users.DB.AssignedRoles(ctx, rbacv6.AssignedRolesOptions{ID: backupsUser, IncludePermissions: true})
			if err != nil {
				return nil, err
			}
			return v6RoleGrants(roles, role)
		}},
		{name: "roles of oidc group", read: func(ctx context.Context, role string) ([]string, error) {
			roles, err := c.root.Groups.AssignedRoles(ctx, rbacv6.AssignedRolesOptions{ID: backupsGroup, IncludePermissions: true})
			if err != nil {
				return nil, err
			}
			return v6RoleGrants(roles, role)
		}},
		{name: "own info", read: func(ctx context.Context, role string) ([]string, error) {
			info, err := c.user.Users.MyUserInfo(ctx)
			if err != nil {
				return nil, err
			}
			return v6RoleGrants(info.Roles, role)
		}},
	}
}

func v6RoleGrants(roles []rbacv6.Role, role string) ([]string, error) {
	for i := range roles {
		if roles[i].ID == role {
			return v6Grants(&roles[i]), nil
		}
	}
	return nil, fmt.Errorf("role %q not in response", role)
}

func v6Grants(r *rbacv6.Role) []string {
	var out []string
	for _, p := range r.Backups {
		var target string
		switch {
		case p.Collection != "":
			target = collectionMarker + p.Collection
		case p.User != "":
			target = " user=" + p.User
		case p.Role != "":
			target = " role=" + p.Role
		default:
			target = " unset"
		}
		if p.Manage {
			out = append(out, manageBackups+target)
		}
		if p.Read {
			out = append(out, readBackups+target)
		}
	}
	return out
}

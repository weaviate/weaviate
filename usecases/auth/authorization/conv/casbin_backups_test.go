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

package conv

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

func TestBackupPrincipalPermissions(t *testing.T) {
	manage := authorization.ManageBackups
	tests := []struct {
		name       string
		backups    *models.PermissionBackups
		wantPolicy authorization.Policy
	}{
		{
			name:       "all users",
			backups:    &models.PermissionBackups{User: authorization.All},
			wantPolicy: authorization.Policy{Resource: "backups/users/.*", Verb: CRUD, Domain: "backups"},
		},
		{
			name:       "one user, case kept",
			backups:    &models.PermissionBackups{User: authorization.String("alice")},
			wantPolicy: authorization.Policy{Resource: "backups/users/alice", Verb: CRUD, Domain: "backups"},
		},
		{
			name:       "an OIDC user whose ID holds a colon",
			backups:    &models.PermissionBackups{User: authorization.String("urn:corp:alice")},
			wantPolicy: authorization.Policy{Resource: "backups/users/urn:corp:alice", Verb: CRUD, Domain: "backups"},
		},
		{
			name:       "a user pattern",
			backups:    &models.PermissionBackups{User: authorization.String("team-*")},
			wantPolicy: authorization.Policy{Resource: "backups/users/team-.*", Verb: CRUD, Domain: "backups"},
		},
		{
			name:       "a user alternation stays confined to its segment",
			backups:    &models.PermissionBackups{User: authorization.String("alice|bob")},
			wantPolicy: authorization.Policy{Resource: "backups/users/(alice|bob)", Verb: CRUD, Domain: "backups"},
		},
		{
			name:       "all roles",
			backups:    &models.PermissionBackups{Role: authorization.All},
			wantPolicy: authorization.Policy{Resource: "backups/roles/.*", Verb: CRUD, Domain: "backups"},
		},
		{
			name:       "one namespace-local role",
			backups:    &models.PermissionBackups{Role: authorization.String("ns1:editor")},
			wantPolicy: authorization.Policy{Resource: "backups/roles/ns1:editor", Verb: CRUD, Domain: "backups"},
		},
		{
			name:       "a role pattern",
			backups:    &models.PermissionBackups{Role: authorization.String("edit?r")},
			wantPolicy: authorization.Policy{Resource: "backups/roles/edit?r", Verb: CRUD, Domain: "backups"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name+" round-trips through a policy", func(t *testing.T) {
			perm := &models.Permission{Action: &manage, Backups: tt.backups}
			policies, err := PermissionToPolicies(perm)
			require.NoError(t, err)
			require.Len(t, policies, 1)
			assert.Equal(t, tt.wantPolicy, *policies[0])

			back, err := PoliciesToPermission(*policies[0])
			require.NoError(t, err)
			require.Len(t, back, 1)
			assert.Equal(t, perm, back[0])
		})
	}

	t.Run("a checked resource reads back as the permission it needs", func(t *testing.T) {
		for resource, want := range map[string]*models.PermissionBackups{
			"backups/users/urn:corp:alice": {User: authorization.String("urn:corp:alice")},
			"backups/roles/*":              {Role: authorization.All},
			"backups/collections/Movies":   {Collection: authorization.String("Movies")},
		} {
			got, err := PathToPermission(authorization.CREATE, resource)
			require.NoError(t, err, resource)
			assert.Equal(t, want, got.Backups, resource)
			assert.Equal(t, "create_backups", *got.Action, resource)
		}
	})

	t.Run("more than one target is rejected", func(t *testing.T) {
		for name, backups := range map[string]*models.PermissionBackups{
			"collection and user": {Collection: authorization.All, User: authorization.All},
			"collection and role": {Collection: authorization.All, Role: authorization.All},
			"user and role":       {User: authorization.All, Role: authorization.All},
		} {
			_, err := PermissionToPolicies(&models.Permission{Action: &manage, Backups: backups})
			require.Error(t, err, name)
		}
	})
}

func TestBackupPrincipalGrants(t *testing.T) {
	manage := authorization.ManageBackups
	restGrants, err := PermissionToPolicies(
		&models.Permission{Action: &manage, Backups: authorization.AllBackupUsers},
		&models.Permission{Action: &manage, Backups: authorization.AllBackupRoles},
	)
	require.NoError(t, err)

	t.Run("a collections grant yields exactly what a REST grant of the users and roles wildcards stores", func(t *testing.T) {
		for _, source := range []authorization.Policy{
			{Resource: CasbinBackups("*"), Verb: CRUD, Domain: authorization.BackupsDomain},
			{Resource: CasbinBackups("Movies"), Verb: CRUD, Domain: authorization.BackupsDomain},
		} {
			got := BackupPrincipalGrants(source)
			require.Len(t, got, 2, source.Resource)
			assert.Equal(t, *restGrants[0], got[0], source.Resource)
			assert.Equal(t, *restGrants[1], got[1], source.Resource)
		}
	})

	t.Run("the grants carry the source row's verb", func(t *testing.T) {
		got := BackupPrincipalGrants(authorization.Policy{Resource: CasbinBackups("*"), Verb: authorization.READ, Domain: authorization.BackupsDomain})
		require.Len(t, got, 2)
		for _, g := range got {
			assert.Equal(t, authorization.READ, g.Verb)
		}
	})

	t.Run("any other row yields nothing", func(t *testing.T) {
		for _, source := range []authorization.Policy{
			*restGrants[0],
			*restGrants[1],
			{Resource: CasbinSchema("Movies", "#"), Verb: CRUD, Domain: authorization.SchemaDomain},
			{Resource: "*", Verb: VALID_VERBS, Domain: "*"},
			{Resource: InternalPlaceHolder, Verb: InternalPlaceHolder, Domain: "*"},
		} {
			assert.Nil(t, BackupPrincipalGrants(source), source.Resource)
		}
	})
}

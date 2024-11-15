package test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/client/schema"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
)

type keys struct {
	admin  string
	rights string
}

type users struct {
	admin  string
	rights string
}

func setupCompose(ctx context.Context, t *testing.T) (*docker.DockerCompose, *keys, *users) {
	adminUser := "admin-user"
	adminKey := "admin-key"

	rightsUser := "rights-user"
	rightsKey := "rights-key"

	adminRole := "admin"
	customRole := "custom"

	compose, err := docker.New().
		WithWeaviate().
		WithRBAC().
		WithRbacUser(adminUser, adminKey, adminRole).
		WithRbacUser(rightsUser, rightsKey, customRole).
		Start(ctx)
	require.Nil(t, err)
	return compose, &keys{
			admin:  adminKey,
			rights: rightsKey,
		}, &users{
			admin:  adminUser,
			rights: rightsUser,
		}
}

func TestSchemaAuthzJourney(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	compose, keys, users := setupCompose(ctx, t)
	defer func() {
		if err := compose.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate test containers: %v", err)
		}
	}()

	helper.SetupClient(compose.GetWeaviate().URI())
	defer helper.ResetClient()

	className := t.Name()
	readAllSchemaRole := "read-schema-all"
	readOneSchemaRole := "read-schema-one"

	t.Run("create class", func(t *testing.T) {
		helper.CreateClassAuth(t, keys.admin, &models.Class{
			Class: className,
		})
	})

	t.Run("create role with read_schema permission on *", func(t *testing.T) {
		helper.CreateRole(t, keys.admin, &models.Role{
			Name: String(readAllSchemaRole),
			Permissions: []*models.Permission{{
				Action:     String("read_schema"),
				Collection: String("*"),
			}},
		})
	})

	t.Run("create role with read_schema permission on className", func(t *testing.T) {
		helper.CreateRole(t, keys.admin, &models.Role{
			Name: String(readOneSchemaRole),
			Permissions: []*models.Permission{{
				Action:     String("read_schema"),
				Collection: String(className),
			}},
		})
	})

	t.Run("verify that GET v1/schema is not allowed for user without collections/*/schema permission", func(t *testing.T) {
		_, err := helper.GetSchemaAuth(t, keys.rights)
		require.NotNil(t, err)
		parsed, forbidden := err.(*schema.SchemaDumpForbidden)
		require.True(t, forbidden)
		fmt.Println(parsed.Payload.Error[0].Message)
		require.Contains(t, parsed.Payload.Error[0].Message, "forbidden")
	})

	t.Run("assign role read-schema-all to custom user", func(t *testing.T) {
		helper.AssignRoleToUser(t, keys.admin, readAllSchemaRole, users.rights)
	})

	t.Run("verify that GET v1/schema is allowed for user with collections/*/schema permission", func(t *testing.T) {
		resp, err := helper.GetSchemaAuth(t, keys.rights)
		require.Nil(t, err)
		require.NotNil(t, resp)
		require.Equal(t, len(resp), 1)
		require.Equal(t, className, resp[0].Class)
	})

	t.Run("verify that GET v1/schema/className is allowed for user with collections/className/schema permission", func(t *testing.T) {
		resp, err := helper.GetClassAuth(t, keys.rights, className)
		require.Nil(t, err)
		require.NotNil(t, resp)
		require.Equal(t, className, resp.Class)
	})

	t.Run("revoke role read-schema-all from custom user", func(t *testing.T) {
		helper.RevokeRoleFromUser(t, keys.admin, readAllSchemaRole, users.rights)
	})

	t.Run("assign role read-schema-one to custom user", func(t *testing.T) {
		helper.AssignRoleToUser(t, keys.admin, readOneSchemaRole, users.rights)
	})

	t.Run("verify that GET v1/schema/* is not allowed for user with collections/className/schema permission", func(t *testing.T) {
		_, err := helper.GetSchemaAuth(t, keys.rights)
		require.NotNil(t, err)
		parsed, forbidden := err.(*schema.SchemaDumpForbidden)
		require.True(t, forbidden)
		fmt.Println(parsed.Payload.Error[0].Message)
		require.Contains(t, parsed.Payload.Error[0].Message, "forbidden")
	})

	t.Run("verify that GET v1/schema/className is allowed for user with collections/className/schema permission", func(t *testing.T) {
		resp, err := helper.GetClassAuth(t, keys.rights, className)
		require.Nil(t, err)
		require.NotNil(t, resp)
		require.Equal(t, className, resp.Class)
	})
}

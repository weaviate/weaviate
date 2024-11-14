package helper

import (
	"testing"

	"github.com/weaviate/weaviate/client/authz"
	"github.com/weaviate/weaviate/entities/models"
)

func CreateRole(t *testing.T, key string, role *models.Role) error {
	resp, err := Client(t).Authz.CreateRole(authz.NewCreateRoleParams().WithBody(role), CreateAuth(key))
	AssertRequestOk(t, resp, err, nil)
	return err
}

func GetRoles(t *testing.T, key string) ([]*models.Role, error) {
	resp, err := Client(t).Authz.GetRoles(authz.NewGetRolesParams(), CreateAuth(key))
	AssertRequestOk(t, resp, err, nil)
	return resp.Payload, err
}

func DeleteRole(t *testing.T, key, role string) error {
	resp, err := Client(t).Authz.DeleteRole(authz.NewDeleteRoleParams().WithID(role), CreateAuth(key))
	AssertRequestOk(t, resp, err, nil)
	return err
}

func GetRoleByName(t *testing.T, key, role string) (*models.Role, error) {
	resp, err := Client(t).Authz.GetRole(authz.NewGetRoleParams().WithID(role), CreateAuth(key))
	AssertRequestOk(t, resp, err, nil)
	return resp.Payload, err
}

func GetRoleByNameError(t *testing.T, key, role string) error {
	_, err := Client(t).Authz.GetRole(authz.NewGetRoleParams().WithID(role), CreateAuth(key))
	return err
}

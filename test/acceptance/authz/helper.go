//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package authz

import (
	"testing"

	"github.com/stretchr/testify/require"
	gql "github.com/weaviate/weaviate/client/graphql"

	"github.com/go-openapi/strfmt"
	"github.com/weaviate/weaviate/client/objects"

	"github.com/weaviate/weaviate/entities/models"

	"github.com/go-openapi/runtime"
	clschema "github.com/weaviate/weaviate/client/schema"
	"github.com/weaviate/weaviate/test/helper"
)

const (
	UUID1 = strfmt.UUID("73f2eb5f-5abf-447a-81ca-74b1dd168241")
	UUID2 = strfmt.UUID("73f2eb5f-5abf-447a-81ca-74b1dd168242")
	UUID3 = strfmt.UUID("73f2eb5f-5abf-447a-81ca-74b1dd168243")
	UUID4 = strfmt.UUID("73f2eb5f-5abf-447a-81ca-74b1dd168244")
	UUID5 = strfmt.UUID("73f2eb5f-5abf-447a-81ca-74b1dd168245")
	UUID6 = strfmt.UUID("73f2eb5f-5abf-447a-81ca-74b1dd168246")
)

const NumBuildInRoles = 2

func deleteObjectClass(t *testing.T, class string, auth runtime.ClientAuthInfoWriter) {
	delParams := clschema.NewSchemaObjectsDeleteParams().WithClassName(class)
	delRes, err := helper.Client(t).Schema.SchemaObjectsDelete(delParams, auth)
	helper.AssertRequestOk(t, delRes, err, nil)
}

func createClass(t *testing.T, class *models.Class, auth runtime.ClientAuthInfoWriter) error {
	params := clschema.NewSchemaObjectsCreateParams().WithObjectClass(class)
	_, err := helper.Client(t).Schema.SchemaObjectsCreate(params, auth)
	return err
}

func generateMissingLists(permissions []*models.Permission) [][]*models.Permission {
	var result [][]*models.Permission

	for i := range permissions {
		missingList := make([]*models.Permission, 0, len(permissions)-1)
		missingList = append(missingList, permissions[:i]...)
		missingList = append(missingList, permissions[i+1:]...)
		result = append(result, missingList)
	}

	return result
}

func createObject(t *testing.T, object *models.Object, key string) (*objects.ObjectsCreateOK, error) {
	params := objects.NewObjectsCreateParams().WithBody(object)
	return helper.Client(t).Objects.ObjectsCreate(params, helper.CreateAuth(key))
}

func getObject(t *testing.T, id strfmt.UUID, key string) (*objects.ObjectsGetOK, error) {
	params := objects.NewObjectsGetParams().WithID(id)
	return helper.Client(t).Objects.ObjectsGet(params, helper.CreateAuth(key))
}

func deleteObject(t *testing.T, id strfmt.UUID, key string) (*objects.ObjectsDeleteNoContent, error) {
	params := objects.NewObjectsDeleteParams().WithID(id)
	return helper.Client(t).Objects.ObjectsDelete(params, helper.CreateAuth(key))
}

func addRef(t *testing.T, fromId strfmt.UUID, fromProp string, ref *models.SingleRef, key string) (*objects.ObjectsReferencesCreateOK, error) {
	params := objects.NewObjectsReferencesCreateParams().WithBody(ref).WithID(fromId).WithPropertyName(fromProp)
	return helper.Client(t).Objects.ObjectsReferencesCreate(params, helper.CreateAuth(key))
}

func updateRef(t *testing.T, fromId strfmt.UUID, fromProp string, ref *models.SingleRef, key string) (*objects.ObjectsReferencesUpdateOK, error) {
	params := objects.NewObjectsReferencesUpdateParams().WithBody(models.MultipleRef{ref}).WithID(fromId).WithPropertyName(fromProp)
	return helper.Client(t).Objects.ObjectsReferencesUpdate(params, helper.CreateAuth(key))
}

func deleteRef(t *testing.T, fromId strfmt.UUID, fromProp string, ref *models.SingleRef, key string) (*objects.ObjectsReferencesDeleteNoContent, error) {
	params := objects.NewObjectsReferencesDeleteParams().WithBody(ref).WithID(fromId).WithPropertyName(fromProp)
	return helper.Client(t).Objects.ObjectsReferencesDelete(params, helper.CreateAuth(key))
}

func String(s string) *string {
	return &s
}

func queryGQL(t *testing.T, query, key string) (*gql.GraphqlPostOK, error) {
	params := gql.NewGraphqlPostParams().WithBody(&models.GraphQLQuery{OperationName: "", Query: query, Variables: nil})
	return helper.Client(t).Graphql.GraphqlPost(params, helper.CreateAuth(key))
}

func assertGQL(t *testing.T, query, key string) *models.GraphQLResponse {
	params := gql.NewGraphqlPostParams().WithBody(&models.GraphQLQuery{OperationName: "", Query: query, Variables: nil})
	resp, err := helper.Client(t).Graphql.GraphqlPost(params, helper.CreateAuth(key))
	require.Nil(t, err)
	if len(resp.Payload.Errors) > 0 {
		t.Logf("Error: %s", resp.Payload.Errors[0].Message)
	}
	require.Equal(t, len(resp.Payload.Errors), 0)
	return resp.Payload
}

func readTenant(t *testing.T, class string, tenant string, key string) error {
	params := clschema.NewTenantsGetOneParams().WithClassName(class).WithTenantName(tenant)
	_, err := helper.Client(t).Schema.TenantsGetOne(params, helper.CreateAuth(key))
	return err
}

func readTenants(t *testing.T, class string, key string) error {
	params := clschema.NewTenantsGetParams().WithClassName(class)
	_, err := helper.Client(t).Schema.TenantsGet(params, helper.CreateAuth(key))
	return err
}

func existsTenant(t *testing.T, class string, tenant string, key string) error {
	params := clschema.NewTenantExistsParams().WithClassName(class).WithTenantName(tenant)
	_, err := helper.Client(t).Schema.TenantExists(params, helper.CreateAuth(key))
	return err
}

func createTenant(t *testing.T, class string, tenants []*models.Tenant, key string) error {
	params := clschema.NewTenantsCreateParams().WithClassName(class).WithBody(tenants)
	_, err := helper.Client(t).Schema.TenantsCreate(params, helper.CreateAuth(key))
	return err
}

func deleteTenant(t *testing.T, class string, tenants []string, key string) error {
	params := clschema.NewTenantsDeleteParams().WithClassName(class).WithTenants(tenants)
	_, err := helper.Client(t).Schema.TenantsDelete(params, helper.CreateAuth(key))
	return err
}

func updateTenantStatus(t *testing.T, class string, tenants []*models.Tenant, key string) error {
	params := clschema.NewTenantsUpdateParams().WithClassName(class).WithBody(tenants)
	_, err := helper.Client(t).Schema.TenantsUpdate(params, helper.CreateAuth(key))
	return err
}

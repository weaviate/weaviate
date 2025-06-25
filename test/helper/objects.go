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

package helper

import (
	"context"
	"strings"
	"testing"

	"github.com/go-openapi/runtime"
	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/entities/modelsext"

	"github.com/weaviate/weaviate/client/batch"
	"github.com/weaviate/weaviate/client/meta"
	"github.com/weaviate/weaviate/client/objects"
	"github.com/weaviate/weaviate/client/schema"
	"github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema/crossref"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
)

func SetupClient(uri string) {
	host, port := "", ""
	res := strings.Split(uri, ":")
	if len(res) == 2 {
		host, port = res[0], res[1]
	}
	ServerHost = host
	ServerPort = port
}

func SetupGRPCClient(t *testing.T, uri string) {
	host, port := "", ""
	res := strings.Split(uri, ":")
	if len(res) == 2 {
		host, port = res[0], res[1]
	}
	ServerGRPCHost = host
	ServerGRPCPort = port
}

func CreateClass(t *testing.T, class *models.Class) {
	t.Helper()

	// if the schema has mixed vectors, we have to create it in two steps as single step creation is forbidden
	var capturedVectorConfig map[string]models.VectorConfig
	if modelsext.ClassHasLegacyVectorIndex(class) && class.VectorConfig != nil {
		capturedVectorConfig = class.VectorConfig
		class.VectorConfig = nil
	}

	params := schema.NewSchemaObjectsCreateParams().WithObjectClass(class)
	resp, err := Client(t).Schema.SchemaObjectsCreate(params, nil)
	AssertRequestOk(t, resp, err, nil)

	if capturedVectorConfig != nil {
		class.VectorConfig = capturedVectorConfig
		updateParams := schema.NewSchemaObjectsUpdateParams().WithClassName(class.Class).WithObjectClass(class)
		updateResp, err := Client(t).Schema.SchemaObjectsUpdate(updateParams, nil)
		AssertRequestOk(t, updateResp, err, nil)
	}
}

func CreateClassAuth(t *testing.T, class *models.Class, key string) {
	t.Helper()
	params := schema.NewSchemaObjectsCreateParams().WithObjectClass(class)
	resp, err := Client(t).Schema.SchemaObjectsCreate(params, CreateAuth(key))
	AssertRequestOk(t, resp, err, nil)
}

func GetClass(t *testing.T, class string) *models.Class {
	t.Helper()
	params := schema.NewSchemaObjectsGetParams().WithClassName(class)
	resp, err := Client(t).Schema.SchemaObjectsGet(params, nil)
	AssertRequestOk(t, resp, err, nil)
	return resp.Payload
}

func GetClassAuth(t *testing.T, class string, key string) *models.Class {
	t.Helper()
	params := schema.NewSchemaObjectsGetParams().WithClassName(class)
	resp, err := Client(t).Schema.SchemaObjectsGet(params, CreateAuth(key))
	AssertRequestOk(t, resp, err, nil)
	return resp.Payload
}

func GetClassWithoutAssert(t *testing.T, class string) (*models.Class, error) {
	t.Helper()
	params := schema.NewSchemaObjectsGetParams().WithClassName(class)
	resp, err := Client(t).Schema.SchemaObjectsGet(params, nil)
	if err != nil {
		return nil, err
	}
	return resp.Payload, nil
}

func UpdateClass(t *testing.T, class *models.Class) {
	t.Helper()
	params := schema.NewSchemaObjectsUpdateParams().
		WithObjectClass(class).WithClassName(class.Class)
	resp, err := Client(t).Schema.SchemaObjectsUpdate(params, nil)
	AssertRequestOk(t, resp, err, nil)
}

func CreateObject(t *testing.T, object *models.Object) error {
	t.Helper()
	params := objects.NewObjectsCreateParams().WithBody(object)
	resp, err := Client(t).Objects.ObjectsCreate(params, nil)
	AssertRequestOk(t, resp, err, nil)
	return err
}

func CreateObjectAuth(t *testing.T, object *models.Object, key string) error {
	t.Helper()
	params := objects.NewObjectsCreateParams().WithBody(object)
	_, err := Client(t).Objects.ObjectsCreate(params, CreateAuth(key))
	return err
}

func CreateObjectWithResponse(t *testing.T, object *models.Object) (*models.Object, error) {
	t.Helper()
	params := objects.NewObjectsCreateParams().WithBody(object)
	resp, err := Client(t).Objects.ObjectsCreate(params, nil)
	if err != nil {
		return nil, err
	}
	return resp.Payload, nil
}

func CreateObjectCL(t *testing.T, object *models.Object, cl types.ConsistencyLevel) error {
	t.Helper()
	cls := string(cl)
	params := objects.NewObjectsCreateParams().WithBody(object).WithConsistencyLevel(&cls)
	resp, err := Client(t).Objects.ObjectsCreate(params, nil)
	AssertRequestOk(t, resp, err, nil)
	return nil
}

func CreateObjectsBatch(t *testing.T, objects []*models.Object) {
	t.Helper()
	params := batch.NewBatchObjectsCreateParams().
		WithBody(batch.BatchObjectsCreateBody{
			Objects: objects,
		})
	resp, err := Client(t).Batch.BatchObjectsCreate(params, nil)
	AssertRequestOk(t, resp, err, nil)
	CheckObjectsBatchResponse(t, resp.Payload, err)
}

func CreateObjectsBatchAuth(t *testing.T, objects []*models.Object, key string) {
	t.Helper()
	params := batch.NewBatchObjectsCreateParams().
		WithBody(batch.BatchObjectsCreateBody{
			Objects: objects,
		})
	resp, err := Client(t).Batch.BatchObjectsCreate(params, CreateAuth(key))
	AssertRequestOk(t, resp, err, nil)
	CheckObjectsBatchResponse(t, resp.Payload, err)
}

func CreateObjectsBatchCL(t *testing.T, objects []*models.Object, cl types.ConsistencyLevel) {
	cls := string(cl)
	params := batch.NewBatchObjectsCreateParams().
		WithBody(batch.BatchObjectsCreateBody{
			Objects: objects,
		}).WithConsistencyLevel(&cls)
	resp, err := Client(t).Batch.BatchObjectsCreate(params, nil)
	AssertRequestOk(t, resp, err, nil)
	CheckObjectsBatchResponse(t, resp.Payload, err)
}

func CheckObjectsBatchResponse(t *testing.T, resp []*models.ObjectsGetResponse, err error) {
	t.Helper()
	AssertRequestOk(t, resp, err, nil)
	for _, elem := range resp {
		if !assert.Nil(t, elem.Result.Errors) {
			t.Logf("expected nil, got: %v",
				elem.Result.Errors.Error[0].Message)
		}
	}
}

func UpdateObject(t *testing.T, object *models.Object) error {
	t.Helper()
	params := objects.NewObjectsUpdateParams().WithID(object.ID).WithBody(object)
	resp, err := Client(t).Objects.ObjectsUpdate(params, nil)
	AssertRequestOk(t, resp, err, nil)
	return err
}

func UpdateObjectCL(t *testing.T, object *models.Object, cl types.ConsistencyLevel) error {
	t.Helper()
	cls := string(cl)
	params := objects.NewObjectsClassPutParams().WithClassName(object.Class).
		WithID(object.ID).WithBody(object).WithConsistencyLevel(&cls)
	resp, err := Client(t).Objects.ObjectsClassPut(params, nil)
	AssertRequestOk(t, resp, err, nil)
	return err
}

func PatchObject(t *testing.T, object *models.Object) error {
	t.Helper()
	params := objects.NewObjectsPatchParams().WithID(object.ID).WithBody(object)
	resp, err := Client(t).Objects.ObjectsPatch(params, nil)
	AssertRequestOk(t, resp, err, nil)
	return err
}

func DeleteClass(t *testing.T, class string) {
	t.Helper()
	delParams := schema.NewSchemaObjectsDeleteParams().WithClassName(class)
	delRes, err := Client(t).Schema.SchemaObjectsDelete(delParams, nil)
	AssertRequestOk(t, delRes, err, nil)
}

func DeleteClassWithAuthz(t *testing.T, class string, authInfo runtime.ClientAuthInfoWriter) {
	t.Helper()
	delParams := schema.NewSchemaObjectsDeleteParams().WithClassName(class)
	delRes, err := Client(t).Schema.SchemaObjectsDelete(delParams, authInfo)
	AssertRequestOk(t, delRes, err, nil)
}

func DeleteClassAuth(t *testing.T, class string, key string) {
	t.Helper()
	delParams := schema.NewSchemaObjectsDeleteParams().WithClassName(class)
	delRes, err := Client(t).Schema.SchemaObjectsDelete(delParams, CreateAuth(key))
	AssertRequestOk(t, delRes, err, nil)
}

func DeleteObject(t *testing.T, object *models.Object) {
	t.Helper()
	params := objects.NewObjectsClassDeleteParams().
		WithClassName(object.Class).WithID(object.ID)
	resp, err := Client(t).Objects.ObjectsClassDelete(params, nil)
	AssertRequestOk(t, resp, err, nil)
}

func DeleteObjectCL(t *testing.T, class string, id strfmt.UUID, cl types.ConsistencyLevel) {
	cls := string(cl)
	params := objects.NewObjectsClassDeleteParams().
		WithClassName(class).WithID(id).WithConsistencyLevel(&cls)
	resp, err := Client(t).Objects.ObjectsClassDelete(params, nil)
	AssertRequestOk(t, resp, err, nil)
}

func DeleteObjectsBatch(t *testing.T, body *models.BatchDelete, cl types.ConsistencyLevel) {
	t.Helper()
	cls := string(cl)
	params := batch.NewBatchObjectsDeleteParams().WithBody(body).WithConsistencyLevel(&cls)
	resp, err := Client(t).Batch.BatchObjectsDelete(params, nil)
	AssertRequestOk(t, resp, err, nil)
}

func DeleteTenantObjectsBatch(t *testing.T, body *models.BatchDelete,
	tenant string,
) (*models.BatchDeleteResponse, error) {
	t.Helper()
	params := batch.NewBatchObjectsDeleteParams().
		WithBody(body).WithTenant(&tenant)
	resp, err := Client(t).Batch.BatchObjectsDelete(params, nil)
	if err != nil {
		return nil, err
	}
	return resp.Payload, nil
}

func DeleteTenantObjectsBatchCL(t *testing.T, body *models.BatchDelete,
	tenant string, cl types.ConsistencyLevel,
) (*models.BatchDeleteResponse, error) {
	cls := string(cl)
	params := batch.NewBatchObjectsDeleteParams().
		WithBody(body).WithTenant(&tenant).WithConsistencyLevel(&cls)
	resp, err := Client(t).Batch.BatchObjectsDelete(params, nil)
	if err != nil {
		return nil, err
	}
	return resp.Payload, nil
}

func AddReferences(t *testing.T, refs []*models.BatchReference) ([]*models.BatchReferenceResponse, error) {
	t.Helper()
	params := batch.NewBatchReferencesCreateParams().WithBody(refs)
	resp, err := Client(t).Batch.BatchReferencesCreate(params, nil)
	if err != nil {
		return nil, err
	}
	return resp.Payload, nil
}

func CheckReferencesBatchResponse(t *testing.T, resp []*models.BatchReferenceResponse, err error) {
	t.Helper()
	AssertRequestOk(t, resp, err, nil)
	for _, elem := range resp {
		if !assert.Nil(t, elem.Result.Errors) {
			t.Logf("expected nil, got: %v", elem.Result.Errors.Error[0].Message)
		}
	}
}

func AddReference(t *testing.T, object *models.Object, ref *models.SingleRef, prop string) {
	t.Helper()
	params := objects.NewObjectsClassReferencesCreateParams().
		WithClassName(object.Class).WithID(object.ID).WithBody(ref).WithPropertyName(prop)
	resp, err := Client(t).Objects.ObjectsClassReferencesCreate(params, nil)
	AssertRequestOk(t, resp, err, nil)
}

func AddReferenceReturn(t *testing.T, ref *models.SingleRef, id strfmt.UUID, class, prop, tenant string, auth runtime.ClientAuthInfoWriter) (*objects.ObjectsClassReferencesCreateOK, error) {
	t.Helper()
	params := objects.NewObjectsClassReferencesCreateParams().
		WithClassName(class).WithID(id).WithBody(ref).WithPropertyName(prop)
	if tenant != "" {
		params.WithTenant(&tenant)
	}
	return Client(t).Objects.ObjectsClassReferencesCreate(params, auth)
}

func ReplaceReferencesReturn(t *testing.T, refs []*models.SingleRef, id strfmt.UUID, class, prop, tenant string, auth runtime.ClientAuthInfoWriter) (*objects.ObjectsClassReferencesPutOK, error) {
	t.Helper()
	params := objects.NewObjectsClassReferencesPutParams().
		WithClassName(class).WithID(id).WithBody(refs).WithPropertyName(prop)
	if tenant != "" {
		params.WithTenant(&tenant)
	}
	return Client(t).Objects.ObjectsClassReferencesPut(params, auth)
}

func DeleteReferenceReturn(t *testing.T, ref *models.SingleRef, id strfmt.UUID, class, prop, tenant string, auth runtime.ClientAuthInfoWriter) (*objects.ObjectsClassReferencesDeleteNoContent, error) {
	t.Helper()
	params := objects.NewObjectsClassReferencesDeleteParams().
		WithClassName(class).WithID(id).WithBody(ref).WithPropertyName(prop)
	if tenant != "" {
		params.WithTenant(&tenant)
	}
	return Client(t).Objects.ObjectsClassReferencesDelete(params, auth)
}

func AddReferenceTenant(t *testing.T, object *models.Object, ref *models.SingleRef, prop string, tenant string) {
	t.Helper()
	params := objects.NewObjectsClassReferencesCreateParams().
		WithClassName(object.Class).WithID(object.ID).WithBody(ref).WithPropertyName(prop).WithTenant(&tenant)
	resp, err := Client(t).Objects.ObjectsClassReferencesCreate(params, nil)
	AssertRequestOk(t, resp, err, nil)
}

func DeleteReference(t *testing.T, object *models.Object, ref *models.SingleRef, prop string) {
	t.Helper()
	params := objects.NewObjectsClassReferencesDeleteParams().
		WithClassName(object.Class).WithID(object.ID).WithBody(ref).WithPropertyName(prop)
	resp, err := Client(t).Objects.ObjectsClassReferencesDelete(params, nil)
	AssertRequestOk(t, resp, err, nil)
}

func DeleteReferenceTenant(t *testing.T, object *models.Object, ref *models.SingleRef, prop string, tenant string) {
	t.Helper()
	params := objects.NewObjectsClassReferencesDeleteParams().
		WithClassName(object.Class).WithID(object.ID).WithBody(ref).WithPropertyName(prop).WithTenant(&tenant)
	resp, err := Client(t).Objects.ObjectsClassReferencesDelete(params, nil)
	AssertRequestOk(t, resp, err, nil)
}

func UpdateReferenceTenant(t *testing.T, object *models.Object, ref models.MultipleRef, prop string, tenant string) {
	t.Helper()
	params := objects.NewObjectsClassReferencesPutParams().
		WithClassName(object.Class).WithID(object.ID).WithBody(ref).WithPropertyName(prop).WithTenant(&tenant)
	resp, err := Client(t).Objects.ObjectsClassReferencesPut(params, nil)
	AssertRequestOk(t, resp, err, nil)
}

func CreateTenants(t *testing.T, class string, tenants []*models.Tenant) {
	t.Helper()
	params := schema.NewTenantsCreateParams().WithClassName(class).WithBody(tenants)
	resp, err := Client(t).Schema.TenantsCreate(params, nil)
	AssertRequestOk(t, resp, err, nil)
}

func CreateTenantsAuth(t *testing.T, class string, tenants []*models.Tenant, key string) {
	t.Helper()
	params := schema.NewTenantsCreateParams().WithClassName(class).WithBody(tenants)
	resp, err := Client(t).Schema.TenantsCreate(params, CreateAuth(key))
	AssertRequestOk(t, resp, err, nil)
}

func UpdateTenants(t *testing.T, class string, tenants []*models.Tenant) {
	t.Helper()
	params := schema.NewTenantsUpdateParams().WithClassName(class).WithBody(tenants)
	resp, err := Client(t).Schema.TenantsUpdate(params, nil)
	AssertRequestOk(t, resp, err, nil)
}

func UpdateTenantsWithAuthz(t *testing.T, class string, tenants []*models.Tenant, authInfo runtime.ClientAuthInfoWriter) {
	t.Helper()
	params := schema.NewTenantsUpdateParams().WithClassName(class).WithBody(tenants)
	resp, err := Client(t).Schema.TenantsUpdate(params, authInfo)
	AssertRequestOk(t, resp, err, nil)
}

func CreateTenantsReturnError(t *testing.T, class string, tenants []*models.Tenant) error {
	t.Helper()
	params := schema.NewTenantsCreateParams().WithClassName(class).WithBody(tenants)
	_, err := Client(t).Schema.TenantsCreate(params, nil)
	return err
}

func UpdateTenantsReturnError(t *testing.T, class string, tenants []*models.Tenant) error {
	t.Helper()
	params := schema.NewTenantsUpdateParams().WithClassName(class).WithBody(tenants)
	_, err := Client(t).Schema.TenantsUpdate(params, nil)
	return err
}

func GetTenants(t *testing.T, class string) (*schema.TenantsGetOK, error) {
	t.Helper()
	params := schema.NewTenantsGetParams().WithClassName(class)
	resp, err := Client(t).Schema.TenantsGet(params, nil)
	return resp, err
}

func GetTenantsWithAuthz(t *testing.T, class string, authInfo runtime.ClientAuthInfoWriter) (*schema.TenantsGetOK, error) {
	t.Helper()
	params := schema.NewTenantsGetParams().WithClassName(class)
	resp, err := Client(t).Schema.TenantsGet(params, authInfo)
	return resp, err
}

func GetOneTenant(t *testing.T, class, tenant string) (*schema.TenantsGetOneOK, error) {
	t.Helper()
	params := schema.NewTenantsGetOneParams().WithClassName(class).WithTenantName(tenant)
	resp, err := Client(t).Schema.TenantsGetOne(params, nil)
	return resp, err
}

func GetTenantsGRPC(t *testing.T, class string) (*pb.TenantsGetReply, error) {
	t.Helper()
	return ClientGRPC(t).TenantsGet(context.TODO(), &pb.TenantsGetRequest{Collection: class})
}

func TenantExists(t *testing.T, class string, tenant string) (*schema.TenantExistsOK, error) {
	params := schema.NewTenantExistsParams().WithClassName(class).WithTenantName(tenant)
	resp, err := Client(t).Schema.TenantExists(params, nil)
	return resp, err
}

func DeleteTenants(t *testing.T, class string, tenants []string) error {
	t.Helper()
	params := schema.NewTenantsDeleteParams().WithClassName(class).WithTenants(tenants)
	_, err := Client(t).Schema.TenantsDelete(params, nil)
	return err
}

func DeleteTenantsWithContext(t *testing.T, ctx context.Context, class string, tenants []string) error {
	t.Helper()
	params := schema.NewTenantsDeleteParams().WithContext(ctx).WithClassName(class).WithTenants(tenants)
	_, err := Client(t).Schema.TenantsDelete(params, nil)
	return err
}

func NewBeacon(className string, id strfmt.UUID) strfmt.URI {
	return crossref.New("localhost", className, id).SingleRef().Beacon
}

func GetMeta(t *testing.T) *models.Meta {
	t.Helper()
	params := meta.NewMetaGetParams()
	resp, err := Client(t).Meta.MetaGet(params, nil)
	AssertRequestOk(t, resp, err, nil)
	return resp.Payload
}

func ObjectContentsProp(contents string) map[string]interface{} {
	props := map[string]interface{}{}
	props["contents"] = contents
	return props
}

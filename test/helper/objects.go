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
	"strings"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/client/batch"
	"github.com/weaviate/weaviate/client/meta"
	"github.com/weaviate/weaviate/client/objects"
	"github.com/weaviate/weaviate/client/schema"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema/crossref"
	"github.com/weaviate/weaviate/usecases/replica"
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

func CreateClass(t *testing.T, class *models.Class) {
	params := schema.NewSchemaObjectsCreateParams().WithObjectClass(class)
	resp, err := Client(t).Schema.SchemaObjectsCreate(params, nil)
	AssertRequestOk(t, resp, err, nil)
}

func GetClass(t *testing.T, class string) *models.Class {
	params := schema.NewSchemaObjectsGetParams().WithClassName(class)
	resp, err := Client(t).Schema.SchemaObjectsGet(params, nil)
	AssertRequestOk(t, resp, err, nil)
	return resp.Payload
}

func UpdateClass(t *testing.T, class *models.Class) {
	params := schema.NewSchemaObjectsUpdateParams().
		WithObjectClass(class).WithClassName(class.Class)
	resp, err := Client(t).Schema.SchemaObjectsUpdate(params, nil)
	AssertRequestOk(t, resp, err, nil)
}

func CreateObject(t *testing.T, object *models.Object) {
	params := objects.NewObjectsCreateParams().WithBody(object)
	resp, err := Client(t).Objects.ObjectsCreate(params, nil)
	AssertRequestOk(t, resp, err, nil)
}

func CreateObjectCL(t *testing.T, object *models.Object, cl replica.ConsistencyLevel) {
	cls := string(cl)
	params := objects.NewObjectsCreateParams().WithBody(object).WithConsistencyLevel(&cls)
	resp, err := Client(t).Objects.ObjectsCreate(params, nil)
	AssertRequestOk(t, resp, err, nil)
}

func CreateObjectsBatch(t *testing.T, objects []*models.Object) {
	params := batch.NewBatchObjectsCreateParams().
		WithBody(batch.BatchObjectsCreateBody{
			Objects: objects,
		})
	resp, err := Client(t).Batch.BatchObjectsCreate(params, nil)
	AssertRequestOk(t, resp, err, nil)
	CheckObjectsBatchResponse(t, resp.Payload, err)
}

func CheckObjectsBatchResponse(t *testing.T, resp []*models.ObjectsGetResponse, err error) {
	AssertRequestOk(t, resp, err, nil)
	for _, elem := range resp {
		if !assert.Nil(t, elem.Result.Errors) {
			t.Logf("expected nil, got: %v",
				elem.Result.Errors.Error[0].Message)
		}
	}
}

func UpdateObject(t *testing.T, object *models.Object) {
	params := objects.NewObjectsUpdateParams().WithID(object.ID).WithBody(object)
	resp, err := Client(t).Objects.ObjectsUpdate(params, nil)
	AssertRequestOk(t, resp, err, nil)
}

func UpdateObjectCL(t *testing.T, object *models.Object, cl replica.ConsistencyLevel) {
	cls := string(cl)
	params := objects.NewObjectsClassPutParams().WithClassName(object.Class).
		WithID(object.ID).WithBody(object).WithConsistencyLevel(&cls)
	resp, err := Client(t).Objects.ObjectsClassPut(params, nil)
	AssertRequestOk(t, resp, err, nil)
}

func PatchObject(t *testing.T, object *models.Object) {
	params := objects.NewObjectsPatchParams().WithID(object.ID).WithBody(object)
	resp, err := Client(t).Objects.ObjectsPatch(params, nil)
	AssertRequestOk(t, resp, err, nil)
}

func DeleteClass(t *testing.T, class string) {
	delParams := schema.NewSchemaObjectsDeleteParams().WithClassName(class)
	delRes, err := Client(t).Schema.SchemaObjectsDelete(delParams, nil)
	AssertRequestOk(t, delRes, err, nil)
}

func DeleteObject(t *testing.T, object *models.Object) {
	params := objects.NewObjectsClassDeleteParams().
		WithClassName(object.Class).WithID(object.ID)
	resp, err := Client(t).Objects.ObjectsClassDelete(params, nil)
	AssertRequestOk(t, resp, err, nil)
}

func DeleteObjectsBatch(t *testing.T, body *models.BatchDelete) {
	params := batch.NewBatchObjectsDeleteParams().WithBody(body)
	resp, err := Client(t).Batch.BatchObjectsDelete(params, nil)
	AssertRequestOk(t, resp, err, nil)
}

func DeleteTenantObjectsBatch(t *testing.T, body *models.BatchDelete,
	tenant string,
) (*models.BatchDeleteResponse, error) {
	params := batch.NewBatchObjectsDeleteParams().
		WithBody(body).WithTenant(&tenant)
	resp, err := Client(t).Batch.BatchObjectsDelete(params, nil)
	if err != nil {
		return nil, err
	}
	return resp.Payload, nil
}

func AddReferences(t *testing.T, refs []*models.BatchReference) ([]*models.BatchReferenceResponse, error) {
	params := batch.NewBatchReferencesCreateParams().WithBody(refs)
	resp, err := Client(t).Batch.BatchReferencesCreate(params, nil)
	if err != nil {
		return nil, err
	}
	return resp.Payload, nil
}

func CheckReferencesBatchResponse(t *testing.T, resp []*models.BatchReferenceResponse, err error) {
	AssertRequestOk(t, resp, err, nil)
	for _, elem := range resp {
		if !assert.Nil(t, elem.Result.Errors) {
			t.Logf("expected nil, got: %v",
				elem.Result.Errors.Error[0].Message)
		}
	}
}

func AddReference(t *testing.T, object *models.Object, ref *models.SingleRef, prop string) {
	params := objects.NewObjectsClassReferencesCreateParams().
		WithClassName(object.Class).WithID(object.ID).WithBody(ref).WithPropertyName(prop)
	resp, err := Client(t).Objects.ObjectsClassReferencesCreate(params, nil)
	AssertRequestOk(t, resp, err, nil)
}

func AddReferenceTenant(t *testing.T, object *models.Object, ref *models.SingleRef, prop string, tenant string) {
	params := objects.NewObjectsClassReferencesCreateParams().
		WithClassName(object.Class).WithID(object.ID).WithBody(ref).WithPropertyName(prop).WithTenant(&tenant)
	resp, err := Client(t).Objects.ObjectsClassReferencesCreate(params, nil)
	AssertRequestOk(t, resp, err, nil)
}

func DeleteReference(t *testing.T, object *models.Object, ref *models.SingleRef, prop string) {
	params := objects.NewObjectsClassReferencesDeleteParams().
		WithClassName(object.Class).WithID(object.ID).WithBody(ref).WithPropertyName(prop)
	resp, err := Client(t).Objects.ObjectsClassReferencesDelete(params, nil)
	AssertRequestOk(t, resp, err, nil)
}

func DeleteReferenceTenant(t *testing.T, object *models.Object, ref *models.SingleRef, prop string, tenant string) {
	params := objects.NewObjectsClassReferencesDeleteParams().
		WithClassName(object.Class).WithID(object.ID).WithBody(ref).WithPropertyName(prop).WithTenant(&tenant)
	resp, err := Client(t).Objects.ObjectsClassReferencesDelete(params, nil)
	AssertRequestOk(t, resp, err, nil)
}

func UpdateReferenceTenant(t *testing.T, object *models.Object, ref models.MultipleRef, prop string, tenant string) {
	params := objects.NewObjectsClassReferencesPutParams().
		WithClassName(object.Class).WithID(object.ID).WithBody(ref).WithPropertyName(prop).WithTenant(&tenant)
	resp, err := Client(t).Objects.ObjectsClassReferencesPut(params, nil)
	AssertRequestOk(t, resp, err, nil)
}

func CreateTenants(t *testing.T, class string, tenants []*models.Tenant) {
	params := schema.NewTenantsCreateParams().WithClassName(class).WithBody(tenants)
	resp, err := Client(t).Schema.TenantsCreate(params, nil)
	AssertRequestOk(t, resp, err, nil)
}

func CreateTenantsReturnError(t *testing.T, class string, tenants []*models.Tenant) error {
	params := schema.NewTenantsCreateParams().WithClassName(class).WithBody(tenants)
	_, err := Client(t).Schema.TenantsCreate(params, nil)
	return err
}

func GetTenants(t *testing.T, class string) (*schema.TenantsGetOK, error) {
	params := schema.NewTenantsGetParams().WithClassName(class)
	resp, err := Client(t).Schema.TenantsGet(params, nil)
	return resp, err
}

func DeleteTenants(t *testing.T, class string, tenants []string) error {
	params := schema.NewTenantsDeleteParams().WithClassName(class).WithTenants(tenants)
	_, err := Client(t).Schema.TenantsDelete(params, nil)
	return err
}

func NewBeacon(className string, id strfmt.UUID) strfmt.URI {
	return crossref.New("localhost", className, id).SingleRef().Beacon
}

func GetMeta(t *testing.T) *models.Meta {
	params := meta.NewMetaGetParams()
	resp, err := Client(t).Meta.MetaGet(params, nil)
	AssertRequestOk(t, resp, err, nil)
	return resp.Payload
}

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
	"net/http"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/weaviate/weaviate/client/objects"
	"github.com/weaviate/weaviate/client/schema"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/replica"
)

func AssertCreateObject(t *testing.T, className string, schema map[string]interface{}) strfmt.UUID {
	t.Helper()
	params := objects.NewObjectsCreateParams().WithBody(
		&models.Object{
			Class:      className,
			Properties: schema,
		})

	resp, err := Client(t).Objects.ObjectsCreate(params, nil)

	var objectID strfmt.UUID

	// Ensure that the response is OK
	AssertRequestOk(t, resp, err, func() {
		objectID = resp.Payload.ID
	})

	return objectID
}

func AssertGetObject(t *testing.T, class string, uuid strfmt.UUID, include ...string) *models.Object {
	t.Helper()
	obj, err := GetObject(t, class, uuid, include...)
	AssertRequestOk(t, obj, err, nil)
	return obj
}

func AssertGetObjectEventually(t *testing.T, class string, uuid strfmt.UUID) *models.Object {
	var (
		resp *objects.ObjectsClassGetOK
		err  error
	)

	checkThunk := func() interface{} {
		resp, err = Client(t).Objects.ObjectsClassGet(objects.NewObjectsClassGetParams().WithClassName(class).WithID(uuid), nil)
		return err == nil
	}

	AssertEventuallyEqual(t, true, checkThunk)

	var object *models.Object

	AssertRequestOk(t, resp, err, func() {
		object = resp.Payload
	})

	return object
}

func AssertGetObjectFailsEventually(t *testing.T, class string, uuid strfmt.UUID) error {
	var err error

	checkThunk := func() interface{} {
		_, err = Client(t).Objects.ObjectsClassGet(objects.NewObjectsClassGetParams().WithClassName(class).WithID(uuid), nil)
		return err != nil
	}

	AssertEventuallyEqual(t, true, checkThunk)

	return err
}

func AssertCreateObjectClass(t *testing.T, class *models.Class) {
	t.Helper()
	params := schema.NewSchemaObjectsCreateParams().WithObjectClass(class)
	resp, err := Client(t).Schema.SchemaObjectsCreate(params, nil)
	AssertRequestOk(t, resp, err, nil)
}

func AssertDeleteObjectClass(t *testing.T, class string) {
	delRes, err := DeleteClassObject(t, class)
	AssertRequestOk(t, delRes, err, nil)
}

func GetObject(t *testing.T, class string, uuid strfmt.UUID, include ...string) (*models.Object, error) {
	req := objects.NewObjectsClassGetParams().WithID(uuid)
	if class != "" {
		req.WithClassName(class)
	}
	if len(include) > 0 {
		req.WithInclude(&include[0])
	}
	getResp, err := Client(t).Objects.ObjectsClassGet(req, nil)
	if err != nil {
		return nil, err
	}
	return getResp.Payload, nil
}

func TenantObject(t *testing.T, class string, id strfmt.UUID, tenant string) (*models.Object, error) {
	req := objects.NewObjectsClassGetParams().
		WithClassName(class).WithID(id).WithTenant(&tenant)
	getResp, err := Client(t).Objects.ObjectsClassGet(req, nil)
	if err != nil {
		return nil, err
	}
	return getResp.Payload, nil
}

func TenantObjectWithInclude(t *testing.T, class string, id strfmt.UUID, tenant string, includes string) (*models.Object, error) {
	req := objects.NewObjectsClassGetParams().
		WithClassName(class).WithID(id).WithTenant(&tenant).WithInclude(&includes)
	getResp, err := Client(t).Objects.ObjectsClassGet(req, nil)
	if err != nil {
		return nil, err
	}
	return getResp.Payload, nil
}

func GetObjectCL(t *testing.T, class string, uuid strfmt.UUID,
	cl replica.ConsistencyLevel, include ...string,
) (*models.Object, error) {
	req := objects.NewObjectsClassGetParams().WithID(uuid)
	if class != "" {
		req.WithClassName(class)
	}
	if len(include) > 0 {
		req.WithInclude(&include[0])
	}
	cls := string(cl)
	req.ConsistencyLevel = &cls
	getResp, err := Client(t).Objects.ObjectsClassGet(req, nil)
	if err != nil {
		return nil, err
	}
	return getResp.Payload, nil
}

func ObjectExistsCL(t *testing.T, class string, id strfmt.UUID, cl replica.ConsistencyLevel) (bool, error) {
	cls := string(cl)
	req := objects.NewObjectsClassHeadParams().
		WithClassName(class).WithID(id).WithConsistencyLevel(&cls)
	resp, err := Client(t).Objects.ObjectsClassHead(req, nil)
	if err != nil {
		return false, err
	}
	return resp.IsCode(http.StatusNoContent), nil
}

func TenantObjectExists(t *testing.T, class string, id strfmt.UUID, tenant string) (bool, error) {
	req := objects.NewObjectsClassHeadParams().
		WithClassName(class).WithID(id).WithTenant(&tenant)
	resp, err := Client(t).Objects.ObjectsClassHead(req, nil)
	if err != nil {
		return false, err
	}
	return resp.IsCode(http.StatusNoContent), nil
}

func GetObjectFromNode(t *testing.T, class string, uuid strfmt.UUID, nodename string) (*models.Object, error) {
	req := objects.NewObjectsClassGetParams().WithID(uuid)
	if class != "" {
		req.WithClassName(class)
	}
	if nodename != "" {
		req.WithNodeName(&nodename)
	}
	getResp, err := Client(t).Objects.ObjectsClassGet(req, nil)
	if err != nil {
		return nil, err
	}
	return getResp.Payload, nil
}

func GetTenantObjectFromNode(t *testing.T, class string, uuid strfmt.UUID, nodename, tenant string) (*models.Object, error) {
	req := objects.NewObjectsClassGetParams().WithID(uuid).
		WithClassName(class).
		WithNodeName(&nodename).
		WithTenant(&tenant)
	getResp, err := Client(t).Objects.ObjectsClassGet(req, nil)
	if err != nil {
		return nil, err
	}
	return getResp.Payload, nil
}

func DeleteClassObject(t *testing.T, class string) (*schema.SchemaObjectsDeleteOK, error) {
	delParams := schema.NewSchemaObjectsDeleteParams().WithClassName(class)
	return Client(t).Schema.SchemaObjectsDelete(delParams, nil)
}

func DeleteTenantObject(t *testing.T, class string, id strfmt.UUID, tenant string) {
	params := objects.NewObjectsClassDeleteParams().
		WithClassName(class).WithID(id).WithTenant(&tenant)
	resp, err := Client(t).Objects.ObjectsClassDelete(params, nil)
	AssertRequestOk(t, resp, err, nil)
}

func ListObjects(t *testing.T, class string) (*models.ObjectsListResponse, error) {
	params := objects.NewObjectsListParams()
	if class != "" {
		params.WithClass(&class)
	}

	resp, err := Client(t).Objects.ObjectsList(params, nil)
	if err != nil {
		return nil, err
	}
	return resp.Payload, nil
}

func TenantListObjects(t *testing.T, class string, tenant string) (*models.ObjectsListResponse, error) {
	params := objects.NewObjectsListParams().WithTenant(&tenant)
	if class != "" {
		params.WithClass(&class)
	}

	resp, err := Client(t).Objects.ObjectsList(params, nil)
	if err != nil {
		return nil, err
	}
	return resp.Payload, nil
}

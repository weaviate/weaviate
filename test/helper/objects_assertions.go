//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package helper

import (
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/client/objects"
	"github.com/semi-technologies/weaviate/client/schema"
	"github.com/semi-technologies/weaviate/entities/models"
)

func AssertCreateObject(t *testing.T, className string, schema map[string]interface{}) strfmt.UUID {
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

func DeleteClassObject(t *testing.T, class string) (*schema.SchemaObjectsDeleteOK, error) {
	delParams := schema.NewSchemaObjectsDeleteParams().WithClassName(class)
	return Client(t).Schema.SchemaObjectsDelete(delParams, nil)
}

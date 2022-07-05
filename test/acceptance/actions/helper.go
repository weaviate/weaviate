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

package test

import (
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/client/objects"
	"github.com/semi-technologies/weaviate/client/schema"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/test/acceptance/helper"
	testhelper "github.com/semi-technologies/weaviate/test/helper"
)

func assertCreateObject(t *testing.T, className string, schema map[string]interface{}) strfmt.UUID {
	params := objects.NewObjectsCreateParams().WithBody(
		&models.Object{
			Class:      className,
			Properties: schema,
		})

	resp, err := helper.Client(t).Objects.ObjectsCreate(params, nil)

	var objectID strfmt.UUID

	// Ensure that the response is OK
	helper.AssertRequestOk(t, resp, err, func() {
		objectID = resp.Payload.ID
	})

	return objectID
}

func assertGetObject(t *testing.T, class string, uuid strfmt.UUID) *models.Object {
	obj, err := getObject(t, class, uuid)
	helper.AssertRequestOk(t, obj, err, nil)
	return obj
}

func assertGetObjectEventually(t *testing.T, class string, uuid strfmt.UUID) *models.Object {
	var (
		resp *objects.ObjectsGetOK
		err  error
	)

	checkThunk := func() interface{} {
		resp, err = helper.Client(t).Objects.ObjectsGet(objects.NewObjectsGetParams().WithID(uuid), nil)
		return err == nil
	}

	testhelper.AssertEventuallyEqual(t, true, checkThunk)

	var object *models.Object

	helper.AssertRequestOk(t, resp, err, func() {
		object = resp.Payload
	})

	return object
}

func assertGetObjectFailsEventually(t *testing.T, uuid strfmt.UUID) error {
	var err error

	checkThunk := func() interface{} {
		_, err = helper.Client(t).Objects.ObjectsGet(objects.NewObjectsGetParams().WithID(uuid), nil)
		return err != nil
	}

	testhelper.AssertEventuallyEqual(t, true, checkThunk)

	return err
}

func assertCreateObjectClass(t *testing.T, class *models.Class) {
	t.Helper()
	params := schema.NewSchemaObjectsCreateParams().WithObjectClass(class)
	resp, err := helper.Client(t).Schema.SchemaObjectsCreate(params, nil)
	helper.AssertRequestOk(t, resp, err, nil)
}

func assertDeleteObjectClass(t *testing.T, class string) {
	delRes, err := deleteClassObject(t, class)
	helper.AssertRequestOk(t, delRes, err, nil)
}

func getObject(t *testing.T, class string, uuid strfmt.UUID) (*models.Object, error) {
	req := objects.NewObjectsClassGetParams().WithID(uuid)
	if class != "" {
		req.WithClassName(class)
	}
	getResp, err := helper.Client(t).Objects.ObjectsClassGet(req, nil)
	if err != nil {
		return nil, err
	}
	return getResp.Payload, nil
}

func deleteClassObject(t *testing.T, class string) (*schema.SchemaObjectsDeleteOK, error) {
	delParams := schema.NewSchemaObjectsDeleteParams().WithClassName(class)
	return helper.Client(t).Schema.SchemaObjectsDelete(delParams, nil)
}

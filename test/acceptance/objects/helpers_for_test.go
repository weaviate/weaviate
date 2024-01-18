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

package test

import (
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/weaviate/weaviate/client/objects"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/helper"
	testhelper "github.com/weaviate/weaviate/test/helper"
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

func assertCreateObjectWithID(t *testing.T, className, tenant string, id strfmt.UUID, schema map[string]interface{}) {
	params := objects.NewObjectsCreateParams().WithBody(
		&models.Object{
			ID:         id,
			Class:      className,
			Properties: schema,
			Tenant:     tenant,
		})

	resp, err := helper.Client(t).Objects.ObjectsCreate(params, nil)

	// Ensure that the response is OK
	helper.AssertRequestOk(t, resp, err, nil)
}

func assertGetObject(t *testing.T, uuid strfmt.UUID) *models.Object {
	getResp, err := helper.Client(t).Objects.ObjectsGet(objects.NewObjectsGetParams().WithID(uuid), nil)

	var object *models.Object

	helper.AssertRequestOk(t, getResp, err, func() {
		object = getResp.Payload
	})

	return object
}

func assertGetObjectWithClass(t *testing.T, uuid strfmt.UUID, class string) *models.Object {
	getResp, err := helper.Client(t).Objects.ObjectsClassGet(objects.NewObjectsClassGetParams().WithID(uuid).WithClassName(class), nil)

	var object *models.Object

	helper.AssertRequestOk(t, getResp, err, func() {
		object = getResp.Payload
	})

	return object
}

func assertGetObjectEventually(t *testing.T, uuid strfmt.UUID) *models.Object {
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

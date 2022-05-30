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

func assertGetObject(t *testing.T, uuid strfmt.UUID) *models.Object {
	getResp, err := helper.Client(t).Objects.ObjectsGet(objects.NewObjectsGetParams().WithID(uuid), nil)

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

func assertGetObjectFailsEventually(t *testing.T, uuid strfmt.UUID) error {
	var err error

	checkThunk := func() interface{} {
		_, err = helper.Client(t).Objects.ObjectsGet(objects.NewObjectsGetParams().WithID(uuid), nil)
		return err != nil
	}

	testhelper.AssertEventuallyEqual(t, true, checkThunk)

	return err
}

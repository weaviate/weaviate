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

// Acceptance tests for objects

import (
	"testing"

	"github.com/semi-technologies/weaviate/client/objects"
	"github.com/semi-technologies/weaviate/test/helper"
)

func removingObjects(t *testing.T) {
	objectId := helper.AssertCreateObject(t, "TestObject", map[string]interface{}{})

	// Yes, it's created
	_ = helper.AssertGetObjectEventually(t, "TestObject", objectId)

	// Now perorm the the deletion
	delResp, err := helper.Client(t).Objects.ObjectsDelete(objects.NewObjectsDeleteParams().WithID(objectId), nil)
	helper.AssertRequestOk(t, delResp, err, nil)

	_ = helper.AssertGetObjectFailsEventually(t, objectId)

	// And verify that the object is gone
	getResp, err := helper.Client(t).Objects.ObjectsGet(objects.NewObjectsGetParams().WithID(objectId), nil)
	helper.AssertRequestFail(t, getResp, err, nil)
}

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

// Acceptance tests for things.

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/client/objects"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/helper"
)

// Test that we can properly check object's existence.
// Create two objects, and check that those objects exist.
// Also check one non existent object that it doesn't exist
// This test is run by setup_test.go
func checkObjects(t *testing.T) {
	params1 := objects.NewObjectsCreateParams().WithBody(
		&models.Object{
			Class:      "TestObject",
			Properties: map[string]interface{}{},
		})
	resp1, err := helper.Client(t).Objects.ObjectsCreate(params1, nil)
	require.Nil(t, err, "creation should succeed")
	object1ID := resp1.Payload.ID

	params2 := objects.NewObjectsCreateParams().WithBody(
		&models.Object{
			Class:      "TestObject",
			Properties: map[string]interface{}{},
		})
	resp2, err := helper.Client(t).Objects.ObjectsCreate(params2, nil)
	assert.Nil(t, err, "creation should succeed")
	object2ID := resp2.Payload.ID

	// wait for both Objects to be indexed
	assertGetObjectEventually(t, object1ID)
	assertGetObjectEventually(t, object2ID)

	headParams := objects.NewObjectsHeadParams().WithID(object1ID)
	resp, err := helper.Client(t).Objects.ObjectsHead(headParams, nil)

	require.Nil(t, err, "should not error")
	assert.True(t, resp != nil, "Did not find object 1")

	headParams = objects.NewObjectsHeadParams().WithID("non-existent-object")
	resp, err = helper.Client(t).Objects.ObjectsHead(headParams, nil)

	require.NotNil(t, err, "should error")
	assert.True(t, resp == nil, "Did find non existent object")

	headParams = objects.NewObjectsHeadParams().WithID(object2ID)
	resp, err = helper.Client(t).Objects.ObjectsHead(headParams, nil)

	require.Nil(t, err, "should not error")
	assert.True(t, resp != nil, "Did not find object 2")
}

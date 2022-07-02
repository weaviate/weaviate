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

// Acceptance tests for things.

import (
	"testing"

	"github.com/semi-technologies/weaviate/client/objects"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/test/acceptance/helper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test that we can properly list objects.
// Create two objects, and check that the list all contains them all.
// This test is run by setup_test.go
func listingObjects(t *testing.T) {
	params1 := objects.NewObjectsCreateParams().WithBody(
		&models.Object{
			Class: "TestObject",
			Properties: map[string]interface{}{
				"testString": "1",
			},
		})
	resp1, err := helper.Client(t).Objects.ObjectsCreate(params1, nil)
	require.Nil(t, err, "creation should succeed")
	object1ID := resp1.Payload.ID

	params2 := objects.NewObjectsCreateParams().WithBody(
		&models.Object{
			Class: "TestObject",
			Properties: map[string]interface{}{
				"testString": "2",
			},
		})
	resp2, err := helper.Client(t).Objects.ObjectsCreate(params2, nil)
	assert.Nil(t, err, "creation should succeed")
	object2ID := resp2.Payload.ID

	// wait for both Objects to be indexed
	assertGetObjectEventually(t, object1ID)
	assertGetObjectEventually(t, object2ID)

	listParams := objects.NewObjectsListParams()
	resp, err := helper.Client(t).Objects.ObjectsList(listParams, nil)
	require.Nil(t, err, "should not error")

	found1 := false
	found2 := false

	for _, object := range resp.Payload.Objects {
		if object.ID == resp1.Payload.ID {
			assert.False(t, found1, "found double ID for object 1!")
			found1 = true
		}

		if object.ID == resp2.Payload.ID {
			assert.False(t, found2, "found double ID for object 2!")
			found2 = true
		}
	}

	assert.True(t, found1, "Did not find object 1")
	assert.True(t, found2, "Did not find object 2")
}

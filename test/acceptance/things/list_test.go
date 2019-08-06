//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2019 Weaviate. All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

package test

// Acceptance tests for things.

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/semi-technologies/weaviate/client/things"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/test/acceptance/helper"
)

// Test that we can properly list things.
// Create two things, and check that the list all contains them all.
func TestListAll(t *testing.T) {
	t.Parallel()

	params1 := things.NewThingsCreateParams().WithBody(
		&models.Thing{
			Class:  "TestThing",
			Schema: map[string]interface{}{},
		})
	resp1, err := helper.Client(t).Things.ThingsCreate(params1, nil)
	assert.Nil(t, err, "creation should succeed")
	thing1ID := resp1.Payload.ID

	params2 := things.NewThingsCreateParams().WithBody(
		&models.Thing{
			Class:  "TestThing",
			Schema: map[string]interface{}{},
		})
	resp2, err := helper.Client(t).Things.ThingsCreate(params2, nil)
	assert.Nil(t, err, "creation should succeed")
	thing2ID := resp2.Payload.ID

	// wait for both things to be indexed
	assertGetThingEventually(t, thing1ID)
	assertGetThingEventually(t, thing2ID)

	listParams := things.NewThingsListParams()
	resp, err := helper.Client(t).Things.ThingsList(listParams, nil)
	require.Nil(t, err, "should not error")

	found1 := false
	found2 := false

	for _, thing := range resp.Payload.Things {
		if thing.ID == resp1.Payload.ID {
			assert.False(t, found1, "found double ID for thing 1!")
			found1 = true
		}

		if thing.ID == resp2.Payload.ID {
			assert.False(t, found2, "found double ID for thing 2!")
			found2 = true
		}
	}

	assert.True(t, found1, "Did not find thing 1")
	assert.True(t, found2, "Did not find thing 2")
}

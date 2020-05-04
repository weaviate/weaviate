//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Holding B.V. (registered @ Dutch Chamber of Commerce no 75221632). All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

package test

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/client/schema"
	"github.com/semi-technologies/weaviate/client/things"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/test/acceptance/helper"
	"github.com/stretchr/testify/assert"
)

// This aimes to prevent a regression on
// https://github.com/semi-technologies/weaviate/issues/1016
// The issue was that rapid POST .../references/... request in succesion would
// overwrite each other due to the eventual consistency nature of the used
// backend (esvector). This bug is considered fix if n items can be rapidly
// added and a subsequent GET request of the source resource contains all
// previously added references.
func Test_RapidlyAddingReferences(t *testing.T) {
	sourceClass := "SequenceReferenceTestSource"
	targetClass := "SequenceReferenceTestTarget"

	sourceID := strfmt.UUID("96ce03ca-58ed-48e1-a0f1-51f63fa9aa12")

	targetIDs := []strfmt.UUID{
		"ce1a4756-b7ce-44fa-b079-45a7ec400882",
		"e1edb4ff-570c-4f0b-a1a1-18af118369aa",
		"25d22c70-3df0-4e5c-b8c1-a88d4d2771ef",
		"6f2a0708-3e8e-4a68-9763-26c465d8bf83",
		"c4dfae47-ebcf-4808-9122-1c67898ec140",
		"754bd925-1900-4f93-9f5d-27631eb618bb",
		"babba820-e3f5-4e8d-a354-76f2cb13fdba",
		"270942da-1999-40cd-a580-a91aa144b6c0",
		"a7a06618-6d50-4654-be75-2c9f639a6368",
		"47ba1d2b-6b8c-4b3b-92a8-46574a069ae8",
	}

	t.Run("adding the required schema", func(t *testing.T) {
		t.Run("target class", func(t *testing.T) {
			params := schema.NewSchemaThingsCreateParams().WithThingClass(
				&models.Class{
					Class: targetClass,
					Properties: []*models.Property{
						&models.Property{
							DataType: []string{"string"},
							Name:     "name",
						},
					},
				},
			)
			resp, err := helper.Client(t).Schema.SchemaThingsCreate(params, nil)
			helper.AssertRequestOk(t, resp, err, nil)
		})

		t.Run("source class", func(t *testing.T) {
			cardinalityMany := "many"
			params := schema.NewSchemaThingsCreateParams().WithThingClass(
				&models.Class{
					Class: sourceClass,
					Properties: []*models.Property{
						&models.Property{
							DataType:    []string{targetClass},
							Name:        "toTarget",
							Cardinality: &cardinalityMany,
						},
						&models.Property{
							DataType: []string{"string"},
							Name:     "name",
						},
					},
				},
			)
			resp, err := helper.Client(t).Schema.SchemaThingsCreate(params, nil)
			helper.AssertRequestOk(t, resp, err, nil)
		})
	})

	t.Run("adding all objects (without referencing)", func(t *testing.T) {
		t.Run("source object", func(t *testing.T) {
			assertCreateThingWithID(t, sourceClass, sourceID, map[string]interface{}{
				"name": "Source Object",
			})
		})

		t.Run("target objects", func(t *testing.T) {
			for i, id := range targetIDs {
				assertCreateThingWithID(t, targetClass, id, map[string]interface{}{
					"name": fmt.Sprintf("target object %d", i),
				})
			}
		})
	})

	t.Run("waiting for the last added object to be present", func(t *testing.T) {
		assertGetThingEventually(t, targetIDs[len(targetIDs)-1])
	})

	t.Run("placing all references in succession", func(t *testing.T) {
		for _, id := range targetIDs {
			params := things.NewThingsReferencesCreateParams().
				WithID(sourceID).
				WithPropertyName("toTarget").
				WithBody(
					&models.SingleRef{
						Beacon: strfmt.URI(fmt.Sprintf("weaviate://localhost/things/%s", id)),
					},
				)

			res, err := helper.Client(t).Things.ThingsReferencesCreate(params, nil)
			helper.AssertRequestOk(t, res, err, nil)
		}
	})

	// wait for index refresh
	time.Sleep(2 * time.Second) // TODO: improve through polling

	t.Run("checking which refs were set", func(t *testing.T) {
		source := assertGetThing(t, sourceID)

		var foundIDs []strfmt.UUID
		// extract IDs
		for _, ref := range source.Schema.(map[string]interface{})["toTarget"].([]interface{}) {
			beacon := ref.(map[string]interface{})["beacon"].(string)
			chunks := strings.Split(beacon, "/")
			foundIDs = append(foundIDs, strfmt.UUID(chunks[len(chunks)-1]))
		}

		assert.ElementsMatch(t, targetIDs, foundIDs)
	})

	// cleanup
	helper.Client(t).Schema.SchemaThingsDelete(
		schema.NewSchemaThingsDeleteParams().WithClassName(sourceClass), nil)
	helper.Client(t).Schema.SchemaThingsDelete(
		schema.NewSchemaThingsDeleteParams().WithClassName(targetClass), nil)

}

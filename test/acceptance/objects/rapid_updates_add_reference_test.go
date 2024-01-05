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
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/client/objects"
	clschema "github.com/weaviate/weaviate/client/schema"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/test/helper"
)

// This aims to prevent a regression on
// https://github.com/weaviate/weaviate/issues/1016
// The issue was that rapid POST .../references/... request in succession would
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
			params := clschema.NewSchemaObjectsCreateParams().WithObjectClass(
				&models.Class{
					Class: targetClass,
					Properties: []*models.Property{
						{
							DataType:     schema.DataTypeText.PropString(),
							Tokenization: models.PropertyTokenizationWhitespace,
							Name:         "name",
						},
					},
				},
			)
			resp, err := helper.Client(t).Schema.SchemaObjectsCreate(params, nil)
			helper.AssertRequestOk(t, resp, err, nil)
		})

		t.Run("source class", func(t *testing.T) {
			params := clschema.NewSchemaObjectsCreateParams().WithObjectClass(
				&models.Class{
					Class: sourceClass,
					Properties: []*models.Property{
						{
							DataType: []string{targetClass},
							Name:     "toTarget",
						},
						{
							DataType:     schema.DataTypeText.PropString(),
							Tokenization: models.PropertyTokenizationWhitespace,
							Name:         "name",
						},
					},
				},
			)
			resp, err := helper.Client(t).Schema.SchemaObjectsCreate(params, nil)
			helper.AssertRequestOk(t, resp, err, nil)
		})
	})

	t.Run("adding all objects (without referencing)", func(t *testing.T) {
		t.Run("source object", func(t *testing.T) {
			assertCreateObjectWithID(t, sourceClass, "", sourceID, map[string]interface{}{
				"name": "Source Object",
			})
		})

		t.Run("target objects", func(t *testing.T) {
			for i, id := range targetIDs {
				assertCreateObjectWithID(t, targetClass, "", id, map[string]interface{}{
					"name": fmt.Sprintf("target object %d", i),
				})
			}
		})
	})

	t.Run("waiting for the last added object to be present", func(t *testing.T) {
		assertGetObjectEventually(t, targetIDs[len(targetIDs)-1])
	})

	t.Run("placing all references in succession", func(t *testing.T) {
		for _, id := range targetIDs {
			params := objects.NewObjectsReferencesCreateParams().
				WithID(sourceID).
				WithPropertyName("toTarget").
				WithBody(
					&models.SingleRef{
						Beacon: strfmt.URI(fmt.Sprintf("weaviate://localhost/%s", id)),
					},
				)

			res, err := helper.Client(t).Objects.ObjectsReferencesCreate(params, nil)
			helper.AssertRequestOk(t, res, err, nil)
		}
	})

	// wait for index refresh
	time.Sleep(2 * time.Second) // TODO: improve through polling

	t.Run("checking which refs were set", func(t *testing.T) {
		source := assertGetObject(t, sourceID)

		var foundIDs []strfmt.UUID
		// extract IDs
		for _, ref := range source.Properties.(map[string]interface{})["toTarget"].([]interface{}) {
			beacon := ref.(map[string]interface{})["beacon"].(string)
			chunks := strings.Split(beacon, "/")
			foundIDs = append(foundIDs, strfmt.UUID(chunks[len(chunks)-1]))
		}

		assert.ElementsMatch(t, targetIDs, foundIDs)
	})

	// cleanup
	helper.Client(t).Schema.SchemaObjectsDelete(
		clschema.NewSchemaObjectsDeleteParams().WithClassName(sourceClass), nil)
	helper.Client(t).Schema.SchemaObjectsDelete(
		clschema.NewSchemaObjectsDeleteParams().WithClassName(targetClass), nil)
}

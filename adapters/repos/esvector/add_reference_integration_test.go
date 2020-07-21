//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

// +build integrationTest

package esvector

import (
	"context"
	"fmt"
	"testing"

	"github.com/elastic/go-elasticsearch/v5"
	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
	"github.com/semi-technologies/weaviate/usecases/traverser"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// This test suite does not care about caching and other side offects of adding
// a ref. This is only a mechanism of adding refs one-by-one without
// overwriting previous writes. See also bug
// https://github.com/semi-technologies/weaviate/issues/1016 for more details

func Test_AddingReferenceOneByOne(t *testing.T) {

	client, err := elasticsearch.NewClient(elasticsearch.Config{
		Addresses: []string{"http://localhost:9201"},
	})
	require.Nil(t, err)
	schema := schema.Schema{
		Things: &models.Schema{
			Classes: []*models.Class{
				&models.Class{
					Class: "AddingReferencesTestTarget",
					Properties: []*models.Property{
						&models.Property{
							Name:     "name",
							DataType: []string{"string"},
						},
					},
				},
				&models.Class{
					Class: "AddingReferencesTestSource",
					Properties: []*models.Property{
						&models.Property{
							Name:     "name",
							DataType: []string{"string"},
						},
						&models.Property{
							Name:     "toTarget",
							DataType: []string{"AddingReferencesTestTarget"},
						},
					},
				},
			},
		},
	}
	schemaGetter := &fakeSchemaGetter{schema: schema}
	logger := logrus.New()
	repo := NewRepo(client, logger, schemaGetter, 1, "0-1")
	waitForEsToBeReady(t, repo)
	migrator := NewMigrator(repo)

	t.Run("add required classes", func(t *testing.T) {
		for _, class := range schema.Things.Classes {
			t.Run(fmt.Sprintf("add %s", class.Class), func(t *testing.T) {
				err := migrator.AddClass(context.Background(), kind.Thing, class)
				require.Nil(t, err)
			})
		}
	})

	targetID := strfmt.UUID("a4a92239-e748-4e55-bbbd-f606926619a7")
	target2ID := strfmt.UUID("325084e7-4faa-43a5-b2b1-56e207be169a")
	sourceID := strfmt.UUID("0826c61b-85c1-44ac-aebb-cfd07ace6a57")

	t.Run("add objects", func(t *testing.T) {
		err := repo.PutThing(context.Background(), &models.Thing{
			ID:    sourceID,
			Class: "AddingReferencesTestSource",
			Schema: map[string]interface{}{
				"name": "source item",
			},
		}, []float32{0.5})
		require.Nil(t, err)

		err = repo.PutThing(context.Background(), &models.Thing{
			ID:    targetID,
			Class: "AddingReferencesTestTarget",
			Schema: map[string]interface{}{
				"name": "target item",
			},
		}, []float32{0.5})

		err = repo.PutThing(context.Background(), &models.Thing{
			ID:    target2ID,
			Class: "AddingReferencesTestTarget",
			Schema: map[string]interface{}{
				"name": "another target item",
			},
		}, []float32{0.5})
		require.Nil(t, err)
	})

	refreshAll(t, client)

	t.Run("add reference between them", func(t *testing.T) {
		err := repo.AddReference(context.Background(), kind.Thing, sourceID, "toTarget", &models.SingleRef{
			Beacon: strfmt.URI(fmt.Sprintf("weaviate://localhost/things/%s", targetID)),
		})
		assert.Nil(t, err)
	})

	refreshAll(t, client)

	t.Run("check reference was added", func(t *testing.T) {
		source, err := repo.ThingByID(context.Background(), sourceID, nil, traverser.UnderscoreProperties{})
		require.Nil(t, err)

		refs := source.Thing().Schema.(map[string]interface{})["toTarget"]
		refsSlice, ok := refs.(models.MultipleRef)
		require.True(t, ok, fmt.Sprintf("toTarget must be models.MultipleRef, but got %#v", refs))

		foundBeacons := []string{}
		for _, ref := range refsSlice {
			foundBeacons = append(foundBeacons, ref.Beacon.String())
		}
		expectedBeacons := []string{
			fmt.Sprintf("weaviate://localhost/things/%s", targetID),
		}

		assert.ElementsMatch(t, foundBeacons, expectedBeacons)
	})

	t.Run("reference a second target", func(t *testing.T) {
		err := repo.AddReference(context.Background(), kind.Thing, sourceID, "toTarget", &models.SingleRef{
			Beacon: strfmt.URI(fmt.Sprintf("weaviate://localhost/things/%s", target2ID)),
		})
		assert.Nil(t, err)
	})

	refreshAll(t, client)

	t.Run("check both references are now present", func(t *testing.T) {
		source, err := repo.ThingByID(context.Background(), sourceID, nil, traverser.UnderscoreProperties{})
		require.Nil(t, err)

		refs := source.Thing().Schema.(map[string]interface{})["toTarget"]
		refsSlice, ok := refs.(models.MultipleRef)
		require.True(t, ok, fmt.Sprintf("toTarget must be models.MultipleRef, but got %#v", refs))

		foundBeacons := []string{}
		for _, ref := range refsSlice {
			foundBeacons = append(foundBeacons, ref.Beacon.String())
		}
		expectedBeacons := []string{
			fmt.Sprintf("weaviate://localhost/things/%s", targetID),
			fmt.Sprintf("weaviate://localhost/things/%s", target2ID),
		}

		assert.ElementsMatch(t, foundBeacons, expectedBeacons)
	})

}

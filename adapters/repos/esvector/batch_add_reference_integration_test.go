//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2019 SeMI Holding B.V. (registered @ Dutch Chamber of Commerce no 75221632). All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
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
	"github.com/semi-technologies/weaviate/entities/schema/crossref"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
	"github.com/semi-technologies/weaviate/usecases/kinds"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// This test suite does not care about caching and other side offects of adding
// a ref. This is only a mechanism of adding refs in a batch fashion as
// outlined in https://github.com/semi-technologies/weaviate/issues/1025 while
// making sure that we don't reintroduce the issues from
// https://github.com/semi-technologies/weaviate/issues/1016

func Test_AddingReferencesInBatches(t *testing.T) {

	client, err := elasticsearch.NewClient(elasticsearch.Config{
		Addresses: []string{"http://localhost:9201"},
	})
	require.Nil(t, err)
	schema := schema.Schema{
		Things: &models.Schema{
			Classes: []*models.Class{
				&models.Class{
					Class: "AddingBatchReferencesTestTarget",
					Properties: []*models.Property{
						&models.Property{
							Name:     "name",
							DataType: []string{"string"},
						},
					},
				},
				&models.Class{
					Class: "AddingBatchReferencesTestSource",
					Properties: []*models.Property{
						&models.Property{
							Name:     "name",
							DataType: []string{"string"},
						},
						&models.Property{
							Name:     "toTarget",
							DataType: []string{"AddingBatchReferencesTestTarget"},
						},
					},
				},
			},
		},
	}
	schemaGetter := &fakeSchemaGetter{schema: schema}
	logger := logrus.New()
	repo := NewRepo(client, logger, schemaGetter, 2, 100)
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

	target1 := strfmt.UUID("7b395e5c-cf4d-4297-b8cc-1d849a057de3")
	target2 := strfmt.UUID("8f9f54f3-a7db-415e-881a-0e6fb79a7ec7")
	target3 := strfmt.UUID("046251cf-cb02-4102-b854-c7c4691cf16f")
	target4 := strfmt.UUID("bc7d8875-3a24-4137-8203-e152096dea4f")
	sourceID := strfmt.UUID("a3c98a66-be4a-4eaf-8cf3-04648a11d0f7")

	t.Run("add objects", func(t *testing.T) {
		err := repo.PutThing(context.Background(), &models.Thing{
			ID:    sourceID,
			Class: "AddingBatchReferencesTestSource",
			Schema: map[string]interface{}{
				"name": "source item",
			},
		}, []float32{0.5})
		require.Nil(t, err)

		targets := []strfmt.UUID{target1, target2, target3, target4}

		for i, target := range targets {
			err = repo.PutThing(context.Background(), &models.Thing{
				ID:    target,
				Class: "AddingBatchReferencesTestTarget",
				Schema: map[string]interface{}{
					"name": fmt.Sprintf("target item %d", i),
				},
			}, []float32{0.5})
			require.Nil(t, err)
		}
	})

	refreshAll(t, client)

	t.Run("add reference between them - first batch", func(t *testing.T) {
		source, err := crossref.ParseSource(fmt.Sprintf(
			"weaviate://localhost/things/AddingBatchReferencesTestSource/%s/toTarget", sourceID))
		require.Nil(t, err)
		targets := []strfmt.UUID{target1, target2}
		refs := make(kinds.BatchReferences, len(targets), len(targets))
		for i, target := range targets {
			to, err := crossref.Parse(fmt.Sprintf("weaviate://localhost/things/%s", target))
			require.Nil(t, err)
			refs[i] = kinds.BatchReference{
				Err:  nil,
				From: source,
				To:   to,
			}
		}
		_, err = repo.AddBatchReferences(context.Background(), refs)
		assert.Nil(t, err)
	})

	t.Run("add reference between them - second batch", func(t *testing.T) {
		source, err := crossref.ParseSource(fmt.Sprintf(
			"weaviate://localhost/things/AddingBatchReferencesTestSource/%s/toTarget", sourceID))
		require.Nil(t, err)
		targets := []strfmt.UUID{target3, target4}
		refs := make(kinds.BatchReferences, len(targets), len(targets))
		for i, target := range targets {
			to, err := crossref.Parse(fmt.Sprintf("weaviate://localhost/things/%s", target))
			require.Nil(t, err)
			refs[i] = kinds.BatchReference{
				Err:  nil,
				From: source,
				To:   to,
			}
		}
		_, err = repo.AddBatchReferences(context.Background(), refs)
		assert.Nil(t, err)
	})

	refreshAll(t, client)

	t.Run("check all references are now present", func(t *testing.T) {
		source, err := repo.ThingByID(context.Background(), sourceID, nil, false)
		require.Nil(t, err)

		refs := source.Thing().Schema.(map[string]interface{})["toTarget"]
		refsSlice, ok := refs.(models.MultipleRef)
		require.True(t, ok, fmt.Sprintf("toTarget must be models.MultipleRef, but got %#v", refs))

		foundBeacons := []string{}
		for _, ref := range refsSlice {
			foundBeacons = append(foundBeacons, ref.Beacon.String())
		}
		expectedBeacons := []string{
			fmt.Sprintf("weaviate://localhost/things/%s", target1),
			fmt.Sprintf("weaviate://localhost/things/%s", target2),
			fmt.Sprintf("weaviate://localhost/things/%s", target3),
			fmt.Sprintf("weaviate://localhost/things/%s", target4),
		}

		assert.ElementsMatch(t, foundBeacons, expectedBeacons)
	})

}

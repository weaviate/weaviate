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

package db

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/schema/crossref"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
	"github.com/semi-technologies/weaviate/usecases/kinds"
	"github.com/semi-technologies/weaviate/usecases/traverser"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_AddingReferencesInBatches(t *testing.T) {

	rand.Seed(time.Now().UnixNano())
	dirName := fmt.Sprintf("./testdata/%d", rand.Intn(10000000))
	os.MkdirAll(dirName, 0777)
	defer func() {
		err := os.RemoveAll(dirName)
		fmt.Println(err)
	}()

	logger := logrus.New()
	schemaGetter := &fakeSchemaGetter{}
	repo := New(logger, Config{RootPath: dirName})
	repo.SetSchemaGetter(schemaGetter)
	err := repo.WaitForStartup(30 * time.Second)
	require.Nil(t, err)
	migrator := NewMigrator(repo)

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

	t.Run("add required classes", func(t *testing.T) {
		for _, class := range schema.Things.Classes {
			t.Run(fmt.Sprintf("add %s", class.Class), func(t *testing.T) {
				err := migrator.AddClass(context.Background(), kind.Thing, class)
				require.Nil(t, err)
			})
		}
	})
	schemaGetter.schema = schema

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

	t.Run("check all references are now present", func(t *testing.T) {
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
			fmt.Sprintf("weaviate://localhost/things/%s", target1),
			fmt.Sprintf("weaviate://localhost/things/%s", target2),
			fmt.Sprintf("weaviate://localhost/things/%s", target3),
			fmt.Sprintf("weaviate://localhost/things/%s", target4),
		}

		assert.ElementsMatch(t, foundBeacons, expectedBeacons)
	})

}

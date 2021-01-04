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
	"github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/schema/crossref"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
	"github.com/semi-technologies/weaviate/usecases/objects"
	"github.com/semi-technologies/weaviate/usecases/traverser"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_AddingReferencesInBatches(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	dirName := fmt.Sprintf("./testdata/%d", rand.Intn(10000000))
	os.MkdirAll(dirName, 0o777)
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
	migrator := NewMigrator(repo, logger)

	schema := schema.Schema{
		Objects: &models.Schema{
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
		for _, class := range schema.Objects.Classes {
			t.Run(fmt.Sprintf("add %s", class.Class), func(t *testing.T) {
				err := migrator.AddClass(context.Background(), kind.Object, class)
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
		err := repo.PutObject(context.Background(), &models.Object{
			ID:    sourceID,
			Class: "AddingBatchReferencesTestSource",
			Schema: map[string]interface{}{
				"name": "source item",
			},
		}, []float32{0.5})
		require.Nil(t, err)

		targets := []strfmt.UUID{target1, target2, target3, target4}

		for i, target := range targets {
			err = repo.PutObject(context.Background(), &models.Object{
				ID:    target,
				Class: "AddingBatchReferencesTestTarget",
				Schema: map[string]interface{}{
					"name": fmt.Sprintf("target item %d", i),
				},
			}, []float32{0.7})
			require.Nil(t, err)
		}
	})

	t.Run("add reference between them - first batch", func(t *testing.T) {
		source, err := crossref.ParseSource(fmt.Sprintf(
			"weaviate://localhost/AddingBatchReferencesTestSource/%s/toTarget",
			sourceID))
		require.Nil(t, err)
		targets := []strfmt.UUID{target1, target2}
		refs := make(objects.BatchReferences, len(targets), len(targets))
		for i, target := range targets {
			to, err := crossref.Parse(fmt.Sprintf("weaviate://localhost/%s",
				target))
			require.Nil(t, err)
			refs[i] = objects.BatchReference{
				Err:           nil,
				From:          source,
				To:            to,
				OriginalIndex: i,
			}
		}
		_, err = repo.AddBatchReferences(context.Background(), refs)
		assert.Nil(t, err)
	})

	t.Run("add reference between them - second batch", func(t *testing.T) {
		source, err := crossref.ParseSource(fmt.Sprintf(
			"weaviate://localhost/AddingBatchReferencesTestSource/%s/toTarget",
			sourceID))
		require.Nil(t, err)
		targets := []strfmt.UUID{target3, target4}
		refs := make(objects.BatchReferences, len(targets), len(targets))
		for i, target := range targets {
			to, err := crossref.Parse(fmt.Sprintf("weaviate://localhost/%s", target))
			require.Nil(t, err)
			refs[i] = objects.BatchReference{
				Err:           nil,
				From:          source,
				To:            to,
				OriginalIndex: i,
			}
		}
		_, err = repo.AddBatchReferences(context.Background(), refs)
		assert.Nil(t, err)
	})

	t.Run("check all references are now present", func(t *testing.T) {
		source, err := repo.ObjectByID(context.Background(), sourceID, nil, traverser.AdditionalProperties{})
		require.Nil(t, err)

		refs := source.Object().Schema.(map[string]interface{})["toTarget"]
		refsSlice, ok := refs.(models.MultipleRef)
		require.True(t, ok, fmt.Sprintf("toTarget must be models.MultipleRef, but got %#v", refs))

		foundBeacons := []string{}
		for _, ref := range refsSlice {
			foundBeacons = append(foundBeacons, ref.Beacon.String())
		}
		expectedBeacons := []string{
			fmt.Sprintf("weaviate://localhost/%s", target1),
			fmt.Sprintf("weaviate://localhost/%s", target2),
			fmt.Sprintf("weaviate://localhost/%s", target3),
			fmt.Sprintf("weaviate://localhost/%s", target4),
		}

		assert.ElementsMatch(t, foundBeacons, expectedBeacons)
	})

	t.Run("verify objects are still searchable through the vector index",
		func(t *testing.T) {
			// prior to making the inverted index and its docIDs immutable, a ref
			// update would not change the doc ID, therefore the batch reference
			// never had to interact with the vector index. Now that they're
			// immutable, the udpated doc ID needs to be "re-inserted" even if the
			// vector is still the same
			res, err := repo.VectorClassSearch(context.Background(), traverser.GetParams{
				Kind:         kind.Object,
				ClassName:    "AddingBatchReferencesTestSource",
				SearchVector: []float32{0.49},
				Pagination: &filters.Pagination{
					Limit: 1,
				},
			})

			require.Nil(t, err)
			require.Len(t, res, 1)
			assert.Equal(t, sourceID, res[0].ID)
		})
}

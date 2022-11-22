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

//go:build integrationTest
// +build integrationTest

package db

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/entities/additional"
	"github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/schema/crossref"
	enthnsw "github.com/semi-technologies/weaviate/entities/vectorindex/hnsw"
	"github.com/semi-technologies/weaviate/usecases/objects"
	"github.com/semi-technologies/weaviate/usecases/traverser"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_AddingReferencesInBatches(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	dirName := t.TempDir()

	logger := logrus.New()
	schemaGetter := &fakeSchemaGetter{shardState: singleShardState()}
	repo := New(logger, Config{
		FlushIdleAfter:            60,
		RootPath:                  dirName,
		QueryMaximumResults:       10000,
		MaxImportGoroutinesFactor: 1,
	}, &fakeRemoteClient{}, &fakeNodeResolver{}, &fakeRemoteNodeClient{}, &fakeReplicationClient{}, nil)
	repo.SetSchemaGetter(schemaGetter)
	err := repo.WaitForStartup(testCtx())

	defer repo.Shutdown(context.Background())

	require.Nil(t, err)
	migrator := NewMigrator(repo, logger)

	s := schema.Schema{
		Objects: &models.Schema{
			Classes: []*models.Class{
				{
					VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
					InvertedIndexConfig: invertedConfig(),
					Class:               "AddingBatchReferencesTestTarget",
					Properties: []*models.Property{
						{
							Name:         "name",
							DataType:     []string{"string"},
							Tokenization: "word",
						},
					},
				},
				{
					VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
					InvertedIndexConfig: invertedConfig(),
					Class:               "AddingBatchReferencesTestSource",
					Properties: []*models.Property{
						{
							Name:         "name",
							DataType:     []string{"string"},
							Tokenization: "word",
						},
						{
							Name:     "toTarget",
							DataType: []string{"AddingBatchReferencesTestTarget"},
						},
					},
				},
			},
		},
	}

	t.Run("add required classes", func(t *testing.T) {
		for _, class := range s.Objects.Classes {
			t.Run(fmt.Sprintf("add %s", class.Class), func(t *testing.T) {
				err := migrator.AddClass(context.Background(), class, schemaGetter.shardState)
				require.Nil(t, err)
			})
		}
	})
	schemaGetter.schema = s

	target1 := strfmt.UUID("7b395e5c-cf4d-4297-b8cc-1d849a057de3")
	target2 := strfmt.UUID("8f9f54f3-a7db-415e-881a-0e6fb79a7ec7")
	target3 := strfmt.UUID("046251cf-cb02-4102-b854-c7c4691cf16f")
	target4 := strfmt.UUID("bc7d8875-3a24-4137-8203-e152096dea4f")
	sourceID := strfmt.UUID("a3c98a66-be4a-4eaf-8cf3-04648a11d0f7")

	t.Run("add objects", func(t *testing.T) {
		err := repo.PutObject(context.Background(), &models.Object{
			ID:    sourceID,
			Class: "AddingBatchReferencesTestSource",
			Properties: map[string]interface{}{
				"name": "source item",
			},
		}, []float32{0.5})
		require.Nil(t, err)

		targets := []strfmt.UUID{target1, target2, target3, target4}

		for i, target := range targets {
			err = repo.PutObject(context.Background(), &models.Object{
				ID:    target,
				Class: "AddingBatchReferencesTestTarget",
				Properties: map[string]interface{}{
					"name": fmt.Sprintf("target item %d", i),
				},
			}, []float32{0.7})
			require.Nil(t, err)
		}
	})

	t.Run("verify ref count through filters", func(t *testing.T) {
		t.Run("count==0 should return the source", func(t *testing.T) {
			filter := buildFilter("toTarget", 0, eq, schema.DataTypeInt)
			res, err := repo.ClassSearch(context.Background(), traverser.GetParams{
				Filters:   filter,
				ClassName: "AddingBatchReferencesTestSource",
				Pagination: &filters.Pagination{
					Limit: 10,
				},
			})

			require.Nil(t, err)
			require.Len(t, res, 1)
			assert.Equal(t, res[0].ID, sourceID)
		})

		t.Run("count>0 should not return anything", func(t *testing.T) {
			filter := buildFilter("toTarget", 0, gt, schema.DataTypeInt)
			res, err := repo.ClassSearch(context.Background(), traverser.GetParams{
				Filters:   filter,
				ClassName: "AddingBatchReferencesTestSource",
				Pagination: &filters.Pagination{
					Limit: 10,
				},
			})

			require.Nil(t, err)
			require.Len(t, res, 0)
		})
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

	t.Run("verify ref count through filters", func(t *testing.T) {
		// so far we have imported two refs (!)
		t.Run("count==2 should return the source", func(t *testing.T) {
			filter := buildFilter("toTarget", 2, eq, schema.DataTypeInt)
			res, err := repo.ClassSearch(context.Background(), traverser.GetParams{
				Filters:   filter,
				ClassName: "AddingBatchReferencesTestSource",
				Pagination: &filters.Pagination{
					Limit: 10,
				},
			})

			require.Nil(t, err)
			require.Len(t, res, 1)
			assert.Equal(t, res[0].ID, sourceID)
		})

		t.Run("count==0 should not return anything", func(t *testing.T) {
			filter := buildFilter("toTarget", 0, eq, schema.DataTypeInt)
			res, err := repo.ClassSearch(context.Background(), traverser.GetParams{
				Filters:   filter,
				ClassName: "AddingBatchReferencesTestSource",
				Pagination: &filters.Pagination{
					Limit: 10,
				},
			})

			require.Nil(t, err)
			require.Len(t, res, 0)
		})
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
		source, err := repo.ObjectByID(context.Background(), sourceID, nil, additional.Properties{})
		require.Nil(t, err)

		refs := source.Object().Properties.(map[string]interface{})["toTarget"]
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

	t.Run("verify ref count through filters", func(t *testing.T) {
		// so far we have imported two refs (!)
		t.Run("count==4 should return the source", func(t *testing.T) {
			filter := buildFilter("toTarget", 4, eq, schema.DataTypeInt)
			res, err := repo.ClassSearch(context.Background(), traverser.GetParams{
				Filters:   filter,
				ClassName: "AddingBatchReferencesTestSource",
				Pagination: &filters.Pagination{
					Limit: 10,
				},
			})

			require.Nil(t, err)
			require.Len(t, res, 1)
			assert.Equal(t, res[0].ID, sourceID)
		})

		t.Run("count==0 should not return anything", func(t *testing.T) {
			filter := buildFilter("toTarget", 0, eq, schema.DataTypeInt)
			res, err := repo.ClassSearch(context.Background(), traverser.GetParams{
				Filters:   filter,
				ClassName: "AddingBatchReferencesTestSource",
				Pagination: &filters.Pagination{
					Limit: 10,
				},
			})

			require.Nil(t, err)
			require.Len(t, res, 0)
		})

		t.Run("count==2 should not return anything", func(t *testing.T) {
			filter := buildFilter("toTarget", 2, eq, schema.DataTypeInt)
			res, err := repo.ClassSearch(context.Background(), traverser.GetParams{
				Filters:   filter,
				ClassName: "AddingBatchReferencesTestSource",
				Pagination: &filters.Pagination{
					Limit: 10,
				},
			})

			require.Nil(t, err)
			require.Len(t, res, 0)
		})
	})

	t.Run("verify search by cross-ref", func(t *testing.T) {
		filter := &filters.LocalFilter{
			Root: &filters.Clause{
				Operator: eq,
				On: &filters.Path{
					Class:    schema.ClassName("AddingBatchReferencesTestSource"),
					Property: schema.PropertyName("toTarget"),
					Child: &filters.Path{
						Class:    schema.ClassName("AddingBatchReferencesTestTarget"),
						Property: schema.PropertyName("name"),
					},
				},
				Value: &filters.Value{
					Value: "item",
					Type:  dtString,
				},
			},
		}
		res, err := repo.ClassSearch(context.Background(), traverser.GetParams{
			Filters:   filter,
			ClassName: "AddingBatchReferencesTestSource",
			Pagination: &filters.Pagination{
				Limit: 10,
			},
		})

		require.Nil(t, err)
		require.Len(t, res, 1)
		assert.Equal(t, res[0].ID, sourceID)
	})

	t.Run("verify objects are still searchable through the vector index",
		func(t *testing.T) {
			// prior to making the inverted index and its docIDs immutable, a ref
			// update would not change the doc ID, therefore the batch reference
			// never had to interact with the vector index. Now that they're
			// immutable, the udpated doc ID needs to be "re-inserted" even if the
			// vector is still the same
			// UPDATE gh-1334: Since batch refs are now a special case where we
			// tolerate a re-use of the doc id, the above assumption is no longer
			// correct. However, this test still adds value, since we were now able
			// to remove the additional storage updates. By still including this
			// test we verify that such an update is indeed no longer necessary
			res, err := repo.VectorClassSearch(context.Background(), traverser.GetParams{
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

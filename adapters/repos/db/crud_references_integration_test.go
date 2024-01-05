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

//go:build integrationTest
// +build integrationTest

package db

import (
	"context"
	"fmt"
	"log"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/schema/crossref"
	"github.com/weaviate/weaviate/entities/search"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

func TestNestedReferences(t *testing.T) {
	dirName := t.TempDir()

	refSchema := schema.Schema{
		Objects: &models.Schema{
			Classes: []*models.Class{
				{
					Class:               "Planet",
					VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
					InvertedIndexConfig: invertedConfig(),
					Properties: []*models.Property{
						{
							Name:         "name",
							DataType:     schema.DataTypeText.PropString(),
							Tokenization: models.PropertyTokenizationWhitespace,
						},
					},
				},
				{
					Class:               "Continent",
					VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
					InvertedIndexConfig: invertedConfig(),
					Properties: []*models.Property{
						{
							Name:         "name",
							DataType:     schema.DataTypeText.PropString(),
							Tokenization: models.PropertyTokenizationWhitespace,
						},
						{
							Name:     "onPlanet",
							DataType: []string{"Planet"},
						},
					},
				},
				{
					Class:               "Country",
					VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
					InvertedIndexConfig: invertedConfig(),
					Properties: []*models.Property{
						{
							Name:         "name",
							DataType:     schema.DataTypeText.PropString(),
							Tokenization: models.PropertyTokenizationWhitespace,
						},
						{
							Name:     "onContinent",
							DataType: []string{"Continent"},
						},
					},
				},
				{
					Class:               "City",
					VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
					InvertedIndexConfig: invertedConfig(),
					Properties: []*models.Property{
						{
							Name:         "name",
							DataType:     schema.DataTypeText.PropString(),
							Tokenization: models.PropertyTokenizationWhitespace,
						},
						{
							Name:     "inCountry",
							DataType: []string{"Country"},
						},
					},
				},
				{
					Class:               "Place",
					VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
					InvertedIndexConfig: invertedConfig(),
					Properties: []*models.Property{
						{
							Name:         "name",
							DataType:     schema.DataTypeText.PropString(),
							Tokenization: models.PropertyTokenizationWhitespace,
						},
						{
							Name:     "inCity",
							DataType: []string{"City"},
						},
					},
				},
			},
		},
	}
	logger := logrus.New()
	schemaGetter := &fakeSchemaGetter{
		schema:     schema.Schema{Objects: &models.Schema{Classes: nil}},
		shardState: singleShardState(),
	}
	repo, err := New(logger, Config{
		MemtablesFlushIdleAfter:   60,
		RootPath:                  dirName,
		MaxImportGoroutinesFactor: 1,
	}, &fakeRemoteClient{}, &fakeNodeResolver{}, &fakeRemoteNodeClient{}, &fakeReplicationClient{}, nil)
	require.Nil(t, err)
	repo.SetSchemaGetter(schemaGetter)
	require.Nil(t, repo.WaitForStartup(testCtx()))
	defer repo.Shutdown(context.Background())
	migrator := NewMigrator(repo, logger)

	t.Run("adding all classes to the schema", func(t *testing.T) {
		for _, class := range refSchema.Objects.Classes {
			t.Run(fmt.Sprintf("add %s", class.Class), func(t *testing.T) {
				err := migrator.AddClass(context.Background(), class, schemaGetter.shardState)
				require.Nil(t, err)
			})
		}
	})

	// update schema getter so it's in sync with class
	schemaGetter.schema = refSchema

	t.Run("importing some thing objects with references", func(t *testing.T) {
		objects := []models.Object{
			{
				Class: "Planet",
				Properties: map[string]interface{}{
					"name": "Earth",
				},
				ID:               "32c69af9-cbbe-4ec9-bf6c-365cd6c22fdf",
				CreationTimeUnix: 1566464889,
			},
			{
				Class: "Continent",
				Properties: map[string]interface{}{
					"name": "North America",
					"onPlanet": models.MultipleRef{
						&models.SingleRef{
							Beacon: "weaviate://localhost/32c69af9-cbbe-4ec9-bf6c-365cd6c22fdf",
						},
					},
				},
				ID:               "4aad8154-e7f3-45b8-81a6-725171419e55",
				CreationTimeUnix: 1566464892,
			},
			{
				Class: "Country",
				Properties: map[string]interface{}{
					"name": "USA",
					"onContinent": models.MultipleRef{
						&models.SingleRef{
							Beacon: "weaviate://localhost/4aad8154-e7f3-45b8-81a6-725171419e55",
						},
					},
				},
				ID:               "18c80a16-346a-477d-849d-9d92e5040ac9",
				CreationTimeUnix: 1566464896,
			},
			{
				Class: "City",
				Properties: map[string]interface{}{
					"name": "San Francisco",
					"inCountry": models.MultipleRef{
						&models.SingleRef{
							Beacon: "weaviate://localhost/18c80a16-346a-477d-849d-9d92e5040ac9",
						},
					},
				},
				ID:               "2297e094-6218-43d4-85b1-3d20af752f23",
				CreationTimeUnix: 1566464899,
			},
			{
				Class: "Place",
				Properties: map[string]interface{}{
					"name": "Tim Apple's Fruit Bar",
					"inCity": models.MultipleRef{
						&models.SingleRef{
							Beacon: "weaviate://localhost/2297e094-6218-43d4-85b1-3d20af752f23",
						},
					},
				},
				ID:               "4ef47fb0-3cf5-44fc-b378-9e217dff13ac",
				CreationTimeUnix: 1566464904,
			},
		}

		for _, thing := range objects {
			t.Run(fmt.Sprintf("add %s", thing.ID), func(t *testing.T) {
				err := repo.PutObject(context.Background(), &thing, []float32{1, 2, 3, 4, 5, 6, 7}, nil)
				require.Nil(t, err)
			})
		}
	})

	t.Run("fully resolving the place", func(t *testing.T) {
		expectedSchema := map[string]interface{}{
			"inCity": []interface{}{
				search.LocalRef{
					Class: "City",
					Fields: map[string]interface{}{
						"inCountry": []interface{}{
							search.LocalRef{
								Class: "Country",
								Fields: map[string]interface{}{
									"onContinent": []interface{}{
										search.LocalRef{
											Class: "Continent",
											Fields: map[string]interface{}{
												"onPlanet": []interface{}{
													search.LocalRef{
														Class: "Planet",
														Fields: map[string]interface{}{
															"name": "Earth",
															"id":   strfmt.UUID("32c69af9-cbbe-4ec9-bf6c-365cd6c22fdf"),
														},
													},
												},
												"name": "North America",
												"id":   strfmt.UUID("4aad8154-e7f3-45b8-81a6-725171419e55"),
											},
										},
									},
									"name": "USA",
									"id":   strfmt.UUID("18c80a16-346a-477d-849d-9d92e5040ac9"),
								},
							},
						},
						"name": "San Francisco",
						"id":   strfmt.UUID("2297e094-6218-43d4-85b1-3d20af752f23"),
					},
				},
			},
			"name": "Tim Apple's Fruit Bar",
			"id":   strfmt.UUID("4ef47fb0-3cf5-44fc-b378-9e217dff13ac"),
		}

		res, err := repo.ObjectByID(context.Background(), "4ef47fb0-3cf5-44fc-b378-9e217dff13ac", fullyNestedSelectProperties(), additional.Properties{}, "")
		require.Nil(t, err)
		assert.Equal(t, expectedSchema, res.Schema)
	})

	t.Run("fully resolving the place with vectors", func(t *testing.T) {
		expectedSchema := map[string]interface{}{
			"inCity": []interface{}{
				search.LocalRef{
					Class: "City",
					Fields: map[string]interface{}{
						"inCountry": []interface{}{
							search.LocalRef{
								Class: "Country",
								Fields: map[string]interface{}{
									"onContinent": []interface{}{
										search.LocalRef{
											Class: "Continent",
											Fields: map[string]interface{}{
												"onPlanet": []interface{}{
													search.LocalRef{
														Class: "Planet",
														Fields: map[string]interface{}{
															"name":   "Earth",
															"id":     strfmt.UUID("32c69af9-cbbe-4ec9-bf6c-365cd6c22fdf"),
															"vector": []float32{1, 2, 3, 4, 5, 6, 7},
														},
													},
												},
												"name":   "North America",
												"id":     strfmt.UUID("4aad8154-e7f3-45b8-81a6-725171419e55"),
												"vector": []float32{1, 2, 3, 4, 5, 6, 7},
											},
										},
									},
									"name":   "USA",
									"id":     strfmt.UUID("18c80a16-346a-477d-849d-9d92e5040ac9"),
									"vector": []float32{1, 2, 3, 4, 5, 6, 7},
								},
							},
						},
						"name":   "San Francisco",
						"id":     strfmt.UUID("2297e094-6218-43d4-85b1-3d20af752f23"),
						"vector": []float32{1, 2, 3, 4, 5, 6, 7},
					},
				},
			},
			"name": "Tim Apple's Fruit Bar",
			"id":   strfmt.UUID("4ef47fb0-3cf5-44fc-b378-9e217dff13ac"),
		}

		res, err := repo.ObjectByID(context.Background(), "4ef47fb0-3cf5-44fc-b378-9e217dff13ac", fullyNestedSelectPropertiesWithVector(), additional.Properties{}, "")
		require.Nil(t, err)
		assert.Equal(t, expectedSchema, res.Schema)
	})

	t.Run("partially resolving the place", func(t *testing.T) {
		expectedSchema := map[string]interface{}{
			"inCity": []interface{}{
				search.LocalRef{
					Class: "City",
					Fields: map[string]interface{}{
						"name": "San Francisco",
						"id":   strfmt.UUID("2297e094-6218-43d4-85b1-3d20af752f23"),
						// why is inCountry present here? We didn't specify it our select
						// properties. Note it is "inCountry" with a lowercase letter
						// (meaning unresolved) whereas "inCountry" would mean it was
						// resolved. In GraphQL this property would simply be hidden (as
						// the GQL is unaware of unresolved properties)
						// However, for caching and other queries it is helpful that this
						// info is still present, the important thing is that we're
						// avoiding the costly resolving of it, if we don't need it.
						"inCountry": models.MultipleRef{
							&models.SingleRef{
								Beacon: "weaviate://localhost/18c80a16-346a-477d-849d-9d92e5040ac9",
							},
						},
					},
				},
			},
			"name": "Tim Apple's Fruit Bar",
			"id":   strfmt.UUID("4ef47fb0-3cf5-44fc-b378-9e217dff13ac"),
		}

		res, err := repo.ObjectByID(context.Background(), "4ef47fb0-3cf5-44fc-b378-9e217dff13ac", partiallyNestedSelectProperties(), additional.Properties{}, "")
		require.Nil(t, err)
		assert.Equal(t, expectedSchema, res.Schema)
	})

	t.Run("resolving without any refs", func(t *testing.T) {
		res, err := repo.ObjectByID(context.Background(), "4ef47fb0-3cf5-44fc-b378-9e217dff13ac", search.SelectProperties{}, additional.Properties{}, "")

		expectedSchema := map[string]interface{}{
			"id": strfmt.UUID("4ef47fb0-3cf5-44fc-b378-9e217dff13ac"),
			"inCity": models.MultipleRef{
				&models.SingleRef{
					Beacon: "weaviate://localhost/2297e094-6218-43d4-85b1-3d20af752f23",
				},
			},
			"name": "Tim Apple's Fruit Bar",
		}

		require.Nil(t, err)

		assert.Equal(t, expectedSchema, res.Schema, "does not contain any resolved refs")
	})

	t.Run("adding a new place to verify idnexing is constantly happening in the background", func(t *testing.T) {
		newPlace := models.Object{
			Class: "Place",
			Properties: map[string]interface{}{
				"name": "John Oliver's Avocados",
				"inCity": models.MultipleRef{
					&models.SingleRef{
						Beacon: "weaviate://localhost/2297e094-6218-43d4-85b1-3d20af752f23",
					},
				},
			},
			ID:               "0f02d525-902d-4dc0-8052-647cb420c1a6",
			CreationTimeUnix: 1566464912,
		}

		err := repo.PutObject(context.Background(), &newPlace, []float32{1, 2, 3, 4, 5, 6, 7}, nil)
		require.Nil(t, err)
	})
}

func fullyNestedSelectProperties() search.SelectProperties {
	return search.SelectProperties{
		search.SelectProperty{
			Name:        "inCity",
			IsPrimitive: false,
			Refs: []search.SelectClass{
				{
					ClassName: "City",
					RefProperties: search.SelectProperties{
						search.SelectProperty{
							Name:        "inCountry",
							IsPrimitive: false,
							Refs: []search.SelectClass{
								{
									ClassName: "Country",
									RefProperties: search.SelectProperties{
										search.SelectProperty{
											Name:        "onContinent",
											IsPrimitive: false,
											Refs: []search.SelectClass{
												{
													ClassName: "Continent",
													RefProperties: search.SelectProperties{
														search.SelectProperty{
															Name:        "onPlanet",
															IsPrimitive: false,
															Refs: []search.SelectClass{
																{
																	ClassName:     "Planet",
																	RefProperties: nil,
																},
															},
														},
													},
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}
}

func fullyNestedSelectPropertiesWithVector() search.SelectProperties {
	return search.SelectProperties{
		search.SelectProperty{
			Name:        "inCity",
			IsPrimitive: false,
			Refs: []search.SelectClass{
				{
					ClassName: "City",
					RefProperties: search.SelectProperties{
						search.SelectProperty{
							Name:        "inCountry",
							IsPrimitive: false,
							Refs: []search.SelectClass{
								{
									ClassName: "Country",
									RefProperties: search.SelectProperties{
										search.SelectProperty{
											Name:        "onContinent",
											IsPrimitive: false,
											Refs: []search.SelectClass{
												{
													ClassName: "Continent",
													RefProperties: search.SelectProperties{
														search.SelectProperty{
															Name:        "onPlanet",
															IsPrimitive: false,
															Refs: []search.SelectClass{
																{
																	ClassName:     "Planet",
																	RefProperties: nil,
																	AdditionalProperties: additional.Properties{
																		Vector: true,
																	},
																},
															},
														},
													},
													AdditionalProperties: additional.Properties{
														Vector: true,
													},
												},
											},
										},
									},
									AdditionalProperties: additional.Properties{
										Vector: true,
									},
								},
							},
						},
					},
					AdditionalProperties: additional.Properties{
						Vector: true,
					},
				},
			},
		},
	}
}

func partiallyNestedSelectProperties() search.SelectProperties {
	return search.SelectProperties{
		search.SelectProperty{
			Name:        "inCity",
			IsPrimitive: false,
			Refs: []search.SelectClass{
				{
					ClassName:     "City",
					RefProperties: search.SelectProperties{},
				},
			},
		},
	}
}

func GetDimensionsFromRepo(repo *DB, className string) int {
	if !repo.config.TrackVectorDimensions {
		log.Printf("Vector dimensions tracking is disabled, returning 0")
		return 0
	}
	index := repo.GetIndex(schema.ClassName(className))
	sum := 0
	index.ForEachShard(func(name string, shard ShardLike) error {
		sum += shard.Dimensions()
		return nil
	})
	return sum
}

func GetQuantizedDimensionsFromRepo(repo *DB, className string, segments int) int {
	if !repo.config.TrackVectorDimensions {
		log.Printf("Vector dimensions tracking is disabled, returning 0")
		return 0
	}
	index := repo.GetIndex(schema.ClassName(className))
	sum := 0
	index.ForEachShard(func(name string, shard ShardLike) error {
		sum += shard.QuantizedDimensions(segments)
		return nil
	})
	return sum
}

func Test_AddingReferenceOneByOne(t *testing.T) {
	dirName := t.TempDir()

	sch := schema.Schema{
		Objects: &models.Schema{
			Classes: []*models.Class{
				{
					Class:               "AddingReferencesTestTarget",
					VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
					InvertedIndexConfig: invertedConfig(),
					Properties: []*models.Property{
						{
							Name:         "name",
							DataType:     schema.DataTypeText.PropString(),
							Tokenization: models.PropertyTokenizationWhitespace,
						},
					},
				},
				{
					Class:               "AddingReferencesTestSource",
					VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
					InvertedIndexConfig: invertedConfig(),
					Properties: []*models.Property{
						{
							Name:         "name",
							DataType:     schema.DataTypeText.PropString(),
							Tokenization: models.PropertyTokenizationWhitespace,
						},
						{
							Name:     "toTarget",
							DataType: []string{"AddingReferencesTestTarget"},
						},
					},
				},
			},
		},
	}
	logger := logrus.New()
	schemaGetter := &fakeSchemaGetter{
		schema:     schema.Schema{Objects: &models.Schema{Classes: nil}},
		shardState: singleShardState(),
	}
	repo, err := New(logger, Config{
		MemtablesFlushIdleAfter:   60,
		RootPath:                  dirName,
		MaxImportGoroutinesFactor: 1,
		TrackVectorDimensions:     true,
	}, &fakeRemoteClient{}, &fakeNodeResolver{}, &fakeRemoteNodeClient{}, &fakeReplicationClient{}, nil)
	require.Nil(t, err)
	repo.SetSchemaGetter(schemaGetter)
	require.Nil(t, repo.WaitForStartup(testCtx()))
	defer repo.Shutdown(context.Background())
	migrator := NewMigrator(repo, logger)

	t.Run("add required classes", func(t *testing.T) {
		for _, class := range sch.Objects.Classes {
			t.Run(fmt.Sprintf("add %s", class.Class), func(t *testing.T) {
				err := migrator.AddClass(context.Background(), class, schemaGetter.shardState)
				require.Nil(t, err)
			})
		}
	})

	schemaGetter.schema = sch
	targetID := strfmt.UUID("a4a92239-e748-4e55-bbbd-f606926619a7")
	target2ID := strfmt.UUID("325084e7-4faa-43a5-b2b1-56e207be169a")
	sourceID := strfmt.UUID("0826c61b-85c1-44ac-aebb-cfd07ace6a57")

	t.Run("add objects", func(t *testing.T) {
		err := repo.PutObject(context.Background(), &models.Object{
			ID:    sourceID,
			Class: "AddingReferencesTestSource",
			Properties: map[string]interface{}{
				"name": "source item",
			},
		}, []float32{0.5}, nil)
		require.Nil(t, err)

		err = repo.PutObject(context.Background(), &models.Object{
			ID:    targetID,
			Class: "AddingReferencesTestTarget",
			Properties: map[string]interface{}{
				"name": "target item",
			},
		}, []float32{0.5}, nil)
		require.Nil(t, err)

		err = repo.PutObject(context.Background(), &models.Object{
			ID:    target2ID,
			Class: "AddingReferencesTestTarget",
			Properties: map[string]interface{}{
				"name": "another target item",
			},
		}, []float32{0.5}, nil)
		require.Nil(t, err)
	})

	t.Run("add reference between them", func(t *testing.T) {
		// Get dimensions before adding reference
		sourceShardDimension := GetDimensionsFromRepo(repo, "AddingReferencesTestSource")
		targetShardDimension := GetDimensionsFromRepo(repo, "AddingReferencesTestTarget")

		source := crossref.NewSource("AddingReferencesTestSource", "toTarget", sourceID)
		target := crossref.New("localhost", "", targetID)

		err := repo.AddReference(context.Background(), source, target, nil, "")
		assert.Nil(t, err)

		// Check dimensions after adding reference
		sourceDimensionAfter := GetDimensionsFromRepo(repo, "AddingReferencesTestSource")
		targetDimensionAfter := GetDimensionsFromRepo(repo, "AddingReferencesTestTarget")

		require.Equalf(t, sourceShardDimension, sourceDimensionAfter, "dimensions of source should not change")
		require.Equalf(t, targetShardDimension, targetDimensionAfter, "dimensions of target should not change")
	})

	t.Run("check reference was added", func(t *testing.T) {
		source, err := repo.ObjectByID(context.Background(), sourceID, nil, additional.Properties{}, "")
		require.Nil(t, err)
		require.NotNil(t, source)
		require.NotNil(t, source.Object())
		require.NotNil(t, source.Object().Properties)

		refs := source.Object().Properties.(map[string]interface{})["toTarget"]
		refsSlice, ok := refs.(models.MultipleRef)
		require.True(t, ok,
			fmt.Sprintf("toTarget must be models.MultipleRef, but got %#v", refs))

		foundBeacons := []string{}
		for _, ref := range refsSlice {
			foundBeacons = append(foundBeacons, ref.Beacon.String())
		}
		expectedBeacons := []string{
			fmt.Sprintf("weaviate://localhost/%s", targetID),
		}

		assert.ElementsMatch(t, foundBeacons, expectedBeacons)
	})

	t.Run("reference a second target", func(t *testing.T) {
		source := crossref.NewSource("AddingReferencesTestSource", "toTarget", sourceID)
		target := crossref.New("localhost", "", target2ID)

		err := repo.AddReference(context.Background(), source, target, nil, "")
		assert.Nil(t, err)
	})

	t.Run("check both references are now present", func(t *testing.T) {
		source, err := repo.ObjectByID(context.Background(), sourceID, nil, additional.Properties{}, "")
		require.Nil(t, err)
		require.NotNil(t, source)
		require.NotNil(t, source.Object())
		require.NotNil(t, source.Object().Properties)

		refs := source.Object().Properties.(map[string]interface{})["toTarget"]
		refsSlice, ok := refs.(models.MultipleRef)
		require.True(t, ok,
			fmt.Sprintf("toTarget must be models.MultipleRef, but got %#v", refs))

		foundBeacons := []string{}
		for _, ref := range refsSlice {
			foundBeacons = append(foundBeacons, ref.Beacon.String())
		}
		expectedBeacons := []string{
			fmt.Sprintf("weaviate://localhost/%s", targetID),
			fmt.Sprintf("weaviate://localhost/%s", target2ID),
		}

		assert.ElementsMatch(t, foundBeacons, expectedBeacons)
	})
}

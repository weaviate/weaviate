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
	"sync"
	"testing"
	"time"

	"github.com/elastic/go-elasticsearch/v5"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
	"github.com/semi-technologies/weaviate/entities/search"
	"github.com/semi-technologies/weaviate/usecases/traverser"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testEsVectorCache(t *testing.T) {
	client, err := elasticsearch.NewClient(elasticsearch.Config{
		Addresses: []string{"http://localhost:9201"},
	})

	refSchema := schema.Schema{
		Things: &models.Schema{
			Classes: []*models.Class{
				&models.Class{
					Class: "Planet",
					Properties: []*models.Property{
						&models.Property{
							Name:     "name",
							DataType: []string{string(schema.DataTypeString)},
						},
					},
				},
				&models.Class{
					Class: "Continent",
					Properties: []*models.Property{
						&models.Property{
							Name:     "name",
							DataType: []string{string(schema.DataTypeString)},
						},
						&models.Property{
							Name:     "onPlanet",
							DataType: []string{"Planet"},
						},
					},
				},
				&models.Class{
					Class: "Country",
					Properties: []*models.Property{
						&models.Property{
							Name:     "name",
							DataType: []string{string(schema.DataTypeString)},
						},
						&models.Property{
							Name:     "onContinent",
							DataType: []string{"Continent"},
						},
					},
				},
				&models.Class{
					Class: "City",
					Properties: []*models.Property{
						&models.Property{
							Name:     "name",
							DataType: []string{string(schema.DataTypeString)},
						},
						&models.Property{
							Name:     "inCountry",
							DataType: []string{"Country"},
						},
					},
				},
				&models.Class{
					Class: "Place",
					Properties: []*models.Property{
						&models.Property{
							Name:     "name",
							DataType: []string{string(schema.DataTypeString)},
						},
						&models.Property{
							Name:     "inCity",
							DataType: []string{"City"},
						},
					},
				},
			},
		},
	}
	require.Nil(t, err)
	logger := logrus.New()
	schemaGetter := &fakeSchemaGetter{schema: refSchema}
	repo := NewRepo(client, logger, schemaGetter, 2, 100, 1, "0-1")
	waitForEsToBeReady(t, repo)
	requestCounter := &testCounter{}
	repo.requestCounter = requestCounter
	migrator := NewMigrator(repo)

	t.Run("adding all classes to the schema", func(t *testing.T) {
		for _, class := range refSchema.Things.Classes {
			t.Run(fmt.Sprintf("add %s", class.Class), func(t *testing.T) {
				err := migrator.AddClass(context.Background(), kind.Thing, class)
				require.Nil(t, err)
			})
		}
	})

	t.Run("importing some thing objects with references", func(t *testing.T) {
		objects := []models.Thing{
			models.Thing{
				Class: "Planet",
				Schema: map[string]interface{}{
					"name": "Earth",
				},
				ID:               "32c69af9-cbbe-4ec9-bf6c-365cd6c22fdf",
				CreationTimeUnix: 1566464889,
			},
			models.Thing{
				Class: "Continent",
				Schema: map[string]interface{}{
					"name": "North America",
					"onPlanet": models.MultipleRef{
						&models.SingleRef{
							Beacon: "weaviate://localhost/things/32c69af9-cbbe-4ec9-bf6c-365cd6c22fdf",
						},
					},
				},
				ID:               "4aad8154-e7f3-45b8-81a6-725171419e55",
				CreationTimeUnix: 1566464892,
			},
			models.Thing{
				Class: "Country",
				Schema: map[string]interface{}{
					"name": "USA",
					"onContinent": models.MultipleRef{
						&models.SingleRef{
							Beacon: "weaviate://localhost/things/4aad8154-e7f3-45b8-81a6-725171419e55",
						},
					},
				},
				ID:               "18c80a16-346a-477d-849d-9d92e5040ac9",
				CreationTimeUnix: 1566464896,
			},
			models.Thing{
				Class: "City",
				Schema: map[string]interface{}{
					"name": "San Francisco",
					"inCountry": models.MultipleRef{
						&models.SingleRef{
							Beacon: "weaviate://localhost/things/18c80a16-346a-477d-849d-9d92e5040ac9",
						},
					},
				},
				ID:               "2297e094-6218-43d4-85b1-3d20af752f23",
				CreationTimeUnix: 1566464899,
			},
			models.Thing{
				Class: "Place",
				Schema: map[string]interface{}{
					"name": "Tim Apple's Fruit Bar",
					"inCity": models.MultipleRef{
						&models.SingleRef{
							Beacon: "weaviate://localhost/things/2297e094-6218-43d4-85b1-3d20af752f23",
						},
					},
				},
				ID:               "4ef47fb0-3cf5-44fc-b378-9e217dff13ac",
				CreationTimeUnix: 1566464904,
			},
		}

		for _, thing := range objects {
			t.Run(fmt.Sprintf("add %s", thing.ID), func(t *testing.T) {
				err := repo.PutThing(context.Background(), &thing, []float32{1, 2, 3, 4, 5, 6, 7})
				require.Nil(t, err)
			})
		}
	})

	refreshAll(t, client)

	var before *search.Result
	_ = before
	t.Run("fully resolving the place before we have cache", func(t *testing.T) {
		expectedSchema := map[string]interface{}{
			"InCity": []interface{}{
				search.LocalRef{
					Class: "City",
					Fields: map[string]interface{}{
						"InCountry": []interface{}{
							search.LocalRef{
								Class: "Country",
								Fields: map[string]interface{}{
									"OnContinent": []interface{}{
										search.LocalRef{
											Class: "Continent",
											Fields: map[string]interface{}{
												"OnPlanet": []interface{}{
													search.LocalRef{
														Class: "Planet",
														Fields: map[string]interface{}{
															"name": "Earth",
															"uuid": "32c69af9-cbbe-4ec9-bf6c-365cd6c22fdf",
														},
													},
												},
												"name": "North America",
												"uuid": "4aad8154-e7f3-45b8-81a6-725171419e55",
											},
										},
									},
									"name": "USA",
									"uuid": "18c80a16-346a-477d-849d-9d92e5040ac9",
								},
							},
						},
						"name": "San Francisco",
						"uuid": "2297e094-6218-43d4-85b1-3d20af752f23",
					},
				},
			},
			"name": "Tim Apple's Fruit Bar",
			"uuid": "4ef47fb0-3cf5-44fc-b378-9e217dff13ac",
		}

		requestCounter.reset()
		res, err := repo.ThingByID(context.Background(), "4ef47fb0-3cf5-44fc-b378-9e217dff13ac",
			fullyNestedSelectProperties(), false)
		require.Nil(t, err)
		assert.Equal(t, false, res.CacheHot)
		assert.Equal(t, expectedSchema, res.Schema)
		// we are expecting 5 request to be made for this, since cache is not hot
		// yet. Quering the initial places is a single request, each nested level
		// is another request; 1+4=5
		assert.Equal(t, 5, requestCounter.count)
		before = res
	})

	t.Run("partially resolving the place before we have cache", func(t *testing.T) {
		expectedSchema := map[string]interface{}{
			"InCity": []interface{}{
				search.LocalRef{
					Class: "City",
					Fields: map[string]interface{}{
						"name": "San Francisco",
						"uuid": "2297e094-6218-43d4-85b1-3d20af752f23",
						// why is inCountry present here? We didn't specify it our select
						// properties. Note it is "inCountry" with a lowercase letter
						// (meaning unresolved) whereas "InCountry" would mean it was
						// resolved. In GraphQL this property would simply be hidden (as
						// the GQL is unaware of unresolved properties)
						// However, for caching and other queries it is helpful that this
						// info is still present, the important thing is that we're
						// avoiding the costly resolving of it, if we don't need it.
						"inCountry": models.MultipleRef{
							&models.SingleRef{
								Beacon: "weaviate://localhost/things/18c80a16-346a-477d-849d-9d92e5040ac9",
							},
						},
					},
				},
			},
			"name": "Tim Apple's Fruit Bar",
			"uuid": "4ef47fb0-3cf5-44fc-b378-9e217dff13ac",
		}

		requestCounter.reset()
		res, err := repo.ThingByID(context.Background(), "4ef47fb0-3cf5-44fc-b378-9e217dff13ac",
			partiallyNestedSelectProperties(), false)
		require.Nil(t, err)
		assert.Equal(t, false, res.CacheHot)
		assert.Equal(t, expectedSchema, res.Schema)
		// 2 Requests: Place + (inCity->City)
		assert.Equal(t, 2, requestCounter.count)
	})

	t.Run("init caching state machine", func(t *testing.T) {
		repo.InitCacheIndexing(50, 200*time.Millisecond, 200*time.Millisecond)
	})

	// wait for both es indexing as well as esvector caching to be complete
	time.Sleep(2000 * time.Millisecond)

	t.Run("all 3 (outer) things must now have a hot cache", func(t *testing.T) {
		res, err := repo.ThingByID(context.Background(), "18c80a16-346a-477d-849d-9d92e5040ac9",
			traverser.SelectProperties{}, false)
		require.Nil(t, err)
		assert.Equal(t, true, res.CacheHot)

		res, err = repo.ThingByID(context.Background(), "18c80a16-346a-477d-849d-9d92e5040ac9",
			traverser.SelectProperties{}, false)
		require.Nil(t, err)
		assert.Equal(t, true, res.CacheHot)

		res, err = repo.ThingByID(context.Background(), "4ef47fb0-3cf5-44fc-b378-9e217dff13ac",
			traverser.SelectProperties{}, false)
		require.Nil(t, err)
		assert.Equal(t, true, res.CacheHot)
	})

	t.Run("inspecting the cache for the place to make sure caching stopped at the configured boundary",
		func(t *testing.T) {
			requestCounter.reset()
			res, err := repo.ThingByID(context.Background(), "4ef47fb0-3cf5-44fc-b378-9e217dff13ac",
				traverser.SelectProperties{}, false)
			require.Nil(t, err)
			// we didn't specify any selectProperties, so there shouldn't be any
			// additional requests at all, only the initial request to get the place
			assert.Equal(t, 1, requestCounter.count)

			// our desired depth is 2, this means 2 refs should be fully
			// resolved, whereas the 3rd one is merely referenced by a beacon.
			// This means we should see fully resolved
			// inCity/City->inCountry/Country->, the next level
			// (onContinent/continent) should merely be referenced by an unresolved beacon

			inCity := res.CacheSchema["inCity"].(map[string]interface{})
			city := inCity["City"].([]interface{})[0].(map[string]interface{})
			inCountry := city["inCountry"].(map[string]interface{})
			country := inCountry["Country"].([]interface{})[0].(map[string]interface{})

			refs, ok := country["onContinent"].([]interface{})
			// if onPlanet were resolved it would be a map. The fact that it's a
			// slice is the first indication that it was unresolved
			assert.True(t, ok)
			require.Len(t, refs, 1)

			firstRef, ok := refs[0].(map[string]interface{})
			assert.True(t, ok)

			beacon, ok := firstRef["beacon"]
			assert.True(t, ok)
			assert.Equal(t, "weaviate://localhost/things/4aad8154-e7f3-45b8-81a6-725171419e55", beacon)
		})

	t.Run("fully resolving the place after cache is hot", func(t *testing.T) {
		requestCounter.reset()
		res, err := repo.ThingByID(context.Background(), "4ef47fb0-3cf5-44fc-b378-9e217dff13ac",
			fullyNestedSelectProperties(), false)
		// we are crossing the cache boundary, so we are expecting an additional request
		assert.Equal(t, 2, requestCounter.count)

		require.Nil(t, err)

		assert.Equal(t, before.Schema, res.Schema, "result without a cache and with a cache should look the same")
	})

	t.Run("resolving without any refs", func(t *testing.T) {
		res, err := repo.ThingByID(context.Background(), "4ef47fb0-3cf5-44fc-b378-9e217dff13ac",
			traverser.SelectProperties{}, false)

		expectedSchema := map[string]interface{}{
			"uuid": "4ef47fb0-3cf5-44fc-b378-9e217dff13ac",
			"inCity": models.MultipleRef{
				&models.SingleRef{
					Beacon: "weaviate://localhost/things/2297e094-6218-43d4-85b1-3d20af752f23",
				},
			},
			"name": "Tim Apple's Fruit Bar",
		}

		require.Nil(t, err)

		assert.Equal(t, expectedSchema, res.Schema, "does not contain any resolved refs")
	})

	t.Run("partially resolving the place after cache is hot", func(t *testing.T) {
		res, err := repo.ThingByID(context.Background(), "4ef47fb0-3cf5-44fc-b378-9e217dff13ac",
			partiallyNestedSelectProperties(), false)
		expectedSchema := map[string]interface{}{
			"InCity": []interface{}{
				search.LocalRef{
					Class: "City",
					Fields: map[string]interface{}{
						"name": "San Francisco",
						"uuid": "2297e094-6218-43d4-85b1-3d20af752f23",
					},
				},
			},
			"name": "Tim Apple's Fruit Bar",
			"uuid": "4ef47fb0-3cf5-44fc-b378-9e217dff13ac",
		}

		require.Nil(t, err)

		assert.Equal(t, expectedSchema, res.Schema, "result without a cache and with a cache should look the same")
	})

	t.Run("adding a new place to verify idnexing is constantly happening in the background", func(t *testing.T) {
		newPlace := models.Thing{
			Class: "Place",
			Schema: map[string]interface{}{
				"name": "John Oliver's Avocados",
				"inCity": models.MultipleRef{
					&models.SingleRef{
						Beacon: "weaviate://localhost/things/2297e094-6218-43d4-85b1-3d20af752f23",
					},
				},
			},
			ID:               "0f02d525-902d-4dc0-8052-647cb420c1a6",
			CreationTimeUnix: 1566464912,
		}

		err := repo.PutThing(context.Background(), &newPlace, []float32{1, 2, 3, 4, 5, 6, 7})
		require.Nil(t, err)
	})
	refreshAll(t, client)

	// wait for both es indexing as well as esvector caching to be complete
	time.Sleep(2000 * time.Millisecond)

	t.Run("the newly added place must have a hot cache by now", func(t *testing.T) {
		res, err := repo.ThingByID(context.Background(), "0f02d525-902d-4dc0-8052-647cb420c1a6", traverser.SelectProperties{},
			false)
		require.Nil(t, err)
		assert.Equal(t, true, res.CacheHot)
	})

	t.Run("the newly added place respects cache boundaries", func(t *testing.T) {
		requestCounter.reset()
		res, err := repo.ThingByID(context.Background(), "0f02d525-902d-4dc0-8052-647cb420c1a6",
			traverser.SelectProperties{}, false)
		require.Nil(t, err)
		// we didn't specify any selectProperties, so there shouldn't be any
		// additional requests at all, only the initial request to get the place
		assert.Equal(t, 1, requestCounter.count)

		// our desired depth is 2, this means 2 refs should be fully
		// resolved, whereas the 3rd one is merely referenced by a beacon.
		// This means we should see fully resolved
		// inCity/City->inCountry/Country->, the next level
		// (onContinent/continent) should merely be referenced by an unresolved beacon

		inCity := res.CacheSchema["inCity"].(map[string]interface{})
		city := inCity["City"].([]interface{})[0].(map[string]interface{})
		inCountry := city["inCountry"].(map[string]interface{})
		country := inCountry["Country"].([]interface{})[0].(map[string]interface{})

		refs, ok := country["onContinent"].([]interface{})
		// if onPlanet were resolved it would be a map. The fact that it's a
		// slice is the first indication that it was unresolved
		require.True(t, ok, fmt.Sprintf("should be a slice, got %#v", country["onContinent"]))
		require.Len(t, refs, 1)

		firstRef, ok := refs[0].(map[string]interface{})
		assert.True(t, ok)

		beacon, ok := firstRef["beacon"]
		assert.True(t, ok)
		assert.Equal(t, "weaviate://localhost/things/4aad8154-e7f3-45b8-81a6-725171419e55", beacon)
	})

	repo.StopCacheIndexing()
}

func fullyNestedSelectProperties() traverser.SelectProperties {
	return traverser.SelectProperties{
		traverser.SelectProperty{
			Name:        "InCity",
			IsPrimitive: false,
			Refs: []traverser.SelectClass{
				traverser.SelectClass{
					ClassName: "City",
					RefProperties: traverser.SelectProperties{
						traverser.SelectProperty{
							Name:        "InCountry",
							IsPrimitive: false,
							Refs: []traverser.SelectClass{
								traverser.SelectClass{
									ClassName: "Country",
									RefProperties: traverser.SelectProperties{
										traverser.SelectProperty{
											Name:        "OnContinent",
											IsPrimitive: false,
											Refs: []traverser.SelectClass{
												traverser.SelectClass{
													ClassName: "Continent",
													RefProperties: traverser.SelectProperties{
														traverser.SelectProperty{
															Name:        "OnPlanet",
															IsPrimitive: false,
															Refs: []traverser.SelectClass{
																traverser.SelectClass{
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

func partiallyNestedSelectProperties() traverser.SelectProperties {
	return traverser.SelectProperties{
		traverser.SelectProperty{
			Name:        "InCity",
			IsPrimitive: false,
			Refs: []traverser.SelectClass{
				traverser.SelectClass{
					ClassName:     "City",
					RefProperties: traverser.SelectProperties{},
				},
			},
		},
	}
}

type testCounter struct {
	sync.Mutex
	count int
}

func (c *testCounter) Inc() {
	c.Lock()
	defer c.Unlock()

	c.count = c.count + 1
}

func (c *testCounter) reset() {
	c.Lock()
	defer c.Unlock()

	c.count = 0
}

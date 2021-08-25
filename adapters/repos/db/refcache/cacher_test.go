//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2021 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package refcache

import (
	"context"
	"fmt"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/entities/additional"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/multi"
	"github.com/semi-technologies/weaviate/entities/search"
	"github.com/semi-technologies/weaviate/usecases/traverser"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCacher(t *testing.T) {
	// some ids to be used in the tests, they carry no meaning outside each test
	id1 := "132bdf92-ffec-4a52-9196-73ea7cbb5a5e"
	id2 := "a60a26dc-791a-41fc-8dda-c0f21f90cc98"

	t.Run("with empty results", func(t *testing.T) {
		repo := newFakeRepo()
		logger, _ := test.NewNullLogger()
		cr := NewCacher(repo, logger)
		err := cr.Build(context.Background(), nil, nil, additional.Properties{})
		assert.Nil(t, err)
	})

	t.Run("with results with nil-schemas", func(t *testing.T) {
		repo := newFakeRepo()
		logger, _ := test.NewNullLogger()
		cr := NewCacher(repo, logger)
		input := []search.Result{
			search.Result{
				ID:        "foo",
				ClassName: "BestClass",
			},
		}
		err := cr.Build(context.Background(), input, nil, additional.Properties{})
		assert.Nil(t, err)
	})

	t.Run("with results without refs in the schema", func(t *testing.T) {
		repo := newFakeRepo()
		logger, _ := test.NewNullLogger()
		cr := NewCacher(repo, logger)
		input := []search.Result{
			search.Result{
				ID:        "foo",
				ClassName: "BestClass",
				Schema: map[string]interface{}{
					"foo": "bar",
					"baz": &models.PhoneNumber{},
				},
			},
		}
		err := cr.Build(context.Background(), input, nil, additional.Properties{})
		assert.Nil(t, err)
	})

	t.Run("with a single ref, but no selectprops", func(t *testing.T) {
		repo := newFakeRepo()
		logger, _ := test.NewNullLogger()
		cr := NewCacher(repo, logger)
		input := []search.Result{
			search.Result{
				ID:        "foo",
				ClassName: "BestClass",
				Schema: map[string]interface{}{
					"refProp": models.MultipleRef{
						&models.SingleRef{
							Beacon: "weaviate://localhost/123",
						},
					},
				},
			},
		}
		err := cr.Build(context.Background(), input, nil, additional.Properties{})
		require.Nil(t, err)
		_, ok := cr.Get(multi.Identifier{ID: "123", ClassName: "SomeClass"})
		assert.False(t, ok)
	})

	t.Run("with a single ref, and a matching select prop", func(t *testing.T) {
		repo := newFakeRepo()
		repo.lookup[multi.Identifier{ID: id1, ClassName: "SomeClass"}] = search.Result{
			ClassName: "SomeClass",
			ID:        strfmt.UUID(id1),
			Schema: map[string]interface{}{
				"bar": "some string",
			},
		}
		logger, _ := test.NewNullLogger()
		cr := NewCacher(repo, logger)
		input := []search.Result{
			search.Result{
				ID:        "foo",
				ClassName: "BestClass",
				Schema: map[string]interface{}{
					"refProp": models.MultipleRef{
						&models.SingleRef{
							Beacon: strfmt.URI(fmt.Sprintf("weaviate://localhost/%s", id1)),
						},
					},
				},
			},
		}
		selectProps := traverser.SelectProperties{
			traverser.SelectProperty{
				Name: "refProp",
				Refs: []traverser.SelectClass{
					traverser.SelectClass{
						ClassName: "SomeClass",
						RefProperties: traverser.SelectProperties{
							traverser.SelectProperty{
								Name:        "bar",
								IsPrimitive: true,
							},
						},
					},
				},
			},
		}

		expected := search.Result{
			ID:        strfmt.UUID(id1),
			ClassName: "SomeClass",
			Schema: map[string]interface{}{
				"bar": "some string",
			},
		}

		err := cr.Build(context.Background(), input, selectProps, additional.Properties{})
		require.Nil(t, err)
		res, ok := cr.Get(multi.Identifier{ID: id1, ClassName: "SomeClass"})
		require.True(t, ok)
		assert.Equal(t, expected, res)
		assert.Equal(t, 1, repo.counter, "required the expected amount of lookups")
	})

	t.Run("with a nested lookup, partially resolved", func(t *testing.T) {
		repo := newFakeRepo()
		repo.lookup[multi.Identifier{ID: id1, ClassName: "SomeClass"}] = search.Result{
			ClassName: "SomeClass",
			ID:        strfmt.UUID(id1),
			Schema: map[string]interface{}{
				"primitive": "foobar",
				"ignoredRef": models.MultipleRef{
					&models.SingleRef{
						Beacon: strfmt.URI("weaviate://localhost/ignoreMe"),
					},
				},
				"nestedRef": models.MultipleRef{
					&models.SingleRef{
						Beacon: strfmt.URI(fmt.Sprintf("weaviate://localhost/%s", id2)),
					},
				},
			},
		}
		repo.lookup[multi.Identifier{ID: id2, ClassName: "SomeNestedClass"}] = search.Result{
			ClassName: "SomeNestedClass",
			ID:        strfmt.UUID(id2),
			Schema: map[string]interface{}{
				"name": "John Doe",
			},
		}
		logger, _ := test.NewNullLogger()
		cr := NewCacher(repo, logger)
		input := []search.Result{
			search.Result{
				ID:        "foo",
				ClassName: "BestClass",
				Schema: map[string]interface{}{
					"refProp": models.MultipleRef{
						&models.SingleRef{
							Beacon: strfmt.URI(fmt.Sprintf("weaviate://localhost/%s", id1)),
						},
					},
				},
			},
		}
		selectProps := traverser.SelectProperties{
			traverser.SelectProperty{
				Name: "refProp",
				Refs: []traverser.SelectClass{
					traverser.SelectClass{
						ClassName: "SomeClass",
						RefProperties: traverser.SelectProperties{
							traverser.SelectProperty{
								Name:        "primitive",
								IsPrimitive: true,
							},
							traverser.SelectProperty{
								Name: "nestedRef",
								Refs: []traverser.SelectClass{
									traverser.SelectClass{
										ClassName: "SomeNestedClass",
										RefProperties: []traverser.SelectProperty{
											traverser.SelectProperty{
												Name:        "name",
												IsPrimitive: true,
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

		expectedOuter := search.Result{
			ID:        strfmt.UUID(id1),
			ClassName: "SomeClass",
			Schema: map[string]interface{}{
				"primitive": "foobar",
				"ignoredRef": models.MultipleRef{
					&models.SingleRef{
						Beacon: strfmt.URI("weaviate://localhost/ignoreMe"),
					},
				},
				"nestedRef": models.MultipleRef{
					&models.SingleRef{
						Beacon: strfmt.URI(fmt.Sprintf("weaviate://localhost/%s", id2)),
					},
				},
			},
		}

		expectedInner := search.Result{
			ClassName: "SomeNestedClass",
			ID:        strfmt.UUID(id2),
			Schema: map[string]interface{}{
				"name": "John Doe",
			},
		}

		err := cr.Build(context.Background(), input, selectProps, additional.Properties{})
		require.Nil(t, err)
		res, ok := cr.Get(multi.Identifier{ID: id1, ClassName: "SomeClass"})
		require.True(t, ok)
		assert.Equal(t, expectedOuter, res)
		res, ok = cr.Get(multi.Identifier{ID: id2, ClassName: "SomeNestedClass"})
		require.True(t, ok)
		assert.Equal(t, expectedInner, res)
		assert.Equal(t, 2, repo.counter, "required the expected amount of lookups")
	})

	t.Run("with multiple items pointing to the same ref", func(t *testing.T) {
		// this test asserts that we do not make unnecessary requests if an object
		// is linked twice on the list. (This is very common if the reference is
		// used for something like a product category, e.g. it would not be
		// uncommon at all if all search results are of the same category)
		repo := newFakeRepo()
		repo.lookup[multi.Identifier{ID: id1, ClassName: "SomeClass"}] = search.Result{
			ClassName: "SomeClass",
			ID:        strfmt.UUID(id1),
			Schema: map[string]interface{}{
				"primitive": "foobar",
				"nestedRef": models.MultipleRef{
					&models.SingleRef{
						Beacon: strfmt.URI(fmt.Sprintf("weaviate://localhost/%s", id2)),
					},
				},
			},
		}
		repo.lookup[multi.Identifier{ID: id2, ClassName: "SomeNestedClass"}] = search.Result{
			ClassName: "SomeNestedClass",
			ID:        strfmt.UUID(id2),
			Schema: map[string]interface{}{
				"name": "John Doe",
			},
		}
		logger, _ := test.NewNullLogger()
		cr := NewCacher(repo, logger)

		// contains three items, all pointing to the same inner class
		input := []search.Result{
			search.Result{
				ID:        "foo",
				ClassName: "BestClass",
				Schema: map[string]interface{}{
					"refProp": models.MultipleRef{
						&models.SingleRef{
							Beacon: strfmt.URI(fmt.Sprintf("weaviate://localhost/%s", id1)),
						},
					},
				},
			},
			search.Result{
				ID:        "bar",
				ClassName: "BestClass",
				Schema: map[string]interface{}{
					"refProp": models.MultipleRef{
						&models.SingleRef{
							Beacon: strfmt.URI(fmt.Sprintf("weaviate://localhost/%s", id1)),
						},
					},
				},
			},
			search.Result{
				ID:        "baz",
				ClassName: "BestClass",
				Schema: map[string]interface{}{
					"refProp": models.MultipleRef{
						&models.SingleRef{
							Beacon: strfmt.URI(fmt.Sprintf("weaviate://localhost/%s", id1)),
						},
					},
				},
			},
		}
		selectProps := traverser.SelectProperties{
			traverser.SelectProperty{
				Name: "refProp",
				Refs: []traverser.SelectClass{
					traverser.SelectClass{
						ClassName: "SomeClass",
						RefProperties: traverser.SelectProperties{
							traverser.SelectProperty{
								Name:        "primitive",
								IsPrimitive: true,
							},
							traverser.SelectProperty{
								Name: "nestedRef",
								Refs: []traverser.SelectClass{
									traverser.SelectClass{
										ClassName: "SomeNestedClass",
										RefProperties: []traverser.SelectProperty{
											traverser.SelectProperty{
												Name:        "name",
												IsPrimitive: true,
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

		expectedOuter := search.Result{
			ID:        strfmt.UUID(id1),
			ClassName: "SomeClass",
			Schema: map[string]interface{}{
				"primitive": "foobar",
				"nestedRef": models.MultipleRef{
					&models.SingleRef{
						Beacon: strfmt.URI(fmt.Sprintf("weaviate://localhost/%s", id2)),
					},
				},
			},
		}

		expectedInner := search.Result{
			ClassName: "SomeNestedClass",
			ID:        strfmt.UUID(id2),
			Schema: map[string]interface{}{
				"name": "John Doe",
			},
		}

		err := cr.Build(context.Background(), input, selectProps, additional.Properties{})
		require.Nil(t, err)
		res, ok := cr.Get(multi.Identifier{ID: id1, ClassName: "SomeClass"})
		require.True(t, ok)
		assert.Equal(t, expectedOuter, res)
		res, ok = cr.Get(multi.Identifier{ID: id2, ClassName: "SomeNestedClass"})
		require.True(t, ok)
		assert.Equal(t, expectedInner, res)
		assert.Equal(t, 2, repo.counter, "required the expected amount of lookup queries")
		assert.Equal(t, 2, repo.counter, "required the expected amount of objects on the lookup queries")
	})

	t.Run("with a nested lookup, and nested refs in nested refs", func(t *testing.T) {
		repo := newFakeRepo()
		idNested2ID := "132bdf92-ffec-4a52-9196-73ea7cbb5a00"
		idNestedInNestedID := "132bdf92-ffec-4a52-9196-73ea7cbb5a01"
		repo.lookup[multi.Identifier{ID: id1, ClassName: "SomeClass"}] = search.Result{
			ClassName: "SomeClass",
			ID:        strfmt.UUID(id1),
			Schema: map[string]interface{}{
				"primitive": "foobar",
				"nestedRef": models.MultipleRef{
					&models.SingleRef{
						Beacon: strfmt.URI(fmt.Sprintf("weaviate://localhost/%s", id2)),
					},
				},
				"nestedRef2": models.MultipleRef{
					&models.SingleRef{
						Beacon: strfmt.URI(fmt.Sprintf("weaviate://localhost/%s", idNested2ID)),
						Schema: map[string]interface{}{
							"title": "nestedRef2Title",
							"nestedRefInNestedRef": models.MultipleRef{
								&models.SingleRef{
									Beacon: strfmt.URI(fmt.Sprintf("weaviate://localhost/%s", idNestedInNestedID)),
								},
							},
						},
					},
				},
			},
		}
		repo.lookup[multi.Identifier{ID: id2, ClassName: "SomeNestedClass"}] = search.Result{
			ClassName: "SomeNestedClass",
			ID:        strfmt.UUID(id2),
			Schema: map[string]interface{}{
				"name": "John Doe",
			},
		}
		repo.lookup[multi.Identifier{ID: idNested2ID, ClassName: "SomeNestedClass2"}] = search.Result{
			ClassName: "SomeNestedClass2",
			ID:        strfmt.UUID(idNested2ID),
			Schema: map[string]interface{}{
				"title": "nestedRef2Title",
				"nestedRefInNestedRef": models.MultipleRef{
					&models.SingleRef{
						Beacon: strfmt.URI(fmt.Sprintf("weaviate://localhost/%s", idNestedInNestedID)),
					},
				},
			},
		}
		repo.lookup[multi.Identifier{ID: idNestedInNestedID, ClassName: "SomeNestedClassNested2"}] = search.Result{
			ClassName: "SomeNestedClassNested2",
			ID:        strfmt.UUID(idNestedInNestedID),
			Schema: map[string]interface{}{
				"titleNested": "Nested In Nested Title",
			},
		}
		logger, _ := test.NewNullLogger()
		cr := NewCacher(repo, logger)
		input := []search.Result{
			search.Result{
				ID:        "foo",
				ClassName: "BestClass",
				Schema: map[string]interface{}{
					"refProp": models.MultipleRef{
						&models.SingleRef{
							Beacon: strfmt.URI(fmt.Sprintf("weaviate://localhost/%s", id1)),
						},
					},
				},
			},
		}
		selectProps := traverser.SelectProperties{
			traverser.SelectProperty{
				Name: "refProp",
				Refs: []traverser.SelectClass{
					traverser.SelectClass{
						ClassName: "SomeClass",
						RefProperties: traverser.SelectProperties{
							traverser.SelectProperty{
								Name:        "primitive",
								IsPrimitive: true,
							},
							traverser.SelectProperty{
								Name: "nestedRef",
								Refs: []traverser.SelectClass{
									traverser.SelectClass{
										ClassName: "SomeNestedClass",
										RefProperties: []traverser.SelectProperty{
											traverser.SelectProperty{
												Name:        "name",
												IsPrimitive: true,
											},
										},
									},
								},
							},
							traverser.SelectProperty{
								Name: "nestedRef2",
								Refs: []traverser.SelectClass{
									traverser.SelectClass{
										ClassName: "SomeNestedClass2",
										RefProperties: []traverser.SelectProperty{
											traverser.SelectProperty{
												Name:        "title",
												IsPrimitive: true,
											},
											traverser.SelectProperty{
												Name: "nestedRefInNestedRef",
												Refs: []traverser.SelectClass{
													traverser.SelectClass{
														ClassName: "SomeNestedClassNested2",
														RefProperties: []traverser.SelectProperty{
															traverser.SelectProperty{
																Name:        "titleNested",
																IsPrimitive: true,
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

		expectedOuter := search.Result{
			ID:        strfmt.UUID(id1),
			ClassName: "SomeClass",
			Schema: map[string]interface{}{
				"primitive": "foobar",
				"nestedRef": models.MultipleRef{
					&models.SingleRef{
						Beacon: strfmt.URI(fmt.Sprintf("weaviate://localhost/%s", id2)),
					},
				},
				"nestedRef2": models.MultipleRef{
					&models.SingleRef{
						Beacon: strfmt.URI(fmt.Sprintf("weaviate://localhost/%s", idNested2ID)),
						Schema: map[string]interface{}{
							"title": "nestedRef2Title",
							"nestedRefInNestedRef": models.MultipleRef{
								&models.SingleRef{
									Beacon: strfmt.URI(fmt.Sprintf("weaviate://localhost/%s", idNestedInNestedID)),
								},
							},
						},
					},
				},
			},
		}

		expectedInner := search.Result{
			ClassName: "SomeNestedClass",
			ID:        strfmt.UUID(id2),
			Schema: map[string]interface{}{
				"name": "John Doe",
			},
		}

		expectedInner2 := search.Result{
			ClassName: "SomeNestedClass2",
			ID:        strfmt.UUID(idNested2ID),
			Schema: map[string]interface{}{
				"title": "nestedRef2Title",
				"nestedRefInNestedRef": models.MultipleRef{
					&models.SingleRef{
						Beacon: strfmt.URI(fmt.Sprintf("weaviate://localhost/%s", idNestedInNestedID)),
					},
				},
			},
		}

		expectedInnerInner := search.Result{
			ClassName: "SomeNestedClassNested2",
			ID:        strfmt.UUID(idNestedInNestedID),
			Schema: map[string]interface{}{
				"titleNested": "Nested In Nested Title",
			},
		}

		err := cr.Build(context.Background(), input, selectProps, additional.Properties{})
		require.Nil(t, err)
		res, ok := cr.Get(multi.Identifier{ID: id1, ClassName: "SomeClass"})
		require.True(t, ok)
		assert.Equal(t, expectedOuter, res)
		input2 := []search.Result{expectedInner, expectedInner2}
		err = cr.Build(context.Background(), input2, nil, additional.Properties{})
		require.Nil(t, err)
		nested1, ok := cr.Get(multi.Identifier{ID: id2, ClassName: "SomeNestedClass"})
		require.True(t, ok)
		assert.Equal(t, expectedInner, nested1)
		nested2, ok := cr.Get(multi.Identifier{ID: idNested2ID, ClassName: "SomeNestedClass2"})
		require.True(t, ok)
		assert.Equal(t, expectedInner2, nested2)
		nestedSchema, ok := nested2.Schema.(map[string]interface{})
		require.True(t, ok)
		nestedRefInNestedRef, ok := nestedSchema["nestedRefInNestedRef"]
		require.True(t, ok)
		require.NotNil(t, nestedRefInNestedRef)
		nestedRefInNestedMultiRef, ok := nestedRefInNestedRef.(models.MultipleRef)
		require.True(t, ok)
		require.NotNil(t, nestedRefInNestedMultiRef)
		require.Nil(t, err)
		res, ok = cr.Get(multi.Identifier{ID: idNestedInNestedID, ClassName: "SomeNestedClassNested2"})
		require.True(t, ok)
		assert.Equal(t, expectedInnerInner, res)
		assert.Equal(t, 4, repo.counter, "required the expected amount of lookups")
	})
}

type fakeRepo struct {
	lookup        map[multi.Identifier]search.Result
	counter       int // count request
	objectCounter int // count total objects on request(s)
}

func newFakeRepo() *fakeRepo {
	return &fakeRepo{
		lookup: map[multi.Identifier]search.Result{},
	}
}

func (f *fakeRepo) MultiGet(ctx context.Context, query []multi.Identifier, additional additional.Properties) ([]search.Result, error) {
	f.counter++
	f.objectCounter += len(query)
	out := make([]search.Result, len(query))
	for i, q := range query {
		if res, ok := f.lookup[q]; ok {
			out[i] = res
		} else {
			out[i] = search.Result{}
		}
	}

	return out, nil
}

//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package refcache

import (
	"context"
	"fmt"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/multi"
	"github.com/weaviate/weaviate/entities/search"
)

func TestCacher(t *testing.T) {
	// some ids to be used in the tests, they carry no meaning outside each test
	id1 := "132bdf92-ffec-4a52-9196-73ea7cbb5a5e"
	id2 := "a60a26dc-791a-41fc-8dda-c0f21f90cc98"
	id3 := "a60a26dc-791a-41fc-8dda-c0f21f90cc99"
	id4 := "a60a26dc-791a-41fc-8dda-c0f21f90cc97"

	t.Run("with empty results", func(t *testing.T) {
		repo := newFakeRepo()
		logger, _ := test.NewNullLogger()
		cr := NewCacher(repo, logger, "")
		err := cr.Build(context.Background(), nil, nil, nil)
		assert.Nil(t, err)
	})

	t.Run("with results with nil-schemas", func(t *testing.T) {
		repo := newFakeRepo()
		logger, _ := test.NewNullLogger()
		cr := NewCacher(repo, logger, "")
		input := []search.Result{
			{
				ID:        "foo",
				ClassName: "BestClass",
			},
		}
		err := cr.Build(context.Background(), input, nil, nil)
		assert.Nil(t, err)
	})

	t.Run("with results without refs in the schema", func(t *testing.T) {
		repo := newFakeRepo()
		logger, _ := test.NewNullLogger()
		cr := NewCacher(repo, logger, "")
		input := []search.Result{
			{
				ID:        "foo",
				ClassName: "BestClass",
				Schema: map[string]interface{}{
					"foo": "bar",
					"baz": &models.PhoneNumber{},
				},
			},
		}
		err := cr.Build(context.Background(), input, nil, nil)
		assert.Nil(t, err)
	})

	t.Run("with a single ref, but no selectprops", func(t *testing.T) {
		repo := newFakeRepo()
		logger, _ := test.NewNullLogger()
		cr := NewCacher(repo, logger, "")
		input := []search.Result{
			{
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
		err := cr.Build(context.Background(), input, nil, nil)
		require.Nil(t, err)
		_, ok := cr.Get(multi.Identifier{ID: "123", ClassName: "SomeClass"})
		assert.False(t, ok)
	})

	t.Run("with a single ref, and a matching select prop", func(t *testing.T) {
		refObj := search.Result{
			ID:        "foo",
			ClassName: "BestClass",
			Schema: map[string]interface{}{
				"refProp": models.MultipleRef{
					&models.SingleRef{
						Beacon: strfmt.URI(fmt.Sprintf("weaviate://localhost/%s", id1)),
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
		selectProps := search.SelectProperties{
			search.SelectProperty{
				Name: "refProp",
				Refs: []search.SelectClass{
					{
						ClassName: "SomeClass",
						RefProperties: search.SelectProperties{
							search.SelectProperty{
								Name:        "bar",
								IsPrimitive: true,
							},
						},
					},
				},
			},
		}

		tests := []struct {
			name  string
			input []search.Result
		}{
			{
				name:  "ref object alone",
				input: []search.Result{refObj},
			},
			{
				// nil-schema object must not abort ref discovery for
				// sibling objects
				name: "nil-schema object precedes the ref object",
				input: []search.Result{
					{ID: "nilSchema", ClassName: "BestClass"},
					refObj,
				},
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				repo := newFakeRepo()
				repo.lookup[multi.Identifier{ID: id1, ClassName: "SomeClass"}] = expected
				logger, _ := test.NewNullLogger()
				cr := NewCacher(repo, logger, "")
				err := cr.Build(context.Background(), tt.input, selectProps, nil)
				require.Nil(t, err)
				res, ok := cr.Get(multi.Identifier{ID: id1, ClassName: "SomeClass"})
				require.True(t, ok, "the ref object's ref must be resolved")
				assert.Equal(t, expected, res)
				assert.Equal(t, 1, repo.counter, "required the expected amount of lookups")
			})
		}
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
		cr := NewCacher(repo, logger, "")
		input := []search.Result{
			{
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
		selectProps := search.SelectProperties{
			search.SelectProperty{
				Name: "refProp",
				Refs: []search.SelectClass{
					{
						ClassName: "SomeClass",
						RefProperties: search.SelectProperties{
							search.SelectProperty{
								Name:        "primitive",
								IsPrimitive: true,
							},
							search.SelectProperty{
								Name: "nestedRef",
								Refs: []search.SelectClass{
									{
										ClassName: "SomeNestedClass",
										RefProperties: []search.SelectProperty{
											{
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

		// ignoredRef is not selected, so the read leaves it out
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

		err := cr.Build(context.Background(), input, selectProps, nil)
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
		cr := NewCacher(repo, logger, "")

		// contains three items, all pointing to the same inner class
		input := []search.Result{
			{
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
			{
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
			{
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
		selectProps := search.SelectProperties{
			search.SelectProperty{
				Name: "refProp",
				Refs: []search.SelectClass{
					{
						ClassName: "SomeClass",
						RefProperties: search.SelectProperties{
							search.SelectProperty{
								Name:        "primitive",
								IsPrimitive: true,
							},
							search.SelectProperty{
								Name: "nestedRef",
								Refs: []search.SelectClass{
									{
										ClassName: "SomeNestedClass",
										RefProperties: []search.SelectProperty{
											{
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

		err := cr.Build(context.Background(), input, selectProps, nil)
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
		cr := NewCacher(repo, logger, "")
		input := []search.Result{
			{
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
		selectProps := search.SelectProperties{
			search.SelectProperty{
				Name: "refProp",
				Refs: []search.SelectClass{
					{
						ClassName: "SomeClass",
						RefProperties: search.SelectProperties{
							search.SelectProperty{
								Name:        "primitive",
								IsPrimitive: true,
							},
							search.SelectProperty{
								Name: "nestedRef",
								Refs: []search.SelectClass{
									{
										ClassName: "SomeNestedClass",
										RefProperties: []search.SelectProperty{
											{
												Name:        "name",
												IsPrimitive: true,
											},
										},
									},
								},
							},
							search.SelectProperty{
								Name: "nestedRef2",
								Refs: []search.SelectClass{
									{
										ClassName: "SomeNestedClass2",
										RefProperties: []search.SelectProperty{
											{
												Name:        "title",
												IsPrimitive: true,
											},
											{
												Name: "nestedRefInNestedRef",
												Refs: []search.SelectClass{
													{
														ClassName: "SomeNestedClassNested2",
														RefProperties: []search.SelectProperty{
															{
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

		err := cr.Build(context.Background(), input, selectProps, nil)
		require.Nil(t, err)
		res, ok := cr.Get(multi.Identifier{ID: id1, ClassName: "SomeClass"})
		require.True(t, ok)
		assert.Equal(t, expectedOuter, res)
		input2 := []search.Result{expectedInner, expectedInner2}
		err = cr.Build(context.Background(), input2, nil, nil)
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
		assert.Equal(t, 3, repo.counter, "required the expected amount of lookups")
	})

	t.Run("with group and with a additional group lookup", func(t *testing.T) {
		repo := newFakeRepo()
		repo.lookup[multi.Identifier{ID: id1, ClassName: "SomeClass"}] = search.Result{
			ClassName: "SomeClass",
			ID:        strfmt.UUID(id1),
			Schema: map[string]interface{}{
				"primitive": "foobar",
			},
			AdditionalProperties: models.AdditionalProperties{
				"group": &additional.Group{
					Hits: []map[string]interface{}{
						{
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
		cr := NewCacher(repo, logger, "")
		input := []search.Result{
			{
				ID:        "foo",
				ClassName: "BestClass",
				Schema: map[string]interface{}{
					"refProp": models.MultipleRef{
						&models.SingleRef{
							Beacon: strfmt.URI(fmt.Sprintf("weaviate://localhost/%s", id1)),
						},
					},
				},
				AdditionalProperties: models.AdditionalProperties{
					"group": &additional.Group{
						Hits: []map[string]interface{}{
							{
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
						},
					},
				},
			},
		}
		groupByProps := search.SelectProperties{
			search.SelectProperty{
				Name: "nestedRef",
				Refs: []search.SelectClass{
					{
						ClassName: "SomeNestedClass",
						RefProperties: []search.SelectProperty{
							{
								Name:        "name",
								IsPrimitive: true,
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

		err := cr.Build(context.Background(), input, nil, groupByProps.Indexed())
		require.Nil(t, err)
		res, ok := cr.Get(multi.Identifier{ID: id2, ClassName: "SomeNestedClass"})
		require.True(t, ok)
		assert.Equal(t, expectedInner, res)
		assert.Equal(t, 1, repo.counter, "required the expected amount of lookups")
	})

	t.Run("with group and with 2 additional group lookups", func(t *testing.T) {
		repo := newFakeRepo()
		repo.lookup[multi.Identifier{ID: id1, ClassName: "SomeClass"}] = search.Result{
			ClassName: "SomeClass",
			ID:        strfmt.UUID(id1),
			Schema: map[string]interface{}{
				"primitive": "foobar",
			},
			AdditionalProperties: models.AdditionalProperties{
				"group": &additional.Group{
					Hits: []map[string]interface{}{
						{
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
		repo.lookup[multi.Identifier{ID: id3, ClassName: "OtherNestedClass"}] = search.Result{
			ClassName: "OtherNestedClass",
			ID:        strfmt.UUID(id3),
			Schema: map[string]interface{}{
				"name": "John Doe",
			},
		}
		logger, _ := test.NewNullLogger()
		cr := NewCacher(repo, logger, "")
		input := []search.Result{
			{
				ID:        "foo",
				ClassName: "BestClass",
				Schema: map[string]interface{}{
					"refProp": models.MultipleRef{
						&models.SingleRef{
							Beacon: strfmt.URI(fmt.Sprintf("weaviate://localhost/%s", id1)),
						},
					},
				},
				AdditionalProperties: models.AdditionalProperties{
					"group": &additional.Group{
						Hits: []map[string]interface{}{
							{
								"primitive": "foobar",
								"nestedRef": models.MultipleRef{
									&models.SingleRef{
										Beacon: strfmt.URI(fmt.Sprintf("weaviate://localhost/%s", id2)),
									},
								},
								"otherNestedRef": models.MultipleRef{
									&models.SingleRef{
										Beacon: strfmt.URI(fmt.Sprintf("weaviate://localhost/%s", id3)),
									},
								},
							},
						},
					},
				},
			},
		}
		groupByProps := search.SelectProperties{
			search.SelectProperty{
				Name: "nestedRef",
				Refs: []search.SelectClass{
					{
						ClassName: "SomeNestedClass",
						RefProperties: []search.SelectProperty{
							{
								Name:        "name",
								IsPrimitive: true,
							},
						},
					},
				},
			},
			search.SelectProperty{
				Name: "otherNestedRef",
				Refs: []search.SelectClass{
					{
						ClassName: "OtherNestedClass",
						RefProperties: []search.SelectProperty{
							{
								Name:        "name",
								IsPrimitive: true,
							},
						},
					},
				},
			},
		}

		expectedSomeNestedClass := search.Result{
			ClassName: "SomeNestedClass",
			ID:        strfmt.UUID(id2),
			Schema: map[string]interface{}{
				"name": "John Doe",
			},
		}

		expectedOtherNestedClass := search.Result{
			ClassName: "OtherNestedClass",
			ID:        strfmt.UUID(id3),
			Schema: map[string]interface{}{
				"name": "John Doe",
			},
		}

		err := cr.Build(context.Background(), input, nil, groupByProps.Indexed())
		require.Nil(t, err)
		res, ok := cr.Get(multi.Identifier{ID: id2, ClassName: "SomeNestedClass"})
		require.True(t, ok)
		assert.Equal(t, expectedSomeNestedClass, res)
		res, ok = cr.Get(multi.Identifier{ID: id3, ClassName: "OtherNestedClass"})
		require.True(t, ok)
		assert.Equal(t, expectedOtherNestedClass, res)
		assert.Equal(t, 1, repo.counter, "required the expected amount of lookups")
	})

	t.Run("with group with a nested lookup and with 2 additional group lookups", func(t *testing.T) {
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
		repo.lookup[multi.Identifier{ID: id3, ClassName: "InnerNestedClass"}] = search.Result{
			ClassName: "InnerNestedClass",
			ID:        strfmt.UUID(id3),
			Schema: map[string]interface{}{
				"name": "John Doe",
			},
		}
		repo.lookup[multi.Identifier{ID: id4, ClassName: "OtherNestedClass"}] = search.Result{
			ClassName: "OtherNestedClass",
			ID:        strfmt.UUID(id4),
			Schema: map[string]interface{}{
				"name": "John Doe",
			},
		}
		logger, _ := test.NewNullLogger()
		cr := NewCacher(repo, logger, "")
		input := []search.Result{
			{
				ID:        "foo",
				ClassName: "BestClass",
				Schema: map[string]interface{}{
					"refProp": models.MultipleRef{
						&models.SingleRef{
							Beacon: strfmt.URI(fmt.Sprintf("weaviate://localhost/%s", id1)),
						},
					},
				},
				AdditionalProperties: models.AdditionalProperties{
					"group": &additional.Group{
						Hits: []map[string]interface{}{
							{
								"primitive": "foobar",
								"innerNestedRef": models.MultipleRef{
									&models.SingleRef{
										Beacon: strfmt.URI(fmt.Sprintf("weaviate://localhost/%s", id3)),
									},
								},
								"otherNestedRef": models.MultipleRef{
									&models.SingleRef{
										Beacon: strfmt.URI(fmt.Sprintf("weaviate://localhost/%s", id4)),
									},
								},
							},
						},
					},
				},
			},
		}
		selectProps := search.SelectProperties{
			search.SelectProperty{
				Name: "refProp",
				Refs: []search.SelectClass{
					{
						ClassName: "SomeClass",
						RefProperties: search.SelectProperties{
							search.SelectProperty{
								Name:        "primitive",
								IsPrimitive: true,
							},
							search.SelectProperty{
								Name: "nestedRef",
								Refs: []search.SelectClass{
									{
										ClassName: "SomeNestedClass",
										RefProperties: []search.SelectProperty{
											{
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
		groupByProps := search.SelectProperties{
			search.SelectProperty{
				Name: "innerNestedRef",
				Refs: []search.SelectClass{
					{
						ClassName: "InnerNestedClass",
						RefProperties: []search.SelectProperty{
							{
								Name:        "name",
								IsPrimitive: true,
							},
						},
					},
				},
			},
			search.SelectProperty{
				Name: "otherNestedRef",
				Refs: []search.SelectClass{
					{
						ClassName: "OtherNestedClass",
						RefProperties: []search.SelectProperty{
							{
								Name:        "name",
								IsPrimitive: true,
							},
						},
					},
				},
			},
		}

		// ignoredRef is not selected, so the read leaves it out
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

		expectedInnerNestedClass := search.Result{
			ClassName: "InnerNestedClass",
			ID:        strfmt.UUID(id3),
			Schema: map[string]interface{}{
				"name": "John Doe",
			},
		}

		expectedOtherNestedClass := search.Result{
			ClassName: "OtherNestedClass",
			ID:        strfmt.UUID(id4),
			Schema: map[string]interface{}{
				"name": "John Doe",
			},
		}

		err := cr.Build(context.Background(), input, selectProps, groupByProps.Indexed())
		require.Nil(t, err)
		res, ok := cr.Get(multi.Identifier{ID: id1, ClassName: "SomeClass"})
		require.True(t, ok)
		assert.Equal(t, expectedOuter, res)
		res, ok = cr.Get(multi.Identifier{ID: id2, ClassName: "SomeNestedClass"})
		require.True(t, ok)
		assert.Equal(t, expectedInner, res)
		res, ok = cr.Get(multi.Identifier{ID: id3, ClassName: "InnerNestedClass"})
		require.True(t, ok)
		assert.Equal(t, expectedInnerNestedClass, res)
		res, ok = cr.Get(multi.Identifier{ID: id4, ClassName: "OtherNestedClass"})
		require.True(t, ok)
		assert.Equal(t, expectedOtherNestedClass, res)
		assert.Equal(t, 2, repo.counter, "required the expected amount of lookups")
	})
}

// TestResolveObjectReachedByTwoPaths resolves one object that two reference
// paths of a query reach with different selections. Each path must get what it
// selected, whichever path the cacher meets first.
func TestResolveObjectReachedByTwoPaths(t *testing.T) {
	const (
		person   = "00000000-0000-0000-0000-000000000001"
		friend   = "00000000-0000-0000-0000-000000000002"
		pet      = "00000000-0000-0000-0000-000000000003"
		reviewer = "00000000-0000-0000-0000-000000000004"
	)
	beacon := func(id string) models.MultipleRef {
		return models.MultipleRef{{Beacon: strfmt.URI("weaviate://localhost/" + id)}}
	}
	prim := func(name string) search.SelectProperty {
		return search.SelectProperty{Name: name, IsPrimitive: true}
	}
	ref := func(name, class string, props ...search.SelectProperty) search.SelectProperty {
		return search.SelectProperty{Name: name, Refs: []search.SelectClass{{ClassName: class, RefProperties: props}}}
	}
	withVectors := func(p search.SelectProperty, addl additional.Properties) search.SelectProperty {
		p.Refs[0].AdditionalProperties = addl
		return p
	}
	store := newFakeRepo()
	for _, obj := range []search.Result{
		{ID: person, ClassName: "Person", Vector: []float32{1, 2}, Vectors: models.Vectors{"title": []float32{3, 4}}, Schema: map[string]interface{}{
			"name": "p", "age": 41.0, "friend": beacon(friend),
		}},
		{ID: friend, ClassName: "Person", Vector: []float32{5, 6}, Schema: map[string]interface{}{"name": "f", "pet": beacon(pet)}},
		{ID: pet, ClassName: "Dog", Schema: map[string]interface{}{"name": "d"}},
		{ID: reviewer, ClassName: "Person", Schema: map[string]interface{}{"name": "q", "mentor": beacon(person)}},
	} {
		store.lookup[multi.Identifier{ID: obj.ID.String(), ClassName: obj.ClassName}] = obj
	}

	type check struct {
		article int
		path    []string // reference properties, then the field read on the last object
		want    interface{}
	}
	cases := []struct {
		name     string
		articles []map[string]string // reference property to target id, per article; author and reviewer of person when nil
		props    search.SelectProperties
		checks   []check
		// goneOnReread is a Person id that reads as missing after the first MultiGet
		goneOnReread string
	}{
		{
			name:     "one article, only the second path selects nested refs",
			articles: []map[string]string{{"author": person, "reviewer": person}},
			props: search.SelectProperties{
				ref("author", "Person", prim("name")),
				ref("reviewer", "Person", ref("friend", "Person", prim("name"))),
			},
			checks: []check{
				{0, []string{"author", "name"}, "p"},
				{0, []string{"reviewer", "friend", "name"}, "f"},
			},
		},
		{
			name: "paths select different depths below the shared object",
			props: search.SelectProperties{
				ref("author", "Person", ref("friend", "Person", prim("name"))),
				ref("reviewer", "Person", ref("friend", "Person", ref("pet", "Dog", prim("name")))),
			},
			checks: []check{
				{0, []string{"author", "friend", "name"}, "f"},
				{1, []string{"reviewer", "friend", "pet", "name"}, "d"},
			},
		},
		{
			name:     "object fetched a level earlier selects another property",
			articles: []map[string]string{{"author": person, "reviewer": reviewer}},
			props: search.SelectProperties{
				ref("author", "Person", prim("name")),
				ref("reviewer", "Person", ref("mentor", "Person", prim("age"))),
			},
			checks: []check{
				{0, []string{"author", "name"}, "p"},
				{0, []string{"reviewer", "mentor", "age"}, 41.0},
			},
		},
		{
			name:     "object fetched a level earlier selects a vector",
			articles: []map[string]string{{"author": person, "reviewer": reviewer}},
			props: search.SelectProperties{
				ref("author", "Person", prim("name")),
				ref("reviewer", "Person", withVectors(ref("mentor", "Person", prim("name")), additional.Properties{Vector: true})),
			},
			checks: []check{{0, []string{"reviewer", "mentor", "vector"}, []float32{1, 2}}},
		},
		{
			name:         "object gone when a later level reads it again",
			articles:     []map[string]string{{"author": person, "reviewer": reviewer}},
			goneOnReread: person,
			props: search.SelectProperties{
				ref("author", "Person", prim("name")),
				ref("reviewer", "Person", ref("mentor", "Person", prim("age"))),
			},
			// every path resolves it as missing, so its beacon stays unresolved
			checks: []check{
				{0, []string{"author"}, beacon(person)},
				{0, []string{"reviewer", "mentor"}, beacon(person)},
			},
		},
		{
			name: "vector selected on one path only",
			props: search.SelectProperties{
				ref("author", "Person", prim("name")),
				withVectors(ref("reviewer", "Person", prim("name")), additional.Properties{Vector: true}),
			},
			checks: []check{
				{0, []string{"author", "vector"}, nil},
				{1, []string{"reviewer", "vector"}, []float32{1, 2}},
			},
		},
		{
			name: "named vector selected on one path only",
			props: search.SelectProperties{
				ref("author", "Person", prim("name")),
				withVectors(ref("reviewer", "Person", prim("name")), additional.Properties{Vectors: []string{"title"}}),
			},
			checks: []check{
				{0, []string{"author", "vectors"}, nil},
				{1, []string{"reviewer", "vectors"}, map[string]models.Vector{"title": []float32{3, 4}}},
			},
		},
		{
			name: "nested vector selected on one path only",
			props: search.SelectProperties{
				ref("author", "Person", ref("friend", "Person", prim("name"))),
				ref("reviewer", "Person", withVectors(ref("friend", "Person", prim("name")), additional.Properties{Vector: true})),
			},
			checks: []check{{1, []string{"reviewer", "friend", "vector"}, []float32{5, 6}}},
		},
		{
			name:     "paths select different targets of a multi-target ref",
			articles: []map[string]string{{"author": friend}, {"reviewer": friend}},
			props: search.SelectProperties{
				ref("author", "Person", ref("pet", "Cat", prim("name"))),
				ref("reviewer", "Person", ref("pet", "Dog", prim("name"))),
			},
			checks: []check{{1, []string{"reviewer", "pet", "name"}, "d"}},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.articles == nil {
				tc.articles = []map[string]string{{"author": person}, {"reviewer": person}}
			}
			// the loop repeats because the cacher walks an object's properties in map order
			for range 20 {
				articles := make([]search.Result, len(tc.articles))
				for i, refs := range tc.articles {
					schema := map[string]interface{}{}
					for prop, id := range refs {
						schema[prop] = beacon(id)
					}
					articles[i] = search.Result{ID: strfmt.UUID(fmt.Sprintf("10000000-0000-0000-0000-%012d", i)), ClassName: "Article", Schema: schema}
				}
				var r repo = store
				if tc.goneOnReread != "" {
					r = &goneOnRereadRepo{fakeRepo: store, gone: multi.Identifier{ID: tc.goneOnReread, ClassName: "Person"}}
				}
				logger, _ := test.NewNullLogger()
				res, err := NewResolver(NewCacher(r, logger, "")).Do(context.Background(), articles, tc.props)
				require.NoError(t, err)

				for _, c := range tc.checks {
					fields := res[c.article].Schema.(map[string]interface{})
					for _, prop := range c.path[:len(c.path)-1] {
						refs, ok := fields[prop].([]interface{})
						require.Truef(t, ok, "%v: %q is not resolved, got %T", c.path, prop, fields[prop])
						require.Len(t, refs, 1)
						fields = refs[0].(search.LocalRef).Fields
					}
					assert.Equal(t, c.want, fields[c.path[len(c.path)-1]], "%v", c.path)
				}
			}
		})
	}
}

// goneOnRereadRepo reads like fakeRepo, except that gone reads as missing
// after the first MultiGet.
type goneOnRereadRepo struct {
	*fakeRepo
	gone  multi.Identifier
	calls int
}

func (g *goneOnRereadRepo) MultiGet(ctx context.Context, query []multi.Identifier,
	selections map[string]search.SelectClass, tenant string,
) ([]search.Result, error) {
	res, err := g.fakeRepo.MultiGet(ctx, query, selections, tenant)
	g.calls++
	for i, q := range query {
		if g.calls > 1 && q == g.gone {
			res[i] = search.Result{}
		}
	}
	return res, err
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

// MultiGet returns each object with only the properties and vectors its
// class's selection names.
func (f *fakeRepo) MultiGet(ctx context.Context, query []multi.Identifier,
	selections map[string]search.SelectClass, tenant string,
) ([]search.Result, error) {
	f.counter++
	f.objectCounter += len(query)
	out := make([]search.Result, len(query))
	for i, q := range query {
		res, ok := f.lookup[q]
		if !ok {
			continue
		}
		sel, ok := selections[q.ClassName]
		if !ok {
			return nil, fmt.Errorf("no selection for class %q", q.ClassName)
		}
		schema := map[string]interface{}{}
		for _, name := range sel.RefProperties.GetPropertyNames() {
			if v, ok := res.Schema.(map[string]interface{})[name]; ok {
				schema[name] = v
			}
		}
		res.Schema = schema
		if !sel.AdditionalProperties.Vector {
			res.Vector = nil
		}
		var vectors models.Vectors
		for _, name := range sel.AdditionalProperties.Vectors {
			if v, ok := res.Vectors[name]; ok {
				if vectors == nil {
					vectors = models.Vectors{}
				}
				vectors[name] = v
			}
		}
		res.Vectors = vectors
		out[i] = res
	}

	return out, nil
}

// BenchmarkCacherBuildManyRefs resolves one reference per root object, the
// shape of a search that returns many objects with a ref property selected.
func BenchmarkCacherBuildManyRefs(b *testing.B) {
	for _, n := range []int{1000, 10000, 50000} {
		b.Run(fmt.Sprintf("refs=%d", n), func(b *testing.B) {
			selectProps := search.SelectProperties{{
				Name: "refProp",
				Refs: []search.SelectClass{{
					ClassName:     "SomeClass",
					RefProperties: search.SelectProperties{{Name: "bar", IsPrimitive: true}},
				}},
			}}
			repo := newFakeRepo()
			input := make([]search.Result, n)
			for i := range input {
				id := strfmt.UUID(fmt.Sprintf("00000000-0000-0000-0000-%012d", i))
				repo.lookup[multi.Identifier{ID: id.String(), ClassName: "SomeClass"}] = search.Result{
					ID: id, ClassName: "SomeClass", Schema: map[string]interface{}{"bar": "x"},
				}
				input[i] = search.Result{
					ID:        strfmt.UUID(fmt.Sprintf("10000000-0000-0000-0000-%012d", i)),
					ClassName: "BestClass",
					Schema: map[string]interface{}{
						"refProp": models.MultipleRef{{Beacon: strfmt.URI("weaviate://localhost/" + id)}},
					},
				}
			}
			logger, _ := test.NewNullLogger()
			b.ResetTimer()
			for range b.N {
				cr := NewCacher(repo, logger, "")
				require.NoError(b, cr.Build(context.Background(), input, selectProps, nil))
			}
		})
	}
}

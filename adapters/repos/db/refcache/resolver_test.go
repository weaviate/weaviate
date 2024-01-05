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

package refcache

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/multi"
	"github.com/weaviate/weaviate/entities/search"
)

func TestResolver(t *testing.T) {
	id1 := "df5d4e49-0c56-4b87-ade1-3d46cc9b425f"
	id2 := "3a08d808-8eb5-49ee-86b2-68b6035e8b69"

	t.Run("with nil input", func(t *testing.T) {
		r := NewResolver(newFakeCacher())
		res, err := r.Do(context.Background(), nil, nil, additional.Properties{})
		require.Nil(t, err)
		assert.Nil(t, res)
	})

	t.Run("with nil-schemas", func(t *testing.T) {
		r := NewResolver(newFakeCacher())
		input := []search.Result{
			{
				ID:        "foo",
				ClassName: "BestClass",
			},
		}

		expected := input
		res, err := r.Do(context.Background(), input, nil, additional.Properties{})
		require.Nil(t, err)
		assert.Equal(t, expected, res)
	})

	t.Run("with single ref but no select props", func(t *testing.T) {
		r := NewResolver(newFakeCacher())
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

		expected := input
		res, err := r.Do(context.Background(), input, nil, additional.Properties{})
		require.Nil(t, err)
		assert.Equal(t, expected, res)
	})

	t.Run("with single ref with vector and matching select prop", func(t *testing.T) {
		getInput := func() []search.Result {
			return []search.Result{
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
		}
		getResolver := func() *Resolver {
			cacher := newFakeCacher()
			r := NewResolver(cacher)
			cacher.lookup[multi.Identifier{ID: id1, ClassName: "SomeClass"}] = search.Result{
				ClassName: "SomeClass",
				ID:        strfmt.UUID(id1),
				Schema: map[string]interface{}{
					"bar": "some string",
				},
				Vector: []float32{0.1, 0.2},
			}
			return r
		}
		getSelectProps := func(withVector bool) search.SelectProperties {
			return search.SelectProperties{
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
							AdditionalProperties: additional.Properties{
								Vector: withVector,
							},
						},
					},
				},
			}
		}
		getExpectedResult := func(withVector bool) []search.Result {
			fields := map[string]interface{}{
				"bar": "some string",
			}
			if withVector {
				fields["vector"] = []float32{0.1, 0.2}
			}
			return []search.Result{
				{
					ID:        "foo",
					ClassName: "BestClass",
					Schema: map[string]interface{}{
						"refProp": []interface{}{
							search.LocalRef{
								Class:  "SomeClass",
								Fields: fields,
							},
						},
					},
				},
			}
		}
		// ask for vector in ref property
		res, err := getResolver().Do(context.Background(), getInput(), getSelectProps(true), additional.Properties{})
		require.Nil(t, err)
		assert.Equal(t, getExpectedResult(true), res)
		// don't ask for vector in ref property
		res, err = getResolver().Do(context.Background(), getInput(), getSelectProps(false), additional.Properties{})
		require.Nil(t, err)
		assert.Equal(t, getExpectedResult(false), res)
	})

	t.Run("with single ref with creation/update timestamps and matching select prop", func(t *testing.T) {
		now := time.Now().UnixMilli()
		getInput := func() []search.Result {
			return []search.Result{
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
		}
		getResolver := func() *Resolver {
			cacher := newFakeCacher()
			r := NewResolver(cacher)
			cacher.lookup[multi.Identifier{ID: id1, ClassName: "SomeClass"}] = search.Result{
				ClassName: "SomeClass",
				ID:        strfmt.UUID(id1),
				Schema: map[string]interface{}{
					"bar": "some string",
				},
				Created: now,
				Updated: now,
			}
			return r
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
						AdditionalProperties: additional.Properties{
							CreationTimeUnix:   true,
							LastUpdateTimeUnix: true,
						},
					},
				},
			},
		}
		expected := []search.Result{
			{
				ID:        "foo",
				ClassName: "BestClass",
				Schema: map[string]interface{}{
					"refProp": []interface{}{
						search.LocalRef{
							Class: "SomeClass",
							Fields: map[string]interface{}{
								"bar":                "some string",
								"creationTimeUnix":   now,
								"lastUpdateTimeUnix": now,
							},
						},
					},
				},
			},
		}
		res, err := getResolver().Do(context.Background(), getInput(), selectProps, additional.Properties{})
		require.Nil(t, err)
		assert.Equal(t, expected, res)
	})

	t.Run("with single ref and matching select prop", func(t *testing.T) {
		cacher := newFakeCacher()
		r := NewResolver(cacher)
		cacher.lookup[multi.Identifier{ID: id1, ClassName: "SomeClass"}] = search.Result{
			ClassName: "SomeClass",
			ID:        strfmt.UUID(id1),
			Schema: map[string]interface{}{
				"bar": "some string",
			},
		}
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
								Name:        "bar",
								IsPrimitive: true,
							},
						},
					},
				},
			},
		}

		expected := []search.Result{
			{
				ID:        "foo",
				ClassName: "BestClass",
				Schema: map[string]interface{}{
					"refProp": []interface{}{
						search.LocalRef{
							Class: "SomeClass",
							Fields: map[string]interface{}{
								"bar": "some string",
							},
						},
					},
				},
			},
		}
		res, err := r.Do(context.Background(), input, selectProps, additional.Properties{})
		require.Nil(t, err)
		assert.Equal(t, expected, res)
	})

	t.Run("with a nested lookup", func(t *testing.T) {
		cacher := newFakeCacher()
		r := NewResolver(cacher)
		cacher.lookup[multi.Identifier{ID: id1, ClassName: "SomeClass"}] = search.Result{
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
		cacher.lookup[multi.Identifier{ID: id2, ClassName: "SomeNestedClass"}] = search.Result{
			ClassName: "SomeNestedClass",
			ID:        strfmt.UUID(id2),
			Schema: map[string]interface{}{
				"name": "John Doe",
			},
		}
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

		expected := []search.Result{
			{
				ID:        "foo",
				ClassName: "BestClass",
				Schema: map[string]interface{}{
					"refProp": []interface{}{
						search.LocalRef{
							Class: "SomeClass",
							Fields: map[string]interface{}{
								"primitive": "foobar",
								"ignoredRef": models.MultipleRef{
									&models.SingleRef{
										Beacon: strfmt.URI("weaviate://localhost/ignoreMe"),
									},
								},
								"nestedRef": []interface{}{
									search.LocalRef{
										Class: "SomeNestedClass",
										Fields: map[string]interface{}{
											"name": "John Doe",
										},
									},
								},
							},
						},
					},
				},
			},
		}
		res, err := r.Do(context.Background(), input, selectProps, additional.Properties{})
		require.Nil(t, err)
		assert.Equal(t, expected, res)
	})

	t.Run("with single ref with vector and matching select prop and group", func(t *testing.T) {
		getInput := func() []search.Result {
			return []search.Result{
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
									"nestedRef": models.MultipleRef{
										&models.SingleRef{
											Beacon: strfmt.URI(fmt.Sprintf("weaviate://localhost/SomeNestedClass/%s", id2)),
										},
									},
								},
							},
						},
					},
				},
			}
		}
		getResolver := func() *Resolver {
			cacher := newFakeCacher()
			r := NewResolverWithGroup(cacher)
			cacher.lookup[multi.Identifier{ID: id1, ClassName: "SomeClass"}] = search.Result{
				ClassName: "SomeClass",
				ID:        strfmt.UUID(id1),
				Schema: map[string]interface{}{
					"bar": "some string",
				},
				Vector: []float32{0.1, 0.2},
			}
			cacher.lookup[multi.Identifier{ID: id2, ClassName: "SomeNestedClass"}] = search.Result{
				ClassName: "SomeNestedClass",
				ID:        strfmt.UUID(id2),
				Schema: map[string]interface{}{
					"name": "John Doe",
				},
			}
			return r
		}
		getSelectProps := func(withVector bool) search.SelectProperties {
			return search.SelectProperties{
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
							AdditionalProperties: additional.Properties{
								Vector: withVector,
							},
						},
					},
				},
				search.SelectProperty{
					Name: "_additional:group:hits:nestedRef",
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
		}
		getExpectedResult := func(withVector bool) []search.Result {
			fields := map[string]interface{}{
				"bar": "some string",
			}
			if withVector {
				fields["vector"] = []float32{0.1, 0.2}
			}
			return []search.Result{
				{
					ID:        "foo",
					ClassName: "BestClass",
					Schema: map[string]interface{}{
						"refProp": []interface{}{
							search.LocalRef{
								Class:  "SomeClass",
								Fields: fields,
							},
						},
					},
					AdditionalProperties: models.AdditionalProperties{
						"group": &additional.Group{
							Hits: []map[string]interface{}{
								{
									"nestedRef": []interface{}{
										search.LocalRef{
											Class: "SomeNestedClass",
											Fields: map[string]interface{}{
												"name": "John Doe",
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
		// ask for vector in ref property
		res, err := getResolver().Do(context.Background(), getInput(), getSelectProps(true), additional.Properties{})
		require.Nil(t, err)
		assert.Equal(t, getExpectedResult(true), res)
		// don't ask for vector in ref property
		res, err = getResolver().Do(context.Background(), getInput(), getSelectProps(false), additional.Properties{})
		require.Nil(t, err)
		assert.Equal(t, getExpectedResult(false), res)
	})
}

func newFakeCacher() *fakeCacher {
	return &fakeCacher{
		lookup: map[multi.Identifier]search.Result{},
	}
}

type fakeCacher struct {
	lookup map[multi.Identifier]search.Result
}

func (f *fakeCacher) Build(ctx context.Context, objects []search.Result, properties search.SelectProperties,
	additional additional.Properties,
) error {
	return nil
}

func (f *fakeCacher) Get(si multi.Identifier) (search.Result, bool) {
	res, ok := f.lookup[si]
	return res, ok
}

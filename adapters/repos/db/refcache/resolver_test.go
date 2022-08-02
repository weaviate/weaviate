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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

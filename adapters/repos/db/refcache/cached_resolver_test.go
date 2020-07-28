package refcache

import (
	"context"
	"fmt"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/multi"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
	"github.com/semi-technologies/weaviate/entities/search"
	"github.com/semi-technologies/weaviate/usecases/traverser"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCachedResolver(t *testing.T) {
	// some ids to be used in the tests, they carry no meaning outside each test
	id1 := "132bdf92-ffec-4a52-9196-73ea7cbb5a5e"
	id2 := "a60a26dc-791a-41fc-8dda-c0f21f90cc98"

	t.Run("with empty results", func(t *testing.T) {
		repo := newFakeRepo()
		logger, _ := test.NewNullLogger()
		cr := NewCachedResolver(repo, logger)
		err := cr.Build(context.Background(), nil, nil, false)
		assert.Nil(t, err)
	})

	t.Run("with results with nil-schemas", func(t *testing.T) {
		repo := newFakeRepo()
		logger, _ := test.NewNullLogger()
		cr := NewCachedResolver(repo, logger)
		input := []search.Result{
			search.Result{
				ID:        "foo",
				ClassName: "BestClass",
			},
		}
		err := cr.Build(context.Background(), input, nil, false)
		assert.Nil(t, err)
	})

	t.Run("with results without refs in the schema", func(t *testing.T) {
		repo := newFakeRepo()
		logger, _ := test.NewNullLogger()
		cr := NewCachedResolver(repo, logger)
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
		err := cr.Build(context.Background(), input, nil, false)
		assert.Nil(t, err)
	})

	t.Run("with a single ref, but no selectprops", func(t *testing.T) {
		repo := newFakeRepo()
		logger, _ := test.NewNullLogger()
		cr := NewCachedResolver(repo, logger)
		input := []search.Result{
			search.Result{
				ID:        "foo",
				ClassName: "BestClass",
				Schema: map[string]interface{}{
					"refProp": models.MultipleRef{
						&models.SingleRef{
							Beacon: "weaviate://localhost/things/123",
						},
					},
				},
			},
		}
		err := cr.Build(context.Background(), input, nil, false)
		require.Nil(t, err)
		_, ok := cr.Get(multi.Identifier{Id: "123", Kind: kind.Thing, ClassName: "SomeClass"})
		assert.False(t, ok)
	})

	t.Run("with a single ref, and a matching select prop", func(t *testing.T) {
		repo := newFakeRepo()
		repo.lookup[multi.Identifier{Id: id1, Kind: kind.Thing, ClassName: "SomeClass"}] = search.Result{
			ClassName: "SomeClass",
			ID:        strfmt.UUID(id1),
			Kind:      kind.Thing,
			Schema: map[string]interface{}{
				"bar": "some string",
			},
		}
		logger, _ := test.NewNullLogger()
		cr := NewCachedResolver(repo, logger)
		input := []search.Result{
			search.Result{
				ID:        "foo",
				ClassName: "BestClass",
				Schema: map[string]interface{}{
					"refProp": models.MultipleRef{
						&models.SingleRef{
							Beacon: strfmt.URI(fmt.Sprintf("weaviate://localhost/things/%s", id1)),
						},
					},
				},
			},
		}
		selectProps := traverser.SelectProperties{
			traverser.SelectProperty{
				Name: "RefProp",
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
			Kind:      kind.Thing,
			ClassName: "SomeClass",
			Schema: map[string]interface{}{
				"bar": "some string",
			},
		}

		err := cr.Build(context.Background(), input, selectProps, false)
		require.Nil(t, err)
		res, ok := cr.Get(multi.Identifier{Id: id1, Kind: kind.Thing, ClassName: "SomeClass"})
		require.True(t, ok)
		assert.Equal(t, expected, res)
		assert.Equal(t, 1, repo.counter, "required the expected amount of lookups")
	})

	t.Run("with a nested lookup, partially resolved", func(t *testing.T) {
		repo := newFakeRepo()
		repo.lookup[multi.Identifier{Id: id1, Kind: kind.Thing, ClassName: "SomeClass"}] = search.Result{
			ClassName: "SomeClass",
			ID:        strfmt.UUID(id1),
			Kind:      kind.Thing,
			Schema: map[string]interface{}{
				"primitive": "foobar",
				"ignoredRef": models.MultipleRef{
					&models.SingleRef{
						Beacon: strfmt.URI(fmt.Sprintf("weaviate://localhost/things/ignoreMe")),
					},
				},
				"nestedRef": models.MultipleRef{
					&models.SingleRef{
						Beacon: strfmt.URI(fmt.Sprintf("weaviate://localhost/things/%s", id2)),
					},
				},
			},
		}
		repo.lookup[multi.Identifier{Id: id2, Kind: kind.Thing, ClassName: "SomeNestedClass"}] = search.Result{
			ClassName: "SomeNestedClass",
			ID:        strfmt.UUID(id2),
			Kind:      kind.Thing,
			Schema: map[string]interface{}{
				"name": "John Doe",
			},
		}
		logger, _ := test.NewNullLogger()
		cr := NewCachedResolver(repo, logger)
		input := []search.Result{
			search.Result{
				ID:        "foo",
				ClassName: "BestClass",
				Schema: map[string]interface{}{
					"refProp": models.MultipleRef{
						&models.SingleRef{
							Beacon: strfmt.URI(fmt.Sprintf("weaviate://localhost/things/%s", id1)),
						},
					},
				},
			},
		}
		selectProps := traverser.SelectProperties{
			traverser.SelectProperty{
				Name: "RefProp",
				Refs: []traverser.SelectClass{
					traverser.SelectClass{
						ClassName: "SomeClass",
						RefProperties: traverser.SelectProperties{
							traverser.SelectProperty{
								Name:        "primitive",
								IsPrimitive: true,
							},
							traverser.SelectProperty{
								Name: "NestedRef",
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
			Kind:      kind.Thing,
			ClassName: "SomeClass",
			Schema: map[string]interface{}{
				"primitive": "foobar",
				"ignoredRef": models.MultipleRef{
					&models.SingleRef{
						Beacon: strfmt.URI(fmt.Sprintf("weaviate://localhost/things/ignoreMe")),
					},
				},
				"nestedRef": models.MultipleRef{
					&models.SingleRef{
						Beacon: strfmt.URI(fmt.Sprintf("weaviate://localhost/things/%s", id2)),
					},
				},
			},
		}

		expectedInner := search.Result{
			ClassName: "SomeNestedClass",
			ID:        strfmt.UUID(id2),
			Kind:      kind.Thing,
			Schema: map[string]interface{}{
				"name": "John Doe",
			},
		}

		err := cr.Build(context.Background(), input, selectProps, false)
		require.Nil(t, err)
		res, ok := cr.Get(multi.Identifier{Id: id1, Kind: kind.Thing, ClassName: "SomeClass"})
		require.True(t, ok)
		assert.Equal(t, expectedOuter, res)
		res, ok = cr.Get(multi.Identifier{Id: id2, Kind: kind.Thing, ClassName: "SomeNestedClass"})
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
		repo.lookup[multi.Identifier{Id: id1, Kind: kind.Thing, ClassName: "SomeClass"}] = search.Result{
			ClassName: "SomeClass",
			ID:        strfmt.UUID(id1),
			Kind:      kind.Thing,
			Schema: map[string]interface{}{
				"primitive": "foobar",
				"nestedRef": models.MultipleRef{
					&models.SingleRef{
						Beacon: strfmt.URI(fmt.Sprintf("weaviate://localhost/things/%s", id2)),
					},
				},
			},
		}
		repo.lookup[multi.Identifier{Id: id2, Kind: kind.Thing, ClassName: "SomeNestedClass"}] = search.Result{
			ClassName: "SomeNestedClass",
			ID:        strfmt.UUID(id2),
			Kind:      kind.Thing,
			Schema: map[string]interface{}{
				"name": "John Doe",
			},
		}
		logger, _ := test.NewNullLogger()
		cr := NewCachedResolver(repo, logger)

		// contains three items, all pointing to the same inner class
		input := []search.Result{
			search.Result{
				ID:        "foo",
				ClassName: "BestClass",
				Schema: map[string]interface{}{
					"refProp": models.MultipleRef{
						&models.SingleRef{
							Beacon: strfmt.URI(fmt.Sprintf("weaviate://localhost/things/%s", id1)),
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
							Beacon: strfmt.URI(fmt.Sprintf("weaviate://localhost/things/%s", id1)),
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
							Beacon: strfmt.URI(fmt.Sprintf("weaviate://localhost/things/%s", id1)),
						},
					},
				},
			},
		}
		selectProps := traverser.SelectProperties{
			traverser.SelectProperty{
				Name: "RefProp",
				Refs: []traverser.SelectClass{
					traverser.SelectClass{
						ClassName: "SomeClass",
						RefProperties: traverser.SelectProperties{
							traverser.SelectProperty{
								Name:        "primitive",
								IsPrimitive: true,
							},
							traverser.SelectProperty{
								Name: "NestedRef",
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
			Kind:      kind.Thing,
			ClassName: "SomeClass",
			Schema: map[string]interface{}{
				"primitive": "foobar",
				"nestedRef": models.MultipleRef{
					&models.SingleRef{
						Beacon: strfmt.URI(fmt.Sprintf("weaviate://localhost/things/%s", id2)),
					},
				},
			},
		}

		expectedInner := search.Result{
			ClassName: "SomeNestedClass",
			ID:        strfmt.UUID(id2),
			Kind:      kind.Thing,
			Schema: map[string]interface{}{
				"name": "John Doe",
			},
		}

		err := cr.Build(context.Background(), input, selectProps, false)
		require.Nil(t, err)
		res, ok := cr.Get(multi.Identifier{Id: id1, Kind: kind.Thing, ClassName: "SomeClass"})
		require.True(t, ok)
		assert.Equal(t, expectedOuter, res)
		res, ok = cr.Get(multi.Identifier{Id: id2, Kind: kind.Thing, ClassName: "SomeNestedClass"})
		require.True(t, ok)
		assert.Equal(t, expectedInner, res)
		assert.Equal(t, 2, repo.counter, "required the expected amount of lookup queries")
		assert.Equal(t, 2, repo.counter, "required the expected amount of objects on the lookup queries")
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

func (f *fakeRepo) MultiGet(ctx context.Context, query []multi.Identifier) ([]search.Result, error) {
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

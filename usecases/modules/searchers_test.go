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

package modules

import (
	"context"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/modulecomponents/generictypes"
)

func TestModulesWithSearchers(t *testing.T) {
	sch := schema.Schema{
		Objects: &models.Schema{
			Classes: []*models.Class{
				{
					Class:      "MyClass",
					Vectorizer: "mod",
					ModuleConfig: map[string]interface{}{
						"mod": map[string]interface{}{
							"some-config": "some-config-value",
						},
					},
				},
			},
		},
	}
	logger, _ := test.NewNullLogger()

	t.Run("get a vector for a class", func(t *testing.T) {
		p := NewProvider(logger, config.Config{})
		p.SetSchemaGetter(&fakeSchemaGetter{
			schema: sch,
		})
		p.Register(newSearcherModule[[]float32]("mod").
			withArg("nearGrape").
			withSearcher("nearGrape", generictypes.VectorForParams(func(ctx context.Context, params interface{},
				className string,
				findVectorFn modulecapabilities.FindVectorFn[[]float32],
				cfg moduletools.ClassConfig,
			) ([]float32, error) {
				// verify that the config tool is set, as this is a per-class search,
				// so it must be set
				assert.NotNil(t, cfg)

				// take the findVectorFn and append one dimension. This doesn't make too
				// much sense, but helps verify that the modules method was used in the
				// decisions
				initial, _, _ := findVectorFn.FindVector(ctx, "class", "123", "", "")
				return append(initial, 4), nil
			})),
		)
		p.Init(context.Background(), nil, logger)

		res, err := p.VectorFromSearchParam(context.Background(), "MyClass", "", "",
			"nearGrape", nil, generictypes.FindVectorFn(fakeFindVector))

		require.Nil(t, err)
		assert.Equal(t, []float32{1, 2, 3, 4}, res)
	})

	t.Run("no module configured for a class", func(t *testing.T) {
		p := NewProvider(logger, config.Config{})
		p.SetSchemaGetter(&fakeSchemaGetter{
			schema: sch,
		})
		p.Register(newSearcherModule[[]float32]("mod").
			withArg("nearGrape").
			withSearcher("nearGrape", generictypes.VectorForParams(func(ctx context.Context, params interface{},
				className string,
				findVectorFn modulecapabilities.FindVectorFn[[]float32],
				cfg moduletools.ClassConfig,
			) ([]float32, error) {
				// verify that the config tool is set, as this is a per-class search,
				// so it must be set
				assert.NotNil(t, cfg)

				// take the findVectorFn and append one dimension. This doesn't make too
				// much sense, but helps verify that the modules method was used in the
				// decisions
				initial, _, _ := findVectorFn.FindVector(ctx, "class", "123", "", "")
				return append(initial, 4), nil
			})),
		)
		p.Init(context.Background(), nil, logger)

		_, err := p.VectorFromSearchParam(context.Background(), "MyClass", "", "",
			"nearDoesNotExist", nil, generictypes.FindVectorFn(fakeFindVector))

		require.NotNil(t, err)
		assert.Contains(t, err.Error(), "could not vectorize input for collection")
	})

	t.Run("get a vector across classes", func(t *testing.T) {
		p := NewProvider(logger, config.Config{})
		p.SetSchemaGetter(&fakeSchemaGetter{
			schema: sch,
		})
		p.Register(newSearcherModule[[]float32]("mod").
			withArg("nearGrape").
			withSearcher("nearGrape", generictypes.VectorForParams(func(ctx context.Context, params interface{},
				className string,
				findVectorFn modulecapabilities.FindVectorFn[[]float32],
				cfg moduletools.ClassConfig,
			) ([]float32, error) {
				// this is a cross-class search, such as is used for Explore{}, in this
				// case we do not have class-based config, but we need at least pass
				// a tenant information, that's why we pass an empty config with empty tenant
				// so that it would be possible to perform cross class searches, without
				// tenant context. Modules must be able to deal with this situation!
				assert.NotNil(t, cfg)
				assert.Equal(t, "", cfg.Tenant())

				// take the findVectorFn and append one dimension. This doesn't make too
				// much sense, but helps verify that the modules method was used in the
				// decisions
				initial, _, _ := findVectorFn.FindVector(ctx, "class", "123", "", "")
				return append(initial, 4), nil
			})),
		)
		p.Init(context.Background(), nil, logger)

		res, targetVector, err := p.CrossClassVectorFromSearchParam(context.Background(),
			"nearGrape", nil, generictypes.FindVectorFn(fakeFindVector))

		require.Nil(t, err)
		assert.Equal(t, []float32{1, 2, 3, 4}, res)
		assert.Equal(t, "", targetVector)
	})

	t.Run("explore no vectorizer", func(t *testing.T) {
		p := NewProvider(logger, config.Config{})
		p.SetSchemaGetter(&fakeSchemaGetter{
			schema: sch,
		})
		p.Register(newSearcherModule[[]float32]("mod").
			withArg("nearGrape").
			withSearcher("nearGrape", generictypes.VectorForParams(func(ctx context.Context, params interface{},
				className string,
				findVectorFn modulecapabilities.FindVectorFn[[]float32],
				cfg moduletools.ClassConfig,
			) ([]float32, error) {
				// this is a cross-class search, such as is used for Explore{}, in this
				// case we do not have class-based config, but we need at least pass
				// a tenant information, that's why we pass an empty config with empty tenant
				// so that it would be possible to perform cross class searches, without
				// tenant context. Modules must be able to deal with this situation!
				assert.NotNil(t, cfg)
				assert.Equal(t, "", cfg.Tenant())

				// take the findVectorFn and append one dimension. This doesn't make too
				// much sense, but helps verify that the modules method was used in the
				// decisions
				initial, _, _ := findVectorFn.FindVector(ctx, "class", "123", "", "")
				return append(initial, 4), nil
			})),
		)
		p.Init(context.Background(), nil, logger)

		_, _, err := p.CrossClassVectorFromSearchParam(context.Background(),
			"nearDoesNotExist", nil, generictypes.FindVectorFn(fakeFindVector))

		require.NotNil(t, err)
	})

	t.Run("get a multi vector for a class", func(t *testing.T) {
		p := NewProvider(logger, config.Config{})
		p.SetSchemaGetter(&fakeSchemaGetter{
			schema: sch,
		})
		p.Register(newSearcherModule[[][]float32]("mod").
			withArg("nearGrape").
			withSearcher("nearGrape", generictypes.MultiVectorForParams(func(ctx context.Context, params interface{},
				className string,
				findVectorFn modulecapabilities.FindVectorFn[[][]float32],
				cfg moduletools.ClassConfig,
			) ([][]float32, error) {
				// verify that the config tool is set, as this is a per-class search,
				// so it must be set
				assert.NotNil(t, cfg)

				// take the findVectorFn and append one dimension. This doesn't make too
				// much sense, but helps verify that the modules method was used in the
				// decisions
				initial, _, _ := findVectorFn.FindVector(ctx, "class", "123", "", "")
				return initial, nil
			})),
		)
		p.Init(context.Background(), nil, logger)

		res, err := p.MultiVectorFromSearchParam(context.Background(), "MyClass", "", "",
			"nearGrape", nil, generictypes.MultiFindVectorFn(multiFakeFindVector))

		require.Nil(t, err)
		assert.Equal(t, [][]float32{{0.1, 0.2, 0.3}, {0.11, 0.22, 0.33}}, res)
	})

	t.Run("get a multi vector across classes", func(t *testing.T) {
		p := NewProvider(logger, config.Config{})
		p.SetSchemaGetter(&fakeSchemaGetter{
			schema: sch,
		})
		p.Register(newSearcherModule[[][]float32]("mod").
			withArg("nearGrape").
			withSearcher("nearGrape", generictypes.MultiVectorForParams(func(ctx context.Context, params interface{},
				className string,
				findVectorFn modulecapabilities.FindVectorFn[[][]float32],
				cfg moduletools.ClassConfig,
			) ([][]float32, error) {
				// this is a cross-class search, such as is used for Explore{}, in this
				// case we do not have class-based config, but we need at least pass
				// a tenant information, that's why we pass an empty config with empty tenant
				// so that it would be possible to perform cross class searches, without
				// tenant context. Modules must be able to deal with this situation!
				assert.NotNil(t, cfg)
				assert.Equal(t, "", cfg.Tenant())

				// take the findVectorFn and append one dimension. This doesn't make too
				// much sense, but helps verify that the modules method was used in the
				// decisions
				initial, _, _ := findVectorFn.FindVector(ctx, "class", "123", "", "")
				return initial, nil
			})),
		)
		p.Init(context.Background(), nil, logger)

		res, targetVector, err := p.MultiCrossClassVectorFromSearchParam(context.Background(),
			"nearGrape", nil, generictypes.MultiFindVectorFn(multiFakeFindVector))

		require.Nil(t, err)
		assert.Equal(t, [][]float32{{0.1, 0.2, 0.3}, {0.11, 0.22, 0.33}}, res)
		assert.Equal(t, "", targetVector)
	})
}

func fakeFindVector(ctx context.Context, className string, id strfmt.UUID, tenant, targetVector string) ([]float32, string, error) {
	return []float32{1, 2, 3}, targetVector, nil
}

func multiFakeFindVector(ctx context.Context, className string, id strfmt.UUID, tenant, targetVector string) ([][]float32, string, error) {
	return [][]float32{{0.1, 0.2, 0.3}, {0.11, 0.22, 0.33}}, targetVector, nil
}

func newSearcherModule[T dto.Embedding](name string) *dummySearcherModule[T] {
	return &dummySearcherModule[T]{
		dummyGraphQLModule: newGraphQLModule(name),
		searchers:          map[string]modulecapabilities.VectorForParams[T]{},
	}
}

type dummySearcherModule[T dto.Embedding] struct {
	*dummyGraphQLModule
	searchers map[string]modulecapabilities.VectorForParams[T]
}

func (m *dummySearcherModule[T]) withArg(arg string) *dummySearcherModule[T] {
	// call the super's withArg
	m.dummyGraphQLModule.withArg(arg)

	// but don't return their return type but ours :)
	return m
}

// a helper for our test
func (m *dummySearcherModule[T]) withSearcher(arg string,
	impl modulecapabilities.VectorForParams[T],
) *dummySearcherModule[T] {
	m.searchers[arg] = impl
	return m
}

// public method to implement the modulecapabilities.Searcher interface
func (m *dummySearcherModule[T]) VectorSearches() map[string]modulecapabilities.VectorForParams[T] {
	return m.searchers
}

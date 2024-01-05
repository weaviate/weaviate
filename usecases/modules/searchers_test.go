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
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/entities/schema"
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
		p := NewProvider()
		p.SetSchemaGetter(&fakeSchemaGetter{
			schema: sch,
		})
		p.Register(newSearcherModule("mod").
			withArg("nearGrape").
			withSearcher("nearGrape", func(ctx context.Context, params interface{},
				className string,
				findVectorFn modulecapabilities.FindVectorFn,
				cfg moduletools.ClassConfig,
			) ([]float32, error) {
				// verify that the config tool is set, as this is a per-class search,
				// so it must be set
				assert.NotNil(t, cfg)

				// take the findVectorFn and append one dimension. This doesn't make too
				// much sense, but helps verify that the modules method was used in the
				// decisions
				initial, _ := findVectorFn(ctx, "class", "123", "")
				return append(initial, 4), nil
			}),
		)
		p.Init(context.Background(), nil, logger)

		res, err := p.VectorFromSearchParam(context.Background(), "MyClass",
			"nearGrape", nil, fakeFindVector, "")

		require.Nil(t, err)
		assert.Equal(t, []float32{1, 2, 3, 4}, res)
	})

	t.Run("get a vector across classes", func(t *testing.T) {
		p := NewProvider()
		p.SetSchemaGetter(&fakeSchemaGetter{
			schema: sch,
		})
		p.Register(newSearcherModule("mod").
			withArg("nearGrape").
			withSearcher("nearGrape", func(ctx context.Context, params interface{},
				className string,
				findVectorFn modulecapabilities.FindVectorFn,
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
				initial, _ := findVectorFn(ctx, "class", "123", "")
				return append(initial, 4), nil
			}),
		)
		p.Init(context.Background(), nil, logger)

		res, err := p.CrossClassVectorFromSearchParam(context.Background(),
			"nearGrape", nil, fakeFindVector)

		require.Nil(t, err)
		assert.Equal(t, []float32{1, 2, 3, 4}, res)
	})
}

func fakeFindVector(ctx context.Context, className string, id strfmt.UUID, tenant string) ([]float32, error) {
	return []float32{1, 2, 3}, nil
}

func newSearcherModule(name string) *dummySearcherModule {
	return &dummySearcherModule{
		dummyGraphQLModule: newGraphQLModule(name),
		searchers:          map[string]modulecapabilities.VectorForParams{},
	}
}

type dummySearcherModule struct {
	*dummyGraphQLModule
	searchers map[string]modulecapabilities.VectorForParams
}

func (m *dummySearcherModule) withArg(arg string) *dummySearcherModule {
	// call the super's withArg
	m.dummyGraphQLModule.withArg(arg)

	// but don't return their return type but ours :)
	return m
}

// a helper for our test
func (m *dummySearcherModule) withSearcher(arg string,
	impl modulecapabilities.VectorForParams,
) *dummySearcherModule {
	m.searchers[arg] = impl
	return m
}

// public method to implement the modulecapabilities.Searcher interface
func (m *dummySearcherModule) VectorSearches() map[string]modulecapabilities.VectorForParams {
	return m.searchers
}

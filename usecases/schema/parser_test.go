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

package schema

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
	"github.com/weaviate/weaviate/entities/vectorindex"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/fakes"
	"github.com/weaviate/weaviate/usecases/sharding/config"
)

const hnswT = vectorindex.VectorIndexTypeHNSW

func TestParser(t *testing.T) {
	cs := fakes.NewFakeClusterState()
	p := NewParser(cs, dummyParseVectorConfig, fakeValidator{}, fakeModulesProvider{}, nil)

	sc := config.Config{DesiredCount: 1, VirtualPerPhysical: 128, ActualCount: 1, DesiredVirtualCount: 128, Key: "_id", Strategy: "hash", Function: "murmur3"}
	vic := enthnsw.NewDefaultUserConfig()
	emptyMap := map[string]interface{}{}
	valueMap := map[string]interface{}{"something": emptyMap}

	testCases := []struct {
		name     string
		old      *models.Class
		update   *models.Class
		expected *models.Class
		error    bool
	}{
		{
			name:     "update description",
			old:      &models.Class{Class: "Test", VectorIndexType: hnswT, VectorIndexConfig: enthnsw.NewDefaultUserConfig(), ShardingConfig: sc},
			update:   &models.Class{Class: "Test", Description: "NEW", VectorIndexType: hnswT, VectorIndexConfig: enthnsw.NewDefaultUserConfig()},
			expected: &models.Class{Class: "Test", Description: "NEW", VectorIndexType: hnswT, VectorIndexConfig: enthnsw.NewDefaultUserConfig(), ShardingConfig: sc},
			error:    false,
		},
		{
			name:     "update generative module - previously not configured",
			old:      &models.Class{Class: "Test", VectorIndexType: hnswT, VectorIndexConfig: vic, ShardingConfig: sc},
			update:   &models.Class{Class: "Test", VectorIndexType: hnswT, VectorIndexConfig: vic, ModuleConfig: map[string]interface{}{"generative-madeup": emptyMap}},
			expected: &models.Class{Class: "Test", VectorIndexType: hnswT, VectorIndexConfig: vic, ShardingConfig: sc, ModuleConfig: map[string]interface{}{"generative-madeup": emptyMap}},
			error:    false,
		},
		{
			name:     "update generative module - previously not configured, other modules present",
			old:      &models.Class{Class: "Test", VectorIndexType: hnswT, VectorIndexConfig: vic, ShardingConfig: sc, ModuleConfig: map[string]interface{}{"text2vec-random": emptyMap}},
			update:   &models.Class{Class: "Test", VectorIndexType: hnswT, VectorIndexConfig: vic, ModuleConfig: map[string]interface{}{"generative-madeup": emptyMap}},
			expected: &models.Class{Class: "Test", VectorIndexType: hnswT, VectorIndexConfig: vic, ShardingConfig: sc, ModuleConfig: map[string]interface{}{"generative-madeup": emptyMap, "text2vec-random": emptyMap}},
			error:    false,
		},
		{
			name:     "update generative module - previously not configured, other generative module present",
			old:      &models.Class{Class: "Test", VectorIndexType: hnswT, VectorIndexConfig: vic, ShardingConfig: sc, ModuleConfig: map[string]interface{}{"generative-random": emptyMap}},
			update:   &models.Class{Class: "Test", VectorIndexType: hnswT, VectorIndexConfig: vic, ModuleConfig: map[string]interface{}{"generative-madeup": emptyMap}},
			expected: &models.Class{Class: "Test", VectorIndexType: hnswT, VectorIndexConfig: vic, ShardingConfig: sc, ModuleConfig: map[string]interface{}{"generative-madeup": emptyMap}},
			error:    false,
		},
		{
			name:     "update reranker module - previously not configured",
			old:      &models.Class{Class: "Test", VectorIndexType: hnswT, VectorIndexConfig: vic, ShardingConfig: sc},
			update:   &models.Class{Class: "Test", VectorIndexType: hnswT, VectorIndexConfig: vic, ModuleConfig: map[string]interface{}{"reranker-madeup": emptyMap}},
			expected: &models.Class{Class: "Test", VectorIndexType: hnswT, VectorIndexConfig: vic, ShardingConfig: sc, ModuleConfig: map[string]interface{}{"reranker-madeup": emptyMap}},
			error:    false,
		},
		{
			name:     "update reranker module - previously not configured, other modules present",
			old:      &models.Class{Class: "Test", VectorIndexType: hnswT, VectorIndexConfig: vic, ShardingConfig: sc, ModuleConfig: map[string]interface{}{"text2vec-random": emptyMap}},
			update:   &models.Class{Class: "Test", VectorIndexType: hnswT, VectorIndexConfig: vic, ModuleConfig: map[string]interface{}{"reranker-madeup": emptyMap}},
			expected: &models.Class{Class: "Test", VectorIndexType: hnswT, VectorIndexConfig: vic, ShardingConfig: sc, ModuleConfig: map[string]interface{}{"reranker-madeup": emptyMap, "text2vec-random": emptyMap}},
			error:    false,
		},
		{
			name:     "update reranker module - previously not configured, other generative module present",
			old:      &models.Class{Class: "Test", VectorIndexType: hnswT, VectorIndexConfig: vic, ShardingConfig: sc, ModuleConfig: map[string]interface{}{"reranker-random": emptyMap, "generative-random": emptyMap}},
			update:   &models.Class{Class: "Test", VectorIndexType: hnswT, VectorIndexConfig: vic, ModuleConfig: map[string]interface{}{"reranker-madeup": emptyMap}},
			expected: &models.Class{Class: "Test", VectorIndexType: hnswT, VectorIndexConfig: vic, ShardingConfig: sc, ModuleConfig: map[string]interface{}{"reranker-madeup": emptyMap, "generative-random": emptyMap}},
			error:    false,
		},
		{
			name:     "update reranker and generative module - previously not configured, other text2vec module present",
			old:      &models.Class{Class: "Test", VectorIndexType: hnswT, VectorIndexConfig: vic, ShardingConfig: sc, ModuleConfig: map[string]interface{}{"reranker-random": emptyMap, "generative-random": emptyMap, "text2vec-random": emptyMap}},
			update:   &models.Class{Class: "Test", VectorIndexType: hnswT, VectorIndexConfig: vic, ModuleConfig: map[string]interface{}{"reranker-madeup": emptyMap, "generative-madeup": emptyMap}},
			expected: &models.Class{Class: "Test", VectorIndexType: hnswT, VectorIndexConfig: vic, ShardingConfig: sc, ModuleConfig: map[string]interface{}{"reranker-madeup": emptyMap, "generative-madeup": emptyMap, "text2vec-random": emptyMap}},
			error:    false,
		},
		{
			name:     "update text2vec - previously not configured, add a new vector index",
			old:      &models.Class{Class: "Test", VectorIndexType: hnswT, VectorIndexConfig: vic, ShardingConfig: sc},
			update:   &models.Class{Class: "Test", VectorIndexType: hnswT, VectorIndexConfig: vic, ModuleConfig: map[string]interface{}{"text2vec-random": emptyMap}},
			expected: &models.Class{Class: "Test", VectorIndexType: hnswT, VectorIndexConfig: vic, ShardingConfig: sc, ModuleConfig: map[string]interface{}{"text2vec-random": emptyMap}},
			error:    false,
		},
		{
			name:   "update text2vec - previously differently configured => error",
			old:    &models.Class{Class: "Test", VectorIndexType: hnswT, VectorIndexConfig: vic, ShardingConfig: sc, ModuleConfig: map[string]interface{}{"text2vec-random": valueMap}},
			update: &models.Class{Class: "Test", VectorIndexType: hnswT, VectorIndexConfig: vic, ModuleConfig: map[string]interface{}{"text2vec-random": emptyMap}},
			error:  true,
		},
		{
			name:     "update text2vec - other modules present => error",
			old:      &models.Class{Class: "Test", VectorIndexType: hnswT, VectorIndexConfig: vic, ShardingConfig: sc, ModuleConfig: map[string]interface{}{"generative-random": valueMap}},
			update:   &models.Class{Class: "Test", VectorIndexType: hnswT, VectorIndexConfig: vic, ModuleConfig: map[string]interface{}{"text2vec-random": emptyMap, "generative-random": valueMap}},
			expected: &models.Class{Class: "Test", VectorIndexType: hnswT, VectorIndexConfig: vic, ShardingConfig: sc, ModuleConfig: map[string]interface{}{"text2vec-random": emptyMap, "generative-random": valueMap}},
			error:    false,
		},
		{
			name:     "update with same text2vec config",
			old:      &models.Class{Class: "Test", VectorIndexType: hnswT, VectorIndexConfig: vic, ShardingConfig: sc, ModuleConfig: map[string]interface{}{"text2vec-random": valueMap}},
			update:   &models.Class{Class: "Test", VectorIndexType: hnswT, VectorIndexConfig: vic, ModuleConfig: map[string]interface{}{"text2vec-random": valueMap}},
			expected: &models.Class{Class: "Test", VectorIndexType: hnswT, VectorIndexConfig: vic, ShardingConfig: sc, ModuleConfig: map[string]interface{}{"text2vec-random": valueMap}},
			error:    false,
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			update, err := p.ParseClassUpdate(test.old, test.update)
			if test.error {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, test.expected.Description, update.Description)
				require.Equal(t, test.expected.ModuleConfig, update.ModuleConfig)
			}
		})
	}
}

func Test_asMap(t *testing.T) {
	t.Run("not nil", func(t *testing.T) {
		m, err := propertyAsMap(&models.Property{
			Name:        "name",
			Description: "description",
			DataType:    []string{"object"},
			NestedProperties: []*models.NestedProperty{{
				Name:        "nested",
				Description: "nested description",
				DataType:    []string{"text"},
			}},
		})
		require.NotNil(t, m)
		require.Nil(t, err)

		_, ok := m["description"]
		require.False(t, ok)

		nps, ok := m["nestedProperties"].([]map[string]any)
		require.True(t, ok)
		require.Len(t, nps, 1)

		_, ok = nps[0]["description"]
		require.False(t, ok)
	})
}

type fakeModulesProvider struct{}

func (m fakeModulesProvider) IsReranker(name string) bool {
	return strings.Contains(name, "reranker")
}

func (m fakeModulesProvider) IsGenerative(name string) bool {
	return strings.Contains(name, "generative")
}

func (m fakeModulesProvider) IsMultiVector(name string) bool {
	return strings.Contains(name, "colbert")
}

func (m fakeModulesProvider) HasModule(name string) bool {
	return name == "none" || strings.Contains(name, "colbert") || strings.Contains(name, "text2vec") || strings.Contains(name, "multi2vec")
}

func (m fakeModulesProvider) MigrateVectorizerSettings(any, any) bool {
	return false
}

func TestParseTargetVectorsIndexConfigErrors(t *testing.T) {
	// parseVectorConfig that returns a multi-vector index config regardless of input
	multiVecParseConfig := func(in interface{}, vectorIndexType string, isMultiVector bool) (schemaConfig.VectorIndexConfig, error) {
		m := schemaConfig.NewMockVectorIndexConfig(t)
		m.EXPECT().IsMultiVector().Return(true).Maybe()
		m.EXPECT().IndexType().Return("fake").Maybe()
		m.EXPECT().DistanceName().Return("cosine").Maybe()
		return m, nil
	}

	cs := fakes.NewFakeClusterState()

	makeClass := func(vectorizerName string) *models.Class {
		return &models.Class{
			Class: "Test",
			VectorConfig: map[string]models.VectorConfig{
				"target": {
					VectorIndexType: hnswT,
					Vectorizer: map[string]interface{}{
						vectorizerName: map[string]interface{}{},
					},
				},
			},
		}
	}

	t.Run("module not registered suggests downgrade", func(t *testing.T) {
		p := NewParser(cs, multiVecParseConfig, fakeValidator{}, fakeModulesProvider{}, nil)
		// "new-module-v2" is not returned by HasModule on fakeModulesProvider
		err := p.ParseClass(makeClass("new-module-v2"))
		require.Error(t, err)
		require.ErrorContains(t, err, "not found with name")
	})

	t.Run("module registered but does not support multi vectors", func(t *testing.T) {
		p := NewParser(cs, multiVecParseConfig, fakeValidator{}, fakeModulesProvider{}, nil)
		// "text2vec-contextionary" is returned by HasModule but not by IsMultiVector
		err := p.ParseClass(makeClass("text2vec-contextionary"))
		require.Error(t, err)
		require.ErrorContains(t, err, "doesn't support multi vectors")
	})

	t.Run("multi vector module succeeds", func(t *testing.T) {
		p := NewParser(cs, multiVecParseConfig, fakeValidator{}, fakeModulesProvider{}, nil)
		// "colbert" satisfies both HasModule and IsMultiVector on fakeModulesProvider
		err := p.ParseClass(makeClass("colbert"))
		require.NoError(t, err)
	})

	t.Run(`vectorizer "none" succeeds for multi vector index`, func(t *testing.T) {
		p := NewParser(cs, multiVecParseConfig, fakeValidator{}, fakeModulesProvider{}, nil)
		err := p.ParseClass(makeClass("none"))
		require.NoError(t, err)
	})
}

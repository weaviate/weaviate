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

package schema

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/vectorindex"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/fakes"
	"github.com/weaviate/weaviate/usecases/sharding/config"
)

const hnswT = vectorindex.VectorIndexTypeHNSW

func TestParser(t *testing.T) {
	cs := fakes.NewFakeClusterState()
	p := NewParser(cs, dummyParseVectorConfig, fakeValidator{}, fakeModulesProvider{})

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

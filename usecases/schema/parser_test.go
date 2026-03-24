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
	"github.com/weaviate/weaviate/entities/vectorindex"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	configRuntime "github.com/weaviate/weaviate/usecases/config/runtime"
	"github.com/weaviate/weaviate/usecases/fakes"
	"github.com/weaviate/weaviate/usecases/sharding/config"
)

const hnswT = vectorindex.VectorIndexTypeHNSW

func TestParser(t *testing.T) {
	cs := fakes.NewFakeClusterState()
	p := NewParser(cs, dummyParseVectorConfig, fakeValidator{}, fakeModulesProvider{}, nil, nil)

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

func TestPropertyProcessingImmutability(t *testing.T) {
	vTrue := true
	p := NewParser(fakes.NewFakeClusterState(), dummyParseVectorConfig, fakeValidator{}, fakeModulesProvider{}, nil, nil)

	baseProp := func(proc *models.PropertyProcessing) *models.Property {
		return &models.Property{
			Name:            "title",
			DataType:        []string{"text"},
			Tokenization:    "word",
			IndexFilterable: &vTrue,
			IndexSearchable: &vTrue,
			Processing:      proc,
		}
	}

	sc := config.Config{DesiredCount: 1, VirtualPerPhysical: 128, ActualCount: 1, DesiredVirtualCount: 128, Key: "_id", Strategy: "hash", Function: "murmur3"}
	vic := enthnsw.NewDefaultUserConfig()

	tests := []struct {
		name        string
		existing    *models.PropertyProcessing
		updated     *models.PropertyProcessing
		expectError bool
	}{
		{
			name:        "no change - both nil",
			existing:    nil,
			updated:     nil,
			expectError: false,
		},
		{
			name:        "no change - both accentInsensitive true",
			existing:    &models.PropertyProcessing{AccentInsensitive: true},
			updated:     &models.PropertyProcessing{AccentInsensitive: true},
			expectError: false,
		},
		{
			name:        "no change - both accentInsensitive false",
			existing:    &models.PropertyProcessing{AccentInsensitive: false},
			updated:     &models.PropertyProcessing{AccentInsensitive: false},
			expectError: false,
		},
		{
			name:        "change accentInsensitive false to true",
			existing:    &models.PropertyProcessing{AccentInsensitive: false},
			updated:     &models.PropertyProcessing{AccentInsensitive: true},
			expectError: true,
		},
		{
			name:        "change accentInsensitive true to false",
			existing:    &models.PropertyProcessing{AccentInsensitive: true},
			updated:     &models.PropertyProcessing{AccentInsensitive: false},
			expectError: true,
		},
		{
			name:        "add processing where none existed",
			existing:    nil,
			updated:     &models.PropertyProcessing{AccentInsensitive: true},
			expectError: true,
		},
		{
			name:        "remove processing",
			existing:    &models.PropertyProcessing{AccentInsensitive: true},
			updated:     nil,
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			old := &models.Class{
				Class:             "Test",
				VectorIndexType:   hnswT,
				VectorIndexConfig: vic,
				ShardingConfig:    sc,
				Properties:        []*models.Property{baseProp(tt.existing)},
			}
			update := &models.Class{
				Class:             "Test",
				VectorIndexType:   hnswT,
				VectorIndexConfig: vic,
				Properties:        []*models.Property{baseProp(tt.updated)},
			}
			_, err := p.ParseClassUpdate(old, update)
			if tt.expectError {
				require.Error(t, err, "expected error for: %s", tt.name)
				require.ErrorIs(t, err, errPropertiesUpdatedInClassUpdate)
			} else {
				require.NoError(t, err, "unexpected error for: %s", tt.name)
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

func (m fakeModulesProvider) MigrateVectorizerSettings(any, any) bool {
	return false
}

func TestParserDefaultShardingCount(t *testing.T) {
	t.Run("zero means use node count", func(t *testing.T) {
		cs := fakes.NewFakeClusterState()
		dsc := configRuntime.NewDynamicValue(0)
		p := NewParser(cs, dummyParseVectorConfig, fakeValidator{}, fakeModulesProvider{}, nil, dsc)

		class := &models.Class{Class: "Test", VectorIndexType: hnswT}
		err := p.ParseClass(class)
		require.NoError(t, err)

		sc := class.ShardingConfig.(config.Config)
		require.Equal(t, cs.NodeCount(), sc.DesiredCount)
	})

	t.Run("override with 12", func(t *testing.T) {
		cs := fakes.NewFakeClusterState()
		dsc := configRuntime.NewDynamicValue(12)
		p := NewParser(cs, dummyParseVectorConfig, fakeValidator{}, fakeModulesProvider{}, nil, dsc)

		class := &models.Class{Class: "Test", VectorIndexType: hnswT}
		err := p.ParseClass(class)
		require.NoError(t, err)

		sc := class.ShardingConfig.(config.Config)
		require.Equal(t, 12, sc.DesiredCount)
	})

	t.Run("user explicit desiredCount wins over override", func(t *testing.T) {
		cs := fakes.NewFakeClusterState()
		dsc := configRuntime.NewDynamicValue(12)
		p := NewParser(cs, dummyParseVectorConfig, fakeValidator{}, fakeModulesProvider{}, nil, dsc)

		class := &models.Class{
			Class:           "Test",
			VectorIndexType: hnswT,
			ShardingConfig:  map[string]interface{}{"desiredCount": 5},
		}
		err := p.ParseClass(class)
		require.NoError(t, err)

		sc := class.ShardingConfig.(config.Config)
		require.Equal(t, 5, sc.DesiredCount)
	})

	t.Run("nil defaultShardingCount uses node count", func(t *testing.T) {
		cs := fakes.NewFakeClusterState()
		p := NewParser(cs, dummyParseVectorConfig, fakeValidator{}, fakeModulesProvider{}, nil, nil)

		class := &models.Class{Class: "Test", VectorIndexType: hnswT}
		err := p.ParseClass(class)
		require.NoError(t, err)

		sc := class.ShardingConfig.(config.Config)
		require.Equal(t, cs.NodeCount(), sc.DesiredCount)
	})

	t.Run("multi-tenancy unaffected by override", func(t *testing.T) {
		cs := fakes.NewFakeClusterState()
		dsc := configRuntime.NewDynamicValue(12)
		p := NewParser(cs, dummyParseVectorConfig, fakeValidator{}, fakeModulesProvider{}, nil, dsc)

		class := &models.Class{
			Class:              "Test",
			VectorIndexType:    hnswT,
			MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true},
		}
		err := p.ParseClass(class)
		require.NoError(t, err)

		sc := class.ShardingConfig.(config.Config)
		require.Equal(t, 0, sc.DesiredCount)
	})
}

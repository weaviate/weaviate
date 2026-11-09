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

package schema_test

import (
	"encoding/json"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
	"github.com/weaviate/weaviate/entities/vectorindex"
	"github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	text2vecopenai "github.com/weaviate/weaviate/modules/text2vec-openai"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/fakes"
	"github.com/weaviate/weaviate/usecases/modules"
	"github.com/weaviate/weaviate/usecases/schema"
	shardingcfg "github.com/weaviate/weaviate/usecases/sharding/config"
)

// text2vec-openai declares baseURL as a migrate property, so baseURL may be missing from the stored settings,
// but every other difference must still be rejected.
func TestParseClassUpdate_ModuleWithMigrateProperties(t *testing.T) {
	const (
		openai = "text2vec-openai"
		hnswT  = vectorindex.VectorIndexTypeHNSW
	)
	logger, _ := test.NewNullLogger()
	provider := modules.NewProvider(logger, config.Config{})
	provider.Register(text2vecopenai.New())
	p := schema.NewParser(fakes.NewFakeClusterState(), parseVectorConfig, acceptingValidator{}, provider, nil, nil)
	shardingConfig := shardingcfg.Config{
		DesiredCount: 1, VirtualPerPhysical: 128, ActualCount: 1, DesiredVirtualCount: 128, Key: "_id", Strategy: "hash", Function: "murmur3",
	}
	stored := func() map[string]any {
		return map[string]any{"model": "text-embedding-3-small", "baseURL": "https://api.openai.com", "vectorizeClassName": true}
	}
	withoutBaseURL := func(settings map[string]any) map[string]any {
		delete(settings, "baseURL")
		return settings
	}
	// A named vector created on v1.24 is stored with only the settings the client sent.
	createdOnV124 := func() map[string]any {
		return map[string]any{"properties": []any{"text"}, "vectorizeClassName": false, "vectorizePropertyName": false}
	}
	with := func(settings map[string]any, key string, value any) map[string]any {
		settings[key] = value
		return settings
	}
	classLevel := func(settings map[string]any) *models.Class {
		return &models.Class{
			Class: "Doc", Vectorizer: openai, VectorIndexType: hnswT, VectorIndexConfig: hnsw.NewDefaultUserConfig(),
			ModuleConfig: map[string]any{openai: settings},
		}
	}
	namedVector := func(settings map[string]any) *models.Class {
		return &models.Class{
			Class: "Doc",
			VectorConfig: map[string]models.VectorConfig{"text": {
				VectorIndexType: hnswT, VectorIndexConfig: hnsw.NewDefaultUserConfig(),
				Vectorizer: map[string]any{openai: settings},
			}},
		}
	}

	tests := []struct {
		name            string
		stored, updated map[string]any
		wantErr         bool
	}{
		{
			name: "model and baseURL missing from the stored settings, update fills the defaults", stored: createdOnV124(),
			updated: with(with(createdOnV124(), "model", "text-embedding-3-small"), "baseURL", "https://api.openai.com"),
		},
		{
			name: "changed model, baseURL missing from the stored settings", stored: withoutBaseURL(stored()),
			updated: with(stored(), "model", "text-embedding-3-large"), wantErr: true,
		},
	}
	for _, build := range []struct {
		name  string
		class func(map[string]any) *models.Class
	}{{"class level", classLevel}, {"named vector", namedVector}} {
		for _, tt := range tests {
			t.Run(build.name+"/"+tt.name, func(t *testing.T) {
				storedClass := build.class(tt.stored)
				storedClass.ShardingConfig = shardingConfig
				storedBefore := classToMap(t, storedClass)

				_, err := p.ParseClassUpdate(storedClass, build.class(tt.updated))
				if tt.wantErr {
					require.Error(t, err)
				} else {
					require.NoError(t, err)
				}
				require.Equal(t, storedBefore, classToMap(t, storedClass), "the stored class must not be changed")
			})
		}
	}
}

type fakeVectorIndexConfig struct{}

func (fakeVectorIndexConfig) IndexType() string    { return "fake" }
func (fakeVectorIndexConfig) DistanceName() string { return "cosine" }
func (fakeVectorIndexConfig) IsMultiVector() bool  { return false }

func parseVectorConfig(any, string, bool) (schemaConfig.VectorIndexConfig, error) {
	return fakeVectorIndexConfig{}, nil
}

type acceptingValidator struct{}

func (acceptingValidator) ValidateVectorIndexConfigUpdate(_, _ schemaConfig.VectorIndexConfig) error {
	return nil
}

func (acceptingValidator) ValidateInvertedIndexConfigUpdate(_, _ *models.InvertedIndexConfig) error {
	return nil
}

func (acceptingValidator) ValidateVectorIndexConfigsUpdate(_, _ map[string]schemaConfig.VectorIndexConfig) error {
	return nil
}

func classToMap(t *testing.T, class *models.Class) map[string]any {
	t.Helper()
	data, err := json.Marshal(class)
	require.NoError(t, err)
	var m map[string]any
	require.NoError(t, json.Unmarshal(data, &m))
	return m
}

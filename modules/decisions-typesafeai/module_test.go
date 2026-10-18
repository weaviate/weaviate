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

package moddecisionstypesafeai

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/modulecomponents/additional/rank"
	"github.com/weaviate/weaviate/usecases/modulecomponents/ent"
	"github.com/weaviate/weaviate/usecases/modules"
)

func TestMaxConcurrentRequestsFromEnv(t *testing.T) {
	tests := []struct {
		name    string
		value   string
		want    int
		wantErr string
	}{
		{name: "unset uses the default", value: "", want: 16},
		{name: "lowest value", value: "1", want: 1},
		{name: "highest value", value: "256", want: 256},
		{name: "zero", value: "0", wantErr: `DECISIONS_TYPESAFEAI_MAX_CONCURRENT_REQUESTS must be a whole number between 1 and 256, got "0"`},
		{name: "above the limit", value: "257", wantErr: `DECISIONS_TYPESAFEAI_MAX_CONCURRENT_REQUESTS must be a whole number between 1 and 256, got "257"`},
		{name: "negative", value: "-4", wantErr: `DECISIONS_TYPESAFEAI_MAX_CONCURRENT_REQUESTS must be a whole number between 1 and 256, got "-4"`},
		{name: "not a number", value: "many", wantErr: `DECISIONS_TYPESAFEAI_MAX_CONCURRENT_REQUESTS must be a whole number between 1 and 256, got "many"`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv(maxConcurrentRequestsEnv, tt.value)

			got, err := maxConcurrentRequestsFromEnv()

			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestDecisionLimits(t *testing.T) {
	tests := []struct {
		name             string
		settings         map[string]any
		wantMaxDocuments int
		wantImages       bool
		wantErr          string
	}{
		{name: "defaults", settings: map[string]any{}, wantMaxDocuments: 100},
		{name: "maxDocuments from the class", settings: map[string]any{"maxDocuments": 40}, wantMaxDocuments: 40},
		{name: "images on", settings: map[string]any{"images": true}, wantMaxDocuments: 100, wantImages: true},
		{name: "images off", settings: map[string]any{"images": false}, wantMaxDocuments: 100},
		{name: "images of another type", settings: map[string]any{"images": "yes"}, wantErr: "images must be true or false, got yes"},
		// A class created before the module was enabled was never validated.
		{name: "maxDocuments above the upper bound", settings: map[string]any{"maxDocuments": 5000}, wantErr: "maxDocuments must be a whole number between 1 and 1000, got 5000"},
		{name: "maxDocuments of another type", settings: map[string]any{"maxDocuments": "many"}, wantErr: "maxDocuments must be a whole number between 1 and 1000, got many"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			limits, err := New().DecisionLimits(classConfig(tt.settings))

			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			dataTypes := []schema.DataType{schema.DataTypeText}
			if tt.wantImages {
				dataTypes = append(dataTypes, schema.DataTypeBlob)
			}
			assert.Equal(t, modulecapabilities.DecisionLimits{
				MaxDocuments: tt.wantMaxDocuments,
				MaxLevels:    10,
				DataTypes:    dataTypes,
			}, limits)
		})
	}
}

func classConfig(settings map[string]any) moduletools.ClassConfig {
	class := &models.Class{
		Class:        "Ticket",
		ModuleConfig: map[string]any{Name: settings},
	}
	// The module name is empty here because that is how the modules provider
	// builds the config it passes to additional properties.
	return modules.NewClassBasedModuleConfig(class, "", "", "", &config.Config{})
}

type fakeRanker struct{}

func (fakeRanker) Rank(ctx context.Context, query string, documents []string, cfg moduletools.ClassConfig) (*ent.RankResult, error) {
	return &ent.RankResult{Query: query}, nil
}

func (fakeRanker) Decide(ctx context.Context, questions []ent.DecisionQuestion, documents []string, cfg moduletools.ClassConfig) ([][]ent.DecisionAnswer, error) {
	return nil, nil
}

func (fakeRanker) MetaInfo() (map[string]any, error) {
	return nil, nil
}

func TestModuleServesRerankAndDecide(t *testing.T) {
	m := New()
	m.setClient(fakeRanker{})
	properties := m.AdditionalProperties()

	require.Len(t, properties, 2)
	for _, name := range []string{"rerank", "annotate"} {
		property, ok := properties[name]
		require.True(t, ok, name)
		assert.NotNil(t, property.SearchFunctions.ExploreGet, name)
		assert.NotNil(t, property.SearchFunctions.ExploreList, name)
	}
	// decide has no GraphQL field: it is served through gRPC only.
	assert.Nil(t, properties["annotate"].GraphQLFieldFunction)
	assert.NotNil(t, properties["rerank"].GraphQLFieldFunction)
}

func TestDecisionDefaults(t *testing.T) {
	tests := []struct {
		name     string
		settings map[string]any
		want     rank.DecisionSettings
		wantErr  string
	}{
		{name: "defaults", settings: map[string]any{}, want: rank.DecisionSettings{Order: rank.OrderSearch}},
		{name: "from the class", settings: map[string]any{"minProbability": 0.7, "order": "probability"}, want: rank.DecisionSettings{MinProbability: 0.7, Order: rank.OrderProbability}},
		{name: "minProbability above 1", settings: map[string]any{"minProbability": 1.5}, wantErr: "minProbability must be between 0 and 1, got 1.5"},
		{name: "unknown order", settings: map[string]any{"order": "newest"}, wantErr: `order must be "search" or "probability", got "newest"`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := decisionDefaults(classConfig(tt.settings))

			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

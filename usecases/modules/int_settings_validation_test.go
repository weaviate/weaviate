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

package modules_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/moduletools"
	modgenerativeanthropic "github.com/weaviate/weaviate/modules/generative-anthropic"
	modgenerativeaws "github.com/weaviate/weaviate/modules/generative-aws"
	modgenerativecohere "github.com/weaviate/weaviate/modules/generative-cohere"
	modgenerativecontextualai "github.com/weaviate/weaviate/modules/generative-contextualai"
	modgenerativedatabricks "github.com/weaviate/weaviate/modules/generative-databricks"
	modgenerativedeepseek "github.com/weaviate/weaviate/modules/generative-deepseek"
	modgenerativedigitalocean "github.com/weaviate/weaviate/modules/generative-digitalocean"
	modgenerativefriendliai "github.com/weaviate/weaviate/modules/generative-friendliai"
	modgenerativegoogle "github.com/weaviate/weaviate/modules/generative-google"
	modgenerativemistral "github.com/weaviate/weaviate/modules/generative-mistral"
	modgenerativenvidia "github.com/weaviate/weaviate/modules/generative-nvidia"
	modgenerativeopenai "github.com/weaviate/weaviate/modules/generative-openai"
	modgenerativexai "github.com/weaviate/weaviate/modules/generative-xai"
	modmulti2vecaws "github.com/weaviate/weaviate/modules/multi2vec-aws"
	modmulti2veccohere "github.com/weaviate/weaviate/modules/multi2vec-cohere"
	modmulti2vecgoogle "github.com/weaviate/weaviate/modules/multi2vec-google"
	modmulti2vecjinaai "github.com/weaviate/weaviate/modules/multi2vec-jinaai"
	modqnaopenai "github.com/weaviate/weaviate/modules/qna-openai"
	modrerankercontextualai "github.com/weaviate/weaviate/modules/reranker-contextualai"
	modtext2multivecjinaai "github.com/weaviate/weaviate/modules/text2multivec-jinaai"
	modtext2vecaws "github.com/weaviate/weaviate/modules/text2vec-aws"
	modtext2veccohere "github.com/weaviate/weaviate/modules/text2vec-cohere"
	modtext2vecgoogle "github.com/weaviate/weaviate/modules/text2vec-google"
	modtext2vecjinaai "github.com/weaviate/weaviate/modules/text2vec-jinaai"
	modtext2vecopenai "github.com/weaviate/weaviate/modules/text2vec-openai"
	modtext2vectransformers "github.com/weaviate/weaviate/modules/text2vec-transformers"
	modtext2vecvoyageai "github.com/weaviate/weaviate/modules/text2vec-voyageai"
	modtext2vecweaviate "github.com/weaviate/weaviate/modules/text2vec-weaviate"
	"github.com/weaviate/weaviate/usecases/config"
	usecasesmodules "github.com/weaviate/weaviate/usecases/modules"
)

// validateWith runs a module's class validation the way the provider does:
// the module's own error first, then what the settings getters reported.
func validateWith(t *testing.T, module modulecapabilities.Module, settings map[string]any) error {
	t.Helper()
	configurator, ok := module.(modulecapabilities.ClassConfigurator)
	require.True(t, ok, "%s does not validate classes", module.Name())
	class := &models.Class{
		Class:        "T",
		Vectorizer:   module.Name(),
		Properties:   []*models.Property{{Name: "text", DataType: []string{"text"}}},
		ModuleConfig: map[string]any{module.Name(): settings},
	}
	cfg := usecasesmodules.NewClassBasedModuleConfig(class, module.Name(), "", "", &config.Config{})
	validation := moduletools.NewValidationClassConfig(cfg)
	if err := configurator.ValidateClass(context.Background(), class, validation); err != nil {
		return err
	}
	return validation.Err()
}

// Every int setting of a module that uses the shared settings helper,
// whichever getter the module reads it with. A new class must not be
// accepted with a fraction in one of them: the stored value would be
// truncated at request time to an int that the module's validation never
// saw, or sent to the provider as a fraction.
func TestIntSettingsRejectAFractionOnClassCreate(t *testing.T) {
	cases := []struct {
		module   modulecapabilities.Module
		settings []string
		// extra holds what the module needs to pass validation otherwise.
		extra map[string]any
	}{
		{modgenerativeanthropic.New(), []string{"maxTokens", "topK"}, nil},
		{modgenerativeaws.New(), []string{"maxTokens", "maxTokenCount", "maxTokensToSample", "topK"}, map[string]any{"service": "bedrock", "region": "us-east-1", "model": "amazon.titan-text-lite-v1"}},
		{modgenerativecohere.New(), []string{"maxTokens", "k"}, nil},
		{modgenerativecontextualai.New(), []string{"maxNewTokens"}, nil},
		{modgenerativedatabricks.New(), []string{"maxTokens", "topK"}, map[string]any{"endpoint": "http://x"}},
		// These two read maxTokens with the float getter and truncate it
		// when they build the request.
		{modgenerativedeepseek.New(), []string{"maxTokens"}, nil},
		{modgenerativedigitalocean.New(), []string{"maxTokens"}, nil},
		{modgenerativefriendliai.New(), []string{"maxTokens"}, nil},
		{modgenerativegoogle.New(), []string{"tokenLimit", "topK"}, map[string]any{"projectId": "p"}},
		{modgenerativemistral.New(), []string{"maxTokens"}, nil},
		{modgenerativenvidia.New(), []string{"maxTokens"}, nil},
		{modgenerativeopenai.New(), []string{"maxTokens"}, nil},
		{modgenerativexai.New(), []string{"maxTokens"}, nil},
		{modmulti2vecaws.New(), []string{"dimensions"}, map[string]any{"textFields": []any{"text"}, "region": "us-east-1"}},
		{modmulti2veccohere.New(), []string{"dimensions"}, map[string]any{"textFields": []any{"text"}}},
		{modmulti2vecgoogle.New(), []string{"dimensions", "videoIntervalSeconds"}, map[string]any{"textFields": []any{"text"}, "location": "us", "projectId": "p"}},
		{modmulti2vecjinaai.New(), []string{"dimensions"}, map[string]any{"textFields": []any{"text"}}},
		// Reads maxTokens with a getter of its own.
		{modqnaopenai.New(), []string{"maxTokens"}, nil},
		{modrerankercontextualai.New(), []string{"topN"}, nil},
		{modtext2multivecjinaai.New(), []string{"dimensions"}, nil},
		{modtext2vecaws.New(), []string{"dimensions"}, map[string]any{"service": "bedrock", "region": "us-east-1", "model": "amazon.titan-embed-text-v2:0"}},
		{modtext2veccohere.New(), []string{"dimensions"}, nil},
		{modtext2vecgoogle.New(), []string{"dimensions"}, map[string]any{"projectId": "p"}},
		{modtext2vecjinaai.New(), []string{"dimensions"}, nil},
		{modtext2vecopenai.New(), []string{"dimensions"}, map[string]any{"model": "text-embedding-3-large"}},
		{modtext2vectransformers.New(), []string{"dimensions"}, nil},
		{modtext2vecvoyageai.New(), []string{"dimensions"}, nil},
		{modtext2vecweaviate.New(), []string{"dimensions"}, nil},
	}
	for _, c := range cases {
		for _, setting := range c.settings {
			t.Run(c.module.Name()+"/"+setting, func(t *testing.T) {
				assertIntSetting(t, c.module, setting, c.extra)
			})
		}
	}
}

// assertIntSetting checks one int setting of a module. extra holds what the
// module needs to pass validation otherwise.
func assertIntSetting(t *testing.T, module modulecapabilities.Module, setting string, extra map[string]any) {
	t.Helper()
	with := func(value any) map[string]any {
		settings := map[string]any{}
		for k, v := range extra {
			settings[k] = v
		}
		if value != nil {
			settings[setting] = value
		}
		return settings
	}
	require.NoError(t, validateWith(t, module, with(nil)), "the fixture must be valid without the setting")

	// The REST API decodes the numbers of a new class as json.Number.
	require.Error(t, validateWith(t, module, with(json.Number("100.5"))))

	// An integer written as a float is the same integer: both are accepted,
	// or both fail the module's own rule for that value.
	asInt := validateWith(t, module, with(json.Number("7")))
	asFloat := validateWith(t, module, with(json.Number("7.0")))
	if asInt == nil {
		assert.NoError(t, asFloat)
	} else {
		assert.EqualError(t, asFloat, asInt.Error())
	}

	// A class read back from the schema or a backup holds float64. One
	// stored with a fraction before this check must still load.
	if err := validateWith(t, module, with(100.5)); err != nil {
		assert.NotContains(t, err.Error(), "must be an integer")
	}
}

// The reproduction of the issue, through the provider: text2vec-openai only
// accepts 256, 1024 and 3072 dimensions for this model, and a fraction used
// to skip that check.
func TestProviderRejectsAFractionInAnIntSetting(t *testing.T) {
	tests := []struct {
		name       string
		dimensions any
		wantErr    string
	}{
		{name: "fraction", dimensions: json.Number("100.5"), wantErr: "module 'text2vec-openai': dimensions must be an integer, got 100.5"},
		{name: "integer the module rejects", dimensions: json.Number("100"), wantErr: "wrong dimensions setting"},
		{name: "the same integer written as a float", dimensions: json.Number("100.0"), wantErr: "wrong dimensions setting"},
		{name: "valid integer", dimensions: json.Number("1024")},
		{name: "valid integer written as a float", dimensions: json.Number("1024.0")},
		// A class restored from a backup: its numbers are float64.
		{name: "stored fraction", dimensions: 1024.5},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, _ := test.NewNullLogger()
			provider := usecasesmodules.NewProvider(logger, config.Config{})
			provider.Register(modtext2vecopenai.New())
			class := &models.Class{
				Class:      "T",
				Properties: []*models.Property{{Name: "text", DataType: []string{"text"}}},
				VectorConfig: map[string]models.VectorConfig{"v": {
					VectorIndexType: "hnsw",
					Vectorizer: map[string]any{"text2vec-openai": map[string]any{
						"model": "text-embedding-3-large", "dimensions": tt.dimensions,
					}},
				}},
			}

			err := provider.ValidateClass(context.Background(), class)

			if tt.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tt.wantErr)
		})
	}
}

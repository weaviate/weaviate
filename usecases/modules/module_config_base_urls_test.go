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

package modules

import (
	"context"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	modgenerativeanthropic "github.com/weaviate/weaviate/modules/generative-anthropic"
	modgenerativeanyscale "github.com/weaviate/weaviate/modules/generative-anyscale"
	modgenerativeaws "github.com/weaviate/weaviate/modules/generative-aws"
	modgenerativecohere "github.com/weaviate/weaviate/modules/generative-cohere"
	modgenerativedatabricks "github.com/weaviate/weaviate/modules/generative-databricks"
	modgenerativedeepseek "github.com/weaviate/weaviate/modules/generative-deepseek"
	modgenerativedigitalocean "github.com/weaviate/weaviate/modules/generative-digitalocean"
	modgenerativefriendliai "github.com/weaviate/weaviate/modules/generative-friendliai"
	modgenerativemeta "github.com/weaviate/weaviate/modules/generative-meta"
	modgenerativemistral "github.com/weaviate/weaviate/modules/generative-mistral"
	modgenerativenvidia "github.com/weaviate/weaviate/modules/generative-nvidia"
	modgenerativeollama "github.com/weaviate/weaviate/modules/generative-ollama"
	modgenerativeopenai "github.com/weaviate/weaviate/modules/generative-openai"
	modgenerativexai "github.com/weaviate/weaviate/modules/generative-xai"
	modm2mvjinaai "github.com/weaviate/weaviate/modules/multi2multivec-jinaai"
	modmulti2vecaws "github.com/weaviate/weaviate/modules/multi2vec-aws"
	modclip "github.com/weaviate/weaviate/modules/multi2vec-clip"
	modmulti2veccohere "github.com/weaviate/weaviate/modules/multi2vec-cohere"
	modmulti2vecjinaai "github.com/weaviate/weaviate/modules/multi2vec-jinaai"
	modmulti2vecnvidia "github.com/weaviate/weaviate/modules/multi2vec-nvidia"
	modtwelvelabs "github.com/weaviate/weaviate/modules/multi2vec-twelvelabs"
	modmulti2vecvoyageai "github.com/weaviate/weaviate/modules/multi2vec-voyageai"
	modqnaopenai "github.com/weaviate/weaviate/modules/qna-openai"
	modrerankercohere "github.com/weaviate/weaviate/modules/reranker-cohere"
	modrerankernvidia "github.com/weaviate/weaviate/modules/reranker-nvidia"
	modtext2multivecjinaai "github.com/weaviate/weaviate/modules/text2multivec-jinaai"
	modtext2vecaws "github.com/weaviate/weaviate/modules/text2vec-aws"
	modcohere "github.com/weaviate/weaviate/modules/text2vec-cohere"
	moddatabricks "github.com/weaviate/weaviate/modules/text2vec-databricks"
	moddigitalocean "github.com/weaviate/weaviate/modules/text2vec-digitalocean"
	modhuggingface "github.com/weaviate/weaviate/modules/text2vec-huggingface"
	modjinaai "github.com/weaviate/weaviate/modules/text2vec-jinaai"
	modmistral "github.com/weaviate/weaviate/modules/text2vec-mistral"
	modmodel2vec "github.com/weaviate/weaviate/modules/text2vec-model2vec"
	modmorph "github.com/weaviate/weaviate/modules/text2vec-morph"
	modnvidia "github.com/weaviate/weaviate/modules/text2vec-nvidia"
	modoctoai "github.com/weaviate/weaviate/modules/text2vec-octoai"
	modollama "github.com/weaviate/weaviate/modules/text2vec-ollama"
	modopenai "github.com/weaviate/weaviate/modules/text2vec-openai"
	modtransformers "github.com/weaviate/weaviate/modules/text2vec-transformers"
	modvoyageai "github.com/weaviate/weaviate/modules/text2vec-voyageai"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/modulecomponents"
)

const attackerURL = "https://attacker.example"

// Each module's host-choosing setting, so that a module whose validation misses it, reads it under a wrong key or is
// never called shows up as a module whose custom host is accepted.
var baseURLSettings = []struct {
	module   modulecapabilities.Module
	settings map[string]any
}{
	{modgenerativeanthropic.New(), map[string]any{"baseURL": attackerURL}},
	{modgenerativeanyscale.New(), map[string]any{"baseURL": attackerURL}},
	{modgenerativecohere.New(), map[string]any{"baseURL": attackerURL}},
	{modgenerativedatabricks.New(), map[string]any{"endpoint": attackerURL}},
	{modgenerativedeepseek.New(), map[string]any{"baseURL": attackerURL}},
	{modgenerativedigitalocean.New(), map[string]any{"baseURL": attackerURL}},
	{modgenerativefriendliai.New(), map[string]any{"baseURL": attackerURL}},
	{modgenerativemeta.New(), map[string]any{"baseURL": attackerURL}},
	{modgenerativemistral.New(), map[string]any{"baseURL": attackerURL}},
	{modgenerativenvidia.New(), map[string]any{"baseURL": attackerURL}},
	{modgenerativeollama.New(), map[string]any{"apiEndpoint": attackerURL}},
	{modgenerativeopenai.New(), map[string]any{"baseURL": attackerURL}},
	{modgenerativexai.New(), map[string]any{"baseURL": attackerURL}},
	{modqnaopenai.New(), map[string]any{"baseURL": attackerURL}},
	{modrerankercohere.New(), map[string]any{"baseURL": attackerURL}},
	{modrerankernvidia.New(), map[string]any{"baseURL": attackerURL}},
	{modopenai.New(), map[string]any{"baseURL": attackerURL}},
	{modcohere.New(), map[string]any{"baseURL": attackerURL}},
	{modhuggingface.New(), map[string]any{"endpointURL": attackerURL}},
	{modhuggingface.New(), map[string]any{"endpointUrl": attackerURL}},
	{modjinaai.New(), map[string]any{"baseURL": attackerURL}},
	{modmistral.New(), map[string]any{"baseURL": attackerURL}},
	{modmorph.New(), map[string]any{"baseURL": attackerURL}},
	{modnvidia.New(), map[string]any{"baseURL": attackerURL}},
	{modoctoai.New(), map[string]any{"baseURL": attackerURL}},
	{modvoyageai.New(), map[string]any{"baseURL": attackerURL}},
	{moddatabricks.New(), map[string]any{"endpoint": attackerURL}},
	{moddigitalocean.New(), map[string]any{"baseURL": attackerURL}},
	{modollama.New(), map[string]any{"apiEndpoint": attackerURL}},
	{modtransformers.New(), map[string]any{"inferenceUrl": attackerURL}},
	{modtransformers.New(), map[string]any{"passageInferenceUrl": attackerURL, "queryInferenceUrl": "http://localhost:8080"}},
	{modtransformers.New(), map[string]any{"passageInferenceUrl": "http://localhost:8080", "queryInferenceUrl": attackerURL}},
	{modmodel2vec.New(), map[string]any{"inferenceUrl": attackerURL}},
	{modtext2multivecjinaai.New(), map[string]any{"baseURL": attackerURL}},
	{modclip.New(), map[string]any{"inferenceUrl": attackerURL}},
	{modmulti2veccohere.New(), map[string]any{"baseURL": attackerURL}},
	{modmulti2vecjinaai.New(), map[string]any{"baseURL": attackerURL}},
	{modmulti2vecnvidia.New(), map[string]any{"baseURL": attackerURL}},
	{modtwelvelabs.New(), map[string]any{"baseURL": attackerURL}},
	{modmulti2vecvoyageai.New(), map[string]any{"baseURL": attackerURL}},
	{modm2mvjinaai.New(), map[string]any{"baseURL": attackerURL}},
	{modgenerativeopenai.New(), map[string]any{"resourceName": "attacker.example/x?", "deploymentId": "d"}},
	{modqnaopenai.New(), map[string]any{"resourceName": "attacker.example/x?", "deploymentId": "d"}},
	{modopenai.New(), map[string]any{"resourceName": "attacker.example/x?", "deploymentId": "d"}},
	{modgenerativeaws.New(), map[string]any{"service": "sagemaker", "region": "attacker.example/", "endpoint": "e"}},
	{modgenerativeaws.New(), map[string]any{"service": "bedrock", "region": "x@attacker.example#"}},
	{modtext2vecaws.New(), map[string]any{"region": "attacker.example/"}},
	{modmulti2vecaws.New(), map[string]any{"region": "attacker.example/"}},
	// Not Azure without a deploymentId, but a request header can supply one.
	{modgenerativeopenai.New(), map[string]any{"resourceName": "attacker.example/x?"}},
}

func baseURLProvider(mods ...modulecapabilities.Module) *Provider {
	logger, _ := test.NewNullLogger()
	p := NewProvider(logger, config.Config{})
	for _, mod := range mods {
		p.Register(mod)
	}
	return p
}

func classLevelSettings(module string, settings map[string]any) *models.Class {
	return &models.Class{Class: "Article", ModuleConfig: map[string]any{module: settings}}
}

func namedVectorSettings(module string, settings map[string]any) *models.Class {
	return &models.Class{Class: "Article", VectorConfig: map[string]models.VectorConfig{
		"body": {Vectorizer: map[string]any{module: settings}},
	}}
}

var baseURLShapes = []struct {
	name         string
	targetVector string
	build        func(module string, settings map[string]any) *models.Class
}{
	{"class level", "", classLevelSettings},
	{"named vector", "body", namedVectorSettings},
}

// requireNoHostError allows the errors a module's validation returns for settings the table leaves out, such as a
// required model, but not one about the host.
func requireNoHostError(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		require.NotContains(t, err.Error(), "is not allowed")
		require.NotContains(t, err.Error(), "is not a valid")
	}
}

func TestValidateModuleConfig_BaseURLs_EveryModule(t *testing.T) {
	t.Setenv("DIGITALOCEAN_APIKEY", "")
	for _, tt := range baseURLSettings {
		for _, shape := range baseURLShapes {
			name := tt.module.Name() + "/" + shape.name
			t.Run(name+"/custom host is rejected", func(t *testing.T) {
				t.Setenv(modulecomponents.AllowedBaseURLsEnv, "")
				p := baseURLProvider(tt.module)
				err := p.ValidateModuleConfig(context.Background(), shape.build(tt.module.Name(), tt.settings), tt.module.Name(), shape.targetVector)
				if _, ok := tt.settings["resourceName"]; ok {
					require.ErrorContains(t, err, "is not a valid Azure resource name")
				} else if _, ok := tt.settings["region"]; ok {
					require.ErrorContains(t, err, "is not a valid AWS region name")
				} else {
					require.ErrorContains(t, err, "is not allowed")
				}
				require.ErrorContains(t, err, tt.module.Name())
			})
			t.Run(name+"/settings left out use the module default", func(t *testing.T) {
				t.Setenv(modulecomponents.AllowedBaseURLsEnv, "")
				p := baseURLProvider(tt.module)
				requireNoHostError(t, p.ValidateModuleConfig(context.Background(), shape.build(tt.module.Name(), map[string]any{}), tt.module.Name(), shape.targetVector))
			})
		}
	}
}

func TestValidateModuleConfig_BaseURLs_ListedHost(t *testing.T) {
	t.Setenv("DIGITALOCEAN_APIKEY", "")
	t.Setenv(modulecomponents.AllowedBaseURLsEnv, "https://other.example, http://localhost:8080, "+attackerURL)
	for _, tt := range baseURLSettings {
		if _, ok := tt.settings["resourceName"]; ok {
			continue
		}
		if _, ok := tt.settings["region"]; ok {
			continue
		}
		t.Run(tt.module.Name(), func(t *testing.T) {
			p := baseURLProvider(tt.module)
			requireNoHostError(t, p.ValidateModuleConfig(context.Background(), classLevelSettings(tt.module.Name(), tt.settings), tt.module.Name(), ""))
		})
	}
}

func TestValidateModuleConfig_BaseURLs_ProviderHosts(t *testing.T) {
	t.Setenv(modulecomponents.AllowedBaseURLsEnv, "")
	tests := []struct {
		name     string
		module   modulecapabilities.Module
		settings map[string]any
	}{
		{"openai default written out", modgenerativeopenai.New(), map[string]any{"baseURL": "https://api.openai.com"}},
		{"openai default with a path", modopenai.New(), map[string]any{"baseURL": "https://api.openai.com/v1"}},
		{"cohere default written out", modrerankercohere.New(), map[string]any{"baseURL": "https://api.cohere.ai"}},
		{"ollama default written out", modgenerativeollama.New(), map[string]any{"apiEndpoint": "http://localhost:11434"}},
		{"azure resource", modgenerativeopenai.New(), map[string]any{"resourceName": "my-resource", "deploymentId": "d"}},
		{"azure resource, qna", modqnaopenai.New(), map[string]any{"resourceName": "my-resource", "deploymentId": "d"}},
		{"azure resource, vectorizer", modopenai.New(), map[string]any{"resourceName": "my-resource", "deploymentId": "d"}},
		{"aws region, sagemaker", modgenerativeaws.New(), map[string]any{"service": "sagemaker", "region": "us-east-1", "endpoint": "e"}},
		{"aws region, vectorizer", modtext2vecaws.New(), map[string]any{"region": "eu-central-1"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := baseURLProvider(tt.module)
			requireNoHostError(t, p.ValidateModuleConfig(context.Background(), classLevelSettings(tt.module.Name(), tt.settings), tt.module.Name(), ""))
		})
	}
}

// A class without a vectorizer still reads the settings of its generative and reranker modules.
func TestValidateClass_NoVectorizerValidatesClassLevelModules(t *testing.T) {
	t.Setenv(modulecomponents.AllowedBaseURLsEnv, "")
	withoutVectorizer := func(moduleConfig map[string]any) *models.Class {
		return &models.Class{Class: "Article", Vectorizer: "none", ModuleConfig: moduleConfig}
	}
	tests := []struct {
		name    string
		class   *models.Class
		wantErr string
	}{
		{name: "generative module with a foreign host", class: withoutVectorizer(map[string]any{"generative-openai": map[string]any{"baseURL": attackerURL}}), wantErr: "module 'generative-openai': baseURL"},
		{name: "reranker module with a foreign host", class: withoutVectorizer(map[string]any{"reranker-cohere": map[string]any{"baseURL": attackerURL}}), wantErr: "module 'reranker-cohere': baseURL"},
		{name: "generative module with its default host", class: withoutVectorizer(map[string]any{"generative-openai": map[string]any{}})},
		{name: "vectorizer settings the class never reads", class: withoutVectorizer(map[string]any{"text2vec-openai": map[string]any{"baseURL": attackerURL}})},
		{name: "module that is not enabled", class: withoutVectorizer(map[string]any{"generative-anthropic": map[string]any{"model": "m"}})},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := baseURLProvider(modgenerativeopenai.New(), modrerankercohere.New(), modopenai.New())
			err := p.ValidateClass(context.Background(), tt.class)
			if tt.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tt.wantErr)
		})
	}
}

// A module that is not enabled is not validated, and its settings take effect once it is enabled, so a create or
// update may not give it a host-choosing setting. Other settings of such a module stay accepted.
func TestValidateModuleConfigKeys_ModulesNotEnabled(t *testing.T) {
	const anthropic = "generative-anthropic"
	hostSettings := map[string]any{"baseURL": attackerURL}
	tests := []struct {
		name             string
		initial, updated *models.Class
		wantErr          string
	}{
		{
			name:    "create",
			updated: classLevelSettings(anthropic, hostSettings),
			wantErr: `module "generative-anthropic" is not enabled`,
		},
		{
			name:    "update adds it",
			initial: classLevelSettings("generative-openai", map[string]any{}),
			updated: classLevelSettings(anthropic, hostSettings),
			wantErr: `module "generative-anthropic" is not enabled`,
		},
		{
			name:    "update changes it",
			initial: classLevelSettings(anthropic, map[string]any{"baseURL": "https://api.anthropic.com"}),
			updated: classLevelSettings(anthropic, hostSettings),
			wantErr: `module "generative-anthropic" is not enabled`,
		},
		{
			name:    "named vector",
			updated: namedVectorSettings("text2vec-disabled", map[string]any{"options": map[string]any{"Endpoint": "x"}}),
			wantErr: `target vector "body": module "text2vec-disabled" is not enabled`,
		},
		{
			name:    "host setting in another case",
			updated: classLevelSettings(anthropic, map[string]any{"BaseUrl": attackerURL}),
			wantErr: "is not enabled",
		},
		{
			name:    "no host setting",
			updated: classLevelSettings(anthropic, map[string]any{"model": "claude"}),
		},
		{
			name:    "stored settings left alone",
			initial: classLevelSettings(anthropic, hostSettings),
			updated: classLevelSettings(anthropic, hostSettings),
		},
		{
			name:    "enabled module with a host setting is left to its own validation",
			updated: classLevelSettings("generative-openai", hostSettings),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := baseURLProvider(modgenerativeopenai.New())
			err := p.ValidateModuleConfigKeys(tt.updated, tt.initial)
			if tt.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tt.wantErr)
		})
	}
}

// Generative, reranker and other additional-property modules read their settings through a config bound to no
// module name, so a module whose own settings are empty reads the "" key. No module's validation sees that key.
func TestValidateModuleConfigKeys_UnnamedSettings(t *testing.T) {
	const openai = "generative-openai"
	unnamed := func(settings map[string]any) map[string]any {
		return map[string]any{openai: map[string]any{}, "": settings}
	}
	tests := []struct {
		name             string
		initial, updated *models.Class
		wantErr          string
	}{
		{
			name:    "create with a host under the empty key",
			updated: &models.Class{Class: "Article", ModuleConfig: map[string]any{"": map[string]any{"baseURL": attackerURL}}},
			wantErr: "empty module name",
		},
		{
			name:    "create with other settings under the empty key",
			updated: &models.Class{Class: "Article", ModuleConfig: map[string]any{"": map[string]any{"model": "m"}}},
			wantErr: "empty module name",
		},
		{
			name:    "update adds a host under the empty key",
			initial: classLevelSettings(openai, map[string]any{"model": "gpt-4o"}),
			updated: &models.Class{Class: "Article", ModuleConfig: unnamed(map[string]any{"baseURL": attackerURL})},
			wantErr: "empty module name",
		},
		{
			name:    "update changes the empty key",
			initial: &models.Class{Class: "Article", ModuleConfig: unnamed(map[string]any{"model": "a"})},
			updated: &models.Class{Class: "Article", ModuleConfig: unnamed(map[string]any{"model": "a", "baseURL": attackerURL})},
			wantErr: "empty module name",
		},
		{
			name:    "update adds a host under the empty key of a named vector",
			initial: namedVectorSettings("text2vec-openai", map[string]any{}),
			updated: &models.Class{Class: "Article", VectorConfig: map[string]models.VectorConfig{
				"body": {Vectorizer: map[string]any{"text2vec-openai": map[string]any{}, "": map[string]any{"baseURL": attackerURL}}},
			}},
			wantErr: `target vector "body": module config has settings under an empty module name`,
		},
		{
			name:    "stored empty key left alone",
			initial: &models.Class{Class: "Article", ModuleConfig: unnamed(map[string]any{"baseURL": attackerURL})},
			updated: &models.Class{Class: "Article", ModuleConfig: unnamed(map[string]any{"baseURL": attackerURL})},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := baseURLProvider(modgenerativeopenai.New(), modopenai.New())
			err := p.ValidateModuleConfigKeys(tt.updated, tt.initial)
			if tt.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tt.wantErr)
		})
	}
}

// An update cannot remove a stored "" key, since the class-level module config is merged into the stored one. A
// module the update leaves with empty settings would read the host stored there.
func TestValidateModuleConfigKeys_StoredUnnamedSettings(t *testing.T) {
	stored := func(fallback map[string]any) *models.Class {
		return &models.Class{Class: "Article", ModuleConfig: map[string]any{
			"generative-openai": map[string]any{"model": "gpt-4o"},
			"":                  fallback,
		}}
	}
	body := func(modules map[string]any) *models.Class {
		return &models.Class{Class: "Article", ModuleConfig: modules}
	}
	hostFallback := map[string]any{"baseURL": attackerURL}
	tests := []struct {
		name     string
		fallback map[string]any
		updated  *models.Class
		wantErr  string
	}{
		{name: "left alone", fallback: hostFallback, updated: stored(hostFallback)},
		{name: "module settings left out", fallback: hostFallback, updated: body(nil)},
		{name: "generative settings emptied", fallback: hostFallback, updated: body(map[string]any{"generative-openai": map[string]any{}}), wantErr: `module "generative-openai" has no settings of its own`},
		{name: "generative settings set to null", fallback: hostFallback, updated: body(map[string]any{"generative-openai": nil}), wantErr: `module "generative-openai" has no settings of its own`},
		{name: "empty reranker settings added", fallback: hostFallback, updated: body(map[string]any{"reranker-cohere": map[string]any{}}), wantErr: `module "reranker-cohere" has no settings of its own`},
		{name: "reranker settings of its own added", fallback: hostFallback, updated: body(map[string]any{"reranker-cohere": map[string]any{"model": "m"}})},
		{name: "generative settings emptied next to a fallback without a host", fallback: map[string]any{"model": "m"}, updated: body(map[string]any{"generative-openai": map[string]any{}})},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := baseURLProvider(modgenerativeopenai.New(), modrerankercohere.New())
			err := p.ValidateModuleConfigKeys(tt.updated, stored(tt.fallback))
			if tt.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tt.wantErr)
		})
	}
}

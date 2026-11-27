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
	"context"
	"encoding/json"
	"maps"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/versioned"
	modgenerativeanthropic "github.com/weaviate/weaviate/modules/generative-anthropic"
	modgenerativedigitalocean "github.com/weaviate/weaviate/modules/generative-digitalocean"
	modgenerativegoogle "github.com/weaviate/weaviate/modules/generative-google"
	modgenerativeopenai "github.com/weaviate/weaviate/modules/generative-openai"
	modrerankercohere "github.com/weaviate/weaviate/modules/reranker-cohere"
	text2vecdigitalocean "github.com/weaviate/weaviate/modules/text2vec-digitalocean"
	text2vechuggingface "github.com/weaviate/weaviate/modules/text2vec-huggingface"
	text2vecopenai "github.com/weaviate/weaviate/modules/text2vec-openai"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/modulecomponents"
	"github.com/weaviate/weaviate/usecases/modules"
	"github.com/weaviate/weaviate/usecases/sharding"
	shardingConfig "github.com/weaviate/weaviate/usecases/sharding/config"
)

const (
	attackerBaseURL = "https://attacker.example"
	gatewayBaseURL  = "https://gateway.example"
)

func newSchemaWithBaseURLModules(t *testing.T, extra ...modulecapabilities.Module) *schemaWithModules {
	logger, _ := test.NewNullLogger()
	provider := modules.NewProvider(logger, config.Config{})
	provider.Register(modgenerativeopenai.New())
	provider.Register(modrerankercohere.New())
	provider.Register(text2vecopenai.New())
	for _, mod := range extra {
		provider.Register(mod)
	}
	handler, schemaManager := newTestHandlerWithModules(t, provider, provider, provider)
	schemaManager.On("AddClass", mock.Anything, mock.Anything).Return(nil)
	schemaManager.On("UpdateClass", mock.Anything, mock.Anything).Return(nil)
	store := NewFakeStore()
	store.parser = handler.parser
	return &schemaWithModules{handler: handler, schemaManager: schemaManager, store: store}
}

// articleClass is the class of the report: no vectorizer, a text property and class-level module settings.
func articleClass(moduleConfig map[string]any) *models.Class {
	return &models.Class{
		Class:             "Article",
		Vectorizer:        "none",
		VectorIndexType:   hnswT,
		ModuleConfig:      moduleConfig,
		Properties:        testProperties(),
		ReplicationConfig: &models.ReplicationConfig{Factor: 1},
	}
}

func generativeSettings(settings map[string]any) map[string]any {
	return map[string]any{"generative-openai": settings}
}

func openaiVectors(vectors map[string]map[string]any) *models.Class {
	vectorizers := map[string]map[string]any{}
	for name, settings := range vectors {
		vectorizers[name] = map[string]any{"text2vec-openai": maps.Clone(settings)}
	}
	return namedVectorsClass(vectorizers)
}

func TestAddClass_BaseURLs(t *testing.T) {
	tests := []struct {
		name      string
		allowlist string
		class     *models.Class
		wantErr   string
	}{
		{
			name:  "generative module with its default host",
			class: articleClass(generativeSettings(map[string]any{"model": "gpt-4o"})),
		},
		{
			name:    "generative module with a foreign host",
			class:   articleClass(generativeSettings(map[string]any{"baseURL": attackerBaseURL})),
			wantErr: `module 'generative-openai': baseURL "https://attacker.example" is not allowed`,
		},
		{
			name:      "generative module with a listed host",
			allowlist: gatewayBaseURL,
			class:     articleClass(generativeSettings(map[string]any{"baseURL": gatewayBaseURL + "/v1"})),
		},
		{
			name:    "generative module with an Azure resource name that leaves Azure",
			class:   articleClass(generativeSettings(map[string]any{"resourceName": "attacker.example/x?", "deploymentId": "d"})),
			wantErr: "is not a valid Azure resource name",
		},
		{
			name:  "generative module with an Azure resource",
			class: articleClass(generativeSettings(map[string]any{"resourceName": "my-resource", "deploymentId": "d"})),
		},
		{
			name:    "reranker module with a foreign host",
			class:   articleClass(map[string]any{"reranker-cohere": map[string]any{"baseURL": attackerBaseURL}}),
			wantErr: `module 'reranker-cohere'`,
		},
		{
			name:    "named vector with a foreign host",
			class:   openaiVectors(map[string]map[string]any{"body": {"baseURL": attackerBaseURL}}),
			wantErr: `module 'text2vec-openai': baseURL`,
		},
		{
			name:  "named vector with its default host",
			class: openaiVectors(map[string]map[string]any{"body": {}}),
		},
		{
			name:    "host under the empty module key, which generative and reranker modules fall back to",
			class:   articleClass(map[string]any{"": map[string]any{"baseURL": attackerBaseURL}}),
			wantErr: "empty module name",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv(modulecomponents.AllowedBaseURLsEnv, tt.allowlist)
			s := newSchemaWithBaseURLModules(t)
			_, _, err := s.handler.AddClass(context.Background(), nil, tt.class)
			if tt.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tt.wantErr)
			s.schemaManager.AssertNotCalled(t, "AddClass", mock.Anything, mock.Anything)
		})
	}
}

// storeWithoutValidation puts a class into the schema as backup restore or Raft replay would, bypassing AddClass.
func (s *schemaWithModules) storeWithoutValidation(t *testing.T, class *models.Class) *models.Class {
	t.Helper()
	require.NoError(t, s.handler.setClassDefaults(class, s.handler.config.Replication))
	require.NoError(t, s.handler.parser.ParseClass(class))
	s.store.AddClass(class)
	s.schemaManager.On("ReadOnlyClass", class.Class).Return(class)
	s.schemaManager.On("QueryReadOnlyClasses", []string{class.Class}).
		Return(map[string]versioned.Class{class.Class: {Class: class}}, nil)
	return class
}

func TestUpdateClass_BaseURLs(t *testing.T) {
	withDescription := func(class *models.Class) *models.Class {
		class.Description = "changed"
		return class
	}
	tests := []struct {
		name      string
		allowlist string
		extra     []modulecapabilities.Module
		stored    *models.Class
		update    *models.Class
		wantErr   string
	}{
		{
			name:    "reported trigger: generative host changed to a foreign one",
			stored:  articleClass(generativeSettings(map[string]any{})),
			update:  articleClass(generativeSettings(map[string]any{"baseURL": attackerBaseURL})),
			wantErr: `module 'generative-openai': baseURL "https://attacker.example" is not allowed`,
		},
		{
			name:    "generative module added with a foreign host",
			stored:  articleClass(nil),
			update:  articleClass(generativeSettings(map[string]any{"baseURL": attackerBaseURL})),
			wantErr: "is not allowed",
		},
		{
			name:    "reranker module added with a foreign host",
			stored:  articleClass(generativeSettings(map[string]any{})),
			update:  articleClass(map[string]any{"reranker-cohere": map[string]any{"baseURL": attackerBaseURL}}),
			wantErr: `module 'reranker-cohere'`,
		},
		{
			name:      "generative host changed to a listed one",
			allowlist: gatewayBaseURL,
			stored:    articleClass(generativeSettings(map[string]any{})),
			update:    articleClass(generativeSettings(map[string]any{"baseURL": gatewayBaseURL})),
		},
		{
			name:    "generative Azure resource name changed to one that leaves Azure",
			stored:  articleClass(generativeSettings(map[string]any{"resourceName": "my-resource", "deploymentId": "d"})),
			update:  articleClass(generativeSettings(map[string]any{"resourceName": "attacker.example/x?", "deploymentId": "d"})),
			wantErr: "is not a valid Azure resource name",
		},
		{
			name:   "unrelated update of a class whose stored host is no longer allowed",
			stored: articleClass(generativeSettings(map[string]any{"baseURL": gatewayBaseURL})),
			update: withDescription(articleClass(generativeSettings(map[string]any{"baseURL": gatewayBaseURL}))),
		},
		{
			name:   "unrelated update that leaves module settings out",
			stored: articleClass(generativeSettings(map[string]any{"baseURL": gatewayBaseURL})),
			update: withDescription(articleClass(nil)),
		},
		{
			name:    "other generative setting changed next to a stored host that is not listed",
			stored:  articleClass(generativeSettings(map[string]any{"baseURL": gatewayBaseURL})),
			update:  articleClass(generativeSettings(map[string]any{"baseURL": gatewayBaseURL, "model": "gpt-4o"})),
			wantErr: "is not allowed",
		},
		{
			name:      "other generative setting changed next to a stored host that is listed",
			allowlist: gatewayBaseURL,
			stored:    articleClass(generativeSettings(map[string]any{"baseURL": gatewayBaseURL})),
			update:    articleClass(generativeSettings(map[string]any{"baseURL": gatewayBaseURL, "model": "gpt-4o"})),
		},
		{
			name:    "generative module replaced by another one with a foreign host",
			extra:   []modulecapabilities.Module{modgenerativeanthropic.New()},
			stored:  articleClass(generativeSettings(map[string]any{})),
			update:  articleClass(map[string]any{"generative-anthropic": map[string]any{"baseURL": attackerBaseURL}}),
			wantErr: "module 'generative-anthropic': baseURL",
		},
		{
			name:    "generative module added with a setting its validation refuses",
			stored:  articleClass(nil),
			update:  articleClass(generativeSettings(map[string]any{"temperature": 5})),
			wantErr: "temperature",
		},
		{
			name:    "generative setting changed to one its validation refuses",
			stored:  articleClass(generativeSettings(map[string]any{"temperature": 0.5})),
			update:  articleClass(generativeSettings(map[string]any{"temperature": 5})),
			wantErr: "temperature",
		},
		{
			name:    "named vector added with a setting its validation refuses",
			stored:  openaiVectors(map[string]map[string]any{"title": {}}),
			update:  openaiVectors(map[string]map[string]any{"title": {}, "body": {"type": "bogus"}}),
			wantErr: "wrong OpenAI type name",
		},
		{
			name:    "vectorizer module added to the class-level settings with a foreign host",
			stored:  articleClass(nil),
			update:  articleClass(map[string]any{"text2vec-openai": map[string]any{"baseURL": attackerBaseURL}}),
			wantErr: "module 'text2vec-openai': baseURL",
		},
		{
			name:    "stored host replaced by another foreign one",
			stored:  articleClass(generativeSettings(map[string]any{"baseURL": gatewayBaseURL})),
			update:  articleClass(generativeSettings(map[string]any{"baseURL": attackerBaseURL})),
			wantErr: "is not allowed",
		},
		{
			name:    "named vector added with a foreign host",
			stored:  openaiVectors(map[string]map[string]any{"title": {}}),
			update:  openaiVectors(map[string]map[string]any{"title": {}, "body": {"baseURL": attackerBaseURL}}),
			wantErr: `module 'text2vec-openai': baseURL`,
		},
		{
			name:      "named vector added with a listed host",
			allowlist: gatewayBaseURL,
			stored:    openaiVectors(map[string]map[string]any{"title": {}}),
			update:    openaiVectors(map[string]map[string]any{"title": {}, "body": {"baseURL": gatewayBaseURL}}),
		},
		{
			name:    "host added under the empty module key next to emptied generative settings",
			stored:  articleClass(generativeSettings(map[string]any{"model": "gpt-4o"})),
			update:  articleClass(map[string]any{"generative-openai": map[string]any{}, "": map[string]any{"baseURL": attackerBaseURL}}),
			wantErr: "empty module name",
		},
		{
			name:   "unrelated update of a class whose stored host fails today's format check",
			stored: articleClass(generativeSettings(map[string]any{"baseURL": "https://münchen.example"})),
			update: withDescription(articleClass(generativeSettings(map[string]any{"baseURL": "https://münchen.example"}))),
		},
		{
			name:   "unrelated update of a class whose named vector has a stored host",
			stored: openaiVectors(map[string]map[string]any{"title": {"baseURL": gatewayBaseURL}}),
			update: withDescription(openaiVectors(map[string]map[string]any{"title": {"baseURL": gatewayBaseURL}})),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv(modulecomponents.AllowedBaseURLsEnv, tt.allowlist)
			s := newSchemaWithBaseURLModules(t, tt.extra...)
			s.storeWithoutValidation(t, tt.stored)
			err := s.update(tt.update)
			if tt.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tt.wantErr)
			s.schemaManager.AssertNotCalled(t, "UpdateClass", mock.Anything, mock.Anything)
		})
	}
}

// Backup restore runs module validation as create does, so a backed-up host must be a module default or listed.
func TestRestoreClass_BaseURLs(t *testing.T) {
	tests := []struct {
		name      string
		allowlist string
		class     *models.Class
		wantErr   string
	}{
		{name: "generative module, host not listed", class: articleClass(generativeSettings(map[string]any{"baseURL": gatewayBaseURL})), wantErr: "is not allowed"},
		{name: "named vector, host not listed", class: openaiVectors(map[string]map[string]any{"body": {"baseURL": gatewayBaseURL}}), wantErr: "is not allowed"},
		{name: "generative module, host listed", allowlist: gatewayBaseURL, class: articleClass(generativeSettings(map[string]any{"baseURL": gatewayBaseURL}))},
		{name: "named vector, host listed", allowlist: gatewayBaseURL, class: openaiVectors(map[string]map[string]any{"body": {"baseURL": gatewayBaseURL}})},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv(modulecomponents.AllowedBaseURLsEnv, tt.allowlist)
			s := newSchemaWithBaseURLModules(t)
			s.schemaManager.On("ReadOnlyClass", tt.class.Class).Return(nil).Maybe()
			s.schemaManager.On("RestoreClass", mock.Anything, mock.Anything).Return(nil)

			schemaBytes, err := json.Marshal(tt.class)
			require.NoError(t, err)
			cfg, err := shardingConfig.ParseConfig(nil, 1)
			require.NoError(t, err)
			state, err := sharding.InitState(tt.class.Class, cfg, "node1", []string{"node1"}, 1, false)
			require.NoError(t, err)
			stateBytes, err := state.JSON()
			require.NoError(t, err)

			descriptor := backup.ClassDescriptor{Name: tt.class.Class, Schema: schemaBytes, ShardingState: stateBytes}
			err = s.handler.RestoreClass(context.Background(), &descriptor, map[string]string{}, false, false)
			if tt.wantErr == "" {
				require.NoError(t, err)
				s.schemaManager.AssertCalled(t, "RestoreClass", mock.Anything, mock.Anything)
				return
			}
			require.ErrorContains(t, err, tt.wantErr)
			s.schemaManager.AssertNotCalled(t, "RestoreClass", mock.Anything, mock.Anything)
		})
	}
}

// Module validation of the DigitalOcean modules lists the provider's models with the server's key, so a rejected host
// must be rejected before that request is sent.
func TestRejectedBaseURLIsNeverCalled(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()
	t.Setenv("DIGITALOCEAN_APIKEY", "server-key")
	t.Setenv(modulecomponents.AllowedBaseURLsEnv, "")

	tests := []struct {
		name  string
		class *models.Class
	}{
		{"generative module next to a named vector", func() *models.Class {
			class := openaiVectors(map[string]map[string]any{"body": {}})
			class.ModuleConfig = map[string]any{"generative-digitalocean": map[string]any{"baseURL": server.URL, "model": "m"}}
			return class
		}()},
		{"named vector", namedVectorsClass(map[string]map[string]any{"body": {"text2vec-digitalocean": map[string]any{"baseURL": server.URL, "model": "m"}}})},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			requests.Store(0)
			s := newSchemaWithBaseURLModules(t, modgenerativedigitalocean.New(), text2vecdigitalocean.New())
			_, _, err := s.handler.AddClass(context.Background(), nil, tt.class)
			require.ErrorContains(t, err, "is not allowed")
			require.Zero(t, requests.Load())
		})
	}

	t.Run("update adding a named vector", func(t *testing.T) {
		requests.Store(0)
		s := newSchemaWithBaseURLModules(t, text2vecdigitalocean.New())
		s.storeWithoutValidation(t, openaiVectors(map[string]map[string]any{"title": {}}))
		update := openaiVectors(map[string]map[string]any{"title": {}})
		update.VectorConfig["body"] = models.VectorConfig{VectorIndexType: hnswT, Vectorizer: map[string]any{
			"text2vec-digitalocean": map[string]any{"baseURL": server.URL, "model": "m"},
		}}
		require.ErrorContains(t, s.update(update), "is not allowed")
		require.Zero(t, requests.Load())
	})

	t.Run("listed host is called by module validation", func(t *testing.T) {
		requests.Store(0)
		t.Setenv(modulecomponents.AllowedBaseURLsEnv, server.URL)
		s := newSchemaWithBaseURLModules(t, text2vecdigitalocean.New())
		_, _, err := s.handler.AddClass(context.Background(), nil,
			namedVectorsClass(map[string]map[string]any{"body": {"text2vec-digitalocean": map[string]any{"baseURL": server.URL, "model": "m"}}}))
		require.ErrorContains(t, err, "list DigitalOcean models")
		require.NotZero(t, requests.Load())
	})
}

// Internal updates (the BlockMax WAND switch, the drop of a vector index) rewrite a copy of the stored class, so they
// must pass whatever module settings it holds, even ones today's checks would refuse.
func TestUpdateClassInternal_KeepsAnyStoredBaseURL(t *testing.T) {
	stored := func() *models.Class {
		class := openaiVectors(map[string]map[string]any{
			"title": {"baseURL": "https://münchen.example"},
			"body":  {"baseURL": attackerBaseURL, "resourceName": "attacker.example/x?"},
		})
		class.ModuleConfig = map[string]any{
			"generative-openai": map[string]any{},
			"":                  map[string]any{"baseURL": attackerBaseURL},
			"reranker-cohere":   map[string]any{"baseURL": "not a url"},
		}
		return class
	}
	t.Setenv(modulecomponents.AllowedBaseURLsEnv, "")
	s := newSchemaWithBaseURLModules(t)
	class := s.storeWithoutValidation(t, stored())

	raw, err := json.Marshal(class)
	require.NoError(t, err)
	var copied models.Class
	require.NoError(t, json.Unmarshal(raw, &copied))
	copied.InvertedIndexConfig.UsingBlockMaxWAND = true
	require.NoError(t, UpdateClassInternal(s.handler, context.Background(), copied.Class, &copied))
}

// The update body is merged into the stored module config at Raft apply, so a stored "" key stays: a body that empties
// or adds a class-level module makes that module read the "" key's host.
func TestUpdateClass_StoredUnnamedSettings(t *testing.T) {
	stored := func() *models.Class {
		return articleClass(map[string]any{
			"generative-openai": map[string]any{"model": "gpt-4o"},
			"":                  map[string]any{"baseURL": attackerBaseURL},
		})
	}
	tests := []struct {
		name    string
		body    map[string]any
		wantErr string
	}{
		{name: "generative settings emptied", body: map[string]any{"generative-openai": map[string]any{}}, wantErr: `module "generative-openai"`},
		{name: "generative settings set to null", body: map[string]any{"generative-openai": nil}, wantErr: `module "generative-openai"`},
		{name: "empty reranker settings added", body: map[string]any{"reranker-cohere": map[string]any{}}, wantErr: `module "reranker-cohere"`},
		{name: "module settings left out", body: nil},
		{name: "generative setting changed", body: map[string]any{"generative-openai": map[string]any{"model": "gpt-4.1"}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv(modulecomponents.AllowedBaseURLsEnv, "")
			s := newSchemaWithBaseURLModules(t)
			s.storeWithoutValidation(t, stored())
			update := articleClass(tt.body)
			update.Description = "changed"
			err := s.update(update)
			if tt.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tt.wantErr)
			require.ErrorContains(t, err, "empty module name")
			s.schemaManager.AssertNotCalled(t, "UpdateClass", mock.Anything, mock.Anything)
		})
	}
}

func TestBaseURLs_ModuleNotEnabled(t *testing.T) {
	anthropic := map[string]any{"generative-anthropic": map[string]any{"baseURL": attackerBaseURL}}
	t.Run("create", func(t *testing.T) {
		t.Setenv(modulecomponents.AllowedBaseURLsEnv, "")
		s := newSchemaWithBaseURLModules(t)
		_, _, err := s.handler.AddClass(context.Background(), nil, articleClass(anthropic))
		require.ErrorContains(t, err, `module "generative-anthropic" is not enabled`)
	})
	t.Run("update", func(t *testing.T) {
		t.Setenv(modulecomponents.AllowedBaseURLsEnv, "")
		s := newSchemaWithBaseURLModules(t)
		s.storeWithoutValidation(t, articleClass(generativeSettings(map[string]any{})))
		require.ErrorContains(t, s.update(articleClass(anthropic)), `module "generative-anthropic" is not enabled`)
	})
	// Refused earlier, when the vector index config is parsed; the provider-level test covers the host rule itself.
	t.Run("named vector", func(t *testing.T) {
		t.Setenv(modulecomponents.AllowedBaseURLsEnv, "")
		s := newSchemaWithBaseURLModules(t)
		s.storeWithoutValidation(t, openaiVectors(map[string]map[string]any{"title": {}}))
		update := openaiVectors(map[string]map[string]any{"title": {}})
		update.VectorConfig["body"] = models.VectorConfig{VectorIndexType: hnswT, Vectorizer: map[string]any{
			"text2vec-disabled": map[string]any{"baseURL": attackerBaseURL},
		}}
		require.ErrorContains(t, s.update(update), `"text2vec-disabled"`)
	})
	t.Run("settings without a host", func(t *testing.T) {
		t.Setenv(modulecomponents.AllowedBaseURLsEnv, "")
		s := newSchemaWithBaseURLModules(t)
		s.storeWithoutValidation(t, articleClass(generativeSettings(map[string]any{})))
		require.NoError(t, s.update(articleClass(map[string]any{"generative-anthropic": map[string]any{"model": "claude"}})))
	})
}

func TestUpdateClass_StoredInvalidResourceNameDoesNotCoverTheBaseURL(t *testing.T) {
	t.Setenv(modulecomponents.AllowedBaseURLsEnv, "")
	s := newSchemaWithBaseURLModules(t)
	s.storeWithoutValidation(t, articleClass(generativeSettings(map[string]any{"resourceName": "a.b", "deploymentId": "d"})))
	err := s.update(articleClass(generativeSettings(map[string]any{"resourceName": "a.b", "deploymentId": "d", "baseURL": attackerBaseURL})))
	require.ErrorContains(t, err, `baseURL "https://attacker.example" is not allowed`)
	s.schemaManager.AssertNotCalled(t, "UpdateClass", mock.Anything, mock.Anything)
}

func huggingFaceVector(endpointURL string) models.VectorConfig {
	return models.VectorConfig{
		VectorIndexType: hnswT,
		Vectorizer:      map[string]any{"text2vec-huggingface": map[string]any{"endpointURL": endpointURL}},
	}
}

func huggingFaceVectors(vectors map[string]string) *models.Class {
	class := articleClass(nil)
	class.Vectorizer, class.VectorIndexType = "", ""
	class.VectorConfig = map[string]models.VectorConfig{}
	for name, endpointURL := range vectors {
		class.VectorConfig[name] = huggingFaceVector(endpointURL)
	}
	return class
}

func googleGenerativeSettings(apiEndpoint string) map[string]any {
	return map[string]any{"generative-google": map[string]any{"apiEndpoint": apiEndpoint}}
}

// Module settings an update adds or changes are validated by the module as on create: text2vec-huggingface sends the
// server's HUGGINGFACE_APIKEY and every inserted object's text to endpointURL, and generative-google keeps its
// apiEndpoint under googleapis.com.
func TestUpdateClass_HuggingFaceAndGoogleModuleSettings(t *testing.T) {
	const (
		inferenceEndpoint = "https://abc.us-east-1.aws.endpoints.huggingface.cloud"
		foreignEndpoint   = "https://attacker.example/e"
		googleEndpoint    = "generativelanguage.googleapis.com"
	)
	withModules := func(class *models.Class, moduleConfig map[string]any) *models.Class {
		class.ModuleConfig = moduleConfig
		return class
	}
	legacyHuggingFace := func(endpointURL string) *models.Class {
		class := articleClass(map[string]any{"text2vec-huggingface": map[string]any{"endpointURL": endpointURL}})
		class.Vectorizer = "text2vec-huggingface"
		return class
	}
	tests := []struct {
		name            string
		stored, updated *models.Class
		wantErr         string
	}{
		{
			name:    "reported trigger: named vector with a foreign endpointURL added",
			stored:  huggingFaceVectors(map[string]string{"main": inferenceEndpoint}),
			updated: huggingFaceVectors(map[string]string{"main": inferenceEndpoint, "x": foreignEndpoint}),
			wantErr: `endpointURL: baseURL "https://attacker.example/e" is not allowed`,
		},
		{
			name:    "named vector with a Hugging Face Inference Endpoint added",
			stored:  huggingFaceVectors(map[string]string{"main": inferenceEndpoint}),
			updated: huggingFaceVectors(map[string]string{"main": inferenceEndpoint, "x": inferenceEndpoint}),
		},
		{
			name:    "existing named vector moved to a foreign endpointURL",
			stored:  huggingFaceVectors(map[string]string{"main": inferenceEndpoint}),
			updated: huggingFaceVectors(map[string]string{"main": foreignEndpoint}),
			wantErr: "immutable",
		},
		{
			name:    "class-level vectorizer moved to a foreign endpointURL",
			stored:  legacyHuggingFace(inferenceEndpoint),
			updated: legacyHuggingFace(foreignEndpoint),
			wantErr: "endpointURL",
		},
		{
			name:    "generative module with a foreign endpoint added",
			stored:  huggingFaceVectors(map[string]string{"main": inferenceEndpoint}),
			updated: withModules(huggingFaceVectors(map[string]string{"main": inferenceEndpoint}), googleGenerativeSettings("attacker.example")),
			wantErr: "apiEndpoint must be a Google API host",
		},
		{
			name:    "generative module moved to a foreign endpoint",
			stored:  withModules(huggingFaceVectors(map[string]string{"main": inferenceEndpoint}), googleGenerativeSettings(googleEndpoint)),
			updated: withModules(huggingFaceVectors(map[string]string{"main": inferenceEndpoint}), googleGenerativeSettings("attacker.example")),
			wantErr: "apiEndpoint must be a Google API host",
		},
		{
			name:    "generative module with a foreign endpoint added to a class without a vectorizer",
			stored:  articleClass(nil),
			updated: articleClass(googleGenerativeSettings("attacker.example")),
			wantErr: "apiEndpoint must be a Google API host",
		},
		{
			name:    "generative module with a Google endpoint added",
			stored:  huggingFaceVectors(map[string]string{"main": inferenceEndpoint}),
			updated: withModules(huggingFaceVectors(map[string]string{"main": inferenceEndpoint}), googleGenerativeSettings(googleEndpoint)),
		},
		{
			name:   "unrelated update of a class whose stored endpointURL is no longer allowed",
			stored: huggingFaceVectors(map[string]string{"main": foreignEndpoint}),
			updated: func() *models.Class {
				class := huggingFaceVectors(map[string]string{"main": foreignEndpoint})
				class.Description = "changed"
				return class
			}(),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv(modulecomponents.AllowedBaseURLsEnv, "")
			s := newSchemaWithBaseURLModules(t, text2vechuggingface.New(), modgenerativegoogle.New())
			s.storeWithoutValidation(t, tt.stored)
			err := s.update(tt.updated)
			if tt.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tt.wantErr)
			s.schemaManager.AssertNotCalled(t, "UpdateClass", mock.Anything, mock.Anything)
		})
	}
}

func TestAddClass_ForeignHuggingFaceEndpoint(t *testing.T) {
	legacy := articleClass(map[string]any{"text2vec-huggingface": map[string]any{"endpointURL": "https://attacker.example/e"}})
	legacy.Vectorizer = "text2vec-huggingface"
	tests := []struct {
		name  string
		class *models.Class
	}{
		{"named vector", huggingFaceVectors(map[string]string{"x": "https://attacker.example/e"})},
		{"class-level vectorizer", legacy},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv(modulecomponents.AllowedBaseURLsEnv, "")
			s := newSchemaWithBaseURLModules(t, text2vechuggingface.New())
			_, _, err := s.handler.AddClass(context.Background(), nil, tt.class)
			require.ErrorContains(t, err, "is not allowed")
			s.schemaManager.AssertNotCalled(t, "AddClass", mock.Anything, mock.Anything)
		})
	}
}

// A named vector the update adds is validated as create validates it, so the checks that are not about module
// settings, such as the vector name, apply too.
func TestUpdateClass_AddedNamedVectorIsValidatedAsOnCreate(t *testing.T) {
	tests := []struct {
		name         string
		targetVector string
		vectorConfig models.VectorConfig
		wantErr      string
	}{
		{
			name:         "name that is not a GraphQL name",
			targetVector: "bad-name!",
			vectorConfig: models.VectorConfig{VectorIndexType: hnswT, Vectorizer: map[string]any{"text2vec-openai": map[string]any{}}},
			wantErr:      `target vector name "bad-name!" is not valid`,
		},
		{
			name:         "name that is too long",
			targetVector: strings.Repeat("v", 300),
			vectorConfig: models.VectorConfig{VectorIndexType: hnswT, Vectorizer: map[string]any{"text2vec-openai": map[string]any{}}},
			wantErr:      "should not be longer than",
		},
		{
			name:         "two vectorizers",
			targetVector: "body",
			vectorConfig: models.VectorConfig{VectorIndexType: hnswT, Vectorizer: map[string]any{
				"text2vec-openai":      map[string]any{},
				"text2vec-huggingface": map[string]any{},
			}},
			wantErr: "must consist only 1 configuration",
		},
		{
			name:         "valid vector",
			targetVector: "body",
			vectorConfig: models.VectorConfig{VectorIndexType: hnswT, Vectorizer: map[string]any{"text2vec-openai": map[string]any{}}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv(modulecomponents.AllowedBaseURLsEnv, "")
			s := newSchemaWithBaseURLModules(t, text2vechuggingface.New())
			s.storeWithoutValidation(t, openaiVectors(map[string]map[string]any{"title": {}}))
			update := openaiVectors(map[string]map[string]any{"title": {}})
			update.VectorConfig[tt.targetVector] = tt.vectorConfig
			err := s.update(update)
			if tt.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tt.wantErr)
			s.schemaManager.AssertNotCalled(t, "UpdateClass", mock.Anything, mock.Anything)
		})
	}
}

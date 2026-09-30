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
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	modgenerativedummy "github.com/weaviate/weaviate/modules/generative-dummy"
	modgoogle "github.com/weaviate/weaviate/modules/text2vec-google"
	"github.com/weaviate/weaviate/modules/text2vec-google/vectorizer"
	modopenai "github.com/weaviate/weaviate/modules/text2vec-openai"
	modweaviateembed "github.com/weaviate/weaviate/modules/text2vec-weaviate"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/modules"
)

const (
	aiStudioEndpoint = "generativelanguage.googleapis.com"
	vertexEndpoint   = "us-central1-aiplatform.googleapis.com"
	googleVectorName = "gemini"
)

type moduleValidationRecorder struct {
	*modules.Provider
	validated []string
}

func (r *moduleValidationRecorder) ValidateModuleConfig(ctx context.Context, class *models.Class, moduleName, targetVector string) error {
	r.validated = append(r.validated, moduleName+"/"+targetVector)
	return r.Provider.ValidateModuleConfig(ctx, class, moduleName, targetVector)
}

type schemaWithModules struct {
	handler       *Handler
	schemaManager *fakeSchemaManager
	store         *fakeStore
	modules       *moduleValidationRecorder
}

func newSchemaWithModules(t *testing.T) *schemaWithModules {
	logger, _ := test.NewNullLogger()
	provider := modules.NewProvider(logger, config.Config{})
	provider.Register(modgoogle.New())
	provider.Register(modopenai.New())
	provider.Register(modweaviateembed.New())
	provider.Register(modgenerativedummy.New())
	recorder := &moduleValidationRecorder{Provider: provider}

	handler, schemaManager := newTestHandlerWithModules(t, provider, recorder, provider)
	schemaManager.On("QueryCollectionsCount").Return(0, nil)
	schemaManager.On("AddClass", mock.Anything, mock.Anything).Return(nil)
	schemaManager.On("UpdateClass", mock.Anything, mock.Anything).Return(nil)

	store := NewFakeStore()
	store.parser = handler.parser
	return &schemaWithModules{handler: handler, schemaManager: schemaManager, store: store, modules: recorder}
}

func (s *schemaWithModules) create(t *testing.T, class *models.Class) *models.Class {
	t.Helper()
	_, _, err := s.handler.AddClass(context.Background(), nil, class)
	require.NoError(t, err)
	s.store.AddClass(class)
	s.schemaManager.On("ReadOnlyClass", class.Class).Return(class)
	return class
}

func (s *schemaWithModules) update(class *models.Class) error {
	if err := s.handler.UpdateClass(context.Background(), nil, class.Class, class); err != nil {
		return err
	}
	return s.store.UpdateClass(class)
}

func aiStudioSettings() map[string]any {
	return map[string]any{
		"apiEndpoint":        aiStudioEndpoint,
		"model":              "gemini-embedding-001",
		"dimensions":         1536,
		"taskType":           "RETRIEVAL_QUERY",
		"titleProperty":      "title",
		"vectorizeClassName": false,
	}
}

func vertexSettings() map[string]any {
	return withSettings(aiStudioSettings(), map[string]any{"apiEndpoint": vertexEndpoint, "projectId": "my-project"})
}

func withSettings(base, changes map[string]any) map[string]any {
	out := maps.Clone(base)
	maps.Copy(out, changes)
	return out
}

func withoutSettings(base map[string]any, keys ...string) map[string]any {
	out := maps.Clone(base)
	for _, key := range keys {
		delete(out, key)
	}
	return out
}

func vectorizerTestProperties() []*models.Property {
	return []*models.Property{
		{Name: "text", DataType: schema.DataTypeText.PropString()},
		{Name: "title", DataType: schema.DataTypeText.PropString()},
	}
}

func legacyVectorizerClass(module string, settings map[string]any) *models.Class {
	return &models.Class{
		Class:             "Docs",
		Vectorizer:        module,
		VectorIndexType:   hnswT,
		ModuleConfig:      map[string]any{module: maps.Clone(settings)},
		Properties:        vectorizerTestProperties(),
		ReplicationConfig: &models.ReplicationConfig{Factor: 1},
	}
}

func namedVectorizerClass(module string, settings map[string]any) *models.Class {
	return namedVectorizersClass(map[string]models.VectorConfig{googleVectorName: namedVectorizer(module, settings)})
}

func namedVectorizersClass(vectors map[string]models.VectorConfig) *models.Class {
	return &models.Class{
		Class:             "Docs",
		VectorConfig:      vectors,
		Properties:        vectorizerTestProperties(),
		ReplicationConfig: &models.ReplicationConfig{Factor: 1},
	}
}

func namedVectorizer(module string, settings map[string]any) models.VectorConfig {
	settings = withSettings(map[string]any{"properties": []any{"text", "title"}}, settings)
	return models.VectorConfig{VectorIndexType: hnswT, Vectorizer: map[string]any{module: settings}}
}

func mixedVectorizerClass(module string, settings map[string]any) *models.Class {
	class := legacyVectorizerClass(module, settings)
	class.VectorConfig = map[string]models.VectorConfig{
		"extra": {VectorIndexType: hnswT, Vectorizer: map[string]any{"none": map[string]any{}}},
	}
	return class
}

func jsonCopyClass(t *testing.T, class *models.Class) *models.Class {
	t.Helper()
	b, err := json.Marshal(class)
	require.NoError(t, err)
	var out models.Class
	require.NoError(t, json.Unmarshal(b, &out))
	return &out
}

type effectiveGoogleSettings struct {
	model      string
	dimensions *int64
}

func effectiveSettings(class *models.Class, module, targetVector string) effectiveGoogleSettings {
	settings := vectorizer.NewClassSettings(modules.NewClassBasedModuleConfig(class, module, "", targetVector, &config.Config{}))
	return effectiveGoogleSettings{model: settings.Model(), dimensions: settings.Dimensions()}
}

func storedVectorizerSettings(t *testing.T, class *models.Class, module, targetVector string) map[string]any {
	t.Helper()
	if targetVector != "" {
		vectorizer, ok := class.VectorConfig[targetVector].Vectorizer.(map[string]any)
		require.True(t, ok)
		return structToMap(vectorizer[module])
	}
	moduleConfig, ok := class.ModuleConfig.(map[string]any)
	require.True(t, ok)
	return structToMap(moduleConfig[module])
}

type vectorizerShape struct {
	name          string
	build         func(module string, settings map[string]any) *models.Class
	targetVector  string
	immutableText string
}

var vectorizerShapes = []vectorizerShape{
	{
		name: "legacy", build: legacyVectorizerClass,
		immutableText: "can only update generative and reranker module configs",
	},
	{
		name: "named vector", build: namedVectorizerClass, targetVector: googleVectorName,
		immutableText: "vectorizer config of vector \"gemini\" is immutable",
	},
	{
		name: "mixed", build: mixedVectorizerClass,
		immutableText: "can only update generative and reranker module configs",
	},
}

func createForShape(t *testing.T, s *schemaWithModules, shape vectorizerShape, module string, settings map[string]any) *models.Class {
	t.Helper()
	if shape.name != "mixed" {
		return s.create(t, shape.build(module, settings))
	}
	stored := s.create(t, legacyVectorizerClass(module, settings))
	require.NoError(t, s.update(mixedVectorizerClass(module, settings)))
	require.Len(t, stored.VectorConfig, 1)
	return stored
}

type acceptedUpdate struct {
	name    string
	initial map[string]any
	update  map[string]any
}

type rejectedUpdate struct {
	name          string
	initial       map[string]any
	update        map[string]any
	namedOnly     bool
	expectedError string
}

func acceptedUpdates() []acceptedUpdate {
	withoutModel := withoutSettings(aiStudioSettings(), "model", "dimensions")
	return []acceptedUpdate{
		{name: "AI Studio to Vertex", initial: aiStudioSettings(), update: vertexSettings()},
		{
			name: "AI Studio to Vertex with location", initial: aiStudioSettings(),
			update: withSettings(vertexSettings(), map[string]any{"apiEndpoint": "europe-west4-aiplatform.googleapis.com", "location": "europe-west4"}),
		},
		{name: "Vertex to AI Studio", initial: vertexSettings(), update: aiStudioSettings()},
		{name: "Vertex project", initial: vertexSettings(), update: withSettings(vertexSettings(), map[string]any{"projectId": "other-project"})},
		{name: "Vertex project number", initial: vertexSettings(), update: withSettings(vertexSettings(), map[string]any{"projectId": "123456789012"})},
		{
			name: "Vertex location", initial: withSettings(vertexSettings(), map[string]any{"location": "us-central1"}),
			update: withSettings(vertexSettings(), map[string]any{"location": "europe-west4"}),
		},
		{
			name: "removing location", initial: withSettings(vertexSettings(), map[string]any{"location": "europe-west4"}),
			update: vertexSettings(),
		},
		{
			name: "AI Studio to Vertex without a model setting", initial: withoutModel,
			update: withSettings(withoutModel, map[string]any{"apiEndpoint": vertexEndpoint, "projectId": "my-project"}),
		},
	}
}

func rejectedUpdates() []rejectedUpdate {
	withoutModel := withoutSettings(aiStudioSettings(), "model", "dimensions")
	return []rejectedUpdate{
		{name: "model", update: withSettings(aiStudioSettings(), map[string]any{"model": "text-embedding-005"})},
		{name: "modelId", update: withSettings(aiStudioSettings(), map[string]any{"modelId": "text-embedding-005"})},
		{name: "dimensions", update: withSettings(aiStudioSettings(), map[string]any{"dimensions": 768})},
		{name: "taskType", update: withSettings(aiStudioSettings(), map[string]any{"taskType": "QUESTION_ANSWERING"})},
		{name: "titleProperty", update: withSettings(aiStudioSettings(), map[string]any{"titleProperty": "text"})},
		{name: "vectorizeClassName", update: withSettings(aiStudioSettings(), map[string]any{"vectorizeClassName": true})},
		{name: "properties", update: withSettings(aiStudioSettings(), map[string]any{"properties": []any{"text"}}), namedOnly: true},
		{name: "endpoint and model together", update: withSettings(vertexSettings(), map[string]any{"model": "text-embedding-005"})},
		{
			name:    "endpoint for a model other than gemini-embedding-001",
			initial: withSettings(aiStudioSettings(), map[string]any{"model": "text-embedding-004"}),
			update:  withSettings(vertexSettings(), map[string]any{"model": "text-embedding-004"}),
		},
		{
			name:    "endpoint while adding an explicit model",
			initial: withoutModel,
			update:  withSettings(withoutModel, map[string]any{"apiEndpoint": vertexEndpoint, "projectId": "my-project", "model": "gemini-embedding-001"}),
		},
		{name: "endpoint while dropping dimensions", update: withoutSettings(vertexSettings(), "dimensions")},
		{
			name:          "Vertex without projectId",
			update:        withSettings(aiStudioSettings(), map[string]any{"apiEndpoint": vertexEndpoint}),
			expectedError: "projectId cannot be empty",
		},
		{
			name:          "projectId carrying a path",
			update:        withSettings(vertexSettings(), map[string]any{"projectId": "my-project/locations/x"}),
			expectedError: "projectId must be a Google Cloud project ID or project number",
		},
		{
			name:          "endpoint outside googleapis.com",
			update:        withSettings(aiStudioSettings(), map[string]any{"apiEndpoint": "attacker.example.com", "projectId": "my-project"}),
			expectedError: "apiEndpoint must be a Google API host",
		},
	}
}

func requireAcceptedUpdate(t *testing.T, module string, shape vectorizerShape, tc acceptedUpdate) {
	s := newSchemaWithModules(t)
	stored := createForShape(t, s, shape, module, tc.initial)
	before := effectiveSettings(jsonCopyClass(t, stored), module, shape.targetVector)
	s.modules.validated = nil

	require.NoError(t, s.update(shape.build(module, tc.update)))

	settings := storedVectorizerSettings(t, stored, module, shape.targetVector)
	for _, key := range []string{"apiEndpoint", "projectId", "location"} {
		require.Equal(t, tc.update[key], settings[key], key)
	}
	require.Equal(t, before, effectiveSettings(stored, module, shape.targetVector))
	require.Equal(t, []string{module + "/" + shape.targetVector}, s.modules.validated)
}

func requireRejectedUpdate(t *testing.T, module string, shape vectorizerShape, tc rejectedUpdate) {
	initial := tc.initial
	if initial == nil {
		initial = aiStudioSettings()
	}
	s := newSchemaWithModules(t)
	createForShape(t, s, shape, module, initial)

	expectedError := tc.expectedError
	if expectedError == "" {
		expectedError = shape.immutableText
	}
	require.ErrorContains(t, s.update(shape.build(module, tc.update)), expectedError)
}

func runShapeSubtests(t *testing.T, module string, shape vectorizerShape) {
	for _, tc := range acceptedUpdates() {
		t.Run(module+"/"+shape.name+"/accepts "+tc.name, func(t *testing.T) {
			requireAcceptedUpdate(t, module, shape, tc)
		})
	}

	for _, tc := range rejectedUpdates() {
		if tc.namedOnly && shape.name != "named vector" {
			continue
		}
		t.Run(module+"/"+shape.name+"/rejects "+tc.name, func(t *testing.T) {
			requireRejectedUpdate(t, module, shape, tc)
		})
	}
}

func requireNamedVectorSwitchWhileOtherUnchanged(t *testing.T) {
	s := newSchemaWithModules(t)
	stored := s.create(t, namedVectorizersClass(map[string]models.VectorConfig{
		"a": namedVectorizer(modgoogle.Name, aiStudioSettings()),
		"b": namedVectorizer(modgoogle.Name, aiStudioSettings()),
	}))

	require.NoError(t, s.update(namedVectorizersClass(map[string]models.VectorConfig{
		"a": namedVectorizer(modgoogle.Name, vertexSettings()),
		"b": namedVectorizer(modgoogle.Name, aiStudioSettings()),
	})))
	require.Equal(t, vertexEndpoint, storedVectorizerSettings(t, stored, modgoogle.Name, "a")["apiEndpoint"])
	require.Equal(t, aiStudioEndpoint, storedVectorizerSettings(t, stored, modgoogle.Name, "b")["apiEndpoint"])
	require.Equal(t, []string{modgoogle.Name + "/a"}, s.modules.validated)
}

func requireNamedVectorSwitchRejectedWhileOtherChangesModel(t *testing.T) {
	s := newSchemaWithModules(t)
	s.create(t, namedVectorizersClass(map[string]models.VectorConfig{
		"a": namedVectorizer(modgoogle.Name, aiStudioSettings()),
		"b": namedVectorizer(modgoogle.Name, aiStudioSettings()),
	}))

	err := s.update(namedVectorizersClass(map[string]models.VectorConfig{
		"a": namedVectorizer(modgoogle.Name, vertexSettings()),
		"b": namedVectorizer(modgoogle.Name, withSettings(aiStudioSettings(), map[string]any{"model": "text-embedding-005"})),
	}))
	require.ErrorContains(t, err, "vectorizer config of vector \"b\" is immutable")
}

func requireEndpointSwitchWithGenerativeChange(t *testing.T) {
	s := newSchemaWithModules(t)
	initial := namedVectorizerClass(modgoogle.Name, aiStudioSettings())
	initial.ModuleConfig = map[string]any{modgenerativedummy.Name: map[string]any{"setting": "a"}}
	stored := s.create(t, initial)

	update := namedVectorizerClass(modgoogle.Name, vertexSettings())
	update.ModuleConfig = map[string]any{modgenerativedummy.Name: map[string]any{"setting": "b"}}
	require.NoError(t, s.update(update))

	require.Equal(t, vertexEndpoint, storedVectorizerSettings(t, stored, modgoogle.Name, googleVectorName)["apiEndpoint"])
	require.Equal(t, "b", storedVectorizerSettings(t, stored, modgenerativedummy.Name, "")["setting"])
}

func requireNoValidationWhenSettingsUnchanged(t *testing.T) {
	for _, shape := range vectorizerShapes {
		s := newSchemaWithModules(t)
		createForShape(t, s, shape, modgoogle.Name, aiStudioSettings())
		s.modules.validated = nil

		update := shape.build(modgoogle.Name, aiStudioSettings())
		update.Description = "new description"
		require.NoError(t, s.update(update), shape.name)
		require.Empty(t, s.modules.validated, shape.name)
	}
}

func requireOpenAIBaseURLChangeAccepted(t *testing.T) {
	settings := map[string]any{"model": "text-embedding-3-small", "baseURL": "https://api.openai.com"}
	s := newSchemaWithModules(t)
	s.create(t, namedVectorizerClass(modopenai.Name, settings))

	require.NoError(t, s.update(namedVectorizerClass(modopenai.Name, withSettings(settings, map[string]any{"baseURL": "https://proxy.example.com"}))))
}

func requirePalmRenameToGoogleRejected(t *testing.T) {
	s := newSchemaWithModules(t)
	s.create(t, namedVectorizerClass(modgoogle.LegacyName, aiStudioSettings()))

	err := s.update(namedVectorizerClass(modgoogle.Name, vertexSettings()))
	require.ErrorContains(t, err, "is immutable")
}

func TestUpdateClass_MutableVectorizerSettings(t *testing.T) {
	for _, module := range []string{modgoogle.Name, modgoogle.LegacyName} {
		for _, shape := range vectorizerShapes {
			runShapeSubtests(t, module, shape)
		}
	}

	t.Run("accepts one named vector switching while another is unchanged", requireNamedVectorSwitchWhileOtherUnchanged)
	t.Run("rejects one named vector switching while another changes its model", requireNamedVectorSwitchRejectedWhileOtherChangesModel)
	t.Run("accepts an endpoint switch together with a generative module change", requireEndpointSwitchWithGenerativeChange)
	t.Run("does not validate module settings when vectorizer settings are unchanged", requireNoValidationWhenSettingsUnchanged)
	t.Run("text2vec-openai baseURL change is accepted", requireOpenAIBaseURLChangeAccepted)
	t.Run("text2vec-palm cannot be renamed to text2vec-google", requirePalmRenameToGoogleRejected)
}

func legacyPalmSettings() map[string]any {
	return map[string]any{
		"apiEndpoint":        aiStudioEndpoint,
		"dimensions":         1536,
		"modelId":            "gemini-embedding-001",
		"properties":         []any{"content"},
		"vectorizeClassName": true,
	}
}

func legacyPalmClass(module string, settings map[string]any) *models.Class {
	return &models.Class{
		Class:      "Chunks",
		Properties: []*models.Property{{Name: "content", DataType: schema.DataTypeText.PropString()}},
		VectorConfig: map[string]models.VectorConfig{
			googleVectorName: {VectorIndexType: hnswT, Vectorizer: map[string]any{module: maps.Clone(settings)}},
		},
		MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true},
		ReplicationConfig:  &models.ReplicationConfig{Factor: 1},
	}
}

func TestUpdateClass_MutableVectorizerSettings_LegacyPalmModelIDShape(t *testing.T) {
	vertex := withSettings(legacyPalmSettings(), map[string]any{
		"apiEndpoint": vertexEndpoint, "projectId": "my-project", "location": "us-central1",
	})
	dimensions := int64(1536)

	tests := []struct {
		name          string
		initial       map[string]any
		module        string
		update        map[string]any
		expectedError string
	}{
		{name: "accepts AI Studio to Vertex", initial: legacyPalmSettings(), module: modgoogle.LegacyName, update: vertex},
		{name: "accepts Vertex to AI Studio", initial: vertex, module: modgoogle.LegacyName, update: legacyPalmSettings()},
		{
			name:          "rejects the model sent as model instead of modelId",
			initial:       legacyPalmSettings(),
			module:        modgoogle.LegacyName,
			update:        withoutSettings(withSettings(vertex, map[string]any{"model": "gemini-embedding-001"}), "modelId"),
			expectedError: `vectorizer config of vector "gemini" is immutable`,
		},
		{
			name:          "rejects the module key sent as text2vec-google",
			initial:       legacyPalmSettings(),
			module:        modgoogle.Name,
			update:        vertex,
			expectedError: `vectorizer config of vector "gemini" is immutable`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := newSchemaWithModules(t)
			stored := s.create(t, legacyPalmClass(modgoogle.LegacyName, tt.initial))
			s.modules.validated = nil

			err := s.update(legacyPalmClass(tt.module, tt.update))
			if tt.expectedError != "" {
				require.ErrorContains(t, err, tt.expectedError)
				return
			}
			require.NoError(t, err)
			require.Equal(t, structToMap(tt.update), storedVectorizerSettings(t, stored, modgoogle.LegacyName, googleVectorName))
			require.Equal(t, effectiveGoogleSettings{model: "gemini-embedding-001", dimensions: &dimensions},
				effectiveSettings(jsonCopyClass(t, stored), modgoogle.LegacyName, googleVectorName))
			require.Equal(t, []string{modgoogle.LegacyName + "/" + googleVectorName}, s.modules.validated)
		})
	}
}

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
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/schema"
	modcohere "github.com/weaviate/weaviate/modules/text2vec-cohere"
	modgoogle "github.com/weaviate/weaviate/modules/text2vec-google"
	modhuggingface "github.com/weaviate/weaviate/modules/text2vec-huggingface"
	modjinaai "github.com/weaviate/weaviate/modules/text2vec-jinaai"
	modopenai "github.com/weaviate/weaviate/modules/text2vec-openai"
	modvoyageai "github.com/weaviate/weaviate/modules/text2vec-voyageai"
	modweaviateembed "github.com/weaviate/weaviate/modules/text2vec-weaviate"
)

func TestUpdateClass_RejectedUpdateLeavesStoredClassUnchanged(t *testing.T) {
	changes := []struct {
		name   string
		update func(shape vectorizerShape) *models.Class
	}{
		{
			name: "endpoint switch with a model change",
			update: func(shape vectorizerShape) *models.Class {
				return shape.build(modgoogle.Name, withSettings(vertexSettings(), map[string]any{"model": "text-embedding-005"}))
			},
		},
		{
			name: "endpoint switch with an added property",
			update: func(shape vectorizerShape) *models.Class {
				class := shape.build(modgoogle.Name, vertexSettings())
				class.Properties = append(class.Properties, &models.Property{Name: "added", DataType: schema.DataTypeText.PropString()})
				return class
			},
		},
		{
			name: "endpoint switch with a renamed property",
			update: func(shape vectorizerShape) *models.Class {
				class := shape.build(modgoogle.Name, vertexSettings())
				class.Properties[0].Name = "renamed"
				return class
			},
		},
	}

	createStored := func(t *testing.T, s *schemaWithModules, shape vectorizerShape) *models.Class {
		stored := createForShape(t, s, shape, modgoogle.Name, aiStudioSettings())
		slices.Reverse(stored.Properties)
		return stored
	}

	for _, shape := range vectorizerShapes {
		for _, change := range changes {
			t.Run(shape.name+"/"+change.name+"/through the handler", func(t *testing.T) {
				s := newSchemaWithModules(t)
				stored := createStored(t, s, shape)
				before := jsonCopyClass(t, stored)

				require.Error(t, s.update(change.update(shape)))
				require.Equal(t, before, jsonCopyClass(t, stored))
			})

			t.Run(shape.name+"/"+change.name+"/through Raft apply", func(t *testing.T) {
				s := newSchemaWithModules(t)
				stored := createStored(t, s, shape)
				before := jsonCopyClass(t, stored)

				_, err := s.handler.parser.ParseClassUpdate(stored, jsonCopyClass(t, change.update(shape)))
				require.Error(t, err)
				require.Equal(t, before, jsonCopyClass(t, stored))
			})
		}
	}
}

func TestUpdateClass_MigratesRenamedVectorizerSetting(t *testing.T) {
	const baseURL = "https://api.embedding.weaviate.io"
	oldSettings := map[string]any{"baseUrl": baseURL, "model": "Snowflake/snowflake-arctic-embed-l-v2.0"}
	newSettings := map[string]any{"baseURL": baseURL, "model": "Snowflake/snowflake-arctic-embed-l-v2.0"}

	shapes := []struct {
		name         string
		build        func(settings map[string]any) *models.Class
		targetVector string
	}{
		{
			name: "legacy",
			build: func(settings map[string]any) *models.Class {
				return legacyVectorizerClass(modweaviateembed.Name, settings)
			},
		},
		{
			name: "named vector",
			build: func(settings map[string]any) *models.Class {
				return namedVectorizerClass(modweaviateembed.Name, settings)
			},
			targetVector: googleVectorName,
		},
	}
	for _, shape := range shapes {
		t.Run(shape.name, func(t *testing.T) {
			s := newSchemaWithModules(t)
			stored := s.create(t, shape.build(oldSettings))
			require.Equal(t, baseURL, storedVectorizerSettings(t, stored, modweaviateembed.Name, shape.targetVector)["baseUrl"])

			require.NoError(t, s.update(shape.build(newSettings)))

			settings := storedVectorizerSettings(t, stored, modweaviateembed.Name, shape.targetVector)
			require.Equal(t, baseURL, settings["baseURL"])
			require.NotContains(t, settings, "baseUrl")
		})
	}
}

func TestUpdateClass_AddingAModuleUnderAnotherNameIsRejected(t *testing.T) {
	tests := []struct {
		name   string
		stored string
		added  string
	}{
		{name: "text2vec-google next to a stored text2vec-palm", stored: modgoogle.LegacyName, added: modgoogle.Name},
		{name: "text2vec-palm next to a stored text2vec-google", stored: modgoogle.Name, added: modgoogle.LegacyName},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := newSchemaWithModules(t)
			stored := s.create(t, legacyVectorizerClass(tt.stored, aiStudioSettings()))
			before := jsonCopyClass(t, stored)

			update := legacyVectorizerClass(tt.stored, aiStudioSettings())
			update.ModuleConfig.(map[string]any)[tt.added] = withSettings(aiStudioSettings(), map[string]any{
				"apiEndpoint": "attacker.example.com",
			})

			err := s.handler.UpdateClass(context.Background(), nil, update.Class, update)
			require.ErrorContains(t, err, "is already configured as")
			require.Equal(t, before, jsonCopyClass(t, stored))
		})
	}

	t.Run("an unrelated module can still be added", func(t *testing.T) {
		s := newSchemaWithModules(t)
		stored := s.create(t, legacyVectorizerClass(modgoogle.Name, aiStudioSettings()))

		update := legacyVectorizerClass(modgoogle.Name, aiStudioSettings())
		update.ModuleConfig.(map[string]any)[modopenai.Name] = map[string]any{"model": "text-embedding-3-small"}

		require.NoError(t, s.update(update))
		require.Contains(t, stored.ModuleConfig, modopenai.Name)
	})
}

// Earlier versions accepted this entry into the Raft log, so apply must still accept it.
func TestParseClassUpdate_AcceptsAModuleUnderAnotherName(t *testing.T) {
	s := newSchemaWithModules(t)
	stored := s.create(t, legacyVectorizerClass(modgoogle.LegacyName, aiStudioSettings()))

	update := jsonCopyClass(t, stored)
	update.ModuleConfig.(map[string]any)[modgoogle.Name] = aiStudioSettings()

	require.NoError(t, s.store.UpdateClass(update))
	require.Contains(t, stored.ModuleConfig, modgoogle.LegacyName)
	require.Contains(t, stored.ModuleConfig, modgoogle.Name)
}

// Documents current behaviour, not the behaviour we want.
// A model change is accepted, which leaves stored vectors from the old model.
func TestUpdateClass_ModelChangeIsAcceptedForModulesWithMigrateProperties(t *testing.T) {
	tests := []struct {
		module       modulecapabilities.Module
		model, other string
	}{
		{module: modopenai.New(), model: "text-embedding-3-small", other: "text-embedding-3-large"},
		{module: modcohere.New(), model: "embed-multilingual-v3.0", other: "embed-english-v3.0"},
		{module: modhuggingface.New(), model: "sentence-transformers/all-MiniLM-L6-v2", other: "sentence-transformers/all-mpnet-base-v2"},
		{module: modjinaai.New(), model: "jina-embeddings-v3", other: "jina-embeddings-v2-base-en"},
		{module: modvoyageai.New(), model: "voyage-3", other: "voyage-3-large"},
		{module: modweaviateembed.New(), model: "Snowflake/snowflake-arctic-embed-l-v2.0", other: "Snowflake/snowflake-arctic-embed-m-v1.5"},
	}
	for _, tt := range tests {
		t.Run(tt.module.Name(), func(t *testing.T) {
			s := newSchemaWithModules(t)
			s.modules.Register(tt.module)
			stored := s.create(t, namedVectorizerClass(tt.module.Name(), map[string]any{"model": tt.model}))

			require.NoError(t, s.update(namedVectorizerClass(tt.module.Name(), map[string]any{"model": tt.other})))
			require.Equal(t, tt.other, storedVectorizerSettings(t, stored, tt.module.Name(), googleVectorName)["model"])
		})
	}
}

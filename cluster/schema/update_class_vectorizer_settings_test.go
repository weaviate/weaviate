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
	"maps"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	command "github.com/weaviate/weaviate/cluster/proto/api"
	clusterSchema "github.com/weaviate/weaviate/cluster/schema"
	"github.com/weaviate/weaviate/entities/models"
	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
	"github.com/weaviate/weaviate/entities/vectorindex"
	modgoogle "github.com/weaviate/weaviate/modules/text2vec-google"
	modweaviateembed "github.com/weaviate/weaviate/modules/text2vec-weaviate"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/fakes"
	"github.com/weaviate/weaviate/usecases/modules"
	"github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
)

const className = "Docs"

type acceptAllIndexUpdates struct{}

func (acceptAllIndexUpdates) ValidateVectorIndexConfigUpdate(old, updated schemaConfig.VectorIndexConfig) error {
	return nil
}

func (acceptAllIndexUpdates) ValidateInvertedIndexConfigUpdate(old, updated *models.InvertedIndexConfig) error {
	return nil
}

func (acceptAllIndexUpdates) ValidateVectorIndexConfigsUpdate(old, updated map[string]schemaConfig.VectorIndexConfig) error {
	return nil
}

func newSchemaManagerWithModules(t *testing.T) *clusterSchema.SchemaManager {
	t.Helper()
	logger, _ := test.NewNullLogger()
	provider := modules.NewProvider(logger, config.Config{})
	provider.Register(modgoogle.New())
	provider.Register(modweaviateembed.New())
	parser := schema.NewParser(fakes.NewFakeClusterState(), vectorindex.ParseAndValidateConfig,
		acceptAllIndexUpdates{}, provider, nil, nil)
	return clusterSchema.NewSchemaManager("node1", nil, parser, prometheus.NewPedanticRegistry(), logrus.New())
}

func applyRequest(t *testing.T, cmdType command.ApplyRequest_Type, className string, version uint64, subCommand any) *command.ApplyRequest {
	t.Helper()
	b, err := json.Marshal(subCommand)
	require.NoError(t, err)
	return &command.ApplyRequest{Type: cmdType, Class: className, Version: version, SubCommand: b}
}

func addClassRequest(t *testing.T, class *models.Class) *command.ApplyRequest {
	t.Helper()
	return applyRequest(t, command.ApplyRequest_TYPE_ADD_CLASS, class.Class, 1, command.AddClassRequest{
		Class: class,
		State: &sharding.State{Physical: map[string]sharding.Physical{}},
	})
}

func updateClassRequest(t *testing.T, class *models.Class, version uint64) *command.ApplyRequest {
	t.Helper()
	return applyRequest(t, command.ApplyRequest_TYPE_UPDATE_CLASS, class.Class, version, command.UpdateClassRequest{Class: class})
}

func aiStudioSettings() map[string]any {
	return map[string]any{
		"apiEndpoint":        "generativelanguage.googleapis.com",
		"model":              "gemini-embedding-001",
		"dimensions":         1536,
		"vectorizeClassName": false,
	}
}

func withSettings(changes map[string]any) map[string]any {
	out := aiStudioSettings()
	maps.Copy(out, changes)
	return out
}

var vertexChanges = map[string]any{"apiEndpoint": "us-central1-aiplatform.googleapis.com", "projectId": "my-project"}

type googleClassShape struct {
	name           string
	build          func(settings map[string]any) *models.Class
	settings       func(c *models.Class) map[string]any
	immutableError string
}

var googleClassShapes = []googleClassShape{
	{
		name:           "legacy module config",
		immutableError: "can only update generative and reranker module configs",
		build: func(settings map[string]any) *models.Class {
			return &models.Class{
				Class:           className,
				Vectorizer:      modgoogle.LegacyName,
				VectorIndexType: "hnsw",
				ModuleConfig:    map[string]any{modgoogle.LegacyName: settings},
			}
		},
		settings: func(c *models.Class) map[string]any {
			return c.ModuleConfig.(map[string]any)[modgoogle.LegacyName].(map[string]any)
		},
	},
	{
		name:           "named vector",
		immutableError: "vectorizer config of vector \"gemini\" is immutable",
		build: func(settings map[string]any) *models.Class {
			return &models.Class{
				Class: className,
				VectorConfig: map[string]models.VectorConfig{
					"gemini": {
						VectorIndexType: "hnsw",
						Vectorizer:      map[string]any{modgoogle.LegacyName: settings},
					},
				},
			}
		},
		settings: func(c *models.Class) map[string]any {
			return c.VectorConfig["gemini"].Vectorizer.(map[string]any)[modgoogle.LegacyName].(map[string]any)
		},
	},
}

func TestSchemaManager_UpdateClass_GoogleEndpointSettings(t *testing.T) {
	for _, shape := range googleClassShapes {
		t.Run(shape.name, func(t *testing.T) {
			addClass := addClassRequest(t, shape.build(aiStudioSettings()))
			switchToVertex := updateClassRequest(t, shape.build(withSettings(vertexChanges)), 2)
			changeModel := updateClassRequest(t, shape.build(withSettings(map[string]any{"model": "text-embedding-005"})), 3)

			requireVertexSettings := func(t *testing.T, sm *clusterSchema.SchemaManager) {
				t.Helper()
				settings := shape.settings(sm.NewSchemaReader().ReadOnlyClass(className))
				require.Equal(t, "us-central1-aiplatform.googleapis.com", settings["apiEndpoint"])
				require.Equal(t, "my-project", settings["projectId"])
				require.Equal(t, "gemini-embedding-001", settings["model"])
				require.EqualValues(t, 1536, settings["dimensions"])
			}

			t.Run("apply, then replay the same entry", func(t *testing.T) {
				sm := newSchemaManagerWithModules(t)
				require.NoError(t, sm.AddClass(addClass, "node1", true, false))
				require.NoError(t, sm.UpdateClass(switchToVertex, "node1", true, false))
				requireVertexSettings(t, sm)

				require.NoError(t, sm.UpdateClass(switchToVertex, "node1", true, false))
				requireVertexSettings(t, sm)
			})

			t.Run("model change is rejected", func(t *testing.T) {
				sm := newSchemaManagerWithModules(t)
				require.NoError(t, sm.AddClass(addClass, "node1", true, false))
				require.NoError(t, sm.UpdateClass(switchToVertex, "node1", true, false))

				err := sm.UpdateClass(changeModel, "node1", true, false)
				require.ErrorIs(t, err, clusterSchema.ErrBadRequest)
				require.ErrorContains(t, err, shape.immutableError)
			})
		})
	}
}

func TestSchemaManager_UpdateClass_MigratesRenamedVectorizerSetting(t *testing.T) {
	const baseURL = "https://api.embedding.weaviate.io"
	build := func(settings map[string]any) *models.Class {
		return &models.Class{
			Class:           className,
			Vectorizer:      modweaviateembed.Name,
			VectorIndexType: "hnsw",
			ModuleConfig:    map[string]any{modweaviateembed.Name: settings},
		}
	}
	sm := newSchemaManagerWithModules(t)
	require.NoError(t, sm.AddClass(addClassRequest(t, build(map[string]any{"baseUrl": baseURL})), "node1", true, false))

	require.NoError(t, sm.UpdateClass(updateClassRequest(t, build(map[string]any{"baseURL": baseURL}), 2), "node1", true, false))

	stored := sm.NewSchemaReader().ReadOnlyClass(className).ModuleConfig.(map[string]any)[modweaviateembed.Name].(map[string]any)
	require.Equal(t, baseURL, stored["baseURL"])
	require.NotContains(t, stored, "baseUrl")
}

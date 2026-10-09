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
	"reflect"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	command "github.com/weaviate/weaviate/cluster/proto/api"
	clusterSchema "github.com/weaviate/weaviate/cluster/schema"
	"github.com/weaviate/weaviate/entities/models"
	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
	"github.com/weaviate/weaviate/entities/vectorindex"
	"github.com/weaviate/weaviate/usecases/fakes"
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

// endpointModules lets every module change its "endpoint" setting and nothing else.
type endpointModules struct{}

func (endpointModules) IsReranker(string) bool                  { return false }
func (endpointModules) IsGenerative(string) bool                { return false }
func (endpointModules) IsMultiVector(string) bool               { return false }
func (endpointModules) HasModule(string) bool                   { return true }
func (endpointModules) MigrateVectorizerSettings(any, any) bool { return false }

func (endpointModules) MutableSettings(_ string, current, updated map[string]any) bool {
	current, updated = maps.Clone(current), maps.Clone(updated)
	delete(current, "endpoint")
	delete(updated, "endpoint")
	return reflect.DeepEqual(current, updated)
}

func newSchemaManagerWithModules(t *testing.T) *clusterSchema.SchemaManager {
	t.Helper()
	parser := schema.NewParser(fakes.NewFakeClusterState(), vectorindex.ParseAndValidateConfig,
		acceptAllIndexUpdates{}, endpointModules{}, nil, nil)
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

const module = "text2vec-mutable"

func moduleSettings(endpoint, model string) map[string]any {
	return map[string]any{"endpoint": endpoint, "model": model}
}

type classShape struct {
	name           string
	build          func(settings map[string]any) *models.Class
	settings       func(c *models.Class) map[string]any
	immutableError string
}

var classShapes = []classShape{
	{
		name:           "class-level module config",
		immutableError: "can only update generative and reranker module configs",
		build: func(settings map[string]any) *models.Class {
			return &models.Class{
				Class:           className,
				Vectorizer:      module,
				VectorIndexType: "hnsw",
				ModuleConfig:    map[string]any{module: maps.Clone(settings)},
			}
		},
		settings: func(c *models.Class) map[string]any {
			return c.ModuleConfig.(map[string]any)[module].(map[string]any)
		},
	},
	{
		name:           "named vector",
		immutableError: `vectorizer config of vector "vec" is immutable`,
		build: func(settings map[string]any) *models.Class {
			return &models.Class{
				Class: className,
				VectorConfig: map[string]models.VectorConfig{
					"vec": {VectorIndexType: "hnsw", Vectorizer: map[string]any{module: maps.Clone(settings)}},
				},
			}
		},
		settings: func(c *models.Class) map[string]any {
			return c.VectorConfig["vec"].Vectorizer.(map[string]any)[module].(map[string]any)
		},
	},
}

func TestSchemaManager_UpdateClass_MutableSettings(t *testing.T) {
	for _, shape := range classShapes {
		t.Run(shape.name, func(t *testing.T) {
			addClass := addClassRequest(t, shape.build(moduleSettings("a", "m")))
			changeEndpoint := updateClassRequest(t, shape.build(moduleSettings("b", "m")), 2)
			changeModel := updateClassRequest(t, shape.build(moduleSettings("b", "other")), 3)

			t.Run("apply, then replay the same entry", func(t *testing.T) {
				sm := newSchemaManagerWithModules(t)
				require.NoError(t, sm.AddClass(addClass, "node1", true, false))
				require.NoError(t, sm.UpdateClass(changeEndpoint, "node1", true, false))
				require.Equal(t, moduleSettings("b", "m"), shape.settings(sm.NewSchemaReader().ReadOnlyClass(className)))

				require.NoError(t, sm.UpdateClass(changeEndpoint, "node1", true, false))
				require.Equal(t, moduleSettings("b", "m"), shape.settings(sm.NewSchemaReader().ReadOnlyClass(className)))
			})

			t.Run("change the module does not allow is rejected", func(t *testing.T) {
				sm := newSchemaManagerWithModules(t)
				require.NoError(t, sm.AddClass(addClass, "node1", true, false))
				require.NoError(t, sm.UpdateClass(changeEndpoint, "node1", true, false))

				err := sm.UpdateClass(changeModel, "node1", true, false)
				require.ErrorIs(t, err, clusterSchema.ErrBadRequest)
				require.ErrorContains(t, err, shape.immutableError)
			})
		})
	}
}

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
	"errors"
	"maps"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/fakes"
	shardingcfg "github.com/weaviate/weaviate/usecases/sharding/config"
)

const (
	mutableModule    = "text2vec-mutable"
	fixedModule      = "text2vec-fixed"
	migratingModule  = "text2vec-migrating"
	generativeModule = "generative-fake"
)

// mutableModules lets a module whose name contains "mutable" change only its "endpoint" setting.
type mutableModules struct{ fakeModulesProvider }

func (mutableModules) MutableSettings(module string, current, updated map[string]any) bool {
	if !strings.Contains(module, "mutable") {
		return false
	}
	current, updated = maps.Clone(current), maps.Clone(updated)
	delete(current, "endpoint")
	delete(updated, "endpoint")
	return reflect.DeepEqual(current, updated)
}

// MigrateVectorizerSettings copies the new settings over the stored ones like modules.Provider does, and reports a
// migration when a module whose name contains "migrating" is present.
func (mutableModules) MigrateVectorizerSettings(initial, updated any) bool {
	initialConfig, _ := initial.(map[string]any)
	updatedConfig, _ := updated.(map[string]any)
	migrated := false
	for module, settings := range updatedConfig {
		initialSettings, initialOk := initialConfig[module].(map[string]any)
		updatedSettings, updatedOk := settings.(map[string]any)
		if initialOk && updatedOk {
			maps.Copy(initialSettings, updatedSettings)
		}
		migrated = migrated || strings.Contains(module, "migrating")
	}
	return migrated
}

func moduleSettings(endpoint, model string) map[string]any {
	return map[string]any{"endpoint": endpoint, "model": model}
}

func TestMutableSettingsChanges(t *testing.T) {
	classLevel := func(modules map[string]any) *models.Class {
		return &models.Class{ModuleConfig: modules}
	}
	named := func(vectorizers map[string]map[string]any) *models.Class {
		vectorConfig := map[string]models.VectorConfig{}
		for name, vectorizer := range vectorizers {
			vectorConfig[name] = models.VectorConfig{Vectorizer: vectorizer}
		}
		return &models.Class{VectorConfig: vectorConfig}
	}

	tests := []struct {
		name             string
		initial, updated *models.Class
		want             map[string][]string
	}{
		{
			name:    "class-level change the module allows",
			initial: classLevel(map[string]any{mutableModule: moduleSettings("a", "m")}),
			updated: classLevel(map[string]any{mutableModule: moduleSettings("b", "m")}),
			want:    map[string][]string{"": {mutableModule}},
		},
		{
			name:    "class-level change the module rejects",
			initial: classLevel(map[string]any{mutableModule: moduleSettings("a", "m")}),
			updated: classLevel(map[string]any{mutableModule: moduleSettings("a", "other")}),
			want:    map[string][]string{},
		},
		{
			name:    "module without the capability",
			initial: classLevel(map[string]any{fixedModule: moduleSettings("a", "m")}),
			updated: classLevel(map[string]any{fixedModule: moduleSettings("b", "m")}),
			want:    map[string][]string{},
		},
		{
			name:    "unchanged settings",
			initial: classLevel(map[string]any{mutableModule: moduleSettings("a", "m")}),
			updated: classLevel(map[string]any{mutableModule: moduleSettings("a", "m")}),
			want:    map[string][]string{},
		},
		{
			name:    "same settings, number decoded as json.Number and as float",
			initial: classLevel(map[string]any{mutableModule: map[string]any{"endpoint": "a", "dimensions": json.Number("768")}}),
			updated: classLevel(map[string]any{mutableModule: map[string]any{"endpoint": "a", "dimensions": float64(768)}}),
			want:    map[string][]string{},
		},
		{
			name:    "module added at class level",
			initial: classLevel(map[string]any{}),
			updated: classLevel(map[string]any{mutableModule: moduleSettings("b", "m")}),
			want:    map[string][]string{},
		},
		{
			name: "named vector change the module allows, other vector unchanged",
			initial: named(map[string]map[string]any{
				"a": {mutableModule: moduleSettings("x", "m")}, "b": {mutableModule: moduleSettings("x", "m")},
			}),
			updated: named(map[string]map[string]any{
				"a": {mutableModule: moduleSettings("y", "m")}, "b": {mutableModule: moduleSettings("x", "m")},
			}),
			want: map[string][]string{"a": {mutableModule}},
		},
		{
			name:    "named vector gets a second module",
			initial: named(map[string]map[string]any{"a": {mutableModule: moduleSettings("x", "m")}}),
			updated: named(map[string]map[string]any{"a": {mutableModule: moduleSettings("y", "m"), fixedModule: map[string]any{}}}),
			want:    map[string][]string{},
		},
		{
			name:    "new named vector",
			initial: named(map[string]map[string]any{}),
			updated: named(map[string]map[string]any{"a": {mutableModule: moduleSettings("y", "m")}}),
			want:    map[string][]string{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var validated []string
			got, err := mutableSettingsChanges(mutableModules{}, tt.initial, tt.updated, func(module, targetVector string) error {
				validated = append(validated, module+"/"+targetVector)
				return nil
			})
			require.NoError(t, err)
			require.Equal(t, tt.want, got)

			var wantValidated []string
			for targetVector, modules := range tt.want {
				for _, module := range modules {
					wantValidated = append(wantValidated, module+"/"+targetVector)
				}
			}
			require.ElementsMatch(t, wantValidated, validated)
		})
	}

	t.Run("returns the validation error", func(t *testing.T) {
		initial := namedVectorClass(mutableModule, moduleSettings("a", "m"))
		updated := namedVectorClass(mutableModule, moduleSettings("b", "m"))
		_, err := mutableSettingsChanges(mutableModules{}, initial, updated, func(string, string) error {
			return errors.New("invalid endpoint")
		})
		require.ErrorContains(t, err, "invalid endpoint")
	})
}

// Which module is checked first depends on map order, so the update is applied to a fresh class many times.
func TestParseClassUpdate_MutableSettingsJudgedAgainstStoredSettings(t *testing.T) {
	p := NewParser(fakes.NewFakeClusterState(), dummyParseVectorConfig, fakeValidator{}, mutableModules{}, nil, nil)
	shardingConfig := shardingcfg.Config{
		DesiredCount: 1, VirtualPerPhysical: 128, ActualCount: 1, DesiredVirtualCount: 128, Key: "_id", Strategy: "hash", Function: "murmur3",
	}
	for range 50 {
		stored := &models.Class{Class: "Docs", VectorIndexType: hnswT, VectorIndexConfig: hnsw.NewDefaultUserConfig(), ShardingConfig: shardingConfig, ModuleConfig: map[string]any{
			mutableModule:   moduleSettings("a", "m"),
			migratingModule: map[string]any{"setting": "a"},
		}}
		update := &models.Class{Class: "Docs", VectorIndexType: hnswT, VectorIndexConfig: hnsw.NewDefaultUserConfig(), ModuleConfig: map[string]any{
			mutableModule:   map[string]any{"model": "other"},
			migratingModule: map[string]any{"setting": "b"},
		}}

		_, err := p.ParseClassUpdate(stored, update)
		require.ErrorContains(t, err, "can only update generative and reranker module configs")
	}
}

type moduleValidationRecorder struct {
	fakeModuleConfig
	validated []string
	err       error
}

func (r *moduleValidationRecorder) SetClassDefaults(*models.Class) {}

func (r *moduleValidationRecorder) ValidateModuleConfig(_ context.Context, _ *models.Class, moduleName, targetVector string) error {
	r.validated = append(r.validated, moduleName+"/"+targetVector)
	return r.err
}

type schemaWithModules struct {
	handler       *Handler
	schemaManager *fakeSchemaManager
	store         *fakeStore
	modules       *moduleValidationRecorder
}

func newSchemaWithModules(t *testing.T) *schemaWithModules {
	recorder := &moduleValidationRecorder{}
	vectorizers := &fakeVectorizerValidator{valid: []string{mutableModule, fixedModule}}
	handler, schemaManager := newTestHandlerWithModules(t, mutableModules{}, recorder, vectorizers)
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

func storedModuleSettings(t *testing.T, class *models.Class, module, targetVector string) map[string]any {
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

func testProperties() []*models.Property {
	return []*models.Property{{Name: "text", DataType: schema.DataTypeText.PropString()}}
}

func legacyClass(module string, settings map[string]any) *models.Class {
	return &models.Class{
		Class:             "Docs",
		Vectorizer:        module,
		VectorIndexType:   hnswT,
		ModuleConfig:      map[string]any{module: maps.Clone(settings)},
		Properties:        testProperties(),
		ReplicationConfig: &models.ReplicationConfig{Factor: 1},
	}
}

func namedVectorsClass(vectorizers map[string]map[string]any) *models.Class {
	vectorConfig := map[string]models.VectorConfig{}
	for name, vectorizer := range vectorizers {
		vectorConfig[name] = models.VectorConfig{VectorIndexType: hnswT, Vectorizer: vectorizer}
	}
	return &models.Class{
		Class:             "Docs",
		VectorConfig:      vectorConfig,
		Properties:        testProperties(),
		ReplicationConfig: &models.ReplicationConfig{Factor: 1},
	}
}

func namedVectorClass(module string, settings map[string]any) *models.Class {
	return namedVectorsClass(map[string]map[string]any{"vec": {module: maps.Clone(settings)}})
}

func mixedClass(module string, settings map[string]any) *models.Class {
	class := legacyClass(module, settings)
	class.VectorConfig = map[string]models.VectorConfig{
		"extra": {VectorIndexType: hnswT, Vectorizer: map[string]any{"none": map[string]any{}}},
	}
	return class
}

type classShape struct {
	name          string
	build         func(module string, settings map[string]any) *models.Class
	targetVector  string
	immutableText string
}

var classShapes = []classShape{
	{name: "legacy", build: legacyClass, immutableText: "can only update generative and reranker module configs"},
	{name: "named vector", build: namedVectorClass, targetVector: "vec", immutableText: `vectorizer config of vector "vec" is immutable`},
	{name: "mixed", build: mixedClass, immutableText: "can only update generative and reranker module configs"},
}

func (shape classShape) create(t *testing.T, s *schemaWithModules, module string, settings map[string]any) *models.Class {
	t.Helper()
	if shape.name != "mixed" {
		return s.create(t, shape.build(module, settings))
	}
	stored := s.create(t, legacyClass(module, settings))
	require.NoError(t, s.update(mixedClass(module, settings)))
	require.Len(t, stored.VectorConfig, 1)
	return stored
}

func TestUpdateClass_MutableSettings(t *testing.T) {
	for _, shape := range classShapes {
		t.Run(shape.name+"/accepts a change the module allows", func(t *testing.T) {
			s := newSchemaWithModules(t)
			stored := shape.create(t, s, mutableModule, moduleSettings("a", "m"))
			s.modules.validated = nil

			require.NoError(t, s.update(shape.build(mutableModule, moduleSettings("b", "m"))))
			require.Equal(t, moduleSettings("b", "m"), storedModuleSettings(t, stored, mutableModule, shape.targetVector))
			require.Equal(t, []string{mutableModule + "/" + shape.targetVector}, s.modules.validated)
		})

		t.Run(shape.name+"/rejects a change the module does not allow", func(t *testing.T) {
			s := newSchemaWithModules(t)
			shape.create(t, s, mutableModule, moduleSettings("a", "m"))

			require.ErrorContains(t, s.update(shape.build(mutableModule, moduleSettings("b", "other"))), shape.immutableText)
		})

		t.Run(shape.name+"/rejects a change of a module without the capability", func(t *testing.T) {
			s := newSchemaWithModules(t)
			shape.create(t, s, fixedModule, moduleSettings("a", "m"))

			require.ErrorContains(t, s.update(shape.build(fixedModule, moduleSettings("b", "m"))), shape.immutableText)
		})

		t.Run(shape.name+"/returns the module's validation error", func(t *testing.T) {
			s := newSchemaWithModules(t)
			shape.create(t, s, mutableModule, moduleSettings("a", "m"))
			s.modules.err = errors.New("invalid endpoint")

			require.ErrorContains(t, s.update(shape.build(mutableModule, moduleSettings("b", "m"))), "invalid endpoint")
		})

		t.Run(shape.name+"/does not validate unchanged settings", func(t *testing.T) {
			s := newSchemaWithModules(t)
			shape.create(t, s, mutableModule, moduleSettings("a", "m"))
			s.modules.validated = nil

			update := shape.build(mutableModule, moduleSettings("a", "m"))
			update.Description = "new description"
			require.NoError(t, s.update(update))
			require.Empty(t, s.modules.validated)
		})
	}

	t.Run("accepts a change the module allows together with a generative change", func(t *testing.T) {
		s := newSchemaWithModules(t)
		initial := namedVectorClass(mutableModule, moduleSettings("a", "m"))
		initial.ModuleConfig = map[string]any{generativeModule: map[string]any{"setting": "a"}}
		stored := s.create(t, initial)

		update := namedVectorClass(mutableModule, moduleSettings("b", "m"))
		update.ModuleConfig = map[string]any{generativeModule: map[string]any{"setting": "b"}}
		require.NoError(t, s.update(update))
		require.Equal(t, "b", storedModuleSettings(t, stored, mutableModule, "vec")["endpoint"])
		require.Equal(t, "b", storedModuleSettings(t, stored, generativeModule, "")["setting"])
	})

	t.Run("rejects a change the module allows when another named vector changes too", func(t *testing.T) {
		s := newSchemaWithModules(t)
		s.create(t, namedVectorsClass(map[string]map[string]any{
			"a": {mutableModule: moduleSettings("a", "m")},
			"b": {mutableModule: moduleSettings("a", "m")},
		}))

		err := s.update(namedVectorsClass(map[string]map[string]any{
			"a": {mutableModule: moduleSettings("b", "m")},
			"b": {mutableModule: moduleSettings("a", "other")},
		}))
		require.ErrorContains(t, err, `vectorizer config of vector "b" is immutable`)
	})
}

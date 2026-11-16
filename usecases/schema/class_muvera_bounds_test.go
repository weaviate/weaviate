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
	"math"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/vectorindex"
	enthfresh "github.com/weaviate/weaviate/entities/vectorindex/hfresh"
	"github.com/weaviate/weaviate/usecases/auth/authorization/mocks"
	clustermocks "github.com/weaviate/weaviate/usecases/cluster/mocks"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/config/runtime"
	"github.com/weaviate/weaviate/usecases/fakes"
	"github.com/weaviate/weaviate/usecases/schema/namespacing"
	"github.com/weaviate/weaviate/usecases/sharding"
	shardingConfig "github.com/weaviate/weaviate/usecases/sharding/config"
)

// newTestHandlerWithRealVectorConfigParser mirrors newTestHandler but wires
// the production vector index config parser instead of the fake, so the
// hfresh muvera bounds actually run against parsed configs.
func newTestHandlerWithRealVectorConfigParser(t *testing.T) *Handler {
	t.Helper()
	schemaManager := &fakeSchemaManager{}
	logger, _ := test.NewNullLogger()
	vectorizerValidator := &fakeVectorizerValidator{
		valid: []string{"text2vec-contextionary", "model1", "model2", "text2multivec-colbert"},
	}
	cfg := config.Config{
		DefaultVectorDistanceMetric: "cosine",
	}
	fakeClusterState := fakes.NewFakeClusterState()
	fakeValidator := &fakeValidator{}
	schemaParser := NewParser(fakeClusterState, vectorindex.ParseAndValidateConfig, fakeValidator, fakeModulesProvider{}, nil, nil)
	handler, err := NewHandler(
		schemaManager, schemaManager, &fakeDB{}, fakeValidator, logger, mocks.NewMockAuthorizer(),
		&cfg.SchemaHandlerConfig, cfg, vectorindex.ParseAndValidateConfig, vectorizerValidator, dummyValidateInvertedConfig,
		&fakeModuleConfig{}, fakeClusterState, nil, *schemaParser, nil, nil, nil, namespacing.Disabled)
	require.NoError(t, err)
	handler.schemaConfig.MaximumAllowedCollectionsCount = runtime.NewDynamicValue(-1)
	return &handler
}

func classWithMuveraParams(ksim, dprojections, repetitions int) *models.Class {
	// values as float64, mirroring how JSON payloads arrive at the parser
	muvera := map[string]interface{}{"enabled": true}
	if ksim > 0 {
		muvera["ksim"] = float64(ksim)
	}
	if dprojections > 0 {
		muvera["dprojections"] = float64(dprojections)
	}
	if repetitions > 0 {
		muvera["repetitions"] = float64(repetitions)
	}
	return &models.Class{
		Class: "MuveraBounds",
		VectorConfig: map[string]models.VectorConfig{
			"mv": {
				VectorIndexType: "hfresh",
				Vectorizer:      map[string]interface{}{"none": nil},
				VectorIndexConfig: map[string]interface{}{
					"multivector": map[string]interface{}{
						"enabled": true,
						"muvera":  muvera,
					},
				},
			},
		},
	}
}

// TestMuveraUpperBoundsOnSchemaWrites pins issue #281: absurd muvera
// parameters must be rejected on schema create and update. The four
// rejection cases mirror e2e TC-030.
func TestMuveraUpperBoundsOnSchemaWrites(t *testing.T) {
	tests := []struct {
		name         string
		ksim         int
		dprojections int
		repetitions  int
		wantErr      string
	}{
		{name: "ksim one million", ksim: 1_000_000, wantErr: "ksim"},
		{name: "ksim INT_MAX", ksim: math.MaxInt32, wantErr: "ksim"},
		{name: "dprojections one hundred thousand", dprojections: 100_000, wantErr: "dprojections"},
		{name: "repetitions one million", repetitions: 1_000_000, wantErr: "repetitions"},
		{name: "ksim at the limit", ksim: 10},
		{name: "dprojections at the limit", dprojections: 1024},
		{name: "repetitions at the limit", repetitions: 256},
		{
			name: "individually valid but combined FDE too large",
			ksim: 10, dprojections: 1024, repetitions: 256,
			wantErr: "encoded vector length",
		},
		{name: "defaults", ksim: 0, dprojections: 0, repetitions: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler := newTestHandlerWithRealVectorConfigParser(t)
			class := classWithMuveraParams(tt.ksim, tt.dprojections, tt.repetitions)

			t.Run("create", func(t *testing.T) {
				err := handler.validateVectorSettingsAgainst(class, nil)
				if tt.wantErr != "" {
					require.ErrorContains(t, err, tt.wantErr)
					return
				}
				require.NoError(t, err)
			})

			t.Run("update", func(t *testing.T) {
				initial := classWithMuveraParams(0, 0, 0)
				err := handler.validateVectorSettingsAgainst(class, initial)
				if tt.wantErr != "" {
					require.ErrorContains(t, err, tt.wantErr)
					return
				}
				require.NoError(t, err)
			})
		})
	}
}

func hfreshClassWithMultivector(muvera *bool) *models.Class {
	multivector := map[string]interface{}{"enabled": true}
	if muvera != nil {
		multivector["muvera"] = map[string]interface{}{"enabled": *muvera}
	}
	return &models.Class{
		Class: "HFreshMultiVector",
		VectorConfig: map[string]models.VectorConfig{
			"mv": {
				VectorIndexType:   "hfresh",
				Vectorizer:        map[string]interface{}{"none": nil},
				VectorIndexConfig: map[string]interface{}{"multivector": multivector},
			},
		},
	}
}

// hfreshLegacyClassWithMultivector is hfreshClassWithMultivector as a
// class-level (legacy) vector index.
func hfreshLegacyClassWithMultivector(muvera *bool) *models.Class {
	named := hfreshClassWithMultivector(muvera).VectorConfig["mv"]
	return &models.Class{
		Class:             "HFreshLegacyMultiVector",
		Vectorizer:        "none",
		VectorIndexType:   named.VectorIndexType,
		VectorIndexConfig: named.VectorIndexConfig,
	}
}

// TestHFreshMultiVectorRequiresMuveraOnSchemaWrites: multivector without
// muvera is rejected on hfresh create and update, but a restore keeps
// accepting it: such a class could be created before the check existed.
func TestHFreshMultiVectorRequiresMuveraOnSchemaWrites(t *testing.T) {
	boolPtr := func(b bool) *bool { return &b }
	tests := []struct {
		name    string
		muvera  *bool
		wantErr string
	}{
		{name: "multivector without muvera block", muvera: nil, wantErr: "muvera"},
		{name: "multivector with muvera disabled", muvera: boolPtr(false), wantErr: "muvera"},
		{name: "multivector with muvera enabled", muvera: boolPtr(true)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler := newTestHandlerWithRealVectorConfigParser(t)
			class := hfreshClassWithMultivector(tt.muvera)

			t.Run("create", func(t *testing.T) {
				err := handler.validateVectorSettingsAgainst(class, nil)
				if tt.wantErr != "" {
					require.ErrorContains(t, err, tt.wantErr)
					return
				}
				require.NoError(t, err)
			})

			t.Run("update", func(t *testing.T) {
				initial := hfreshClassWithMultivector(boolPtr(true))
				err := handler.validateVectorSettingsAgainst(class, initial)
				if tt.wantErr != "" {
					require.ErrorContains(t, err, tt.wantErr)
					return
				}
				require.NoError(t, err)
			})

			t.Run("restore accepts persisted state", func(t *testing.T) {
				require.NoError(t, handler.validateVectorSettingsFor(class, nil, true))
			})

			t.Run("legacy config", func(t *testing.T) {
				if tt.wantErr == "" {
					t.Skip("a legacy multi-vector index is refused for being multi-vector, before the muvera check")
				}
				legacy := hfreshLegacyClassWithMultivector(tt.muvera)
				require.ErrorContains(t, handler.validateVectorSettingsAgainst(legacy, nil), tt.wantErr)
				require.NoError(t, handler.validateVectorSettingsFor(legacy, nil, true))
			})
		})
	}
}

// TestRestoreClass_HFreshMultiVectorWithoutMuvera drives the full
// RestoreClass path: a backed-up hfresh class with multivector enabled and
// muvera disabled is restored as it was, for named and legacy configs.
func TestRestoreClass_HFreshMultiVectorWithoutMuvera(t *testing.T) {
	disabled := false
	tests := []struct {
		name  string
		class *models.Class
	}{
		{name: "named vector", class: hfreshClassWithMultivector(&disabled)},
		{name: "legacy config", class: hfreshLegacyClassWithMultivector(&disabled)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler := newTestHandlerWithRealVectorConfigParser(t)
			require.ErrorContains(t, handler.validateVectorSettingsAgainst(tt.class, nil), "muvera",
				"the same class must still be rejected on create")

			schemaBytes, err := json.Marshal(tt.class)
			require.NoError(t, err)
			shardingCfg, err := shardingConfig.ParseConfig(nil, 1)
			require.NoError(t, err)
			nodes := clustermocks.NewMockNodeSelector("node1")
			shardingState, err := sharding.InitState(tt.class.Class, shardingCfg, nodes.LocalName(), nodes.StorageCandidates(), 1, false)
			require.NoError(t, err)
			shardingBytes, err := shardingState.JSON()
			require.NoError(t, err)

			manager := handler.schemaManager.(*fakeSchemaManager)
			manager.On("RestoreClass", mock.Anything, mock.Anything).Return(nil)
			descriptor := backup.ClassDescriptor{Name: tt.class.Class, Schema: schemaBytes, ShardingState: shardingBytes}
			require.NoError(t, handler.RestoreClass(context.Background(), &descriptor, map[string]string{}, false, false))
			manager.AssertExpectations(t)

			restored := manager.Calls[len(manager.Calls)-1].Arguments.Get(0).(*models.Class)
			var cfg enthfresh.UserConfig
			if tt.class.VectorIndexType != "" {
				cfg = restored.VectorIndexConfig.(enthfresh.UserConfig)
			} else {
				cfg = restored.VectorConfig["mv"].VectorIndexConfig.(enthfresh.UserConfig)
			}
			require.True(t, cfg.Multivector.Enabled, "multivector flag must survive the restore")
			require.False(t, cfg.Multivector.MuveraConfig.Enabled, "the restore must not switch the index mode on")
		})
	}
}

func TestHFreshMultiVectorModuleOnSchemaWrites(t *testing.T) {
	handler := newTestHandlerWithRealVectorConfigParser(t)
	class := &models.Class{
		Class: "HFreshColbert",
		VectorConfig: map[string]models.VectorConfig{
			"colbert": {
				VectorIndexType: "hfresh",
				Vectorizer:      map[string]interface{}{"text2multivec-colbert": map[string]interface{}{}},
			},
		},
	}
	require.NoError(t, handler.validateVectorSettingsAgainst(class, nil))

	require.NoError(t, handler.parser.ParseClass(class))
	parsed, ok := class.VectorConfig["colbert"].VectorIndexConfig.(enthfresh.UserConfig)
	require.True(t, ok, "expected hfresh config, got %T", class.VectorConfig["colbert"].VectorIndexConfig)
	require.True(t, parsed.IsMultiVector())
	require.True(t, parsed.Multivector.MuveraConfig.Enabled)
}

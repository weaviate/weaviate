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
	"math"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/vectorindex"
	"github.com/weaviate/weaviate/usecases/auth/authorization/mocks"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/config/runtime"
	"github.com/weaviate/weaviate/usecases/fakes"
)

// newTestHandlerWithRealVectorConfigParser mirrors newTestHandler but wires
// the production vector index config parser instead of the fake, so the
// hfresh muvera bounds actually run against parsed configs.
func newTestHandlerWithRealVectorConfigParser(t *testing.T) *Handler {
	t.Helper()
	schemaManager := &fakeSchemaManager{}
	logger, _ := test.NewNullLogger()
	vectorizerValidator := &fakeVectorizerValidator{
		valid: []string{"text2vec-contextionary", "model1", "model2"},
	}
	cfg := config.Config{
		DefaultVectorizerModule:     config.VectorizerModuleNone,
		DefaultVectorDistanceMetric: "cosine",
		HFreshEnabled:               true,
	}
	fakeClusterState := fakes.NewFakeClusterState()
	fakeValidator := &fakeValidator{}
	schemaParser := NewParser(fakeClusterState, vectorindex.ParseAndValidateConfig, fakeValidator, fakeModulesProvider{}, nil, nil)
	handler, err := NewHandler(
		schemaManager, schemaManager, fakeValidator, logger, mocks.NewMockAuthorizer(),
		&cfg.SchemaHandlerConfig, cfg, vectorindex.ParseAndValidateConfig, vectorizerValidator, dummyValidateInvertedConfig,
		&fakeModuleConfig{}, fakeClusterState, nil, *schemaParser, nil, nil)
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

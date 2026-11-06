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
	"fmt"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
	"github.com/weaviate/weaviate/entities/vectorindex"
	"github.com/weaviate/weaviate/entities/vectorindex/dynamic"
	"github.com/weaviate/weaviate/entities/vectorindex/flat"
	"github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/config/runtime"
)

func rqMap(bits int, centering bool) map[string]interface{} {
	// values as float64, mirroring how JSON payloads arrive at the parser
	m := map[string]interface{}{"enabled": true, "bits": float64(bits)}
	if centering {
		m["centering"] = true
		m["trainingLimit"] = float64(100)
	}
	return m
}

// rqIndexConfig builds the raw vector index config for indexType with rq on
// arm ("" for hnsw/flat, "hnsw"/"flat" for a dynamic index); rq nil leaves
// RQ off.
func rqIndexConfig(indexType, arm, distance string, rq map[string]interface{}) map[string]interface{} {
	cfg := map[string]interface{}{"distance": distance}
	if rq == nil {
		return cfg
	}
	if indexType == "dynamic" {
		cfg[arm] = map[string]interface{}{"rq": rq}
	} else {
		cfg["rq"] = rq
	}
	return cfg
}

func legacyRQClass(indexType string, cfg map[string]interface{}) *models.Class {
	return &models.Class{
		Class:             "RQDistance",
		Vectorizer:        "none",
		VectorIndexType:   indexType,
		VectorIndexConfig: cfg,
	}
}

func namedRQClass(indexType string, cfg map[string]interface{}) *models.Class {
	return &models.Class{
		Class: "RQDistance",
		VectorConfig: map[string]models.VectorConfig{
			"v": {
				VectorIndexType:   indexType,
				Vectorizer:        map[string]interface{}{"none": nil},
				VectorIndexConfig: cfg,
			},
		},
	}
}

var rqClassShapes = []struct {
	name  string
	class func(string, map[string]interface{}) *models.Class
}{{"legacy", legacyRQClass}, {"named", namedRQClass}}

// rqTargets is every index (or dynamic arm) and bit width that accepts RQ.
var rqTargets = []struct {
	indexType, arm string
	bits           int
	centering      bool
}{
	{"hnsw", "", 1, false},
	{"hnsw", "", 1, true},
	{"hnsw", "", 4, false},
	{"hnsw", "", 4, true},
	{"hnsw", "", 8, false},
	{"flat", "", 1, false},
	{"flat", "", 8, false},
	{"dynamic", "hnsw", 1, true},
	{"dynamic", "hnsw", 4, false},
	{"dynamic", "hnsw", 8, false},
	{"dynamic", "flat", 1, false},
	{"dynamic", "flat", 8, false},
}

func rqTargetName(indexType, arm string, bits int, centering bool) string {
	name := indexType
	if arm != "" {
		name += "-" + arm
	}
	name += fmt.Sprintf("/rq%d", bits)
	if centering {
		name += "-centered"
	}
	return name
}

// TestRQDistanceOnSchemaWrites pins the QA finding on #13656 and its
// extension in review: RQ at any bit width, on hnsw, flat or either arm of a
// dynamic index, must be rejected on a distance the quantizers cannot
// compress (hamming, manhattan), on create and on the update that enables it.
// Accepted, 1-bit RQ retries activation on every queue tick, and 4- and 8-bit
// RQ compress and then fail every distance call.
func TestRQDistanceOnSchemaWrites(t *testing.T) {
	distances := []struct {
		name    string
		wantErr bool
	}{
		{"hamming", true},
		{"manhattan", true},
		{"cosine", false},
		{"dot", false},
		{"l2-squared", false},
	}
	for _, target := range rqTargets {
		for _, d := range distances {
			for _, shape := range rqClassShapes {
				name := rqTargetName(target.indexType, target.arm, target.bits, target.centering) + "/" + d.name + "/" + shape.name
				t.Run(name, func(t *testing.T) {
					handler := newTestHandlerWithRealVectorConfigParser(t)
					handler.asyncIndexingEnabled = true // dynamic requires it
					rq := rqMap(target.bits, target.centering)
					check := func(t *testing.T, err error) {
						if d.wantErr {
							require.ErrorContains(t, err, "RQ does not support distance")
							return
						}
						require.NoError(t, err)
					}
					enabled := shape.class(target.indexType, rqIndexConfig(target.indexType, target.arm, d.name, rq))

					t.Run("create", func(t *testing.T) {
						check(t, handler.validateVectorSettingsAgainst(enabled, nil))
					})
					t.Run("update enabling rq", func(t *testing.T) {
						initial := shape.class(target.indexType, rqIndexConfig(target.indexType, target.arm, d.name, nil))
						check(t, handler.validateVectorSettingsAgainst(enabled, initial))
					})
				})
			}
		}
	}

	t.Run("rq disabled on hamming is accepted", func(t *testing.T) {
		handler := newTestHandlerWithRealVectorConfigParser(t)
		cfg := rqIndexConfig("hnsw", "", "hamming", map[string]interface{}{"enabled": false, "bits": float64(1)})
		require.NoError(t, handler.validateVectorSettingsAgainst(legacyRQClass("hnsw", cfg), nil))
	})
}

// TestRQDistanceGrandfathersExistingClasses: a class persisted before the
// check keeps loading (the config parser, which also runs on startup and
// restore, does not apply it) and stays updatable, since distance and bits
// cannot change once RQ is enabled.
func TestRQDistanceGrandfathersExistingClasses(t *testing.T) {
	for _, target := range rqTargets {
		t.Run(rqTargetName(target.indexType, target.arm, target.bits, target.centering), func(t *testing.T) {
			cfg := rqIndexConfig(target.indexType, target.arm, "hamming", rqMap(target.bits, target.centering))

			_, err := vectorindex.ParseAndValidateConfig(cfg, target.indexType, false)
			require.NoError(t, err, "startup parsing must not reject a persisted class")

			handler := newTestHandlerWithRealVectorConfigParser(t)
			handler.asyncIndexingEnabled = true
			for _, shape := range rqClassShapes {
				require.NoError(t, handler.validateVectorSettingsAgainst(
					shape.class(target.indexType, cfg), shape.class(target.indexType, cfg)), shape.name)
			}
		})
	}
}

// rqEnabledArms reports, per RQ-capable arm of a stored config, whether RQ is
// on and whether it was set by the server default.
func rqEnabledArms(t *testing.T, cfg schemaConfig.VectorIndexConfig) map[string][2]bool {
	t.Helper()
	switch uc := cfg.(type) {
	case hnsw.UserConfig:
		return map[string][2]bool{"": {uc.RQ.Enabled, uc.TrackDefaultQuantization}}
	case flat.UserConfig:
		return map[string][2]bool{"": {uc.RQ.Enabled, uc.TrackDefaultQuantization}}
	case dynamic.UserConfig:
		return map[string][2]bool{
			"hnsw": {uc.HnswUC.RQ.Enabled, uc.HnswUC.TrackDefaultQuantization},
			"flat": {uc.FlatUC.RQ.Enabled, uc.FlatUC.TrackDefaultQuantization},
		}
	}
	t.Fatalf("unexpected config type %T", cfg)
	return nil
}

// TestRQDefaultQuantizationSkipsUnsupportedDistance: DEFAULT_QUANTIZATION is
// applied after schema validation, so it bypassed the check above and still
// reached the activation retry loop on hamming. The default must be skipped
// there instead (the class is created uncompressed, no error: the user never
// asked for RQ), while supported distances still get it. With RQ off, the
// index never reports ShouldUpgrade, so the queue never calls Upgrade.
func TestRQDefaultQuantizationSkipsUnsupportedDistance(t *testing.T) {
	for _, tt := range []struct {
		indexType   string
		compression string
	}{
		{"hnsw", "rq-1"},
		{"hnsw", "rq-4"},
		{"hnsw", "rq-8"},
		{"flat", "rq-1"},
		{"flat", "rq-8"},
		{"dynamic", "rq-1"},
		{"dynamic", "rq-8"},
	} {
		for _, d := range []struct {
			name       string
			compressed bool
		}{{"hamming", false}, {"manhattan", false}, {"cosine", true}, {"l2-squared", true}} {
			for _, shape := range rqClassShapes {
				t.Run(tt.indexType+"/"+tt.compression+"/"+d.name+"/"+shape.name, func(t *testing.T) {
					handler, manager := newTestHandlerAndManagerWithRealVectorConfigParser(t)
					handler.asyncIndexingEnabled = true
					handler.config.DefaultQuantization = runtime.NewDynamicValue(tt.compression)
					var stored *models.Class
					manager.On("AddClass", mock.Anything, mock.Anything).
						Run(func(args mock.Arguments) { stored = args.Get(0).(*models.Class) }).
						Return(nil)

					class := shape.class(tt.indexType, rqIndexConfig(tt.indexType, "", d.name, nil))
					class.ReplicationConfig = &models.ReplicationConfig{Factor: 1}
					_, _, err := handler.AddClass(context.Background(), nil, class)
					require.NoError(t, err)
					require.NotNil(t, stored)

					cfg := stored.VectorIndexConfig
					if shape.name == "named" {
						cfg = stored.VectorConfig["v"].VectorIndexConfig
					}
					for arm, state := range rqEnabledArms(t, cfg.(schemaConfig.VectorIndexConfig)) {
						require.Equal(t, d.compressed, state[0], "arm %q: rq enabled", arm)
						require.Equal(t, d.compressed, state[1], "arm %q: tracked as default", arm)
					}
				})
			}
		}
	}
}

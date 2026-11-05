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
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/vectorindex"
)

func rqVectorIndexConfig(distance string, rq map[string]interface{}) map[string]interface{} {
	return map[string]interface{}{"distance": distance, "rq": rq}
}

func rq1Map(centering bool) map[string]interface{} {
	// values as float64, mirroring how JSON payloads arrive at the parser
	return map[string]interface{}{
		"enabled": true, "bits": float64(1), "centering": centering, "trainingLimit": float64(100),
	}
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

// TestRQ1DistanceOnSchemaWrites pins the QA finding on #13656: 1-bit RQ on a
// distance the 1-bit quantizer cannot compress (hamming, manhattan) was
// accepted and then retried activation on every queue tick. It must be
// rejected on create and on the update that enables it, centered or not.
func TestRQ1DistanceOnSchemaWrites(t *testing.T) {
	dynamicCfg := func(distance string, rq map[string]interface{}) map[string]interface{} {
		return map[string]interface{}{"distance": distance, "hnsw": map[string]interface{}{"rq": rq}}
	}
	tests := []struct {
		name      string
		indexType string
		cfg       map[string]interface{}
		wantErr   bool
	}{
		{name: "hamming centered rq1 (QA repro)", indexType: "hnsw", cfg: rqVectorIndexConfig("hamming", rq1Map(true)), wantErr: true},
		{name: "hamming uncentered rq1", indexType: "hnsw", cfg: rqVectorIndexConfig("hamming", rq1Map(false)), wantErr: true},
		{name: "manhattan centered rq1", indexType: "hnsw", cfg: rqVectorIndexConfig("manhattan", rq1Map(true)), wantErr: true},
		{name: "dynamic hamming rq1", indexType: "dynamic", cfg: dynamicCfg("hamming", rq1Map(true)), wantErr: true},
		{name: "cosine centered rq1", indexType: "hnsw", cfg: rqVectorIndexConfig("cosine", rq1Map(true))},
		{name: "dot rq1", indexType: "hnsw", cfg: rqVectorIndexConfig("dot", rq1Map(false))},
		{name: "l2-squared centered rq1", indexType: "hnsw", cfg: rqVectorIndexConfig("l2-squared", rq1Map(true))},
		{name: "dynamic dot rq1", indexType: "dynamic", cfg: dynamicCfg("dot", rq1Map(true))},
		{name: "hamming rq1 disabled", indexType: "hnsw", cfg: rqVectorIndexConfig("hamming", map[string]interface{}{"enabled": false, "bits": float64(1)})},
	}

	for _, tt := range tests {
		for _, shape := range []struct {
			name  string
			class func(string, map[string]interface{}) *models.Class
		}{{"legacy", legacyRQClass}, {"named", namedRQClass}} {
			t.Run(tt.name+"/"+shape.name, func(t *testing.T) {
				handler := newTestHandlerWithRealVectorConfigParser(t)
				handler.asyncIndexingEnabled = true // dynamic requires it
				check := func(t *testing.T, err error) {
					if tt.wantErr {
						require.ErrorContains(t, err, "RQ bits=1 does not support distance")
						return
					}
					require.NoError(t, err)
				}

				t.Run("create", func(t *testing.T) {
					check(t, handler.validateVectorSettingsAgainst(shape.class(tt.indexType, tt.cfg), nil))
				})

				t.Run("update enabling rq", func(t *testing.T) {
					distance := tt.cfg["distance"].(string)
					initialCfg := map[string]interface{}{"distance": distance}
					initial := shape.class(tt.indexType, initialCfg)
					check(t, handler.validateVectorSettingsAgainst(shape.class(tt.indexType, tt.cfg), initial))
				})
			})
		}
	}
}

// TestRQ1DistanceGrandfathersExistingClasses: a class persisted before the
// check keeps loading (the config parser does not apply it) and stays
// updatable, since bits and distance cannot change once rq is enabled.
func TestRQ1DistanceGrandfathersExistingClasses(t *testing.T) {
	cfg := rqVectorIndexConfig("hamming", rq1Map(false))

	_, err := vectorindex.ParseAndValidateConfig(cfg, "hnsw", false)
	require.NoError(t, err, "startup parsing must not reject a persisted class")

	handler := newTestHandlerWithRealVectorConfigParser(t)
	for _, class := range []func(string, map[string]interface{}) *models.Class{legacyRQClass, namedRQClass} {
		require.NoError(t, handler.validateVectorSettingsAgainst(class("hnsw", cfg), class("hnsw", cfg)))
	}
}

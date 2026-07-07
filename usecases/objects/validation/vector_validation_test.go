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

package validation

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modelsext"
	enthfresh "github.com/weaviate/weaviate/entities/vectorindex/hfresh"
)

func TestVectors(t *testing.T) {
	specs := map[string]struct {
		class  *models.Class
		obj    *models.Object
		objNew *models.Object
		expErr bool
	}{
		"multiple named vectors with 'old' vector": {
			class: &models.Class{
				VectorConfig: map[string]models.VectorConfig{"first": {}, "second": {}, "third": {}}, // content does not matter
			},
			obj:    &models.Object{Vector: []float32{1, 2, 3}},
			expErr: true,
		},
		"multiple named vectors with vectors": {
			class: &models.Class{
				VectorConfig: map[string]models.VectorConfig{"first": {}, "second": {}, "third": {}}, // content does not matter
			},
			obj: &models.Object{
				Vectors: models.Vectors{"first": []float32{1, 2, 3}},
			},
			expErr: false,
		},
		"single named vectors with vector": {
			class: &models.Class{
				VectorConfig: map[string]models.VectorConfig{"first": {}}, // content does not matter
			},
			obj: &models.Object{Vector: []float32{1, 2, 3}},
			objNew: &models.Object{
				Vectors: models.Vectors{"first": []float32{1, 2, 3}},
			},
			expErr: false,
		},
		"old vector with named vectors": {
			class: &models.Class{
				VectorIndexConfig: models.VectorConfig{}, // content does not matter
			},
			obj: &models.Object{
				Vectors: models.Vectors{"first": []float32{1, 2, 3}},
			},
			expErr: true,
		},
		"non existent named vectors": {
			class: &models.Class{
				VectorConfig: map[string]models.VectorConfig{"first": {}, "second": {}},
			},
			obj: &models.Object{
				Vectors: models.Vectors{"third": []float32{1, 2, 3}},
			},
			expErr: true,
		},
		"mixed vectors": {
			class: &models.Class{
				Vectorizer:      "legacy",
				VectorIndexType: "hnsw",
				VectorConfig:    map[string]models.VectorConfig{"first": {}, "second": {}},
			},
			obj: &models.Object{
				Vector:  []float32{1, 2, 3},
				Vectors: models.Vectors{"first": []float32{1, 2, 3}, "second": []float32{4, 5, 6}},
			},
			expErr: false,
		},
		"default vector set to legacy in mixed vector class": {
			class: &models.Class{
				Vectorizer:      "legacy",
				VectorIndexType: "hnsw",
				VectorConfig:    map[string]models.VectorConfig{"first": {}, "second": {}},
			},
			obj: &models.Object{
				Vectors: models.Vectors{"first": []float32{1, 2, 3}, "second": []float32{4, 5, 6}, modelsext.DefaultNamedVectorName: []float32{7, 8, 9}},
			},
			objNew: &models.Object{
				Vectors: models.Vectors{"first": []float32{1, 2, 3}, "second": []float32{4, 5, 6}},
				Vector:  []float32{7, 8, 9},
			},
		},
		"default vector not touched in named vector class": {
			class: &models.Class{
				VectorConfig: map[string]models.VectorConfig{modelsext.DefaultNamedVectorName: {}},
			},
			obj: &models.Object{
				Vectors: models.Vectors{modelsext.DefaultNamedVectorName: []float32{1, 2, 3}},
			},
			objNew: &models.Object{
				Vectors: models.Vectors{modelsext.DefaultNamedVectorName: []float32{1, 2, 3}},
			},
		},
	}
	for name, spec := range specs {
		t.Run(name, func(t *testing.T) {
			validator := &Validator{exists: func(_ context.Context, class string, _ strfmt.UUID, _ *additional.ReplicationProperties, _ string) (bool, error) {
				return true, nil
			}}
			gotErr := validator.vector(context.Background(), spec.class, spec.obj)

			if spec.objNew != nil {
				require.Equal(t, spec.objNew, spec.obj)
			}

			if spec.expErr {
				require.Error(t, gotErr)
				return
			}
			require.NoError(t, gotErr)
		})
	}
}

// TestVectorShapes covers the schema-aware shape validation added for issues
// #278 (single vector into a multi-vector target) and #280 (empty
// multi-vector). See also TC-003/TC-009 in the e2e suite.
func TestVectorShapes(t *testing.T) {
	singleCfg := func() models.VectorConfig {
		return models.VectorConfig{
			VectorIndexType:   "hfresh",
			VectorIndexConfig: enthfresh.NewDefaultUserConfig(),
			Vectorizer:        map[string]interface{}{"none": nil},
		}
	}
	multiCfg := func(vectorizer string) models.VectorConfig {
		uc := enthfresh.NewDefaultUserConfig()
		uc.Multivector.Enabled = true
		uc.Multivector.MuveraConfig.Enabled = true
		return models.VectorConfig{
			VectorIndexType:   "hfresh",
			VectorIndexConfig: uc,
			Vectorizer:        map[string]interface{}{vectorizer: nil},
		}
	}
	class := func(cfg models.VectorConfig) *models.Class {
		return &models.Class{
			Class:        "ShapeTest",
			Vectorizer:   "none",
			VectorConfig: map[string]models.VectorConfig{"vec": cfg},
		}
	}
	vectors := func(v models.Vector) models.Vectors {
		return models.Vectors{"vec": v}
	}

	specs := map[string]struct {
		class      *models.Class
		vec        models.Vector
		expErr     string
		expDropped bool
	}{
		"single vector into multi-vector target rejected": {
			class:  class(multiCfg("none")),
			vec:    []float32{0.1, 0.2, 0.3},
			expErr: "configured as multi-vector",
		},
		"single interface-shaped vector into multi-vector target rejected": {
			class:  class(multiCfg("none")),
			vec:    []interface{}{0.1, 0.2},
			expErr: "configured as multi-vector",
		},
		"valid multi-vector accepted": {
			class: class(multiCfg("none")),
			vec:   [][]float32{{0.1, 0.2}, {0.3, 0.4}},
		},
		"empty multi-vector rejected on self-provided target": {
			class:  class(multiCfg("none")),
			vec:    [][]float32{},
			expErr: "cannot be empty",
		},
		"empty single-shaped vector rejected on self-provided multi-vector target": {
			class:  class(multiCfg("none")),
			vec:    []float32{},
			expErr: "cannot be empty",
		},
		"empty vector dropped on vectorizer-backed multi-vector target": {
			class:      class(multiCfg("text2vec-contextionary")),
			vec:        []float32{},
			expDropped: true,
		},
		"empty token rejected": {
			class:  class(multiCfg("none")),
			vec:    [][]float32{{}},
			expErr: "token 0 is empty",
		},
		"empty interface-shaped token rejected": {
			class:  class(multiCfg("none")),
			vec:    []interface{}{[]interface{}{}},
			expErr: "token 0 is empty",
		},
		"multi-vector into single-vector target rejected": {
			class:  class(singleCfg()),
			vec:    [][]float32{{0.1, 0.2}},
			expErr: "not configured as multi-vector",
		},
		"single vector into single-vector target accepted": {
			class: class(singleCfg()),
			vec:   []float32{0.1, 0.2},
		},
		"empty vector dropped on single-vector target": {
			class:      class(singleCfg()),
			vec:        []float32{},
			expDropped: true,
		},
		"untyped index config skips shape validation": {
			class: class(models.VectorConfig{VectorIndexType: "hfresh"}),
			vec:   []float32{0.1, 0.2},
		},
	}

	for name, spec := range specs {
		t.Run(name, func(t *testing.T) {
			validator := &Validator{exists: func(_ context.Context, class string, _ strfmt.UUID, _ *additional.ReplicationProperties, _ string) (bool, error) {
				return true, nil
			}}
			obj := &models.Object{Class: spec.class.Class, Vectors: vectors(spec.vec)}
			gotErr := validator.vector(context.Background(), spec.class, obj)

			if spec.expErr != "" {
				require.ErrorContains(t, gotErr, spec.expErr)
				return
			}
			require.NoError(t, gotErr)
			_, present := obj.Vectors["vec"]
			require.Equal(t, !spec.expDropped, present,
				"expected dropped=%v, vectors=%v", spec.expDropped, obj.Vectors)
		})
	}
}

// TestVectorsUnmarshalKeepsEmpty pins the models.Vectors JSON contract the
// shape validation relies on: an explicit empty array is kept (so
// multi-vector targets can reject it), while null still means "no vector
// provided". The custom UnmarshalJSON is injected by
// tools/swagger_custom_code; if this test fails after regenerating swagger
// code, that tool was not run or its template drifted.
func TestVectorsUnmarshalKeepsEmpty(t *testing.T) {
	specs := map[string]struct {
		json    string
		expVec  models.Vector
		expKept bool
	}{
		"empty array kept":  {json: `{"vec": []}`, expVec: []float32{}, expKept: true},
		"null dropped":      {json: `{"vec": null}`, expKept: false},
		"single kept":       {json: `{"vec": [0.5]}`, expVec: []float32{0.5}, expKept: true},
		"multi kept":        {json: `{"vec": [[0.5],[0.25]]}`, expVec: [][]float32{{0.5}, {0.25}}, expKept: true},
		"empty token kept":  {json: `{"vec": [[]]}`, expVec: [][]float32{{}}, expKept: true},
		"empty object kept": {json: `{}`, expKept: false},
	}
	for name, spec := range specs {
		t.Run(name, func(t *testing.T) {
			var v models.Vectors
			require.NoError(t, json.Unmarshal([]byte(spec.json), &v))
			got, ok := v["vec"]
			require.Equal(t, spec.expKept, ok)
			if spec.expKept {
				require.Equal(t, spec.expVec, got)
			}
		})
	}
}

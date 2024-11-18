//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package validation

import (
	"context"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
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

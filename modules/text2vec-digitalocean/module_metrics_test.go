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

package moddigitalocean

import (
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/moduletools"
	objectsvectorizer "github.com/weaviate/weaviate/usecases/modulecomponents/vectorizer"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

type fakeVectorizer struct{}

func (fakeVectorizer) Texts(context.Context, []string, moduletools.ClassConfig) ([]float32, error) {
	return nil, nil
}

func (fakeVectorizer) Object(context.Context, *models.Object, moduletools.ClassConfig, objectsvectorizer.ClassSettings) ([]float32, models.AdditionalProperties, error) {
	return nil, nil, nil
}

func (fakeVectorizer) ObjectBatch(context.Context, []*models.Object, []bool, moduletools.ClassConfig) ([][]float32, map[int]error) {
	return nil, nil
}

func TestModuleRequestMetricLabels(t *testing.T) {
	m := &DigitalOceanModule{vectorizer: fakeVectorizer{}}
	metrics := monitoring.GetMetrics()
	ctx := context.Background()

	sizeVec := metrics.ModuleExternalRequestSize
	sizeVec.WithLabelValues("vectorizeTexts", Name)
	sizeSeries := testutil.CollectAndCount(sizeVec)

	tests := []struct {
		op   string
		vec  *prometheus.CounterVec
		call func(t *testing.T)
	}{
		{"vectorizeObject", metrics.ModuleExternalRequestSingleCount, func(t *testing.T) {
			_, _, err := m.VectorizeObject(ctx, &models.Object{}, nil)
			require.NoError(t, err)
		}},
		{"vectorizeBatch", metrics.ModuleExternalRequestBatchCount, func(t *testing.T) {
			_, _, errs := m.VectorizeBatch(ctx, []*models.Object{{}}, []bool{false}, nil)
			require.Empty(t, errs)
		}},
		{"vectorizeTexts", metrics.ModuleExternalRequestSingleCount, func(t *testing.T) {
			_, err := m.VectorizeInput(ctx, "hello", nil)
			require.NoError(t, err)
		}},
	}
	for _, tt := range tests {
		t.Run(tt.op, func(t *testing.T) {
			series := tt.vec.WithLabelValues(tt.op, Name)
			before := testutil.ToFloat64(series)
			tt.call(t)
			assert.Equal(t, before+1, testutil.ToFloat64(series))
		})
	}

	assert.Equal(t, sizeSeries, testutil.CollectAndCount(sizeVec))
}

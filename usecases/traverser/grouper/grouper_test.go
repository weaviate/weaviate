package grouper

import (
	"testing"

	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/search"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGrouper_ModeClosest(t *testing.T) {
	in := []search.Result{
		search.Result{
			Vector: []float32{0.1, 0.1, 0.98},
			Schema: map[string]interface{}{
				"name": "A1",
			},
		},
		search.Result{
			Vector: []float32{0.1, 0.1, 0.96},
			Schema: map[string]interface{}{
				"name": "A2",
			},
		},
		search.Result{
			Vector: []float32{0.1, 0.1, 0.93},
			Schema: map[string]interface{}{
				"name": "A3",
			},
		},
		search.Result{
			Vector: []float32{0.1, 0.98, 0.1},
			Schema: map[string]interface{}{
				"name": "B1",
			},
		},
		search.Result{
			Vector: []float32{0.1, 0.93, 0.1},
			Schema: map[string]interface{}{
				"name": "B2",
			},
		},
		search.Result{
			Vector: []float32{0.1, 0.92, 0.1},
			Schema: map[string]interface{}{
				"name": "B3",
			},
		},
	}

	expectedOut := []search.Result{
		search.Result{
			Vector: []float32{0.1, 0.1, 0.98},
			Schema: map[string]interface{}{
				"name": "A1",
			},
		},
		search.Result{
			Vector: []float32{0.1, 0.98, 0.1},
			Schema: map[string]interface{}{
				"name": "B1",
			},
		},
	}

	log, _ := test.NewNullLogger()
	res, err := New(log).Group(in, "closest", 0.2)
	require.Nil(t, err)
	assert.Equal(t, expectedOut, res)
}

func TestGrouper_ModeMerge(t *testing.T) {
	in := []search.Result{
		search.Result{
			Vector: []float32{0.1, 0.1, 0.98},
			Schema: map[string]interface{}{
				"name":    "A1",
				"count":   10.0,
				"illegal": true,
				"location": &models.GeoCoordinates{
					Latitude:  20,
					Longitude: 20,
				},
			},
		},
		search.Result{
			Vector: []float32{0.1, 0.1, 0.96},
			Schema: map[string]interface{}{
				"name":    "A2",
				"count":   11.0,
				"illegal": true,
			},
		},
		search.Result{
			Vector: []float32{0.1, 0.1, 0.96},
			Schema: map[string]interface{}{
				"name":    "A2",
				"count":   11.0,
				"illegal": true,
			},
		},
		search.Result{
			Vector: []float32{0.1, 0.1, 0.93},
			Schema: map[string]interface{}{
				"name":    "A3",
				"count":   12.0,
				"illegal": false,
				"location": &models.GeoCoordinates{
					Latitude:  22,
					Longitude: 18,
				},
			},
		},
		search.Result{
			Vector: []float32{0.1, 0.98, 0.1},
			Schema: map[string]interface{}{
				"name": "B1",
			},
		},
		search.Result{
			Vector: []float32{0.1, 0.93, 0.1},
			Schema: map[string]interface{}{
				"name": "B2",
			},
		},
		search.Result{
			Vector: []float32{0.1, 0.92, 0.1},
			Schema: map[string]interface{}{
				"name": "B3",
			},
		},
	}

	expectedOut := []search.Result{
		search.Result{
			// Vector: []float32{0.1, 0.1, 0.98},
			Schema: map[string]interface{}{
				"name":    "A1 (A2, A3)", // note that A2 is only contained once, even though its twice in the input set
				"count":   11.0,          // mean of all inputs
				"illegal": true,          // the most common input value, with a bias towards true on equal count
				"location": &models.GeoCoordinates{
					Latitude:  21,
					Longitude: 19,
				},
			},
		},
		search.Result{
			// Vector: []float32{0.1, 0.98, 0.1},
			Schema: map[string]interface{}{
				"name": "B1 (B2, B3)",
			},
		},
	}

	log, _ := test.NewNullLogger()
	res, err := New(log).Group(in, "merge", 0.2)
	require.Nil(t, err)
	assert.Equal(t, expectedOut, res)
}

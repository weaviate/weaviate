package grouper

import (
	"testing"

	"github.com/semi-technologies/weaviate/entities/search"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGrouper(t *testing.T) {

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

	res, err := NewGrouper().Group(in, "closest", 0.2)
	require.Nil(t, err)
	assert.Equal(t, expectedOut, res)
}

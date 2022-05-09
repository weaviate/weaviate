package distancer

import (
	"testing"

	"github.com/semi-technologies/weaviate/adapters/repos/db/vector/hnsw/distancer/asm"
	"github.com/stretchr/testify/assert"
)

func L2PureGo(a, b []float32) float32 {
	var sum float32

	for i := range a {
		diff := a[i] - b[i]
		sum += diff * diff
	}

	return sum
}

func Test_L2_DistanceImplementation_Simple_32(t *testing.T) {
	length := 32
	x := make([]float32, length)
	y := make([]float32, length)
	for i := range x {
		x[i] = 2.0
		y[i] = 1.0
	}

	control := L2PureGo(x, y)
	asmResult := asm.L2(x, y)

	assert.Equal(t, control, asmResult)
}

func Test_L2_DistanceImplementation_Simple_35(t *testing.T) {
	length := 35
	x := make([]float32, length)
	y := make([]float32, length)
	for i := range x {
		x[i] = 2.0
		y[i] = 1.0
	}

	control := L2PureGo(x, y)
	asmResult := asm.L2(x, y)

	assert.Equal(t, control, asmResult)
}

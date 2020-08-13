package hnsw

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReusableDistancer(t *testing.T) {
	vec1 := []float32{0.1, 0.3, 0.7}
	vec2 := []float32{0.2, 0.2, 0.2}

	dist, err := newReusableDistancer(vec1).distance(vec2)
	require.Nil(t, err)
	control, err := cosineDist(vec1, vec2)
	require.Nil(t, err)
	assert.Equal(t, control, dist)

}

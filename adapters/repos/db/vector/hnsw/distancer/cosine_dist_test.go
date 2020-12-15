//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package distancer

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCosineDistancer(t *testing.T) {
	vec1 := []float32{0.1, 0.3, 0.7}
	vec2 := []float32{0.2, 0.2, 0.2}

	dist, ok, err := NewCosineProvider().New(vec1).Distance(vec2)
	require.Nil(t, err)
	require.True(t, ok)
	control, ok, err := NewCosineProvider().SingleDist(vec1, vec2)
	require.True(t, ok)
	require.Nil(t, err)
	assert.Equal(t, control, dist)
}

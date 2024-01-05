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

package distancer

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGeoSpatialDistance(t *testing.T) {
	t.Run("between Munich and Stuttgart", func(t *testing.T) {
		munich := []float32{48.137154, 11.576124}
		stuttgart := []float32{48.783333, 9.183333}

		dist, ok, err := NewGeoProvider().New(munich).Distance(stuttgart)
		require.Nil(t, err)
		require.True(t, ok)
		assert.InDelta(t, 190000, dist, 1000)
	})
}

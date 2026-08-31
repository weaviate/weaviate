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

package db

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/storobj"
)

func TestFilterNilObjects(t *testing.T) {
	t.Run("filters out nil objects and corresponding distances", func(t *testing.T) {
		objs := []*storobj.Object{
			storobj.New(11),
			nil,
			storobj.New(13),
			nil,
			storobj.New(15),
		}
		dists := []float32{0.1, 0.2, 0.3, 0.4, 0.5}

		filteredObjs, filteredDists := filterNilObjects(objs, dists)

		require.Len(t, filteredObjs, 3)
		require.Len(t, filteredDists, 3)

		assert.Equal(t, uint64(11), filteredObjs[0].DocID)
		assert.Equal(t, float32(0.1), filteredDists[0])

		assert.Equal(t, uint64(13), filteredObjs[1].DocID)
		assert.Equal(t, float32(0.3), filteredDists[1])

		assert.Equal(t, uint64(15), filteredObjs[2].DocID)
		assert.Equal(t, float32(0.5), filteredDists[2])
	})

	t.Run("handles all nil objects", func(t *testing.T) {
		objs := []*storobj.Object{nil, nil, nil}
		dists := []float32{0.1, 0.2, 0.3}

		filteredObjs, filteredDists := filterNilObjects(objs, dists)

		assert.Empty(t, filteredObjs)
		assert.Empty(t, filteredDists)
	})

	t.Run("handles no nil objects", func(t *testing.T) {
		objs := []*storobj.Object{
			storobj.New(1),
			storobj.New(2),
			storobj.New(3),
		}
		dists := []float32{0.1, 0.2, 0.3}

		filteredObjs, filteredDists := filterNilObjects(objs, dists)

		require.Len(t, filteredObjs, 3)
		require.Len(t, filteredDists, 3)
	})

	t.Run("handles empty slices", func(t *testing.T) {
		filteredObjs, filteredDists := filterNilObjects(nil, nil)
		assert.Empty(t, filteredObjs)
		assert.Empty(t, filteredDists)
	})

	t.Run("handles length mismatch (dists shorter)", func(t *testing.T) {
		objs := []*storobj.Object{
			storobj.New(1),
			storobj.New(2),
			storobj.New(3),
		}
		dists := []float32{0.1}

		filteredObjs, filteredDists := filterNilObjects(objs, dists)

		require.Len(t, filteredObjs, 1)
		require.Len(t, filteredDists, 1)
		assert.Equal(t, uint64(1), filteredObjs[0].DocID)
		assert.Equal(t, float32(0.1), filteredDists[0])
	})
}

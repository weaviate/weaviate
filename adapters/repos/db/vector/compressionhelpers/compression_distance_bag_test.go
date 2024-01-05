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

//go:build !race

package compressionhelpers_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	testinghelpers "github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
)

func Test_NoRaceQuantizedDistanceBag(t *testing.T) {
	compressor, err := compressionhelpers.NewBQCompressor(distancer.NewCosineDistanceProvider(), 1e12, nil, testinghelpers.NewDummyStore(t))
	assert.Nil(t, err)
	compressor.Preload(1, []float32{-0.5, 0.5})
	compressor.Preload(2, []float32{0.25, 0.7})
	compressor.Preload(3, []float32{0.5, 0.5})

	t.Run("returns error when id has not been loaded", func(t *testing.T) {
		bag := compressor.NewBag()
		_, err = bag.Distance(1, 2)
		assert.NotNil(t, err)
	})

	t.Run("returns error when id has not been loaded", func(t *testing.T) {
		bag := compressor.NewBag()
		bag.Load(context.Background(), 1)
		bag.Load(context.Background(), 2)
		bag.Load(context.Background(), 3)

		d, err := bag.Distance(1, 2)
		assert.Nil(t, err)
		assert.Equal(t, float32(1), d)

		d, err = bag.Distance(2, 3)
		assert.Nil(t, err)
		assert.Equal(t, float32(0), d)
	})
}

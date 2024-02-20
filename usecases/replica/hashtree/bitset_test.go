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

package hashtree

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBitSet(t *testing.T) {
	bsetSize := 2 << 15

	bset := NewBitset(bsetSize)

	require.Zero(t, bset.SetCount())

	for i := 0; i < bsetSize; i++ {
		require.False(t, bset.IsSet(i))
	}

	require.False(t, bset.AllSet())

	for i := 0; i < bsetSize; i++ {
		bset.Set(i)
		require.True(t, bset.IsSet(i))
	}

	require.True(t, bset.AllSet())
	require.Equal(t, bsetSize, bset.SetCount())

	bset.Reset()
	require.Zero(t, bset.SetCount())

	require.Panics(t, func() {
		bset.IsSet(bsetSize)
	})

	require.Panics(t, func() {
		bset.Set(bsetSize)
	})

	require.Panics(t, func() {
		bset.Unset(bsetSize)
	})
}

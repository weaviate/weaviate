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

package aggregator

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBoolAggregator_AddBoolRow pins the on-disk bool decoding (any non-zero
// byte is true), the count==0 skip, and the empty-value guard.
func TestBoolAggregator_AddBoolRow(t *testing.T) {
	t.Run("accumulates true and false weighted by count", func(t *testing.T) {
		a := newBoolAggregator()
		require.NoError(t, a.AddBoolRow([]byte{1}, 3))
		require.NoError(t, a.AddBoolRow([]byte{0}, 2))
		require.NoError(t, a.AddBoolRow([]byte{1}, 1))

		res := a.Res()
		assert.Equal(t, 6, res.Count)
		assert.Equal(t, 4, res.TotalTrue)
		assert.Equal(t, 2, res.TotalFalse)
	})

	t.Run("any non-zero byte is true", func(t *testing.T) {
		a := newBoolAggregator()
		require.NoError(t, a.AddBoolRow([]byte{0xff}, 1))
		assert.Equal(t, 1, a.Res().TotalTrue)
		assert.Equal(t, 0, a.Res().TotalFalse)
	})

	t.Run("zero-count row does not change the result", func(t *testing.T) {
		a := newBoolAggregator()
		require.NoError(t, a.AddBoolRow([]byte{1}, 0))
		assert.Equal(t, 0, a.Res().Count)
	})

	t.Run("empty value errors instead of panicking", func(t *testing.T) {
		a := newBoolAggregator()
		for _, empty := range [][]byte{nil, {}} {
			require.ErrorContains(t, a.AddBoolRow(empty, 1), "empty value")
		}
	})
}

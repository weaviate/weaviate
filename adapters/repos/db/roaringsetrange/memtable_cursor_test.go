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

package roaringsetrange

import (
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMemtableCursor(t *testing.T) {
	logger, _ := test.NewNullLogger()

	mem := NewMemtable(logger)
	mem.Insert(13, []uint64{113, 213}) // ...1101
	mem.Insert(5, []uint64{15, 25})    // ...0101
	mem.Insert(0, []uint64{10, 20})    // ...0000

	t.Run("starting from beginning", func(t *testing.T) {
		c := NewMemtableCursor(mem)
		key, layer, ok := c.First()
		require.True(t, ok)
		assert.Equal(t, uint8(0), key)
		assert.ElementsMatch(t, []uint64{10, 20, 15, 25, 113, 213}, layer.Additions.ToArray())
		assert.ElementsMatch(t, []uint64{10, 20, 15, 25, 113, 213}, layer.Deletions.ToArray())
	})

	t.Run("starting from beginning, page through all", func(t *testing.T) {
		c := NewMemtableCursor(mem)

		key1, layer1, ok1 := c.First()
		key2, layer2, ok2 := c.Next()
		key3, layer3, ok3 := c.Next()
		key4, layer4, ok4 := c.Next()
		key5, layer5, ok5 := c.Next()
		key6, layer6, ok6 := c.Next()

		require.True(t, ok1)
		assert.Equal(t, uint8(0), key1)
		assert.ElementsMatch(t, []uint64{10, 20, 15, 25, 113, 213}, layer1.Additions.ToArray())
		assert.ElementsMatch(t, []uint64{10, 20, 15, 25, 113, 213}, layer1.Deletions.ToArray())

		require.True(t, ok2)
		assert.Equal(t, uint8(1), key2)
		assert.ElementsMatch(t, []uint64{15, 25, 113, 213}, layer2.Additions.ToArray())
		assert.True(t, layer2.Deletions.IsEmpty())

		require.True(t, ok3)
		assert.Equal(t, uint8(3), key3)
		assert.ElementsMatch(t, []uint64{15, 25, 113, 213}, layer3.Additions.ToArray())
		assert.True(t, layer3.Deletions.IsEmpty())

		require.True(t, ok4)
		assert.Equal(t, uint8(4), key4)
		assert.ElementsMatch(t, []uint64{113, 213}, layer4.Additions.ToArray())
		assert.True(t, layer4.Deletions.IsEmpty())

		assert.False(t, ok5)
		assert.Equal(t, uint8(0), key5)
		assert.Nil(t, layer5.Additions)
		assert.Nil(t, layer5.Deletions)

		assert.False(t, ok6)
		assert.Equal(t, uint8(0), key6)
		assert.Nil(t, layer6.Additions)
		assert.Nil(t, layer6.Deletions)
	})

	t.Run("no first, page through all", func(t *testing.T) {
		c := NewMemtableCursor(mem)

		key1, layer1, ok1 := c.Next()
		key2, layer2, ok2 := c.Next()
		key3, layer3, ok3 := c.Next()
		key4, layer4, ok4 := c.Next()
		key5, layer5, ok5 := c.Next()
		key6, layer6, ok6 := c.Next()

		require.True(t, ok1)
		assert.Equal(t, uint8(0), key1)
		assert.ElementsMatch(t, []uint64{10, 20, 15, 25, 113, 213}, layer1.Additions.ToArray())
		assert.ElementsMatch(t, []uint64{10, 20, 15, 25, 113, 213}, layer1.Deletions.ToArray())

		require.True(t, ok2)
		assert.Equal(t, uint8(1), key2)
		assert.ElementsMatch(t, []uint64{15, 25, 113, 213}, layer2.Additions.ToArray())
		assert.True(t, layer2.Deletions.IsEmpty())

		require.True(t, ok3)
		assert.Equal(t, uint8(3), key3)
		assert.ElementsMatch(t, []uint64{15, 25, 113, 213}, layer3.Additions.ToArray())
		assert.True(t, layer3.Deletions.IsEmpty())

		require.True(t, ok4)
		assert.Equal(t, uint8(4), key4)
		assert.ElementsMatch(t, []uint64{113, 213}, layer4.Additions.ToArray())
		assert.True(t, layer4.Deletions.IsEmpty())

		assert.False(t, ok5)
		assert.Equal(t, uint8(0), key5)
		assert.Nil(t, layer5.Additions)
		assert.Nil(t, layer5.Deletions)

		assert.False(t, ok6)
		assert.Equal(t, uint8(0), key6)
		assert.Nil(t, layer6.Additions)
		assert.Nil(t, layer6.Deletions)
	})

	t.Run("starting from beginning, page through all, start again", func(t *testing.T) {
		c := NewMemtableCursor(mem)

		key1, layer1, ok1 := c.First()
		key2, layer2, ok2 := c.Next()
		key3, layer3, ok3 := c.Next()
		key4, layer4, ok4 := c.Next()
		key5, layer5, ok5 := c.Next()
		key6, layer6, ok6 := c.First()
		key7, layer7, ok7 := c.Next()

		require.True(t, ok1)
		assert.Equal(t, uint8(0), key1)
		assert.ElementsMatch(t, []uint64{10, 20, 15, 25, 113, 213}, layer1.Additions.ToArray())
		assert.ElementsMatch(t, []uint64{10, 20, 15, 25, 113, 213}, layer1.Deletions.ToArray())

		require.True(t, ok2)
		assert.Equal(t, uint8(1), key2)
		assert.ElementsMatch(t, []uint64{15, 25, 113, 213}, layer2.Additions.ToArray())
		assert.True(t, layer2.Deletions.IsEmpty())

		require.True(t, ok3)
		assert.Equal(t, uint8(3), key3)
		assert.ElementsMatch(t, []uint64{15, 25, 113, 213}, layer3.Additions.ToArray())
		assert.True(t, layer3.Deletions.IsEmpty())

		require.True(t, ok4)
		assert.Equal(t, uint8(4), key4)
		assert.ElementsMatch(t, []uint64{113, 213}, layer4.Additions.ToArray())
		assert.True(t, layer4.Deletions.IsEmpty())

		assert.False(t, ok5)
		assert.Equal(t, uint8(0), key5)
		assert.Nil(t, layer5.Additions)
		assert.Nil(t, layer5.Deletions)

		require.True(t, ok6)
		assert.Equal(t, uint8(0), key6)
		assert.ElementsMatch(t, []uint64{10, 20, 15, 25, 113, 213}, layer6.Additions.ToArray())
		assert.ElementsMatch(t, []uint64{10, 20, 15, 25, 113, 213}, layer6.Deletions.ToArray())

		require.True(t, ok7)
		assert.Equal(t, uint8(1), key7)
		assert.ElementsMatch(t, []uint64{15, 25, 113, 213}, layer7.Additions.ToArray())
		assert.True(t, layer7.Deletions.IsEmpty())
	})
}

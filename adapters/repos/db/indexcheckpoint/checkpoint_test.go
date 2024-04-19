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

package indexcheckpoint

import (
	"io"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

func TestCheckpoint(t *testing.T) {
	l := logrus.New()
	l.SetOutput(io.Discard)

	c, err := New(t.TempDir(), l)
	require.NoError(t, err)
	defer c.Close()

	t.Run("get non-existing", func(t *testing.T) {
		v, ok, err := c.Get("shard1", "a")
		require.NoError(t, err)
		require.False(t, ok)
		require.Zero(t, v)
	})

	t.Run("set and get", func(t *testing.T) {
		err := c.Update("shard1", "a", 123)
		require.NoError(t, err)

		v, ok, err := c.Get("shard1", "a")
		require.NoError(t, err)
		require.True(t, ok)
		require.EqualValues(t, 123, v)
	})

	t.Run("set and get: no target", func(t *testing.T) {
		err := c.Update("shard1", "", 123)
		require.NoError(t, err)

		v, ok, err := c.Get("shard1", "")
		require.NoError(t, err)
		require.True(t, ok)
		require.EqualValues(t, 123, v)
	})

	t.Run("overwrite", func(t *testing.T) {
		err := c.Update("shard1", "a", 456)
		require.NoError(t, err)

		v, ok, err := c.Get("shard1", "a")
		require.NoError(t, err)
		require.True(t, ok)
		require.EqualValues(t, 456, v)
	})

	t.Run("delete", func(t *testing.T) {
		err := c.Delete("shard1", "a")
		require.NoError(t, err)

		v, ok, err := c.Get("shard1", "a")
		require.NoError(t, err)
		require.False(t, ok)
		require.Zero(t, v)
	})

	t.Run("drop", func(t *testing.T) {
		c, err := New(t.TempDir(), l)
		require.NoError(t, err)
		defer c.Close()

		err = c.Drop()
		require.NoError(t, err)

		_, _, err = c.Get("shard1", "a")
		require.Error(t, err)

		err = c.Update("shard1", "a", 123)
		require.Error(t, err)

		err = c.Delete("shard1", "a")
		require.Error(t, err)

		err = c.Drop()
		require.Error(t, err)
	})
}

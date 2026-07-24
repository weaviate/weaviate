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

package indexcounter

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReadOnDisk(t *testing.T) {
	t.Run("missing counter file reads as 0", func(t *testing.T) {
		count, err := Read(t.TempDir())
		require.NoError(t, err)
		require.Equal(t, uint64(0), count)
	})

	t.Run("does not create the counter file", func(t *testing.T) {
		dir := t.TempDir()
		_, err := Read(dir)
		require.NoError(t, err)
		_, statErr := os.Stat(filepath.Join(dir, "indexcount"))
		require.True(t, os.IsNotExist(statErr), "ReadOnDisk must not create the counter file")
	})

	t.Run("empty counter file reads as 0", func(t *testing.T) {
		dir := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(dir, "indexcount"), nil, 0o644))
		count, err := Read(dir)
		require.NoError(t, err)
		require.Equal(t, uint64(0), count)
	})

	t.Run("returns the persisted value", func(t *testing.T) {
		dir := t.TempDir()
		var buf [8]byte
		binary.LittleEndian.PutUint64(buf[:], 42)
		require.NoError(t, os.WriteFile(filepath.Join(dir, "indexcount"), buf[:], 0o644))
		count, err := Read(dir)
		require.NoError(t, err)
		require.Equal(t, uint64(42), count)
	})

	t.Run("agrees with the counter after increments", func(t *testing.T) {
		dir := t.TempDir()
		c, err := New(dir)
		require.NoError(t, err)
		for range 3 {
			_, err := c.GetAndInc()
			require.NoError(t, err)
		}
		count, err := Read(dir)
		require.NoError(t, err)
		require.Equal(t, c.Get(), count)
		require.Equal(t, uint64(3), count)
	})
}

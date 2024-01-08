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

package hnsw

import (
	"os"
	"strings"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

func TestMmapCondensor(t *testing.T) {
	t.Skip() // TODO

	rootPath := t.TempDir()

	logger, _ := test.NewNullLogger()
	uncondensed, err := NewCommitLogger(rootPath, "uncondensed", logger,
		cyclemanager.NewCallbackGroupNoop())
	require.Nil(t, err)

	perfect, err := NewCommitLogger(rootPath, "perfect", logger,
		cyclemanager.NewCallbackGroupNoop())
	require.Nil(t, err)

	t.Run("add redundant data to the original log", func(t *testing.T) {
		uncondensed.AddNode(&vertex{id: 0, level: 3})
		uncondensed.AddNode(&vertex{id: 1, level: 3})
		uncondensed.AddNode(&vertex{id: 2, level: 3})
		uncondensed.AddNode(&vertex{id: 3, level: 3})

		// below are some pointless connection replacements, we expect that most of
		// these will be gone after condensing, this gives us a good way of testing
		// whether they're really gone
		for level := 0; level <= 3; level++ {
			uncondensed.ReplaceLinksAtLevel(0, level, []uint64{1, 2, 3})
			uncondensed.ReplaceLinksAtLevel(0, level, []uint64{1, 2})
			uncondensed.ReplaceLinksAtLevel(0, level, []uint64{1})
			uncondensed.ReplaceLinksAtLevel(0, level, []uint64{2})
			uncondensed.ReplaceLinksAtLevel(0, level, []uint64{3})
			uncondensed.ReplaceLinksAtLevel(0, level, []uint64{2, 3})
			uncondensed.ReplaceLinksAtLevel(0, level, []uint64{1, 2, 3})
			uncondensed.ReplaceLinksAtLevel(1, level, []uint64{0, 2, 3})
			uncondensed.ReplaceLinksAtLevel(1, level, []uint64{0, 2})
			uncondensed.ReplaceLinksAtLevel(1, level, []uint64{0})
			uncondensed.ReplaceLinksAtLevel(1, level, []uint64{2})
			uncondensed.ReplaceLinksAtLevel(1, level, []uint64{3})
			uncondensed.ReplaceLinksAtLevel(1, level, []uint64{2, 3})
			uncondensed.ReplaceLinksAtLevel(1, level, []uint64{0, 2, 3})
			uncondensed.ReplaceLinksAtLevel(2, level, []uint64{0, 1, 3})
			uncondensed.ReplaceLinksAtLevel(2, level, []uint64{0, 1})
			uncondensed.ReplaceLinksAtLevel(2, level, []uint64{0})
			uncondensed.ReplaceLinksAtLevel(2, level, []uint64{1})
			uncondensed.ReplaceLinksAtLevel(2, level, []uint64{3})
			uncondensed.ReplaceLinksAtLevel(2, level, []uint64{1, 3})
			uncondensed.ReplaceLinksAtLevel(2, level, []uint64{0, 1, 3})
			uncondensed.ReplaceLinksAtLevel(3, level, []uint64{0, 1, 2})
			uncondensed.ReplaceLinksAtLevel(3, level, []uint64{0, 1})
			uncondensed.ReplaceLinksAtLevel(3, level, []uint64{0})
			uncondensed.ReplaceLinksAtLevel(3, level, []uint64{1})
			uncondensed.ReplaceLinksAtLevel(3, level, []uint64{2})
			uncondensed.ReplaceLinksAtLevel(3, level, []uint64{1, 2})
			uncondensed.ReplaceLinksAtLevel(3, level, []uint64{0, 1, 2})
		}
		uncondensed.SetEntryPointWithMaxLayer(3, 3)
		uncondensed.AddTombstone(2)

		require.Nil(t, uncondensed.Flush())
	})

	t.Run("create a hypothetical perfect log", func(t *testing.T) {
		perfect.AddNode(&vertex{id: 0, level: 3})
		perfect.AddNode(&vertex{id: 1, level: 3})
		perfect.AddNode(&vertex{id: 2, level: 3})
		perfect.AddNode(&vertex{id: 3, level: 3})

		// below are some pointless connection replacements, we expect that most of
		// these will be gone after condensing, this gives us a good way of testing
		// whether they're really gone
		for level := 0; level <= 3; level++ {
			perfect.ReplaceLinksAtLevel(0, level, []uint64{1, 2, 3})
			perfect.ReplaceLinksAtLevel(1, level, []uint64{0, 2, 3})
			perfect.ReplaceLinksAtLevel(2, level, []uint64{0, 1, 3})
			perfect.ReplaceLinksAtLevel(3, level, []uint64{0, 1, 2})
		}
		perfect.SetEntryPointWithMaxLayer(3, 3)
		perfect.AddTombstone(2)

		require.Nil(t, perfect.Flush())
	})

	t.Run("condense the original and verify against the perfect one", func(t *testing.T) {
		input, ok, err := getCurrentCommitLogFileName(commitLogDirectory(rootPath, "uncondensed"))
		require.Nil(t, err)
		require.True(t, ok)

		err = NewMmapCondensor(3).Do(commitLogFileName(rootPath, "uncondensed", input))
		require.Nil(t, err)

		control, ok, err := getCurrentCommitLogFileName(
			commitLogDirectory(rootPath, "perfect"))
		require.Nil(t, err)
		require.True(t, ok)

		actual, ok, err := getCurrentCommitLogFileName(
			commitLogDirectory(rootPath, "uncondensed"))
		require.Nil(t, err)
		require.True(t, ok)

		assert.True(t, strings.HasSuffix(actual, ".condensed"),
			"commit log is now saved as condensed")

		controlStat, err := os.Stat(commitLogFileName(rootPath, "perfect", control))
		require.Nil(t, err)

		actualStat, err := os.Stat(commitLogFileName(rootPath, "uncondensed", actual))
		require.Nil(t, err)

		assert.Equal(t, controlStat.Size(), actualStat.Size())

		// dumpIndexFromCommitLog(t, commitLogFileName(rootPath, "uncondensed", actual))
		// dumpIndexFromCommitLog(t, commitLogFileName(rootPath, "perfect", control))
	})
}

// func TestCondensorWithoutEntrypoint(t *testing.T) {
// 	rand.Seed(time.Now().UnixNano())
// 	rootPath := t.TempDir()

// 	logger, _ := test.NewNullLogger()
// 	uncondensed, err := NewCommitLogger(rootPath, "uncondensed", logger,
// 		cyclemanager.NewCallbackGroupNoop())
// 	require.Nil(t, err)

// 	t.Run("add data, but do not set an entrypoint", func(t *testing.T) {
// 		uncondensed.AddNode(&vertex{id: 0, level: 3})

// 		require.Nil(t, uncondensed.Flush())
// 	})

// 	t.Run("condense the original and verify it doesn't overwrite the EP", func(t *testing.T) {
// 		input, ok, err := getCurrentCommitLogFileName(commitLogDirectory(rootPath, "uncondensed"))
// 		require.Nil(t, err)
// 		require.True(t, ok)

// 		err = NewMemoryCondensor2(logger).Do(commitLogFileName(rootPath, "uncondensed", input))
// 		require.Nil(t, err)

// 		actual, ok, err := getCurrentCommitLogFileName(
// 			commitLogDirectory(rootPath, "uncondensed"))
// 		require.Nil(t, err)
// 		require.True(t, ok)

// 		assert.True(t, strings.HasSuffix(actual, ".condensed"),
// 			"commit log is now saved as condensed")

// 		initialState := DeserializationResult{
// 			Nodes:      nil,
// 			Entrypoint: 17,
// 			Level:      3,
// 		}
// 		fd, err := os.Open(commitLogFileName(rootPath, "uncondensed", actual))
// 		require.Nil(t, err)

// 		bufr := bufio.NewReader(fd)
// 		res, err := NewDeserializer(logger).Do(bufr, &initialState)
// 		require.Nil(t, err)

// 		assert.Contains(t, res.Nodes, &vertex{id: 0, level: 3, connections: map[int][]uint64{}})
// 		assert.Equal(t, uint64(17), res.Entrypoint)
// 		assert.Equal(t, uint16(3), res.Level)

// 	})
// }

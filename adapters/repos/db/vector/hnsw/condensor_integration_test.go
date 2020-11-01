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

// +build integrationTest

package hnsw

import (
	"fmt"
	"math/rand"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCondensor(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	rootPath := fmt.Sprintf("./testdata/%d", rand.Intn(10000000))
	os.MkdirAll(rootPath, 0o777)
	defer func() {
		err := os.RemoveAll(rootPath)
		fmt.Println(err)
	}()

	logger, _ := test.NewNullLogger()
	uncondensed, err := NewCommitLogger(rootPath, "uncondensed", 0, logger)
	require.Nil(t, err)

	perfect, err := NewCommitLogger(rootPath, "perfect", 0, logger)
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
			uncondensed.ReplaceLinksAtLevel(0, level, []uint32{1, 2, 3})
			uncondensed.ReplaceLinksAtLevel(0, level, []uint32{1, 2})
			uncondensed.ReplaceLinksAtLevel(0, level, []uint32{1})
			uncondensed.ReplaceLinksAtLevel(0, level, []uint32{2})
			uncondensed.ReplaceLinksAtLevel(0, level, []uint32{3})
			uncondensed.ReplaceLinksAtLevel(0, level, []uint32{2, 3})
			uncondensed.ReplaceLinksAtLevel(0, level, []uint32{1, 2, 3})
			uncondensed.ReplaceLinksAtLevel(1, level, []uint32{0, 2, 3})
			uncondensed.ReplaceLinksAtLevel(1, level, []uint32{0, 2})
			uncondensed.ReplaceLinksAtLevel(1, level, []uint32{0})
			uncondensed.ReplaceLinksAtLevel(1, level, []uint32{2})
			uncondensed.ReplaceLinksAtLevel(1, level, []uint32{3})
			uncondensed.ReplaceLinksAtLevel(1, level, []uint32{2, 3})
			uncondensed.ReplaceLinksAtLevel(1, level, []uint32{0, 2, 3})
			uncondensed.ReplaceLinksAtLevel(2, level, []uint32{0, 1, 3})
			uncondensed.ReplaceLinksAtLevel(2, level, []uint32{0, 1})
			uncondensed.ReplaceLinksAtLevel(2, level, []uint32{0})
			uncondensed.ReplaceLinksAtLevel(2, level, []uint32{1})
			uncondensed.ReplaceLinksAtLevel(2, level, []uint32{3})
			uncondensed.ReplaceLinksAtLevel(2, level, []uint32{1, 3})
			uncondensed.ReplaceLinksAtLevel(2, level, []uint32{0, 1, 3})
			uncondensed.ReplaceLinksAtLevel(3, level, []uint32{0, 1, 2})
			uncondensed.ReplaceLinksAtLevel(3, level, []uint32{0, 1})
			uncondensed.ReplaceLinksAtLevel(3, level, []uint32{0})
			uncondensed.ReplaceLinksAtLevel(3, level, []uint32{1})
			uncondensed.ReplaceLinksAtLevel(3, level, []uint32{2})
			uncondensed.ReplaceLinksAtLevel(3, level, []uint32{1, 2})
			uncondensed.ReplaceLinksAtLevel(3, level, []uint32{0, 1, 2})
		}
		uncondensed.SetEntryPointWithMaxLayer(3, 3)
		uncondensed.AddTombstone(2)

		time.Sleep(100 * time.Millisecond) // make sure evertyhing is flushed
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
			perfect.ReplaceLinksAtLevel(0, level, []uint32{1, 2, 3})
			perfect.ReplaceLinksAtLevel(1, level, []uint32{0, 2, 3})
			perfect.ReplaceLinksAtLevel(2, level, []uint32{0, 1, 3})
			perfect.ReplaceLinksAtLevel(3, level, []uint32{0, 1, 2})
		}
		perfect.SetEntryPointWithMaxLayer(3, 3)
		perfect.AddTombstone(2)

		time.Sleep(100 * time.Millisecond) // make sure evertyhing is flushed
	})

	t.Run("condense the original and verify against the perfect one", func(t *testing.T) {
		input, ok, err := getCurrentCommitLogFileName(commitLogDirectory(rootPath, "uncondensed"))
		require.Nil(t, err)
		require.True(t, ok)

		err = NewMemoryCondensor(logger).Do(commitLogFileName(rootPath, "uncondensed", input))
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

func TestCondensorWithoutEntrypoint(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	rootPath := fmt.Sprintf("./testdata/%d", rand.Intn(10000000))
	os.MkdirAll(rootPath, 0o777)
	defer func() {
		err := os.RemoveAll(rootPath)
		fmt.Println(err)
	}()

	logger, _ := test.NewNullLogger()
	uncondensed, err := NewCommitLogger(rootPath, "uncondensed", 0, logger)
	require.Nil(t, err)

	t.Run("add data, but do not set an entrypoint", func(t *testing.T) {
		uncondensed.AddNode(&vertex{id: 0, level: 3})

		time.Sleep(100 * time.Millisecond) // make sure evertyhing is flushed
	})

	t.Run("condense the original and verify it doesn't overwrite the EP", func(t *testing.T) {
		input, ok, err := getCurrentCommitLogFileName(commitLogDirectory(rootPath, "uncondensed"))
		require.Nil(t, err)
		require.True(t, ok)

		err = NewMemoryCondensor(logger).Do(commitLogFileName(rootPath, "uncondensed", input))
		require.Nil(t, err)

		actual, ok, err := getCurrentCommitLogFileName(
			commitLogDirectory(rootPath, "uncondensed"))
		require.Nil(t, err)
		require.True(t, ok)

		assert.True(t, strings.HasSuffix(actual, ".condensed"),
			"commit log is now saved as condensed")

		initialState := DeserializationResult{
			Nodes:      nil,
			Entrypoint: 17,
			Level:      3,
		}
		fd, err := os.Open(commitLogFileName(rootPath, "uncondensed", actual))
		require.Nil(t, err)
		res, err := NewDeserializer(logger).Do(fd, &initialState)
		require.Nil(t, err)

		assert.Contains(t, res.Nodes, &vertex{id: 0, level: 3, connections: map[int][]uint32{}})
		assert.Equal(t, uint32(17), res.Entrypoint)
		assert.Equal(t, uint16(3), res.Level)
	})
}

func dumpIndexFromCommitLog(t *testing.T, fileName string) {
	fd, err := os.Open(fileName)
	require.Nil(t, err)
	logger, _ := test.NewNullLogger()
	res, err := NewDeserializer(logger).Do(fd, nil)
	require.Nil(t, err)

	index := &hnsw{
		nodes:               res.Nodes,
		currentMaximumLayer: int(res.Level),
		entryPointID:        int(res.Entrypoint),
		tombstones:          res.Tombstones,
	}

	dumpIndex(index)
}

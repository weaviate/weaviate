//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

//go:build integrationTest
// +build integrationTest

package hnsw

import (
	"bufio"
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

		err = NewMemoryCondensor2(logger).Do(commitLogFileName(rootPath, "uncondensed", input))
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

func TestCondensorAppendNodeLinks(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	rootPath := fmt.Sprintf("./testdata/%d", rand.Intn(10000000))
	os.MkdirAll(rootPath, 0o777)
	defer func() {
		err := os.RemoveAll(rootPath)
		fmt.Println(err)
	}()

	logger, _ := test.NewNullLogger()
	uncondensed1, err := NewCommitLogger(rootPath, "uncondensed1", 0, logger)
	require.Nil(t, err)

	uncondensed2, err := NewCommitLogger(rootPath, "uncondensed2", 0, logger)
	require.Nil(t, err)

	control, err := NewCommitLogger(rootPath, "control", 0, logger)
	require.Nil(t, err)

	t.Run("add data to the first log", func(t *testing.T) {
		uncondensed1.AddLinkAtLevel(0, 0, 1)
		uncondensed1.AddLinkAtLevel(0, 0, 2)
		uncondensed1.AddLinkAtLevel(0, 0, 3)

		require.Nil(t, uncondensed1.Flush())
	})

	t.Run("append data to the second log", func(t *testing.T) {
		uncondensed2.AddLinkAtLevel(0, 0, 4)
		uncondensed2.AddLinkAtLevel(0, 0, 5)
		uncondensed2.AddLinkAtLevel(0, 0, 6)

		require.Nil(t, uncondensed2.Flush())
	})

	t.Run("create a control log", func(t *testing.T) {
		control.AddNode(&vertex{id: 0, level: 0})
		control.ReplaceLinksAtLevel(0, 0, []uint64{1, 2, 3, 4, 5, 6})

		require.Nil(t, control.Flush())
	})

	t.Run("condense both logs and verify the contents against the control", func(t *testing.T) {
		input, ok, err := getCurrentCommitLogFileName(commitLogDirectory(rootPath, "uncondensed1"))
		require.Nil(t, err)
		require.True(t, ok)

		err = NewMemoryCondensor2(logger).Do(commitLogFileName(rootPath, "uncondensed1", input))
		require.Nil(t, err)

		input, ok, err = getCurrentCommitLogFileName(commitLogDirectory(rootPath, "uncondensed2"))
		require.Nil(t, err)
		require.True(t, ok)

		err = NewMemoryCondensor2(logger).Do(commitLogFileName(rootPath, "uncondensed2", input))
		require.Nil(t, err)

		control, ok, err := getCurrentCommitLogFileName(
			commitLogDirectory(rootPath, "control"))
		require.Nil(t, err)
		require.True(t, ok)

		condensed1, ok, err := getCurrentCommitLogFileName(
			commitLogDirectory(rootPath, "uncondensed1"))
		require.Nil(t, err)
		require.True(t, ok)

		condensed2, ok, err := getCurrentCommitLogFileName(
			commitLogDirectory(rootPath, "uncondensed2"))
		require.Nil(t, err)
		require.True(t, ok)

		assert.True(t, strings.HasSuffix(condensed1, ".condensed"),
			"commit log is now saved as condensed")
		assert.True(t, strings.HasSuffix(condensed2, ".condensed"),
			"commit log is now saved as condensed")

		assertIndicesFromCommitLogsMatch(t, commitLogFileName(rootPath, "control", control),
			[]string{
				commitLogFileName(rootPath, "uncondensed1", condensed1),
				commitLogFileName(rootPath, "uncondensed2", condensed2),
			})
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

		require.Nil(t, uncondensed.Flush())
	})

	t.Run("condense the original and verify it doesn't overwrite the EP", func(t *testing.T) {
		input, ok, err := getCurrentCommitLogFileName(commitLogDirectory(rootPath, "uncondensed"))
		require.Nil(t, err)
		require.True(t, ok)

		err = NewMemoryCondensor2(logger).Do(commitLogFileName(rootPath, "uncondensed", input))
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

		bufr := bufio.NewReader(fd)
		res, err := NewDeserializer(logger).Do(bufr, &initialState)
		require.Nil(t, err)

		assert.Contains(t, res.Nodes, &vertex{id: 0, level: 3, connections: map[int][]uint64{}})
		assert.Equal(t, uint64(17), res.Entrypoint)
		assert.Equal(t, uint16(3), res.Level)
	})
}

func dumpIndexFromCommitLog(t *testing.T, fileName string) {
	fd, err := os.Open(fileName)
	require.Nil(t, err)

	bufr := bufio.NewReader(fd)
	logger, _ := test.NewNullLogger()
	res, err := NewDeserializer(logger).Do(bufr, nil)
	require.Nil(t, err)

	index := &hnsw{
		nodes:               res.Nodes,
		currentMaximumLayer: int(res.Level),
		entryPointID:        res.Entrypoint,
		tombstones:          res.Tombstones,
	}

	dumpIndex(index)
}

func assertIndicesFromCommitLogsMatch(t *testing.T, fileNameControl string,
	fileNames []string) {
	control := readFromCommitLogs(t, fileNameControl)
	actual := readFromCommitLogs(t, fileNames...)

	assert.Equal(t, control, actual)
}

func readFromCommitLogs(t *testing.T, fileNames ...string) *hnsw {
	var res *DeserializationResult

	for _, fileName := range fileNames {
		fd, err := os.Open(fileName)
		require.Nil(t, err)

		bufr := bufio.NewReader(fd)
		logger, _ := test.NewNullLogger()
		res, _, err = NewDeserializer2(logger).Do(bufr, res, false)
		require.Nil(t, err)
	}

	return &hnsw{
		nodes:               removeTrailingNilNodes(res.Nodes),
		currentMaximumLayer: int(res.Level),
		entryPointID:        res.Entrypoint,
		tombstones:          res.Tombstones,
	}
}

// just a test helper to make the output easier to compare, remove all trailing
// nil nodes by starting from the last and stopping as soon as a node is not
// nil
func removeTrailingNilNodes(in []*vertex) []*vertex {
	pos := len(in) - 1

	for pos >= 0 {
		if in[pos] != nil {
			break
		}

		pos--
	}

	return in[:pos+1]
}

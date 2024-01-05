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

//go:build integrationTest
// +build integrationTest

package hnsw

import (
	"bufio"
	"context"
	"os"
	"strings"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

func TestCondensor(t *testing.T) {
	rootPath := t.TempDir()
	ctx := context.Background()

	logger, _ := test.NewNullLogger()
	uncondensed, err := NewCommitLogger(rootPath, "uncondensed", logger,
		cyclemanager.NewCallbackGroupNoop())
	require.Nil(t, err)
	defer uncondensed.Shutdown(ctx)

	perfect, err := NewCommitLogger(rootPath, "perfect", logger,
		cyclemanager.NewCallbackGroupNoop())
	require.Nil(t, err)
	defer perfect.Shutdown(ctx)

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

func TestCondensorAppendNodeLinks(t *testing.T) {
	rootPath := t.TempDir()
	ctx := context.Background()

	logger, _ := test.NewNullLogger()
	uncondensed1, err := NewCommitLogger(rootPath, "uncondensed1", logger,
		cyclemanager.NewCallbackGroupNoop())
	require.Nil(t, err)
	defer uncondensed1.Shutdown(ctx)

	uncondensed2, err := NewCommitLogger(rootPath, "uncondensed2", logger,
		cyclemanager.NewCallbackGroupNoop())
	require.Nil(t, err)
	defer uncondensed2.Shutdown(ctx)

	control, err := NewCommitLogger(rootPath, "control", logger,
		cyclemanager.NewCallbackGroupNoop())
	require.Nil(t, err)
	defer control.Shutdown(ctx)

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

		err = NewMemoryCondensor(logger).Do(commitLogFileName(rootPath, "uncondensed1", input))
		require.Nil(t, err)

		input, ok, err = getCurrentCommitLogFileName(commitLogDirectory(rootPath, "uncondensed2"))
		require.Nil(t, err)
		require.True(t, ok)

		err = NewMemoryCondensor(logger).Do(commitLogFileName(rootPath, "uncondensed2", input))
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

// This test was added as part of
// https://github.com/weaviate/weaviate/issues/1868 to rule out that
// replace links broken across two independent commit logs. It turned out that
// this was green and not the cause for the bug. The bug could be reproduced
// with the new test added in index_too_many_links_bug_integration_test.go.
// Nevertheless it makes sense to keep this test around as this might have been
// a potential cause as well and by having this test, we can prevent a
// regression.
func TestCondensorReplaceNodeLinks(t *testing.T) {
	rootPath := t.TempDir()
	ctx := context.Background()

	logger, _ := test.NewNullLogger()
	uncondensed1, err := NewCommitLogger(rootPath, "uncondensed1", logger,
		cyclemanager.NewCallbackGroupNoop())
	require.Nil(t, err)
	defer uncondensed1.Shutdown(ctx)

	uncondensed2, err := NewCommitLogger(rootPath, "uncondensed2", logger,
		cyclemanager.NewCallbackGroupNoop())
	require.Nil(t, err)
	defer uncondensed2.Shutdown(ctx)

	control, err := NewCommitLogger(rootPath, "control", logger,
		cyclemanager.NewCallbackGroupNoop())
	require.Nil(t, err)
	defer control.Shutdown(ctx)

	t.Run("add data to the first log", func(t *testing.T) {
		uncondensed1.AddNode(&vertex{id: 0, level: 1})
		uncondensed1.AddLinkAtLevel(0, 0, 1)
		uncondensed1.AddLinkAtLevel(0, 0, 2)
		uncondensed1.AddLinkAtLevel(0, 0, 3)
		uncondensed1.AddLinkAtLevel(0, 1, 1)
		uncondensed1.AddLinkAtLevel(0, 1, 2)

		require.Nil(t, uncondensed1.Flush())
	})

	t.Run("replace all data from previous log", func(t *testing.T) {
		uncondensed2.AddLinkAtLevel(0, 0, 10)
		uncondensed2.ReplaceLinksAtLevel(0, 0, []uint64{4, 5, 6})
		uncondensed2.AddLinkAtLevel(0, 0, 7)
		uncondensed2.ReplaceLinksAtLevel(0, 1, []uint64{8})

		require.Nil(t, uncondensed2.Flush())
	})

	t.Run("create a control log", func(t *testing.T) {
		control.AddNode(&vertex{id: 0, level: 1})
		control.ReplaceLinksAtLevel(0, 0, []uint64{4, 5, 6, 7})
		control.ReplaceLinksAtLevel(0, 1, []uint64{8})

		require.Nil(t, control.Flush())
	})

	t.Run("condense both logs and verify the contents against the control", func(t *testing.T) {
		input, ok, err := getCurrentCommitLogFileName(commitLogDirectory(rootPath, "uncondensed1"))
		require.Nil(t, err)
		require.True(t, ok)

		err = NewMemoryCondensor(logger).Do(commitLogFileName(rootPath, "uncondensed1", input))
		require.Nil(t, err)

		input, ok, err = getCurrentCommitLogFileName(commitLogDirectory(rootPath, "uncondensed2"))
		require.Nil(t, err)
		require.True(t, ok)

		err = NewMemoryCondensor(logger).Do(commitLogFileName(rootPath, "uncondensed2", input))
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

// This test was added as part of the investigation and fixing of
// https://github.com/weaviate/weaviate/issues/1868. We used the new
// (higher level) test in index_too_many_links_bug_integration_test.go to
// reproduce the problem without knowing what causes it. Eventually we came to
// the conclusion that "ClearLinksAtLevel" was not propagated correctly across
// two independently condensed commit logs. While the higher-level test already
// makes sure that the bug is gone and prevents regressions, this test was
// still added to test the broken (now fixed) behavior in relative isolation.
func TestCondensorClearLinksAtLevel(t *testing.T) {
	rootPath := t.TempDir()
	ctx := context.Background()

	logger, _ := test.NewNullLogger()
	uncondensed1, err := NewCommitLogger(rootPath, "uncondensed1", logger,
		cyclemanager.NewCallbackGroupNoop())
	require.Nil(t, err)
	defer uncondensed1.Shutdown(ctx)

	uncondensed2, err := NewCommitLogger(rootPath, "uncondensed2", logger,
		cyclemanager.NewCallbackGroupNoop())
	require.Nil(t, err)
	defer uncondensed2.Shutdown(ctx)

	control, err := NewCommitLogger(rootPath, "control", logger,
		cyclemanager.NewCallbackGroupNoop())
	require.Nil(t, err)
	defer control.Shutdown(ctx)

	t.Run("add data to the first log", func(t *testing.T) {
		uncondensed1.AddNode(&vertex{id: 0, level: 1})
		uncondensed1.AddLinkAtLevel(0, 0, 1)
		uncondensed1.AddLinkAtLevel(0, 0, 2)
		uncondensed1.AddLinkAtLevel(0, 0, 3)
		uncondensed1.AddLinkAtLevel(0, 1, 1)
		uncondensed1.AddLinkAtLevel(0, 1, 2)

		require.Nil(t, uncondensed1.Flush())
	})

	t.Run("replace all data from previous log", func(t *testing.T) {
		uncondensed2.AddLinkAtLevel(0, 0, 10)
		uncondensed2.ClearLinksAtLevel(0, 0)
		uncondensed2.AddLinkAtLevel(0, 0, 4)
		uncondensed2.AddLinkAtLevel(0, 0, 5)
		uncondensed2.AddLinkAtLevel(0, 0, 6)
		uncondensed2.AddLinkAtLevel(0, 0, 7)
		uncondensed2.ClearLinksAtLevel(0, 1)
		uncondensed2.AddLinkAtLevel(0, 1, 8)

		require.Nil(t, uncondensed2.Flush())
	})

	t.Run("create a control log", func(t *testing.T) {
		control.AddNode(&vertex{id: 0, level: 1})
		control.ReplaceLinksAtLevel(0, 0, []uint64{4, 5, 6, 7})
		control.ReplaceLinksAtLevel(0, 1, []uint64{8})

		require.Nil(t, control.Flush())
	})

	t.Run("condense both logs and verify the contents against the control", func(t *testing.T) {
		input, ok, err := getCurrentCommitLogFileName(commitLogDirectory(rootPath, "uncondensed1"))
		require.Nil(t, err)
		require.True(t, ok)

		err = NewMemoryCondensor(logger).Do(commitLogFileName(rootPath, "uncondensed1", input))
		require.Nil(t, err)

		input, ok, err = getCurrentCommitLogFileName(commitLogDirectory(rootPath, "uncondensed2"))
		require.Nil(t, err)
		require.True(t, ok)

		err = NewMemoryCondensor(logger).Do(commitLogFileName(rootPath, "uncondensed2", input))
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
	rootPath := t.TempDir()
	ctx := context.Background()

	logger, _ := test.NewNullLogger()
	uncondensed, err := NewCommitLogger(rootPath, "uncondensed", logger,
		cyclemanager.NewCallbackGroupNoop())
	require.Nil(t, err)
	defer uncondensed.Shutdown(ctx)

	t.Run("add data, but do not set an entrypoint", func(t *testing.T) {
		uncondensed.AddNode(&vertex{id: 0, level: 3})

		require.Nil(t, uncondensed.Flush())
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

		bufr := bufio.NewReader(fd)
		res, _, err := NewDeserializer(logger).Do(bufr, &initialState, false)
		require.Nil(t, err)

		assert.Contains(t, res.Nodes, &vertex{id: 0, level: 3, connections: make([][]uint64, 4)})
		assert.Equal(t, uint64(17), res.Entrypoint)
		assert.Equal(t, uint16(3), res.Level)
	})
}

func TestCondensorWithPQInformation(t *testing.T) {
	rootPath := t.TempDir()
	ctx := context.Background()

	logger, _ := test.NewNullLogger()
	uncondensed, err := NewCommitLogger(rootPath, "uncondensed", logger,
		cyclemanager.NewCallbackGroupNoop())
	require.Nil(t, err)
	defer uncondensed.Shutdown(ctx)

	encoders := []compressionhelpers.PQEncoder{
		compressionhelpers.NewKMeansWithCenters(
			4,
			2,
			0,
			[][]float32{{1, 2}, {3, 4}, {5, 6}, {7, 8}},
		),
		compressionhelpers.NewKMeansWithCenters(
			4,
			2,
			1,
			[][]float32{{8, 7}, {6, 5}, {4, 3}, {2, 1}},
		),
		compressionhelpers.NewKMeansWithCenters(
			4,
			2,
			2,
			[][]float32{{1, 2}, {3, 4}, {5, 6}, {7, 8}},
		),
	}

	t.Run("add pq info", func(t *testing.T) {
		uncondensed.AddPQ(compressionhelpers.PQData{
			Ks:                  4,
			M:                   3,
			Dimensions:          6,
			EncoderType:         compressionhelpers.UseKMeansEncoder,
			EncoderDistribution: uint8(0),
			Encoders:            encoders,
			UseBitsEncoding:     false,
		})

		require.Nil(t, uncondensed.Flush())
	})

	t.Run("condense the original and verify the PQ info is present", func(t *testing.T) {
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

		initialState := DeserializationResult{}
		fd, err := os.Open(commitLogFileName(rootPath, "uncondensed", actual))
		require.Nil(t, err)

		bufr := bufio.NewReader(fd)
		res, _, err := NewDeserializer(logger).Do(bufr, &initialState, false)
		require.Nil(t, err)

		assert.True(t, res.Compressed)
		expected := compressionhelpers.PQData{
			Ks:                  4,
			M:                   3,
			Dimensions:          6,
			EncoderType:         compressionhelpers.UseKMeansEncoder,
			EncoderDistribution: uint8(0),
			Encoders:            encoders,
			UseBitsEncoding:     false,
		}

		assert.Equal(t, expected, res.PQData)
	})
}

func assertIndicesFromCommitLogsMatch(t *testing.T, fileNameControl string,
	fileNames []string,
) {
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
		res, _, err = NewDeserializer(logger).Do(bufr, res, false)
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

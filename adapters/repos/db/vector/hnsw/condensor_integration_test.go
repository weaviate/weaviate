//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

//go:build integrationTest

package hnsw

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/packedconn"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/multivector"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

func TestCondensor(t *testing.T) {
	rootPath := t.TempDir()
	ctx := context.Background()

	fs := common.NewOSFS()

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
		input, ok, err := getCurrentCommitLogFileName(commitLogDirectory(rootPath, "uncondensed"), fs)
		require.Nil(t, err)
		require.True(t, ok)

		err = NewMemoryCondensor(logger).Do(commitLogFileName(rootPath, "uncondensed", input))
		require.Nil(t, err)

		control, ok, err := getCurrentCommitLogFileName(
			commitLogDirectory(rootPath, "perfect"), fs)
		require.Nil(t, err)
		require.True(t, ok)

		actual, ok, err := getCurrentCommitLogFileName(
			commitLogDirectory(rootPath, "uncondensed"), fs)
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

	fs := common.NewOSFS()

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
		input, ok, err := getCurrentCommitLogFileName(commitLogDirectory(rootPath, "uncondensed1"), fs)
		require.Nil(t, err)
		require.True(t, ok)

		err = NewMemoryCondensor(logger).Do(commitLogFileName(rootPath, "uncondensed1", input))
		require.Nil(t, err)

		input, ok, err = getCurrentCommitLogFileName(commitLogDirectory(rootPath, "uncondensed2"), fs)
		require.Nil(t, err)
		require.True(t, ok)

		err = NewMemoryCondensor(logger).Do(commitLogFileName(rootPath, "uncondensed2", input))
		require.Nil(t, err)

		control, ok, err := getCurrentCommitLogFileName(
			commitLogDirectory(rootPath, "control"), fs)
		require.Nil(t, err)
		require.True(t, ok)

		condensed1, ok, err := getCurrentCommitLogFileName(
			commitLogDirectory(rootPath, "uncondensed1"), fs)
		require.Nil(t, err)
		require.True(t, ok)

		condensed2, ok, err := getCurrentCommitLogFileName(
			commitLogDirectory(rootPath, "uncondensed2"), fs)
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

	fs := common.NewOSFS()

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
		input, ok, err := getCurrentCommitLogFileName(commitLogDirectory(rootPath, "uncondensed1"), fs)
		require.Nil(t, err)
		require.True(t, ok)

		err = NewMemoryCondensor(logger).Do(commitLogFileName(rootPath, "uncondensed1", input))
		require.Nil(t, err)

		input, ok, err = getCurrentCommitLogFileName(commitLogDirectory(rootPath, "uncondensed2"), fs)
		require.Nil(t, err)
		require.True(t, ok)

		err = NewMemoryCondensor(logger).Do(commitLogFileName(rootPath, "uncondensed2", input))
		require.Nil(t, err)

		control, ok, err := getCurrentCommitLogFileName(
			commitLogDirectory(rootPath, "control"), fs)
		require.Nil(t, err)
		require.True(t, ok)

		condensed1, ok, err := getCurrentCommitLogFileName(
			commitLogDirectory(rootPath, "uncondensed1"), fs)
		require.Nil(t, err)
		require.True(t, ok)

		condensed2, ok, err := getCurrentCommitLogFileName(
			commitLogDirectory(rootPath, "uncondensed2"), fs)
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

	fs := common.NewOSFS()

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
		input, ok, err := getCurrentCommitLogFileName(commitLogDirectory(rootPath, "uncondensed1"), fs)
		require.Nil(t, err)
		require.True(t, ok)

		err = NewMemoryCondensor(logger).Do(commitLogFileName(rootPath, "uncondensed1", input))
		require.Nil(t, err)

		input, ok, err = getCurrentCommitLogFileName(commitLogDirectory(rootPath, "uncondensed2"), fs)
		require.Nil(t, err)
		require.True(t, ok)

		err = NewMemoryCondensor(logger).Do(commitLogFileName(rootPath, "uncondensed2", input))
		require.Nil(t, err)

		control, ok, err := getCurrentCommitLogFileName(
			commitLogDirectory(rootPath, "control"), fs)
		require.Nil(t, err)
		require.True(t, ok)

		condensed1, ok, err := getCurrentCommitLogFileName(
			commitLogDirectory(rootPath, "uncondensed1"), fs)
		require.Nil(t, err)
		require.True(t, ok)

		condensed2, ok, err := getCurrentCommitLogFileName(
			commitLogDirectory(rootPath, "uncondensed2"), fs)
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

func TestCondensorTombstones(t *testing.T) {
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

	fs := common.NewOSFS()

	t.Run("add tombstone data", func(t *testing.T) {
		uncondensed1.AddNode(&vertex{id: 0, level: 1})
		uncondensed1.AddNode(&vertex{id: 1, level: 1})
		uncondensed1.AddNode(&vertex{id: 2, level: 1})
		uncondensed1.AddNode(&vertex{id: 3, level: 1})

		uncondensed1.RemoveTombstone(0)
		uncondensed1.AddTombstone(1)
		uncondensed1.RemoveTombstone(1)
		uncondensed1.AddTombstone(2)

		require.Nil(t, uncondensed1.Flush())
	})

	t.Run("remove all tombstones except the first", func(t *testing.T) {
		uncondensed2.RemoveTombstone(2)
		uncondensed2.AddTombstone(3)
		uncondensed2.RemoveTombstone(3)

		require.Nil(t, uncondensed2.Flush())
	})

	t.Run("create a control log", func(t *testing.T) {
		control.AddNode(&vertex{id: 0, level: 1})
		control.AddNode(&vertex{id: 1, level: 1})
		control.AddNode(&vertex{id: 2, level: 1})
		control.AddNode(&vertex{id: 3, level: 1})

		control.RemoveTombstone(0)

		require.Nil(t, control.Flush())
	})

	t.Run("condense both logs and verify the contents against the control", func(t *testing.T) {
		input, ok, err := getCurrentCommitLogFileName(commitLogDirectory(rootPath, "uncondensed1"), fs)
		require.Nil(t, err)
		require.True(t, ok)

		err = NewMemoryCondensor(logger).Do(commitLogFileName(rootPath, "uncondensed1", input))
		require.Nil(t, err)

		input, ok, err = getCurrentCommitLogFileName(commitLogDirectory(rootPath, "uncondensed2"), fs)
		require.Nil(t, err)
		require.True(t, ok)

		err = NewMemoryCondensor(logger).Do(commitLogFileName(rootPath, "uncondensed2", input))
		require.Nil(t, err)

		control, ok, err := getCurrentCommitLogFileName(
			commitLogDirectory(rootPath, "control"), fs)
		require.Nil(t, err)
		require.True(t, ok)

		condensed1, ok, err := getCurrentCommitLogFileName(
			commitLogDirectory(rootPath, "uncondensed1"), fs)
		require.Nil(t, err)
		require.True(t, ok)

		condensed2, ok, err := getCurrentCommitLogFileName(
			commitLogDirectory(rootPath, "uncondensed2"), fs)
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

func TestCondensorPhantom(t *testing.T) {
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

	fs := common.NewOSFS()

	t.Run("add node via replace links", func(t *testing.T) {
		uncondensed1.ReplaceLinksAtLevel(0, 0, []uint64{1, 2, 3})
		require.Nil(t, uncondensed1.Flush())
	})

	t.Run("start tombstone job, delete node, remove tombstone", func(t *testing.T) {
		uncondensed1.AddTombstone(0)
		uncondensed2.DeleteNode(0)
		uncondensed2.RemoveTombstone(0)
		require.Nil(t, uncondensed2.Flush())
	})

	t.Run("create a control log", func(t *testing.T) {
		require.Nil(t, control.Flush())
	})

	t.Run("condense both logs and verify the contents against the control", func(t *testing.T) {
		input, ok, err := getCurrentCommitLogFileName(commitLogDirectory(rootPath, "uncondensed1"), fs)
		require.Nil(t, err)
		require.True(t, ok)

		err = NewMemoryCondensor(logger).Do(commitLogFileName(rootPath, "uncondensed1", input))
		require.Nil(t, err)

		input, ok, err = getCurrentCommitLogFileName(commitLogDirectory(rootPath, "uncondensed2"), fs)
		require.Nil(t, err)
		require.True(t, ok)

		err = NewMemoryCondensor(logger).Do(commitLogFileName(rootPath, "uncondensed2", input))
		require.Nil(t, err)

		control, ok, err := getCurrentCommitLogFileName(
			commitLogDirectory(rootPath, "control"), fs)
		require.Nil(t, err)
		require.True(t, ok)

		condensed1, ok, err := getCurrentCommitLogFileName(
			commitLogDirectory(rootPath, "uncondensed1"), fs)
		require.Nil(t, err)
		require.True(t, ok)

		condensed2, ok, err := getCurrentCommitLogFileName(
			commitLogDirectory(rootPath, "uncondensed2"), fs)
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

	fs := common.NewOSFS()

	t.Run("add data, but do not set an entrypoint", func(t *testing.T) {
		uncondensed.AddNode(&vertex{id: 0, level: 3})

		require.Nil(t, uncondensed.Flush())
	})

	t.Run("condense the original and verify it doesn't overwrite the EP", func(t *testing.T) {
		input, ok, err := getCurrentCommitLogFileName(commitLogDirectory(rootPath, "uncondensed"), fs)
		require.Nil(t, err)
		require.True(t, ok)

		err = NewMemoryCondensor(logger).Do(commitLogFileName(rootPath, "uncondensed", input))
		require.Nil(t, err)

		actual, ok, err := getCurrentCommitLogFileName(
			commitLogDirectory(rootPath, "uncondensed"), fs)
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

		conns, _ := packedconn.NewWithMaxLayer(3)
		assert.Contains(t, res.Nodes, &vertex{id: 0, level: 3, connections: conns})
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

	fs := common.NewOSFS()

	encoders := []compressionhelpers.PQEncoder{
		compressionhelpers.NewKMeansEncoderWithCenters(
			4,
			2,
			0,
			[][]float32{{1, 2}, {3, 4}, {5, 6}, {7, 8}},
		),
		compressionhelpers.NewKMeansEncoderWithCenters(
			4,
			2,
			1,
			[][]float32{{8, 7}, {6, 5}, {4, 3}, {2, 1}},
		),
		compressionhelpers.NewKMeansEncoderWithCenters(
			4,
			2,
			2,
			[][]float32{{1, 2}, {3, 4}, {5, 6}, {7, 8}},
		),
	}

	t.Run("add pq info", func(t *testing.T) {
		uncondensed.AddPQCompression(compressionhelpers.PQData{
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
		input, ok, err := getCurrentCommitLogFileName(commitLogDirectory(rootPath, "uncondensed"), fs)
		require.Nil(t, err)
		require.True(t, ok)

		err = NewMemoryCondensor(logger).Do(commitLogFileName(rootPath, "uncondensed", input))
		require.Nil(t, err)

		actual, ok, err := getCurrentCommitLogFileName(
			commitLogDirectory(rootPath, "uncondensed"), fs)
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

		assert.Equal(t, expected, *res.CompressionPQData)
	})
}

func TestCondensorWithMUVERAInformation(t *testing.T) {
	rootPath := t.TempDir()
	ctx := context.Background()

	logger, _ := test.NewNullLogger()
	uncondensed, err := NewCommitLogger(rootPath, "uncondensed", logger,
		cyclemanager.NewCallbackGroupNoop())
	require.Nil(t, err)
	defer uncondensed.Shutdown(ctx)

	fs := common.NewOSFS()

	gaussians := [][][]float32{
		{
			{1, 2, 3, 4, 5}, // cluster 1
			{1, 2, 3, 4, 5}, // cluster 2
		}, // rep 1
		{
			{5, 6, 7, 8, 9}, // cluster 1
			{5, 6, 7, 8, 9}, // cluster 2
		}, // rep 2
	} // (repetitions, kSim, dimensions)

	s := [][][]float32{
		{
			{-1, 1, 1, -1, 1}, // dprojection 1
			{1, -1, 1, 1, -1}, // dprojection 2
		}, // rep 1
		{
			{-1, 1, 1, -1, 1}, // dprojection 1
			{1, -1, 1, 1, -1}, // dprojection 2
		}, // rep 2
	} // (repetitions, dProjections, dimensions)

	t.Run("add muvera info", func(t *testing.T) {
		uncondensed.AddMuvera(multivector.MuveraData{
			KSim:         2,
			NumClusters:  4,
			Dimensions:   5,
			DProjections: 2,
			Repetitions:  2,
			Gaussians:    gaussians,
			S:            s,
		})

		require.Nil(t, uncondensed.Flush())
	})

	t.Run("condense the original and verify the MUVERA info is present", func(t *testing.T) {
		input, ok, err := getCurrentCommitLogFileName(commitLogDirectory(rootPath, "uncondensed"), fs)
		require.Nil(t, err)
		require.True(t, ok)

		err = NewMemoryCondensor(logger).Do(commitLogFileName(rootPath, "uncondensed", input))
		require.Nil(t, err)

		actual, ok, err := getCurrentCommitLogFileName(
			commitLogDirectory(rootPath, "uncondensed"), fs)
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

		assert.True(t, res.MuveraEnabled)
		expected := multivector.MuveraData{
			KSim:         2,
			NumClusters:  4,
			Dimensions:   5,
			DProjections: 2,
			Repetitions:  2,
			Gaussians:    gaussians,
			S:            s,
		}

		assert.Equal(t, expected, *res.EncoderMuvera)
	})
}

func TestCondensorWithRQ8Information(t *testing.T) {
	rootPath := t.TempDir()
	ctx := context.Background()

	logger, _ := test.NewNullLogger()
	uncondensed, err := NewCommitLogger(rootPath, "uncondensed", logger,
		cyclemanager.NewCallbackGroupNoop())
	require.Nil(t, err)
	defer uncondensed.Shutdown(ctx)

	rqData := compressionhelpers.RQData{
		InputDim: 10,
		Bits:     8,
		Rotation: compressionhelpers.FastRotation{
			OutputDim: 4,
			Rounds:    5,
			Swaps: [][]compressionhelpers.Swap{
				{
					{I: 0, J: 2},
					{I: 1, J: 3},
				},
				{
					{I: 4, J: 6},
					{I: 5, J: 7},
				},
				{
					{I: 8, J: 10},
					{I: 9, J: 11},
				},
				{
					{I: 12, J: 14},
					{I: 13, J: 15},
				},
				{
					{I: 16, J: 18},
					{I: 17, J: 19},
				},
			},
			Signs: [][]float32{
				{1, -1, 1, -1},
				{1, -1, 1, -1},
				{1, -1, 1, -1},
				{1, -1, 1, -1},
				{1, -1, 1, -1},
			},
		},
	}

	t.Run("add rotational quantization info", func(t *testing.T) {
		uncondensed.AddRQCompression(rqData)

		require.Nil(t, uncondensed.Flush())
	})

	t.Run("condense the original and verify the RQ info is present", func(t *testing.T) {
		fs := common.NewOSFS()
		input, ok, err := getCurrentCommitLogFileName(commitLogDirectory(rootPath, "uncondensed"), fs)
		require.Nil(t, err)
		require.True(t, ok)

		err = NewMemoryCondensor(logger).Do(commitLogFileName(rootPath, "uncondensed", input))
		require.Nil(t, err)

		actual, ok, err := getCurrentCommitLogFileName(
			commitLogDirectory(rootPath, "uncondensed"), fs)
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
		expected := rqData

		assert.Equal(t, expected, *res.CompressionRQData)
	})
}

func TestCondensorWithRQ1Information(t *testing.T) {
	rootPath := t.TempDir()
	ctx := context.Background()

	logger, _ := test.NewNullLogger()
	uncondensed, err := NewCommitLogger(rootPath, "uncondensed", logger,
		cyclemanager.NewCallbackGroupNoop())
	require.Nil(t, err)
	defer uncondensed.Shutdown(ctx)

	brqData := compressionhelpers.BRQData{
		InputDim: 10,
		Rotation: compressionhelpers.FastRotation{
			OutputDim: 4,
			Rounds:    5,
			Swaps: [][]compressionhelpers.Swap{
				{
					{I: 0, J: 2},
					{I: 1, J: 3},
				},
				{
					{I: 4, J: 6},
					{I: 5, J: 7},
				},
				{
					{I: 8, J: 10},
					{I: 9, J: 11},
				},
				{
					{I: 12, J: 14},
					{I: 13, J: 15},
				},
				{
					{I: 16, J: 18},
					{I: 17, J: 19},
				},
			},
			Signs: [][]float32{
				{1, -1, 1, -1},
				{1, -1, 1, -1},
				{1, -1, 1, -1},
				{1, -1, 1, -1},
				{1, -1, 1, -1},
			},
		},
		Rounding: []float32{0.1, 0.2, 0.3, 0.4},
	}

	t.Run("add binary rotational quantization info", func(t *testing.T) {
		uncondensed.AddBRQCompression(brqData)

		require.Nil(t, uncondensed.Flush())
	})

	t.Run("condense the original and verify the BRQ info is present", func(t *testing.T) {
		fs := common.NewOSFS()
		input, ok, err := getCurrentCommitLogFileName(commitLogDirectory(rootPath, "uncondensed"), fs)
		require.Nil(t, err)
		require.True(t, ok)

		err = NewMemoryCondensor(logger).Do(commitLogFileName(rootPath, "uncondensed", input))
		require.Nil(t, err)

		actual, ok, err := getCurrentCommitLogFileName(
			commitLogDirectory(rootPath, "uncondensed"), fs)
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
		expected := brqData

		assert.Equal(t, expected, *res.CompressionBRQData)
	})
}

func newMemoryCondensor(t *testing.T, rootPath string, fs common.FS) (*MemoryCondensor, string) {
	ctx := context.Background()

	logger, _ := test.NewNullLogger()
	cl, err := NewCommitLogger(rootPath, "memory_condensor", logger,
		cyclemanager.NewCallbackGroupNoop())
	require.Nil(t, err)
	t.Cleanup(func() {
		cl.Shutdown(ctx)
	})

	cl.AddNode(&vertex{id: 0, level: 3})
	cl.AddNode(&vertex{id: 1, level: 3})
	cl.AddNode(&vertex{id: 2, level: 3})
	cl.AddNode(&vertex{id: 3, level: 3})

	// below are some pointless connection replacements, we expect that most of
	// these will be gone after condensing, this gives us a good way of testing
	// whether they're really gone
	for level := 0; level <= 3; level++ {
		cl.ReplaceLinksAtLevel(0, level, []uint64{1, 2, 3})
		cl.ReplaceLinksAtLevel(0, level, []uint64{1, 2})
		cl.ReplaceLinksAtLevel(0, level, []uint64{1})
		cl.ReplaceLinksAtLevel(0, level, []uint64{2})
		cl.ReplaceLinksAtLevel(0, level, []uint64{3})
		cl.ReplaceLinksAtLevel(0, level, []uint64{2, 3})
		cl.ReplaceLinksAtLevel(0, level, []uint64{1, 2, 3})
		cl.ReplaceLinksAtLevel(1, level, []uint64{0, 2, 3})
		cl.ReplaceLinksAtLevel(1, level, []uint64{0, 2})
		cl.ReplaceLinksAtLevel(1, level, []uint64{0})
		cl.ReplaceLinksAtLevel(1, level, []uint64{2})
		cl.ReplaceLinksAtLevel(1, level, []uint64{3})
		cl.ReplaceLinksAtLevel(1, level, []uint64{2, 3})
		cl.ReplaceLinksAtLevel(1, level, []uint64{0, 2, 3})
		cl.ReplaceLinksAtLevel(2, level, []uint64{0, 1, 3})
		cl.ReplaceLinksAtLevel(2, level, []uint64{0, 1})
		cl.ReplaceLinksAtLevel(2, level, []uint64{0})
		cl.ReplaceLinksAtLevel(2, level, []uint64{1})
		cl.ReplaceLinksAtLevel(2, level, []uint64{3})
		cl.ReplaceLinksAtLevel(2, level, []uint64{1, 3})
		cl.ReplaceLinksAtLevel(2, level, []uint64{0, 1, 3})
		cl.ReplaceLinksAtLevel(3, level, []uint64{0, 1, 2})
		cl.ReplaceLinksAtLevel(3, level, []uint64{0, 1})
		cl.ReplaceLinksAtLevel(3, level, []uint64{0})
		cl.ReplaceLinksAtLevel(3, level, []uint64{1})
		cl.ReplaceLinksAtLevel(3, level, []uint64{2})
		cl.ReplaceLinksAtLevel(3, level, []uint64{1, 2})
		cl.ReplaceLinksAtLevel(3, level, []uint64{0, 1, 2})
	}
	cl.SetEntryPointWithMaxLayer(3, 3)
	cl.AddTombstone(2)

	var encoders []compressionhelpers.PQEncoder
	m := 32000
	for i := 0; i < m; i++ {
		encoders = append(encoders,
			compressionhelpers.NewKMeansEncoderWithCenters(
				4,
				2,
				i,
				[][]float32{{1, 2}, {3, 4}, {5, 6}, {7, 8}},
			),
		)
	}

	cl.AddPQCompression(compressionhelpers.PQData{
		Ks:                  4,
		M:                   uint16(m),
		Dimensions:          64000,
		EncoderType:         compressionhelpers.UseKMeansEncoder,
		EncoderDistribution: uint8(0),
		Encoders:            encoders,
		UseBitsEncoding:     false,
	})

	require.Nil(t, cl.Flush())

	clFilename, ok, err := getCurrentCommitLogFileName(commitLogDirectory(rootPath, "memory_condensor"), fs)
	require.Nil(t, err)
	require.True(t, ok)

	return &MemoryCondensor{logger: logger, fs: fs}, commitLogFileName(rootPath, "memory_condensor", clFilename)
}

func getCondensedFileSizes(t *testing.T, rootPath string) []int64 {
	fs := common.NewOSFS()
	files, err := fs.ReadDir(commitLogDirectory(rootPath, "memory_condensor"))
	require.Nil(t, err)
	sizes := make([]int64, 0, len(files))
	for _, file := range files {
		if !strings.HasSuffix(file.Name(), ".condensed") {
			continue
		}
		fileInfo, err := file.Info()
		require.Nil(t, err)
		sizes = append(sizes, fileInfo.Size())
	}
	return sizes
}

func TestCondensorCrashSafety(t *testing.T) {
	// condense once with no error to get a baseline
	rootPath := t.TempDir()
	m, clFilename := newMemoryCondensor(t, rootPath, common.NewOSFS())
	err := m.Do(clFilename)
	require.Nil(t, err)
	sizes := getCondensedFileSizes(t, rootPath)
	want := sizes[0]

	t.Run("recovers partially condensed files", func(t *testing.T) {
		// with a 64 bytes buffer, there are about 12 writes happening in the memory condensor.
		// change this value if the number of writes change
		for i := range 12 {
			t.Run(fmt.Sprintf("fails on write number %d", i+1), func(t *testing.T) {
				// create a new memory condensor with a failing file system:
				rootPath = t.TempDir()
				var counter int
				fs := common.NewTestFS()
				fs.OnOpenFile = func(f common.File) common.File {
					return &common.TestFile{
						File: f,
						OnWrite: func(b []byte) (n int, err error) {
							counter++
							if counter == i+1 {
								return 0, errors.Errorf("fake temp error: %d writes", counter)
							}
							return f.Write(b)
						},
					}
				}

				m, clFilename = newMemoryCondensor(t, rootPath, fs)
				m.bufferSize = 64

				// condense once, disk state should be: ["001", "001.condensed"]
				err = m.Do(clFilename)
				require.Error(t, err)
				sizes = getCondensedFileSizes(t, rootPath)
				brokenSize := sizes[0]
				require.Less(t, brokenSize, want)

				// condense again, this time with no FS error, disk state should be: ["001.condensed"]
				err = m.Do(clFilename)
				require.Nil(t, err)
				sizes = getCondensedFileSizes(t, rootPath)
				got := sizes[0]
				require.Equal(t, want, got)
			})
		}
	})

	t.Run("ensure fsync is called", func(t *testing.T) {
		rootPath = t.TempDir()

		var fsyncCalled bool
		fs := common.NewTestFS()
		fs.OnOpenFile = func(f common.File) common.File {
			return &common.TestFile{
				File: f,
				OnSync: func() error {
					fsyncCalled = true
					return f.Sync()
				},
			}
		}

		m, clFilename = newMemoryCondensor(t, rootPath, fs)
		err = m.Do(clFilename)
		require.NoError(t, err)
		require.True(t, fsyncCalled)
	})
}

func assertIndicesFromCommitLogsMatch(t *testing.T, fileNameControl string, fileNames []string) {
	control := readFromCommitLogs(t, fileNameControl)
	actual := readFromCommitLogs(t, fileNames...)

	// hide tombstones for comparison
	// and compare them manually
	cTombstones := control.tombstones
	control.tombstones = nil
	aTombstones := actual.tombstones
	actual.tombstones = nil
	assert.Equal(t, cTombstones.Size(), aTombstones.Size(),
		"tombstone count should match")
	cTombstones.Range(func(key uint64, _ struct{}) bool {
		_, ok := aTombstones.Load(key)
		assert.True(t, ok, "tombstone for ID %d should be present", key)
		return true
	})

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

// +build integrationTest

package hnsw

import (
	"fmt"
	"math/rand"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCondensor(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	rootPath := fmt.Sprintf("./testdata/%d", rand.Intn(10000000))
	os.MkdirAll(rootPath, 0777)
	defer func() {
		err := os.RemoveAll(rootPath)
		fmt.Println(err)
	}()

	uncondensed, err := NewCommitLogger(rootPath, "uncondensed", 0)
	require.Nil(t, err)

	perfect, err := NewCommitLogger(rootPath, "perfect", 0)
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

		err = NewMemoryCondensor().Do(commitLogFileName(rootPath, "uncondensed", input))
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

func dumpIndexFromCommitLog(t *testing.T, fileName string) {
	fd, err := os.Open(fileName)
	require.Nil(t, err)
	res, err := newDeserializer().Do(fd, nil)
	require.Nil(t, err)

	index := &hnsw{
		nodes:               res.nodes,
		currentMaximumLayer: int(res.level),
		entryPointID:        int(res.entrypoint),
		tombstones:          res.tombstones,
	}

	dumpIndex(index)
}

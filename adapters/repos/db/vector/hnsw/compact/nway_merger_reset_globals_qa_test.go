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

package compact

import (
	"bufio"
	"bytes"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mergeGlobalCommits fails closed on a ResetIndexCommit in a merge input,
// because the merger can only ever ingest sorted + snapshot iterators and
// neither file type can contain a ResetIndexCommit:
//
//   - ResetIndexCommit is only written to raw WAL files by the live commit logger
//     (commit_logger.Reset -> WriteResetIndex; delete.go resetUnlocked).
//   - The compactor converts raw/condensed files to sorted FIRST (RunCycle ->
//     convertToSorted -> InMemoryReader.Do, which APPLIES Reset and materializes
//     post-reset state, then SortedWriter, which has no WriteResetIndex path).
//   - NWayMerger (and thus mergeGlobalCommits) only ever ingests sorted + snapshot
//     iterators; neither file type can contain a ResetIndexCommit.
//
// So a ResetIndexCommit in a merge input signals a malformed file and aborts the
// merge with an error. These tests pin that contract and prove the real
// conversion path produces input the merger accepts.

// Test_QA_MergeGlobals_RejectsUnexpectedReset asserts the merger fails closed when
// an input iterator carries a ResetIndexCommit (which can only happen on a
// malformed sorted/snapshot file).
func Test_QA_MergeGlobals_RejectsUnexpectedReset(t *testing.T) {
	t.Run("reset_in_single_iterator", func(t *testing.T) {
		it, err := NewIterator(newFakeCommitReader([]Commit{
			&ResetIndexCommit{},
			&SetEntryPointMaxLevelCommit{Entrypoint: 42, Level: 3},
			&AddNodeCommit{ID: 42, Level: 3},
		}), 0, logrus.New())
		require.NoError(t, err)

		_, err = NewNWayMerger([]IteratorLike{it}, logrus.New())
		require.Error(t, err)
		require.Contains(t, err.Error(), "ResetIndexCommit in merge input")
	})

	t.Run("reset_in_older_of_two_iterators", func(t *testing.T) {
		older, err := NewIterator(newFakeCommitReader([]Commit{
			&ResetIndexCommit{},
			&AddNodeCommit{ID: 1, Level: 0},
		}), 0, logrus.New())
		require.NoError(t, err)
		newer, err := NewIterator(newFakeCommitReader([]Commit{
			&SetEntryPointMaxLevelCommit{Entrypoint: 42, Level: 3},
			&AddNodeCommit{ID: 42, Level: 3},
		}), 1, logrus.New())
		require.NoError(t, err)

		_, err = NewNWayMerger([]IteratorLike{older, newer}, logrus.New())
		require.Error(t, err)
	})
}

func hasEntryPoint(commits []Commit) bool {
	for _, c := range commits {
		if _, ok := c.(*SetEntryPointMaxLevelCommit); ok {
			return true
		}
	}
	return false
}

// Test_QA_PostReset_GlobalsSurviveRealConversionPath proves the production path
// (raw WAL -> InMemoryReader.Do -> SortedWriter) preserves post-reset entrypoint
// state and strips ResetIndex, so the merger both accepts the result (no
// fail-closed error) and keeps the post-reset entrypoint.
func Test_QA_PostReset_GlobalsSurviveRealConversionPath(t *testing.T) {
	logger := logrus.New()

	// Raw WAL: old entrypoint, then a reset, then post-reset entrypoint + node.
	var raw bytes.Buffer
	w := NewWALWriter(&raw)
	require.NoError(t, w.WriteSetEntryPointMaxLevel(7, 1))
	require.NoError(t, w.WriteResetIndex())
	require.NoError(t, w.WriteSetEntryPointMaxLevel(42, 3))
	require.NoError(t, w.WriteAddNode(42, 3))

	// Materialize (this is what convertFileToSorted does).
	memReader := NewInMemoryReader(NewWALCommitReader(bufio.NewReader(&raw), logger), logger)
	result, err := memReader.Do(nil, true)
	require.NoError(t, err)
	require.Equal(t, uint64(42), result.Graph.Entrypoint, "post-reset entrypoint must survive replay")
	require.Equal(t, uint16(3), result.Graph.Level)

	// Convert to sorted (no ResetIndex path in SortedWriter).
	var sorted bytes.Buffer
	require.NoError(t, NewSortedWriter(&sorted, logger).WriteAll(result))
	sortedBytes := sorted.Bytes()

	// The sorted file must NOT contain a ResetIndexCommit.
	scan := NewWALCommitReader(bufio.NewReader(bytes.NewReader(sortedBytes)), logger)
	for {
		c, err := scan.ReadNextCommit()
		if err != nil {
			break
		}
		_, isReset := c.(*ResetIndexCommit)
		require.False(t, isReset, "sorted file must not contain a ResetIndexCommit")
	}

	// The merger accepts the converted file (no fail-closed error) and keeps the
	// post-reset entrypoint.
	mIt, err := NewIterator(NewWALCommitReader(bufio.NewReader(bytes.NewReader(sortedBytes)), logger), 0, logger)
	require.NoError(t, err)
	merger, err := NewNWayMerger([]IteratorLike{mIt}, logger)
	require.NoError(t, err, "merger must accept a sorted file produced by the real conversion path")
	assert.True(t, hasEntryPoint(merger.GlobalCommits()), "post-reset entrypoint preserved through merge")
}

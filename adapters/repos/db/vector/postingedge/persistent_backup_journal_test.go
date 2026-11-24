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

package postingedge

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// While a backup holds the index its journal is not emptied, nor is the mark
// of the centroid graph taken away: the copy of its files is taken after the
// store of the shard is flushed, and what was written in between is only in
// the journal. A shard prepares again on every halt, and lets go once, after
// the last: the index is held from the first PrepareForBackup to the
// ResumeAfterBackup, and the next flush after that empties the journal.
func TestPersistentBackupHoldsTheJournal(t *testing.T) {
	ctx := context.Background()
	const total = persistentBase + 500
	f := newPersistentFixtureWith(t, total, func(cfg *PersistentConfig) { cfg.noBackground = true })
	f.add(t, 0, persistentBase+100)
	require.NoError(t, f.index.Flush())
	g := f.index.g
	journal := filepath.Join(g.dir, growJournalFile)
	mark := filepath.Join(g.dir, growGraphMark)
	require.Zero(t, g.journal.size, "a flush empties the journal")
	require.False(t, fileThere(t, mark))

	require.NoError(t, f.index.PrepareForBackup(ctx))
	require.NoError(t, f.index.PrepareForBackup(ctx), "a second halt of the shard prepares again")
	postings := g.meta.Postings
	f.add(t, persistentBase+100, total)
	require.NoError(t, f.index.Delete(3, 5, persistentBase+150))
	require.Greater(t, g.meta.Postings, postings, "postings split while the backup held the index")
	for range 2 {
		require.NoError(t, f.index.Flush())
	}
	records, err := readGrowJournal(journal)
	require.NoError(t, err)
	assert.NotEmpty(t, records, "every write since the backup began")
	assert.True(t, fileThere(t, mark), "the centroid graph changed since the backup began")

	require.NoError(t, f.index.ResumeAfterBackup(ctx))
	require.NoError(t, f.index.Flush())
	require.Zero(t, g.journal.size, "the first flush after the backup empties the journal")
	assert.False(t, fileThere(t, mark))
}

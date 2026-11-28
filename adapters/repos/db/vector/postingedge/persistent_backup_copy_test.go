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
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
)

// What changes in place is copied by SnapshotMutableFiles, all of it at one
// point between two writes of the index: the meta, the journal, the slots of
// the vectors, the table of when their edges were built, the centroids, and
// the mark of the centroid graph when there is one. The copy is what the
// files were then, whatever is written after it. It is of a backup, and only
// while one holds the index.
func TestPersistentBackupCopiesWhatChangesInPlace(t *testing.T) {
	ctx := context.Background()
	const total = persistentBase + 600
	tests := []struct {
		name  string
		added int
		// split has postings split after the backup began.
		split bool
	}{
		{name: "gathering", added: 300},
		{name: "the base", added: persistentBase + 100},
		{name: "split during the backup", added: persistentBase + 100, split: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newPersistentFixtureWith(t, total, func(cfg *PersistentConfig) { cfg.noBackground = true })
			f.add(t, 0, tt.added)
			_, err := f.index.SnapshotMutableFiles(ctx, f.root, stagedUnder(t.TempDir()))
			require.Error(t, err, "no copy without a backup that holds the index")

			require.NoError(t, f.index.PrepareForBackup(ctx))
			if tt.split {
				postings := f.index.g.meta.Postings
				f.add(t, tt.added, total-100)
				require.Greater(t, f.index.g.meta.Postings, postings)
			}
			staging := t.TempDir()
			staged, err := f.index.SnapshotMutableFiles(ctx, f.root, stagedUnder(staging))
			require.NoError(t, err)

			dir := helpers.PostingEdgeDirName(f.cfg.ID)
			var want []string
			if g := f.index.g; g != nil {
				for _, name := range []string{
					diskMetaFile, growJournalFile, growSlotsName(g.meta.VectorGen), growBuiltFile, diskCentroidFile,
				} {
					want = append(want, filepath.Join(dir, name))
				}
				if tt.split {
					want = append(want, filepath.Join(dir, growGraphMark))
				}
			}
			assert.ElementsMatch(t, want, staged)
			copies := map[string][]byte{}
			for _, rel := range staged {
				live, err := os.ReadFile(filepath.Join(f.root, rel))
				require.NoError(t, err)
				copied, err := os.ReadFile(filepath.Join(staging, rel))
				require.NoError(t, err)
				require.True(t, bytes.Equal(live, copied), "%s is copied as it is", rel)
				copies[rel] = copied
			}

			f.add(t, max(tt.added, total-100), total)
			require.NoError(t, f.index.Flush())
			for rel, copied := range copies {
				now, err := os.ReadFile(filepath.Join(staging, rel))
				require.NoError(t, err)
				require.True(t, bytes.Equal(copied, now), "%s stays as it was copied", rel)
			}
			require.NoError(t, f.index.ResumeAfterBackup(ctx))
		})
	}
}

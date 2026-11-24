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
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
)

// What the index lists for a backup is what never changes in place: the mean
// of the quantizer, the file of vectors of the generation the meta names,
// which only grows, and the files of the commit log of the centroid graph
// that are closed, as an HNSW lists them. Never a file of another generation,
// nor one written next to its place, nor one that changes in place, which
// SnapshotMutableFiles copies. The vectors kept aside are in the store. The
// directory of a build of the base goes too when it holds the list of what
// the build wrote: without it the index that is restored does not open
// (integration-plan.md, section 9).
func TestPersistentBackupListsItsFiles(t *testing.T) {
	ctx := context.Background()
	const total = 2*persistentBase + 300
	tests := []struct {
		name  string
		added int
		// laidOut has the vectors written again once.
		laidOut bool
		// buildList leaves the list of a build of the base in its directory.
		buildList bool
	}{
		{name: "gathering", added: 300},
		{name: "gathering, with the list of a build", added: 300, buildList: true},
		{name: "the base", added: persistentBase + 100},
		{name: "laid out", added: total, laidOut: true},
		{name: "laid out, with the list of a build", added: total, laidOut: true, buildList: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newPersistentFixtureWith(t, total, func(cfg *PersistentConfig) { cfg.noBackground = true })
			f.add(t, 0, tt.added)
			if tt.laidOut {
				require.NoError(t, f.index.layOut(ctx))
			}
			dir := helpers.PostingEdgeDirName(f.cfg.ID)
			buildDir := helpers.PostingEdgeBuildDirName(f.cfg.ID)
			if tt.buildList {
				require.NoError(t, os.MkdirAll(filepath.Join(f.root, buildDir), 0o755))
				for _, name := range []string{buildListFile, buildSourcesFile} {
					require.NoError(t, os.WriteFile(filepath.Join(f.root, buildDir, name), []byte("x"), 0o644))
				}
			}
			// What a pass or a write leaves that is not of the index.
			for _, name := range []string{growVectorsName(9), growSlotsName(9), diskMetaFile + ".tmp"} {
				require.NoError(t, os.WriteFile(filepath.Join(f.root, dir, name), []byte("x"), 0o644))
			}

			require.NoError(t, f.index.PrepareForBackup(ctx))
			listed, err := f.index.ListFiles(ctx, f.root)
			require.NoError(t, err)
			var want []string
			if f.index.g != nil {
				gen := f.index.g.meta.VectorGen
				want = append(want, filepath.Join(dir, diskMeanFile), filepath.Join(dir, growVectorsName(gen)))
				graph, err := f.index.g.st.centroids.graph.ListFiles(ctx, f.root)
				require.NoError(t, err)
				require.NotEmpty(t, graph, "the commit log of the centroid graph")
				want = append(want, graph...)
			}
			if tt.buildList {
				want = append(want, filepath.Join(buildDir, buildListFile), filepath.Join(buildDir, buildSourcesFile))
			}
			assert.ElementsMatch(t, want, listed)
			require.NoError(t, f.index.ResumeAfterBackup(ctx))
		})
	}
}

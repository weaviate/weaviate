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

package db

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/schema"
)

// plantMergedResidue lays down the on-disk shape a node leaves behind when
// a runtime swap dies after markMerged but before markTidied: a tracker
// with merged.mig but no tidied.mig, plus the ingest dir holding the new
// bucket data. Returns the shard's lsm path.
func plantMergedResidue(t *testing.T, rootPath, className, shardName string) string {
	t.Helper()

	lsmPath := shardPathLSM(filepath.Join(rootPath, strings.ToLower(className)), shardName)
	migDir := filepath.Join(lsmPath, ".migrations", "searchable_retokenize_text_1")
	require.NoError(t, os.MkdirAll(migDir, 0o755))
	for _, s := range []string{"reindexed.mig", "prepended.mig", "merged.mig"} {
		require.NoError(t, os.WriteFile(filepath.Join(migDir, s), nil, 0o644))
	}
	require.NoError(t, os.WriteFile(
		filepath.Join(migDir, "properties.mig"), []byte("text"), 0o644))

	ingestDir := filepath.Join(lsmPath, "property_text_searchable__retokenize_ingest_1")
	require.NoError(t, os.MkdirAll(ingestDir, 0o755))
	require.NoError(t, os.WriteFile(
		filepath.Join(ingestDir, "segment.db"), []byte("ingest-data"), 0o644))

	return lsmPath
}

// snapshotTree records every path under root plus file contents, so a test
// can assert that a code path left the tree byte-identical.
func snapshotTree(t *testing.T, root string) map[string]string {
	t.Helper()

	out := map[string]string{}
	require.NoError(t, filepath.Walk(root, func(p string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, relErr := filepath.Rel(root, p)
		if relErr != nil {
			return relErr
		}
		if info.IsDir() {
			out[rel] = "<dir>"
			return nil
		}
		b, readErr := os.ReadFile(p)
		if readErr != nil {
			return readErr
		}
		out[rel] = string(b)
		return nil
	}))
	return out
}

func shardForFinalize(t *testing.T, rootPath, className, shardName string, reindexDisabled bool) *Shard {
	t.Helper()

	logger, _ := test.NewNullLogger()
	return &Shard{
		name: shardName,
		index: &Index{
			logger: logger,
			Config: IndexConfig{
				RootPath:               rootPath,
				ClassName:              schema.ClassName(className),
				RuntimeReindexDisabled: reindexDisabled,
			},
		},
	}
}

// TestFinalizeCompletedMigrations_RuntimeReindexDisabled pins that shard
// load does not resume a mid-migration node while runtime reindex is off:
// the merged-but-untidied tracker keeps every byte it had.
//
// With the flag on the same layout is promoted — the tracker is consumed
// and the ingest dir is renamed to its canonical name.
func TestFinalizeCompletedMigrations_RuntimeReindexDisabled(t *testing.T) {
	const className, shardName = "ResidueClass", "shard1"

	t.Run("disabled leaves disk untouched", func(t *testing.T) {
		rootPath := t.TempDir()
		lsmPath := plantMergedResidue(t, rootPath, className, shardName)
		before := snapshotTree(t, lsmPath)

		shardForFinalize(t, rootPath, className, shardName, true).finalizeCompletedMigrations()

		require.Equal(t, before, snapshotTree(t, lsmPath),
			"runtime reindex off must not resume, rename, or delete anything on disk")
	})

	t.Run("enabled promotes as before", func(t *testing.T) {
		rootPath := t.TempDir()
		lsmPath := plantMergedResidue(t, rootPath, className, shardName)

		shardForFinalize(t, rootPath, className, shardName, false).finalizeCompletedMigrations()

		got, err := os.ReadFile(
			filepath.Join(lsmPath, "property_text_searchable", "segment.db"))
		require.NoError(t, err, "flag on must still promote the ingest dir")
		require.Equal(t, "ingest-data", string(got))
	})
}

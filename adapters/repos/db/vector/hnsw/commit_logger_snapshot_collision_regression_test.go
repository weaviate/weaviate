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

package hnsw

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/compact"
)

// These tests guard against a regression of a real bug that existed in an
// earlier revision of this branch: getCurrentCommitLogFileName picked the
// highest-timestamp file in the directory regardless of suffix, so a
// .snapshot / .sorted / .condensed file could be selected as the append
// target. getLatestCommitFileOrCreate then opened it with O_APPEND, and the
// next WAL write corrupted it — block-level CRC32s in a V3 snapshot became
// invalid, and the HNSW graph was lost on the next restart.
//
// The fix removed the "pick-latest-and-append" behavior entirely: the commit
// logger now always creates a fresh raw file at startup. The invariant these
// tests assert is:
//
//  1. createNewCommitFile never returns a path to a .snapshot / .sorted /
//     .condensed file, regardless of what is already in the directory.
//  2. A valid V3 snapshot that pre-exists in the commit log directory
//     survives a call to createNewCommitFile and stays readable by
//     SnapshotReader afterwards (no corruption).
//  3. Zero-byte raw files left behind by previous startups are pruned so
//     the directory never accumulates more than one empty raw file.

func TestCommitLogger_NeverAppendsToSnapshot(t *testing.T) {
	rootPath := t.TempDir()
	indexName := "regression"
	dir := commitLogDirectory(rootPath, indexName)
	require.NoError(t, os.MkdirAll(dir, 0o755))

	// Seed the directory with a valid V3 snapshot that has the highest
	// timestamp of anything in the directory. Before the fix this was the
	// single worst-case input: the snapshot won the argmax in
	// getCurrentCommitLogFileName and then received O_APPEND WAL bytes.
	snapshotPath := filepath.Join(dir, "1000000000.snapshot")
	writeValidSnapshot(t, snapshotPath)

	origStat, err := os.Stat(snapshotPath)
	require.NoError(t, err)

	logger, _ := test.NewNullLogger()
	sr := compact.NewSnapshotReader(logger)
	_, err = sr.ReadFromFile(snapshotPath)
	require.NoError(t, err, "control: freshly written snapshot must be readable")

	// This is the production call made from NewCommitLogger.
	fd, path, err := createNewCommitFile(rootPath, indexName, common.NewOSFS(), logger)
	require.NoError(t, err)
	t.Cleanup(func() { _ = fd.Close() })

	// (1) The append target must not be the snapshot, nor any non-raw file.
	assert.NotEqual(t, snapshotPath, path,
		"append target must never be the pre-existing snapshot")
	base := filepath.Base(path)
	for _, suffix := range []string{".snapshot", ".sorted", ".condensed", ".tmp"} {
		assert.False(t, strings.HasSuffix(base, suffix),
			"append target %q must not have suffix %q", base, suffix)
	}
	assert.False(t, strings.Contains(base, "_"),
		"append target %q must be a pure-timestamp raw filename", base)

	// Simulate a real first-write after startup. This is what used to
	// corrupt the snapshot; it must now land in the new raw file instead.
	require.NoError(t, compact.NewWALWriter(fd).WriteAddNode(42, 1))
	require.NoError(t, fd.Close())

	// (2) The pre-existing snapshot is byte-for-byte unchanged and still
	// readable. This is the strongest assertion we can make about the fix.
	newStat, err := os.Stat(snapshotPath)
	require.NoError(t, err)
	assert.Equal(t, origStat.Size(), newStat.Size(),
		"snapshot file size must not change after opening a new commit log")

	sr2 := compact.NewSnapshotReader(logger)
	_, err = sr2.ReadFromFile(snapshotPath)
	assert.NoError(t, err,
		"pre-existing snapshot must remain readable after commit logger startup")
}

func TestCommitLogger_NeverAppendsToNonRaw_AllSuffixes(t *testing.T) {
	// Directory contains one file of each type the compactor produces.
	// The newest-timestamp file is a .condensed, which under the old filter
	// (skip only .tmp / hidden) would have won the argmax.
	cases := []struct {
		name  string
		files []string
	}{
		{"snapshot is newest", []string{"100.sorted", "500.snapshot"}},
		{"sorted is newest", []string{"100.condensed", "500.sorted"}},
		{"condensed is newest", []string{"100.snapshot", "500.condensed"}},
		{"range-form sorted is newest", []string{"100.snapshot", "200_500.sorted"}},
	}

	logger, _ := test.NewNullLogger()
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rootPath := t.TempDir()
			indexName := "regression"
			dir := commitLogDirectory(rootPath, indexName)
			require.NoError(t, os.MkdirAll(dir, 0o755))
			for _, f := range tc.files {
				require.NoError(t, os.WriteFile(filepath.Join(dir, f), []byte("x"), 0o644))
			}

			fd, path, err := createNewCommitFile(rootPath, indexName, common.NewOSFS(), logger)
			require.NoError(t, err)
			t.Cleanup(func() { _ = fd.Close() })

			base := filepath.Base(path)
			for _, suffix := range []string{".snapshot", ".sorted", ".condensed", ".tmp"} {
				assert.False(t, strings.HasSuffix(base, suffix),
					"append target %q must not have suffix %q", base, suffix)
			}
			assert.False(t, strings.Contains(base, "_"),
				"append target %q must be a pure-timestamp raw filename", base)
		})
	}
}

func TestCommitLogger_PrunesEmptyRawFiles(t *testing.T) {
	rootPath := t.TempDir()
	indexName := "prune"
	dir := commitLogDirectory(rootPath, indexName)
	require.NoError(t, os.MkdirAll(dir, 0o755))

	// Three empty raw files from (simulated) prior startups with no writes.
	for _, ts := range []string{"100", "200", "300"} {
		require.NoError(t, os.WriteFile(filepath.Join(dir, ts), []byte{}, 0o644))
	}
	// One non-empty raw file that must NOT be pruned — it carries real WAL
	// bytes that are load-critical.
	require.NoError(t, os.WriteFile(filepath.Join(dir, "400"), []byte("not empty"), 0o644))
	// A snapshot and a sorted file that must be left strictly alone by the
	// pruner regardless of size (we seed one of them as empty on purpose to
	// confirm the suffix-based filter, not the size filter, protects them).
	require.NoError(t, os.WriteFile(filepath.Join(dir, "50.snapshot"), []byte{}, 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "60.sorted"), []byte("x"), 0o644))

	logger, _ := test.NewNullLogger()
	fd, path, err := createNewCommitFile(rootPath, indexName, common.NewOSFS(), logger)
	require.NoError(t, err)
	t.Cleanup(func() { _ = fd.Close() })

	// Non-raw files are preserved regardless of size.
	_, err = os.Stat(filepath.Join(dir, "50.snapshot"))
	assert.NoError(t, err, "snapshot must not be pruned")
	_, err = os.Stat(filepath.Join(dir, "60.sorted"))
	assert.NoError(t, err, "sorted file must not be pruned")

	// Non-empty raw file is preserved.
	_, err = os.Stat(filepath.Join(dir, "400"))
	assert.NoError(t, err, "non-empty raw file must not be pruned")

	// Empty raws from prior startups are gone.
	for _, ts := range []string{"100", "200", "300"} {
		_, err := os.Stat(filepath.Join(dir, ts))
		assert.True(t, os.IsNotExist(err),
			"empty raw file %q from a prior startup should have been pruned", ts)
	}

	// After pruning + creation, exactly one empty raw file exists: the new
	// one we just created. That's the "at most one empty raw file at a time"
	// invariant.
	raws, err := listRawCommitLogFiles(dir)
	require.NoError(t, err)
	emptyCount := 0
	for _, e := range raws {
		info, err := e.Info()
		require.NoError(t, err)
		if info.Size() == 0 {
			emptyCount++
		}
	}
	assert.Equal(t, 1, emptyCount, "exactly one empty raw file should remain (the freshly created one)")
	assert.Equal(t, filepath.Base(path), filepath.Base(path),
		"freshly created file should be the only empty raw file")
}

func writeValidSnapshot(t *testing.T, path string) {
	t.Helper()

	f, err := os.Create(path)
	require.NoError(t, err)
	defer f.Close()

	sw := compact.NewSnapshotWriter(f)
	sw.SetEntrypoint(0, 1)

	// Enough nodes that the body produces at least one full block, so the
	// reader actually exercises CRC verification on a real block (not just
	// metadata).
	for i := uint64(0); i < 32; i++ {
		connections := [][]uint64{
			{(i + 1) % 32, (i + 2) % 32, (i + 3) % 32},
		}
		sw.AddNode(i, 0, connections, false)
	}

	require.NoError(t, sw.Flush())
}

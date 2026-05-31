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

//go:build integrationTest

package lsmkv

import (
	"context"
	"encoding/binary"
	"math"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

// isNaNOrInf reports whether x is NaN or infinite (the avg-scalar must be neither
// after recovery -- G8).
func isNaNOrInf(x float64) bool {
	return math.IsNaN(x) || math.IsInf(x, 0)
}

// writeUint64 overwrites the little-endian uint64 at off in buf in place. Used by
// the S14c torn-section forging to bump n / size past their honest values.
func writeUint64(buf []byte, off int, v uint64) {
	binary.LittleEndian.PutUint64(buf[off:off+8], v)
}

// dbFileNames returns the sorted base names of the .db segment files in dir.
func dbFileNames(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	var out []string
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".db" {
			out = append(out, e.Name())
		}
	}
	sort.Strings(out)
	return out
}

// newV2TestSegmentMmapTorn builds a minimal mmap-mode *segment whose contents hold
// the given (possibly torn) V2 section at a non-zero PropertyLengthsOffset, with a
// REAL logrus logger so the torn-section WARN path is exercised (and a hook can
// observe it if needed). It deliberately does NOT set Version-guard fields beyond
// what loadPropertyLengthsV2 reads, so the test drives the section-level
// size/remaining-bytes cross-check directly.
//
// Unlike newV2TestSegmentMmap, the contents are exactly [pad][section] with the
// segment's contents length == pad+len(section), so segmentByteLen() returns the
// TRUE byte length the read path indexes -- which is what makes the remaining-bytes
// bound meaningful for a truncated/forged section.
func newV2TestSegmentMmapTorn(t *testing.T, section []byte) *segment {
	t.Helper()
	const pad = 64
	contents := make([]byte, pad+len(section))
	copy(contents[pad:], section)

	return &segment{
		path:           "test-segment-torn",
		strategy:       segmentindex.StrategyInverted,
		readFromMemory: true,
		contents:       contents,
		logger:         logrus.New(),
		invertedHeader: &segmentindex.HeaderInverted{
			Version:               segmentindex.SegmentInvertedVersionV2,
			PropertyLengthsOffset: pad,
		},
		invertedData: &segmentInvertedData{},
	}
}

// craftLeftoverCompactionTmp recreates the on-disk state a crash leaves AFTER the
// compactor wrote the merged .db.tmp but BEFORE switchOnDisk renamed it: the two
// input .db files still present, plus a leftover .db.tmp (named with BOTH input
// IDs joined by '_') holding the REAL merged compaction output.
//
// It does this faithfully without reimplementing compaction: it copies the two
// input .db files aside, runs the REAL compactOnce on the live bucket (which
// produces the merged output and removes the inputs), copies the merged output to
// the correctly-named .db.tmp, and restores the two original input .db files. The
// result is byte-identical to the genuine pre-rename artifact. inputDBs are the two
// input .db base names (sorted); the returned name is the .db.tmp base name placed
// in dir.
func craftLeftoverCompactionTmp(t *testing.T, ctx context.Context, logger logrus.FieldLogger, dir string, inputDBs []string) string {
	t.Helper()
	require.Len(t, inputDBs, 2)

	// 1. Stash byte-copies of the two inputs so we can restore them after the real
	//    compaction consumes them.
	stash := t.TempDir()
	stashed := make([]string, 2)
	for i, name := range inputDBs {
		stashed[i] = filepath.Join(stash, name)
		copyFile(t, filepath.Join(dir, name), stashed[i])
		// also stash sibling derived files (bloom, cna) so the restored inputs open.
		copySiblingDerived(t, dir, stash, name)
	}

	// 2. Run the REAL compaction on a throwaway bucket pointed at the SAME dir, so
	//    the merged output is produced by the production convert path. Use noop
	//    cyclemanagers and the V2 write flag.
	cb := newInvertedBucket(t, ctx, dir, logger, true)
	for {
		compacted, err := cb.disk.compactOnce(context.Background())
		require.NoError(t, err)
		if !compacted {
			break
		}
	}
	require.NoError(t, cb.Shutdown(ctx))

	// 3. The surviving .db is the merged output. Build the pre-rename .db.tmp name
	//    from the two ORIGINAL input IDs (default bucket: segment-<id0>_<id1>.db.tmp).
	merged := dbFileNames(t, dir)
	require.Len(t, merged, 1, "compaction converged to one output before we stage the crash artifact")
	mergedPath := filepath.Join(dir, merged[0])

	id0 := segmentID(inputDBs[0])
	id1 := segmentID(inputDBs[1])
	tmpName := "segment-" + id0 + "_" + id1 + ".db.tmp"
	copyFile(t, mergedPath, filepath.Join(dir, tmpName))

	// 4. Remove the merged output and restore the two original inputs (and their
	//    derived siblings): the on-disk state is now exactly input0.db + input1.db
	//    + segment-id0_id1.db.tmp -- the rename-did-not-land crash state.
	require.NoError(t, os.Remove(mergedPath))
	removeSiblingDerived(t, dir, merged[0])
	for i, name := range inputDBs {
		copyFile(t, stashed[i], filepath.Join(dir, name))
		restoreSiblingDerived(t, stash, dir, name)
	}

	return tmpName
}

// derivedExts are the sibling files a segment carries (bloom filter, count-net-
// additions). They share the segment's base name with these extra suffixes.
var derivedExts = []string{".bloom", ".cna", ".secondary.0.bloom", ".secondary.1.bloom"}

func segmentBase(dbName string) string {
	// strip the trailing ".db"
	return dbName[:len(dbName)-len(".db")]
}

func copySiblingDerived(t *testing.T, dir, stash, dbName string) {
	t.Helper()
	base := segmentBase(dbName)
	for _, ext := range derivedExts {
		src := filepath.Join(dir, base+ext)
		if _, err := os.Stat(src); err == nil {
			copyFile(t, src, filepath.Join(stash, base+ext))
		}
	}
}

func restoreSiblingDerived(t *testing.T, stash, dir, dbName string) {
	t.Helper()
	base := segmentBase(dbName)
	for _, ext := range derivedExts {
		src := filepath.Join(stash, base+ext)
		if _, err := os.Stat(src); err == nil {
			copyFile(t, src, filepath.Join(dir, base+ext))
		}
	}
}

func removeSiblingDerived(t *testing.T, dir, dbName string) {
	t.Helper()
	base := segmentBase(dbName)
	for _, ext := range derivedExts {
		p := filepath.Join(dir, base+ext)
		if _, err := os.Stat(p); err == nil {
			require.NoError(t, os.Remove(p))
		}
	}
}

// compile-time anchor so the helper file stays coupled to the cyclemanager noop
// the bucket constructor uses (keeps imports honest if a refactor drops a call).
var _ = cyclemanager.NewCallbackGroupNoop

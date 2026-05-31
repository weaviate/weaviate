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
	"path/filepath"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

// T6 - crash recovery + WAL replay for the V2 property-length format.
//
// These tests drive the REAL Bucket pipeline (flush + WAL + compaction + reopen)
// in-process: crash-mid-flush and crash-mid-compaction are LSM/WAL-level concerns
// that do NOT need a container. A "crash" is modeled faithfully, never faked:
//
//   - mid-flush  : write through MapSet -> WriteWAL (durable commit log) -> ABANDON
//     the bucket with NO clean Shutdown/FlushAndSwitch (the active memtable was
//     never flushed to a .db; only the .wal exists -- the exact post-crash state)
//     -> open a NEW bucket on the same dir. mayRecoverFromCommitLogs replays the
//     WAL into a fresh memtable and flushes it.
//   - mid-compaction : the compactor writes the merged segment into a .db.tmp,
//     then switchOnDisk renames .db.tmp -> .db. The crash window is between the
//     tmp-write and the rename. newSegmentGroup's init handles both survivor
//     states on reopen (both inputs present -> drop the .tmp, keep inputs; left
//     input already deleted -> finish the rename to the V2 output).
//
// Every assertion reads back the RELOADED on-disk segment (a freshly opened bucket
// or a fresh loadPropertyLengths*), never an in-memory copy from the pre-crash
// bucket, so the test proves the bytes on disk are intact -- not that the writer's
// RAM was correct.

// newInvertedBucket opens an inverted bucket on dir with noop cycle managers (no
// background auto-flush / auto-compaction, so the test controls every flush and
// compaction explicitly). writeV2 selects the V2 flat-column write path.
func newInvertedBucket(t *testing.T, ctx context.Context, dir string, logger logrus.FieldLogger, writeV2 bool) *Bucket {
	t.Helper()
	opts := []BucketOption{WithStrategy(StrategyInverted)}
	if writeV2 {
		opts = append(opts, WithWriteInvertedSegmentV2(true))
	}
	b, err := NewBucketCreator().NewBucket(ctx, dir, dir, logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), opts...)
	require.NoError(t, err)
	b.SetMemtableThreshold(1e9) // never auto-flush mid-segment
	return b
}

// simulateCrash models a process crash: the in-memory bucket state (including the
// GlobalBucketRegistry entry, which a real process restart wipes) is discarded
// WITHOUT a clean Shutdown -- no flush, no commit-log close. The on-disk .wal (and
// any already-flushed .db) is left exactly as the crashed process left it. The
// registry Remove is the only in-memory teardown a restart would perform for us;
// everything else (the unflushed memtable) is intentionally LOST from RAM and must
// be reconstructed from the WAL on reopen.
func simulateCrash(dir string) {
	GlobalBucketRegistry.Remove(dir)
}

// residentSegmentPropLengths reads every (docID,length) pair back from a resident
// segment via the production reader, dispatching on the segment's own format (V2
// flat column or V0 gob), and returns a docID->length map. It also returns the
// persisted avg scalar + count read from the section's 24-byte prefix. This reads
// the on-disk section the real pipeline produced, through a FRESH load, so it
// reflects reloaded bytes and not any pre-crash in-memory state.
func residentSegmentPropLengths(t *testing.T, seg *segment) (map[uint64]uint32, float64, uint64) {
	t.Helper()
	out := make(map[uint64]uint32)
	if seg.isPropLengthV2() {
		require.NoError(t, seg.loadPropertyLengthsV2())
		for i := uint64(0); i < seg.invertedData.propLengthV2Count; i++ {
			d, err := seg.readDocIDV2(i, nil)
			require.NoError(t, err)
			l, err := seg.readLenV2(i, nil)
			require.NoError(t, err)
			out[d] = l
		}
	} else {
		require.NoError(t, seg.loadPropertyLengths())
		run, err := seg.loadPropertyLengthsV0Lossless()
		require.NoError(t, err)
		for i, d := range run.docIDs {
			out[d] = run.lengths[i]
		}
	}
	return out, seg.invertedData.avgPropertyLengthsAvg, seg.invertedData.avgPropertyLengthsCount
}

// bucketResidentSegments returns the bucket's resident disk segments as concrete
// *segment values for direct on-disk reads.
func bucketResidentSegments(t *testing.T, b *Bucket) []*segment {
	t.Helper()
	segs := make([]*segment, 0, len(b.disk.segments))
	for i := range b.disk.segments {
		seg, ok := b.disk.segments[i].(*segment)
		require.True(t, ok)
		segs = append(segs, seg)
	}
	return segs
}

// readAllPropLengthsAcrossSegments merges the per-doc lengths the score path would
// see across every resident segment, NEWER-wins (later segment overrides earlier),
// matching the segment-list scan order. Used to assert the post-recovery view of
// every committed doc regardless of how many segments recovery produced.
func readAllPropLengthsAcrossSegments(t *testing.T, b *Bucket) map[uint64]uint32 {
	t.Helper()
	merged := make(map[uint64]uint32)
	for _, seg := range bucketResidentSegments(t, b) {
		lengths, _, _ := residentSegmentPropLengths(t, seg)
		for d, l := range lengths {
			merged[d] = l // later segments are newer
		}
	}
	return merged
}

// scorePropLength reads the BM25-relevant per-doc length (the substrate of the
// exact BM25 tf) for docID off a resident segment via the production score
// dispatch (propLengthForScore). A stateless cursor (start 0) exercises the full
// gallop/binary-search on V2 and the resident slice on V0.
func scorePropLength(seg *segment, docID uint64) uint32 {
	var cur uint64
	return seg.propLengthForScore(docID, &cur)
}

// TestCrashRecoveryV2_S13_MidFlush_WALReplay is the S13 crash-recovery test. It
// commits docs through a real inverted Bucket, makes the commit log durable
// (WriteWAL), then ABANDONS the bucket WITHOUT a clean Shutdown -- the active
// memtable is never flushed, so on disk there is only a .wal and (in the mixed
// case) one already-flushed .db segment. A NEW bucket opened on the same dir runs
// mayRecoverFromCommitLogs, replaying the WAL into a fresh memtable and flushing
// it. The test asserts every committed doc's property length, the avg scalar, and
// the per-doc BM25 score-length survive the crash, read back from the RELOADED
// on-disk segment(s).
//
// This test catches a recovery path that loses lengths or mis-populates the avg
// scalar after a WAL replay -- e.g. a recovered V2 flush that fails to write the
// flat column, or that writes a section whose avg prefix the V2 reader cannot
// read back (G8) -- because the post-reopen read goes through the SAME
// loadPropertyLengthsV2/V0 + propLengthForScore the query path uses and asserts
// the exact pre-crash values incl the >65535 overflow doc. It covers write-new OFF
// (V0) and ON (V2), and a mixed state (one .db already flushed + the rest in WAL).
func TestCrashRecoveryV2_S13_MidFlush_WALReplay(t *testing.T) {
	ctx := context.Background()

	// docs committed before the crash. doc 3 is the >65535 overflow doc; doc 2 is
	// updated (22 -> 999) so a merge-order regression would surface.
	const overflowLen = uint32(200000) // exact in float32 (<2^24)

	cases := []struct {
		name           string
		writeV2        bool
		flushOneFirst  bool // mixed: flush one segment to .db before the crash
		wantV2OnReopen bool
	}{
		{name: "write-new-OFF-V0", writeV2: false, wantV2OnReopen: false},
		{name: "write-new-ON-V2", writeV2: true, wantV2OnReopen: true},
		{name: "write-new-ON-V2-mixed-disk-and-wal", writeV2: true, flushOneFirst: true, wantV2OnReopen: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			logger, _ := test.NewNullLogger()
			dir := t.TempDir()
			key := []byte("the-term")

			b := newInvertedBucket(t, ctx, dir, logger, tc.writeV2)

			if tc.flushOneFirst {
				// Pre-crash durable segment: docs 1 and 3 (incl the overflow doc) get
				// flushed to a real .db. Recovery must combine this with the WAL-only docs.
				require.NoError(t, b.MapSet(key, NewMapPairFromDocIdAndTf(1, 1.0, 11, false)))
				require.NoError(t, b.MapSet(key, NewMapPairFromDocIdAndTf(3, 1.0, float32(overflowLen), false)))
				require.NoError(t, b.FlushAndSwitch())
				// these go into the active memtable + WAL only (the "crash" loses no
				// committed data because WriteWAL makes the log durable below).
				require.NoError(t, b.MapSet(key, NewMapPairFromDocIdAndTf(2, 1.0, 22, false)))
				require.NoError(t, b.MapSet(key, NewMapPairFromDocIdAndTf(2, 1.0, 999, false))) // newer wins
				require.NoError(t, b.MapSet(key, NewMapPairFromDocIdAndTf(4, 1.0, 44, false)))
			} else {
				require.NoError(t, b.MapSet(key, NewMapPairFromDocIdAndTf(1, 1.0, 11, false)))
				require.NoError(t, b.MapSet(key, NewMapPairFromDocIdAndTf(2, 1.0, 22, false)))
				require.NoError(t, b.MapSet(key, NewMapPairFromDocIdAndTf(2, 1.0, 999, false))) // newer wins
				require.NoError(t, b.MapSet(key, NewMapPairFromDocIdAndTf(3, 1.0, float32(overflowLen), false)))
				require.NoError(t, b.MapSet(key, NewMapPairFromDocIdAndTf(4, 1.0, 44, false)))
			}

			// Make the commit log durable, then SIMULATE A CRASH: do NOT Shutdown,
			// do NOT FlushAndSwitch. The active memtable is never flushed; only the
			// .wal (and, in the mixed case, the pre-flushed .db) survive on disk.
			// simulateCrash drops the in-memory registry entry a process restart
			// would wipe, so the reopen below is a genuine restart-on-the-same-dir.
			require.NoError(t, b.WriteWAL())
			simulateCrash(dir)

			// Sanity: the not-yet-flushed docs live ONLY in the WAL -- there is at
			// most one .db (the mixed pre-flush) and at least one .wal on disk.
			dbFiles, walFiles := countDbAndWalFiles(t, dir)
			require.GreaterOrEqual(t, walFiles, 1, "the active memtable's WAL must be on disk (the crash left it unflushed)")
			if tc.flushOneFirst {
				require.Equal(t, 1, dbFiles, "the pre-crash flush produced exactly one .db segment")
			} else {
				require.Equal(t, 0, dbFiles, "nothing was flushed before the crash -- all committed data is in the WAL")
			}

			// REOPEN on the same dir: mayRecoverFromCommitLogs replays the WAL into
			// the active memtable (the LAST .wal is recovered in-memory, not flushed).
			b2 := newInvertedBucket(t, ctx, dir, logger, tc.writeV2)
			t.Cleanup(func() { _ = b2.Shutdown(ctx) })

			// Flush the recovered active memtable to a real .db so the recovered data
			// is asserted ON DISK -- the recovered memtable, when flushed, must
			// produce a correct (V0/V2) section. This is the round-trip the next flush
			// after a crash performs in production.
			require.NoError(t, b2.FlushAndSwitch())

			// The recovered segment(s) must carry the committed lengths, read back
			// from the reloaded on-disk section.
			merged := readAllPropLengthsAcrossSegments(t, b2)
			require.Equal(t, uint32(11), merged[1], "doc 1 recovered")
			require.Equal(t, uint32(999), merged[2], "doc 2 NEWER value (999) recovered, not the stale 22")
			require.Equal(t, overflowLen, merged[3],
				"the >65535 overflow doc survived crash + WAL replay LOSSLESSLY (no re-clamp)")
			require.Equal(t, uint32(44), merged[4], "doc 4 recovered")

			// The recovered flush must honor the write-new flag: a V2-configured
			// recovery must produce V2 section(s); a V0-configured one must not.
			for _, seg := range bucketResidentSegments(t, b2) {
				if tc.wantV2OnReopen {
					require.Equal(t, segmentindex.SegmentInvertedVersionV2, seg.invertedHeader.Version,
						"V2-configured WAL recovery must flush a V2 section")
				} else {
					require.NotEqual(t, segmentindex.SegmentInvertedVersionV2, seg.invertedHeader.Version,
						"V0-configured WAL recovery must flush a V0 section")
				}
			}

			// The avg scalar must survive: finite, non-NaN/Inf, and equal to
			// sum(length)/count over the docs in each recovered section (G8). We
			// check section-level consistency rather than a single global number
			// because recovery may produce more than one segment.
			for _, seg := range bucketResidentSegments(t, b2) {
				lengths, avg, count := residentSegmentPropLengths(t, seg)
				require.False(t, isNaNOrInf(avg), "recovered avg scalar must be finite, got %v", avg)
				if count > 0 {
					var sum uint64
					for _, l := range lengths {
						sum += uint64(l)
					}
					require.InDelta(t, float64(sum)/float64(count), avg, 1e-6,
						"recovered avg scalar must equal sum/count over the section's docs (G8)")
				}
			}

			// The BM25 score path must read the exact recovered length per doc.
			// Score-read each doc off the segment that holds it (NEWER-wins for
			// updated doc 2 -> assert via the merged view, then spot-check the
			// score dispatch returns a non-zero length for a present doc).
			scoreSeen := map[uint64]uint32{}
			for _, seg := range bucketResidentSegments(t, b2) {
				lengths, _, _ := residentSegmentPropLengths(t, seg)
				for d := range lengths {
					scoreSeen[d] = scorePropLength(seg, d)
				}
			}
			require.Equal(t, uint32(11), scoreSeen[1], "score-read length for doc 1")
			require.Equal(t, uint32(999), scoreSeen[2], "score-read length for doc 2 (newer)")
			require.Equal(t, overflowLen, scoreSeen[3], "score-read length for the overflow doc is lossless")
			require.Equal(t, uint32(44), scoreSeen[4], "score-read length for doc 4")
		})
	}
}

// TestCrashMidCompactionV2_S14a_RenameDidNotLand simulates a crash AFTER the
// compactor wrote the merged .db.tmp but BEFORE switchOnDisk renamed it -- the
// state where BOTH input segments still exist on disk alongside the leftover
// .db.tmp. newSegmentGroup's init (segment_group.go:238-244) detects both inputs
// present, DROPS the .tmp, and keeps the inputs. The survivor is therefore the
// COMPLETE set of V2 INPUTS -- never a torn/half segment. The test drives a REAL
// flush of two V2 segments, copies the merged compaction output to a leftover
// .db.tmp (the pre-rename crash artifact) while leaving the inputs in place,
// reopens, and asserts the inputs survived intact and the .tmp was cleaned up.
//
// This test catches a recovery path that mounts a pre-rename .db.tmp as a live
// segment (which could expose a half-written or torn section) instead of dropping
// it and keeping the durable inputs: such a regression would change the segment
// count and/or surface the tmp's bytes. It asserts every input length (incl the
// >65535 overflow doc) is intact after the reopen and exactly the two inputs
// remain.
func TestCrashMidCompactionV2_S14a_RenameDidNotLand(t *testing.T) {
	ctx := context.Background()
	logger, _ := test.NewNullLogger()
	dir := t.TempDir()
	key := []byte("the-term")
	const overflowLen = uint32(200000)

	b := newInvertedBucket(t, ctx, dir, logger, true /* writeV2 */)

	// Two flushed V2 segments. Doc 2 is updated in segment 2 (newer wins on merge);
	// doc 3 carries the overflow length.
	require.NoError(t, b.MapSet(key, NewMapPairFromDocIdAndTf(1, 1.0, 11, false)))
	require.NoError(t, b.MapSet(key, NewMapPairFromDocIdAndTf(2, 1.0, 22, false)))
	require.NoError(t, b.MapSet(key, NewMapPairFromDocIdAndTf(3, 1.0, float32(overflowLen), false)))
	require.NoError(t, b.FlushAndSwitch())
	require.NoError(t, b.MapSet(key, NewMapPairFromDocIdAndTf(2, 1.0, 999, false)))
	require.NoError(t, b.MapSet(key, NewMapPairFromDocIdAndTf(4, 1.0, 44, false)))
	require.NoError(t, b.FlushAndSwitch())
	require.GreaterOrEqual(t, len(b.disk.segments), 2)

	// Capture the two input .db filenames BEFORE any compaction so we can prove
	// they survive.
	inputDBs := dbFileNames(t, dir)
	require.Len(t, inputDBs, 2, "two flushed V2 input segments")

	// Shut the bucket down cleanly first so the dir is deregistered and its file
	// handles are released. The clean shutdown only flushes the empty active
	// memtable; it does NOT touch the two input .db files.
	require.NoError(t, b.Shutdown(ctx))

	// Produce a REAL merged compaction output, then stage it as a pre-rename crash
	// artifact: a leftover .db.tmp whose name encodes BOTH input IDs, with the two
	// inputs STILL present (the exact state if a crash hit between tmp-write and
	// rename). craftLeftoverCompactionTmp runs the real convert path and restores
	// the inputs, leaving exactly: input1.db + input2.db + leftover.db.tmp.
	tmpName := craftLeftoverCompactionTmp(t, ctx, logger, dir, inputDBs)

	require.FileExists(t, filepath.Join(dir, tmpName), "leftover .db.tmp staged in the live dir")
	for _, in := range inputDBs {
		require.FileExists(t, filepath.Join(dir, in), "input segment must still be present (rename did not land)")
	}

	// REOPEN: newSegmentGroup must drop the .tmp (both inputs present) and keep the
	// two inputs -- the survivor is the complete inputs, never a torn segment.
	b2 := newInvertedBucket(t, ctx, dir, logger, true)
	t.Cleanup(func() { _ = b2.Shutdown(ctx) })

	require.NoFileExists(t, filepath.Join(dir, tmpName),
		"the pre-rename .db.tmp must be dropped on reopen, not mounted as a live segment")
	require.Len(t, b2.disk.segments, 2, "both input segments survive (rename did not land)")

	merged := readAllPropLengthsAcrossSegments(t, b2)
	require.Equal(t, uint32(11), merged[1])
	require.Equal(t, uint32(999), merged[2], "newer value survives across the two inputs")
	require.Equal(t, overflowLen, merged[3], "overflow doc intact in the surviving input (lossless)")
	require.Equal(t, uint32(44), merged[4])
}

// TestCrashMidCompactionV2_S14b_RenameLanded is the complementary survivor state:
// the crash hit AFTER the rename completed, so the COMPLETE V2 OUTPUT is on disk
// and the inputs are gone. The test runs a REAL convert-on-compaction to a single
// V2 segment, reopens the bucket on the same dir (the full recovery read path),
// and asserts the merged V2 output is intact: lossless lengths incl the >65535
// overflow doc, NEWER-wins, a finite avg scalar, and the size cross-check holds
// (the section loads without a torn-section error).
//
// This test catches a converged V2 output that does not survive a clean reopen --
// e.g. a section whose self-describing size does not match its n (loadPropertyLengthsV2
// would reject it on the reopen) or a converged segment that mis-reads after being
// re-mmap'd from disk rather than read from the writer's RAM.
func TestCrashMidCompactionV2_S14b_RenameLanded(t *testing.T) {
	ctx := context.Background()
	logger, _ := test.NewNullLogger()
	dir := t.TempDir()
	key := []byte("the-term")
	const overflowLen = uint32(200000)

	b := newInvertedBucket(t, ctx, dir, logger, true /* writeV2 */)
	require.NoError(t, b.MapSet(key, NewMapPairFromDocIdAndTf(1, 1.0, 11, false)))
	require.NoError(t, b.MapSet(key, NewMapPairFromDocIdAndTf(2, 1.0, 22, false)))
	require.NoError(t, b.MapSet(key, NewMapPairFromDocIdAndTf(3, 1.0, float32(overflowLen), false)))
	require.NoError(t, b.FlushAndSwitch())
	require.NoError(t, b.MapSet(key, NewMapPairFromDocIdAndTf(2, 1.0, 999, false)))
	require.NoError(t, b.MapSet(key, NewMapPairFromDocIdAndTf(4, 1.0, 44, false)))
	require.NoError(t, b.FlushAndSwitch())

	// Real convert-on-compaction: rename lands, inputs are replaced by one V2 output.
	for {
		compacted, err := b.disk.compactOnce(context.Background())
		require.NoError(t, err)
		if !compacted {
			break
		}
	}
	require.Len(t, b.disk.segments, 1, "compaction converged to a single V2 output")
	require.NoError(t, b.Shutdown(ctx))

	// REOPEN: read the converged V2 output back off disk through the full open path.
	b2 := newInvertedBucket(t, ctx, dir, logger, true)
	t.Cleanup(func() { _ = b2.Shutdown(ctx) })
	require.Len(t, b2.disk.segments, 1, "the single converged V2 output survives reopen")

	seg := bucketResidentSegments(t, b2)[0]
	require.Equal(t, segmentindex.SegmentInvertedVersionV2, seg.invertedHeader.Version)

	// loadPropertyLengthsV2 runs the size cross-check; a torn section would error here.
	lengths, avg, count := residentSegmentPropLengths(t, seg)
	require.Equal(t, uint32(11), lengths[1])
	require.Equal(t, uint32(999), lengths[2], "NEWER value survived the convert + reopen")
	require.Equal(t, overflowLen, lengths[3], "overflow doc lossless through convert + reopen")
	require.Equal(t, uint32(44), lengths[4])
	require.False(t, isNaNOrInf(avg), "converged avg scalar must be finite")
	require.Len(t, lengths, 4, "four DISTINCT docs (1,2,3,4) in the converged section")
	// count is the cumulative propLength COUNT carried in the prefix (the BM25
	// denominator's running total), not the distinct-doc count: doc 2 was written
	// in both flushed segments, so the combined count is 5, not 4. Assert it is
	// positive and finite-consistent with the persisted avg rather than pinning a
	// distinct-doc number the field does not represent.
	require.Greater(t, count, uint64(0), "converged count scalar must be positive")

	// score path reads the exact lengths off the reloaded V2 segment.
	require.Equal(t, uint32(999), scorePropLength(seg, 2))
	require.Equal(t, overflowLen, scorePropLength(seg, 3))
}

// TestCrashMidCompactionV2_S14c_TornSectionRejectedOnOpen asserts that a TORN V2
// property-length section -- the failure a crash could leave if a short write
// truncated the columns under a written n, or a mis-decode produced a bogus n --
// is REJECTED LOUDLY by the T1/M1 size cross-check on open, never silently served
// as a half segment. It produces a real V2 compacted segment, then corrupts its
// proplen section two ways (truncate the body below the declared size, and bump n
// past EOF) and asserts loadPropertyLengthsV2 returns the "torn V2 property-length
// section" error each time.
//
// RED-first mechanism: on a tree WITHOUT the size cross-check + remaining-bytes
// bound (the T1/M1 guard), loadPropertyLengthsV2 would compute column offsets from
// the bogus n and read OUT OF BOUNDS -- panicking on mmap or returning garbage
// lengths -- rather than returning a clean error. The guard converts that into a
// loud, greppable rejection. This test passes a section the guard MUST reject and
// fails (no error / panic) on an unguarded tree.
//
// This catches the crash-mid-compaction torn-output hazard the synthesis flags
// (subsystem-impact §7 #2): a short write that truncates a column under a written
// n must be rejected on next open, not mis-merged or mis-scored.
func TestCrashMidCompactionV2_S14c_TornSectionRejectedOnOpen(t *testing.T) {
	docIDs := []uint64{1, 2, 3, 4}
	lengths := []uint32{11, 999, 200000, 44}

	t.Run("body-truncated-below-declared-size", func(t *testing.T) {
		// A well-formed section, then physically truncate the trailing length column
		// so the body is SHORTER than the declared size (8 + n*12). The
		// remaining-bytes bound rejects it because n declares more pairs than fit.
		section := buildV2Section(12.5, uint64(len(docIDs)), docIDs, lengths, 0)
		// drop the last 6 bytes (part of the final length entries) -- the section now
		// claims n=4 pairs but the bytes for them no longer all fit.
		torn := section[:len(section)-6]
		seg := newV2TestSegmentMmapTorn(t, torn)
		err := seg.loadPropertyLengthsV2()
		require.Error(t, err, "a body truncated below the declared n must be rejected on open")
		require.Contains(t, err.Error(), "torn V2 property-length section")
	})

	t.Run("n-bumped-past-EOF", func(t *testing.T) {
		// Forge a section whose declared size matches a HUGE n, so the size
		// cross-check alone would pass but the n points far past the file end. The
		// remaining-bytes bound rejects it.
		n := uint64(len(docIDs))
		section := buildV2Section(12.5, n, docIDs, lengths, 0)
		// overwrite the n field (immediately after the 24-byte prefix) with a value
		// that points past EOF, and set size to the matching 8 + n*12 so the size
		// cross-check is satisfied and only the remaining-bytes bound can catch it.
		hugeN := uint64(1 << 40)
		writeUint64(section, propLengthV2PrefixSize, hugeN)
		writeUint64(section, 16, propLengthV2NField+hugeN*(propLengthV2DocIDWidth+propLengthV2LenWidth)) // size field
		seg := newV2TestSegmentMmapTorn(t, section)
		err := seg.loadPropertyLengthsV2()
		require.Error(t, err, "an n pointing past EOF must be rejected on open")
		require.Contains(t, err.Error(), "torn V2 property-length section")
	})
}

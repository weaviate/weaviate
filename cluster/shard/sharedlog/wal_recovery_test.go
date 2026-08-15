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

package sharedlog

import (
	"context"
	"encoding/binary"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"
)

// newWALTestStore opens a Store on dir with a test-sized segment cap
// (0 = default). Crash-safety tests reopen the same dir to exercise replay.
func newWALTestStore(t *testing.T, dir string, segMax int64) *Store {
	t.Helper()
	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)
	s, err := Open(Options{Path: dir, Logger: log, SegmentMaxBytes: segMax})
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func walSegmentPaths(t *testing.T, dir string) []string {
	t.Helper()
	paths, err := filepath.Glob(filepath.Join(dir, "*.wal"))
	require.NoError(t, err)
	sort.Strings(paths)
	return paths
}

func fileSize(t *testing.T, path string) int64 {
	t.Helper()
	st, err := os.Stat(path)
	require.NoError(t, err)
	return st.Size()
}

func flipByteAt(t *testing.T, path string, off int64) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	require.NoError(t, err)
	defer f.Close()
	b := make([]byte, 1)
	_, err = f.ReadAt(b, off)
	require.NoError(t, err)
	b[0] ^= 0xFF
	_, err = f.WriteAt(b, off)
	require.NoError(t, err)
}

func truncateTo(t *testing.T, path string, size int64) {
	t.Helper()
	require.NoError(t, os.Truncate(path, size))
}

// TestWAL_TornTailRecovery pins standard WAL crash recovery: a record torn
// by a crash mid-write (short frame, corrupt checksum, zero-filled or
// garbage tail) is truncated away on reopen, everything acknowledged before
// it survives, and the log accepts appends at the truncated position.
func TestWAL_TornTailRecovery(t *testing.T) {
	cases := []struct {
		name    string
		corrupt func(t *testing.T, path string, pre, post int64)
	}{
		{"truncate_mid_record_header", func(t *testing.T, path string, pre, _ int64) {
			truncateTo(t, path, pre+4)
		}},
		{"truncate_mid_record_body", func(t *testing.T, path string, _, post int64) {
			truncateTo(t, path, post-3)
		}},
		{"corrupt_record_crc", func(t *testing.T, path string, pre, _ int64) {
			flipByteAt(t, path, pre+recHeaderSize+2)
		}},
		{"zero_filled_tail", func(t *testing.T, path string, pre, _ int64) {
			truncateTo(t, path, pre)
			f, err := os.OpenFile(path, os.O_RDWR, 0)
			require.NoError(t, err)
			defer f.Close()
			_, err = f.WriteAt(make([]byte, 64), pre)
			require.NoError(t, err)
		}},
		{"length_field_overrun", func(t *testing.T, path string, pre, _ int64) {
			f, err := os.OpenFile(path, os.O_RDWR, 0)
			require.NoError(t, err)
			defer f.Close()
			var huge [4]byte
			binary.LittleEndian.PutUint32(huge[:], 0xFFFFFF00)
			_, err = f.WriteAt(huge[:], pre)
			require.NoError(t, err)
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			ctx := context.Background()

			s := newWALTestStore(t, dir, 0)
			require.NoError(t, s.Append(ctx, GroupWrite{
				GroupID:   7,
				Entries:   []raftpb.Entry{mkEntry(1, 1, "e1"), mkEntry(2, 1, "e2")},
				HardState: &raftpb.HardState{Term: 1, Commit: 2},
			}))
			segPath := walSegmentPaths(t, dir)[0]
			pre := fileSize(t, segPath)

			require.NoError(t, s.Append(ctx, GroupWrite{
				GroupID:   7,
				Entries:   []raftpb.Entry{mkEntry(3, 1, "e3")},
				HardState: &raftpb.HardState{Term: 1, Commit: 3},
			}))
			post := fileSize(t, segPath)
			require.Greater(t, post, pre)
			require.NoError(t, s.Close())

			tc.corrupt(t, segPath, pre, post)

			s2 := newWALTestStore(t, dir, 0)
			g := s2.Storage(7)
			li, err := g.LastIndex()
			require.NoError(t, err)
			assert.Equal(t, uint64(2), li, "the torn write must be gone, the acked one intact")
			hs, _, err := g.InitialState()
			require.NoError(t, err)
			assert.Equal(t, uint64(2), hs.Commit, "the torn write's HardState must be gone")
			ents, err := g.Entries(1, 3, 4096)
			require.NoError(t, err)
			require.Len(t, ents, 2)
			assert.Equal(t, []byte("e1"), ents[0].Data)
			assert.Equal(t, []byte("e2"), ents[1].Data)

			// The log must accept appends at the truncated position…
			require.NoError(t, s2.Append(ctx, GroupWrite{
				GroupID:   7,
				Entries:   []raftpb.Entry{mkEntry(3, 2, "recovered")},
				HardState: &raftpb.HardState{Term: 2, Commit: 3},
			}))
			require.NoError(t, s2.Close())

			// …and those appends must survive another replay.
			s3 := newWALTestStore(t, dir, 0)
			g = s3.Storage(7)
			li, err = g.LastIndex()
			require.NoError(t, err)
			assert.Equal(t, uint64(3), li)
			term, err := g.Term(3)
			require.NoError(t, err)
			assert.Equal(t, uint64(2), term)
		})
	}
}

// TestWAL_SnapshotWriteAtomicity pins the composite-record atomicity that
// replaces bbolt's transaction atomicity for the one shape where it is
// safety-critical: a snapshot-install write (snapshot + HardState in one
// GroupWrite). A tear inside the record must drop snapshot and HardState
// TOGETHER — persisting either without the other leaves a state etcd/raft
// panics on at restart (commit outside [snapshot.Index, LastIndex]).
func TestWAL_SnapshotWriteAtomicity(t *testing.T) {
	cases := []struct {
		name string
		tear bool
	}{
		{"torn_composite_drops_both", true},
		{"intact_composite_applies_both", false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			ctx := context.Background()

			s := newWALTestStore(t, dir, 0)
			require.NoError(t, s.Append(ctx, GroupWrite{
				GroupID: 1,
				Entries: []raftpb.Entry{
					mkEntry(1, 1, "a"), mkEntry(2, 1, "b"), mkEntry(3, 1, "c"),
					mkEntry(4, 1, "d"), mkEntry(5, 1, "e"),
				},
				HardState: &raftpb.HardState{Term: 1, Commit: 5},
			}))
			segPath := walSegmentPaths(t, dir)[0]

			// The snapshot-install shape: snapshot and covering HardState in
			// ONE GroupWrite = one composite record.
			require.NoError(t, s.Append(ctx, GroupWrite{
				GroupID: 1,
				Snapshot: &raftpb.Snapshot{
					Data: []byte("snap"),
					Metadata: raftpb.SnapshotMetadata{
						Index:     20,
						Term:      2,
						ConfState: raftpb.ConfState{Voters: []uint64{1, 2, 3}},
					},
				},
				HardState: &raftpb.HardState{Term: 2, Commit: 20},
			}))
			post := fileSize(t, segPath)
			require.NoError(t, s.Close())

			if tc.tear {
				truncateTo(t, segPath, post-3)
			}

			s2 := newWALTestStore(t, dir, 0)
			g := s2.Storage(1)
			fi, err := g.FirstIndex()
			require.NoError(t, err)
			li, err := g.LastIndex()
			require.NoError(t, err)
			hs, cs, err := g.InitialState()
			require.NoError(t, err)

			if tc.tear {
				assert.Equal(t, uint64(1), fi, "torn install: snapshot must be absent")
				assert.Equal(t, uint64(5), li)
				assert.Equal(t, uint64(5), hs.Commit, "torn install: HardState must have torn off with the snapshot")
			} else {
				assert.Equal(t, uint64(21), fi)
				assert.Equal(t, uint64(20), li)
				assert.Equal(t, uint64(20), hs.Commit)
				assert.Equal(t, []uint64{1, 2, 3}, cs.Voters)
			}
			// The etcd restart invariant either way: commit within
			// [FirstIndex-1, LastIndex].
			assert.GreaterOrEqual(t, hs.Commit, fi-1)
			assert.LessOrEqual(t, hs.Commit, li)
		})
	}
}

// TestWAL_CorruptMiddleSegmentFailsOpen: torn-tail truncation is only legal
// in the highest-numbered segment (nothing in it was acknowledged). The same
// damage in an older segment is corruption of acknowledged data — Open must
// refuse rather than silently truncate.
func TestWAL_CorruptMiddleSegmentFailsOpen(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	s := newWALTestStore(t, dir, 512)
	// Few enough appends to stay under the rewrite threshold — the oldest
	// segment must still be there to corrupt.
	for i := uint64(1); i <= 6; i++ {
		require.NoError(t, s.Append(ctx, GroupWrite{
			GroupID:   1,
			Entries:   []raftpb.Entry{mkEntry(i, 1, strings.Repeat("x", 200))},
			HardState: &raftpb.HardState{Term: 1, Commit: i},
		}))
	}
	require.NoError(t, s.Close())

	paths := walSegmentPaths(t, dir)
	require.GreaterOrEqual(t, len(paths), 3, "test needs multiple segments")

	flipByteAt(t, paths[0], segHeaderSize+recHeaderSize+2)

	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)
	_, err := Open(Options{Path: dir, Logger: log, SegmentMaxBytes: 512})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "refusing to open")
}

// TestWAL_UncoveredSnapshotWarning pins the defensive guard on the one write
// shape composite-record atomicity cannot protect: a bare snapshot above the
// group's durable commit relies on a commit persisted in a DIFFERENT record,
// which a torn tail could strand. The guard warns (it cannot error: bare
// snapshots at or below the commit are the legitimate local-snapshot-persist
// shape, and the spec suite writes bare snapshots freely).
func TestWAL_UncoveredSnapshotWarning(t *testing.T) {
	logger, hook := logrustest.NewNullLogger()
	logger.SetLevel(logrus.WarnLevel)
	s, err := Open(Options{Path: filepath.Join(t.TempDir(), "wal"), Logger: logger})
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	ctx := context.Background()

	uncoveredWarned := func() bool {
		for _, e := range hook.AllEntries() {
			if e.Level == logrus.WarnLevel && strings.Contains(e.Message, "without a HardState") {
				return true
			}
		}
		return false
	}

	// Bare snapshot above the durable commit (0): warn.
	require.NoError(t, s.Append(ctx, GroupWrite{
		GroupID:  9,
		Snapshot: &raftpb.Snapshot{Metadata: raftpb.SnapshotMetadata{Index: 20, Term: 1}},
	}))
	assert.True(t, uncoveredWarned(), "bare snapshot above the durable commit must warn")
	hook.Reset()

	// Snapshot with its HardState in the same GroupWrite: covered, no warn.
	require.NoError(t, s.Append(ctx, GroupWrite{
		GroupID:   9,
		Snapshot:  &raftpb.Snapshot{Metadata: raftpb.SnapshotMetadata{Index: 30, Term: 1}},
		HardState: &raftpb.HardState{Term: 1, Commit: 30},
	}))
	assert.False(t, uncoveredWarned())

	// Bare snapshot at or below the durable commit — the local
	// snapshot-persist shape: no warn.
	require.NoError(t, s.Append(ctx, GroupWrite{
		GroupID:  9,
		Snapshot: &raftpb.Snapshot{Metadata: raftpb.SnapshotMetadata{Index: 30, Term: 1}},
	}))
	assert.False(t, uncoveredWarned())
}

// TestOpen_PathIsRegularFile: a regular file at the WAL path (a legacy bbolt
// shared log, or a misconfiguration) must fail loudly, not be overwritten.
func TestOpen_PathIsRegularFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "shard-raft-log")
	require.NoError(t, os.WriteFile(path, []byte("not a directory"), 0o600))

	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)
	_, err := Open(Options{Path: path, Logger: log})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "regular file")
}

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
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

// TestWAL_BootValidation_PoisonsSplitBrainGroup builds the "Constructor C"
// state of minor-issues.md #9 deterministically and proves boot validation
// quarantines exactly the damaged group:
//
//  1. a victim group fills several segments with entries, then persists a
//     snapshot covering them — the eager compaction makes the early segments
//     fully dead and reclamation DELETES them (all legitimate, all fsynced);
//  2. the tail segment is then damaged so the snapshot record (and everything
//     after it) is lost, while the segment deletions it authorized survive —
//     the replay-divergence a hard kill plus torn-page/fs-recovery damage
//     produces;
//  3. on reopen the rebuilt index holds entries starting above 1 with no
//     covering snapshot: pre-W-A this state panicked the group's leader
//     ("need non-empty snapshot") on the first lagging-follower catch-up.
//
// The WAL must open, poison the victim group (its Store refuses to start),
// and leave the healthy group fully servable — per-group blast radius, not
// node-wide.
func TestWAL_BootValidation_PoisonsSplitBrainGroup(t *testing.T) {
	dir := t.TempDir()
	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)
	ctx := context.Background()

	s, err := Open(Options{Path: dir, Logger: log, SegmentMaxBytes: 4 << 10, BatchMaxWait: 50 * time.Microsecond})
	require.NoError(t, err)

	const victim, healthy = uint64(1), uint64(2)

	// Fill several segments with victim entries (the "early" history).
	next := uint64(1)
	appendVictim := func(n int) {
		ents := make([]raftpb.Entry, n)
		for i := range ents {
			ents[i] = raftpb.Entry{Index: next + uint64(i), Term: 1, Data: make([]byte, 200)}
		}
		next += uint64(n)
		require.NoError(t, s.Append(ctx, GroupWrite{
			GroupID: victim, Entries: ents,
			HardState: &raftpb.HardState{Term: 1, Commit: next - 1},
		}))
	}
	for len(s.w.segs) < 4 {
		appendVictim(4)
	}
	firstTailSeq := s.w.segs[len(s.w.segs)-1].seq
	// A few victim entries into the current tail so the post-damage index has
	// a surviving entry range that starts well above 1.
	appendVictim(4)
	// The healthy group's state, also in the tail: entries + a proper
	// snapshot, exercising the snapshot-bearing validation path.
	require.NoError(t, s.Append(ctx, GroupWrite{
		GroupID:   healthy,
		Entries:   []raftpb.Entry{{Index: 1, Term: 3, Data: []byte("h1")}, {Index: 2, Term: 3, Data: []byte("h2")}},
		HardState: &raftpb.HardState{Term: 3, Commit: 2},
	}))
	require.NoError(t, s.Append(ctx, GroupWrite{
		GroupID: healthy,
		Snapshot: &raftpb.Snapshot{Data: []byte("hs"), Metadata: raftpb.SnapshotMetadata{
			Index: 1, Term: 3, ConfState: raftpb.ConfState{Voters: []uint64{1, 2}},
		}},
	}))

	tail := s.w.segs[len(s.w.segs)-1]
	require.Equal(t, firstTailSeq, tail.seq, "harness: tail rotated mid-build; loosen the sizes")
	cut := tail.size
	tailPath := tail.path

	// Victim snapshot covering its whole log: eager compaction + reclamation
	// delete the early segments — the effect that must not outlive the
	// record's durability.
	require.NoError(t, s.Append(ctx, GroupWrite{
		GroupID: victim,
		Snapshot: &raftpb.Snapshot{Data: []byte("vs"), Metadata: raftpb.SnapshotMetadata{
			Index: next - 1, Term: 1, ConfState: raftpb.ConfState{Voters: []uint64{1, 2}},
		}},
	}))
	require.NoError(t, s.Compact(victim, next)) // production order (persistLocalSnapshot)
	require.Greater(t, s.w.segs[0].seq, uint64(1), "harness: early segments were not reclaimed")
	require.NoError(t, s.Close())

	// The damage: lose the tail from just before the snapshot record.
	require.NoError(t, os.Truncate(tailPath, cut))

	s2, err := Open(Options{Path: dir, Logger: log, SegmentMaxBytes: 4 << 10})
	require.NoError(t, err, "boot validation must quarantine, not fail the whole WAL")
	defer s2.Close()

	// The rebuilt victim index IS the split-brain the live panic required.
	vst := s2.Storage(victim)
	fi, _ := vst.FirstIndex()
	li, _ := vst.LastIndex()
	require.Equal(t, uint64(1), fi, "no snapshot survived")
	require.Greater(t, li, uint64(1))
	_, entErr := vst.Entries(fi, li+1, 1<<20)
	require.Error(t, entErr, "the visible range is not backed by entries — the pre-W-A panic state")

	// The victim is poisoned, with the mechanism named.
	reason, poisoned := s2.PoisonedReason(victim)
	require.True(t, poisoned, "split-brain group must be quarantined by boot validation")
	require.Contains(t, reason, "no snapshot")
	require.Equal(t, 1, s2.PoisonedGroupCount())

	// The healthy group is untouched: not poisoned, snapshot servable,
	// entries clean.
	_, hp := s2.PoisonedReason(healthy)
	require.False(t, hp)
	hst := s2.Storage(healthy)
	hsnap, err := hst.Snapshot()
	require.NoError(t, err)
	require.Equal(t, uint64(1), hsnap.Metadata.Index)
	hfi, _ := hst.FirstIndex()
	hli, _ := hst.LastIndex()
	hents, err := hst.Entries(hfi, hli+1, 1<<20)
	require.NoError(t, err)
	require.Len(t, hents, 1)

	// A tombstone is the sanctioned exit from quarantine (the drop path).
	require.NoError(t, s2.DeleteGroup(victim))
	_, poisoned = s2.PoisonedReason(victim)
	require.False(t, poisoned)
	require.Zero(t, s2.PoisonedGroupCount())
}

// TestWAL_CrashReplay_ServabilityInvariant is the promoted crash-replay
// property test: random production-shaped histories (appends, HardStates,
// snapshot-then-compact in persistLocalSnapshot's order) are damaged by
// truncating the tail segment at an arbitrary byte — a superset of the pure
// fsync-tear crash model that includes torn-page/fs-recovery damage of
// acknowledged bytes — and reopened. Every outcome must be classified: the
// rebuilt group either upholds the full servability invariant (visible range
// backed by entries, any compaction backed by a readable, abutting snapshot)
// or is poisoned. A silently-broken servable state is the bug (pre-fix, the
// exploration probe hit one on its first cycle).
func TestWAL_CrashReplay_ServabilityInvariant(t *testing.T) {
	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)
	ctx := context.Background()

	for cycle := 0; cycle < 30; cycle++ {
		t.Run(fmt.Sprintf("cycle-%d", cycle), func(t *testing.T) {
			rng := rand.New(rand.NewSource(int64(cycle) * 104729))
			dir := t.TempDir()
			s, err := Open(Options{Path: dir, Logger: log, SegmentMaxBytes: 4 << 10, BatchMaxWait: 50 * time.Microsecond})
			require.NoError(t, err)

			next, lastSnap := uint64(1), uint64(0)
			for i := 0; i < 40+rng.Intn(60); i++ {
				n := 1 + rng.Intn(3)
				ents := make([]raftpb.Entry, n)
				for j := range ents {
					ents[j] = raftpb.Entry{Index: next + uint64(j), Term: 1, Data: make([]byte, 32+rng.Intn(128))}
				}
				next += uint64(n)
				commit := next - 1
				require.NoError(t, s.Append(ctx, GroupWrite{GroupID: 1, Entries: ents, HardState: &raftpb.HardState{Term: 1, Commit: commit}}))
				if commit-lastSnap >= 15 {
					idx := commit - uint64(rng.Intn(4))
					if idx > lastSnap {
						require.NoError(t, s.Append(ctx, GroupWrite{GroupID: 1, Snapshot: &raftpb.Snapshot{
							Data:     []byte("meta"),
							Metadata: raftpb.SnapshotMetadata{Index: idx, Term: 1, ConfState: raftpb.ConfState{Voters: []uint64{1, 2}}},
						}}))
						require.NoError(t, s.Compact(1, idx+1))
						lastSnap = idx
					}
				}
			}
			require.NoError(t, s.Close())

			segs, err := os.ReadDir(dir)
			require.NoError(t, err)
			tailPath := filepath.Join(dir, segs[len(segs)-1].Name())
			fi, err := os.Stat(tailPath)
			require.NoError(t, err)
			if fi.Size() > segHeaderSize {
				cut := segHeaderSize + rng.Int63n(fi.Size()-segHeaderSize+1)
				require.NoError(t, os.Truncate(tailPath, cut))
			}

			s2, err := Open(Options{Path: dir, Logger: log, SegmentMaxBytes: 4 << 10})
			require.NoError(t, err, "tail damage must never fail the open")
			defer s2.Close()

			if _, poisoned := s2.PoisonedReason(1); poisoned {
				return // damage detected and quarantined — classified outcome
			}
			st := s2.Storage(1)
			fIdx, _ := st.FirstIndex()
			lIdx, _ := st.LastIndex()
			if fIdx > 1 {
				snap, serr := st.Snapshot()
				require.NoError(t, serr, "FirstIndex=%d but snapshot unservable", fIdx)
				require.False(t, raft.IsEmptySnap(snap))
				require.Equal(t, fIdx-1, snap.Metadata.Index)
			}
			if fIdx <= lIdx {
				_, eerr := st.Entries(fIdx, lIdx+1, 1<<20)
				require.NoError(t, eerr, "Entries(%d,%d) on a non-poisoned rebuilt index", fIdx, lIdx+1)
				for i := fIdx; i <= lIdx; i++ {
					_, terr := st.Term(i)
					require.NoError(t, terr, "Term(%d) on a non-poisoned rebuilt index", i)
				}
			}
		})
	}
}

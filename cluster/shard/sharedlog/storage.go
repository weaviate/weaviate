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
	"fmt"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

// groupStorage mirrors etcd's MemoryStorage semantics over the WAL index:
//
//   - Empty group: FirstIndex=1, LastIndex=0, Term(0)=0.
//   - Term(snapshot.Index) returns snapshot.Term even though the entry
//     itself has been compacted out.
//   - Out of range: ErrCompacted below FirstIndex, ErrUnavailable above
//     LastIndex.
//
// Every method except Entries and Snapshot answers from RAM; those two
// pread payloads from segment files while holding the index read-lock,
// which excludes segment reclamation for the duration of the read.
type groupStorage struct {
	store   *Store
	groupID uint64
}

// snapBounds returns the group's snapshot metadata index/term (zero when
// none is persisted). Caller must hold indexMu (read).
func snapBounds(g *groupState) (idx, term uint64) {
	if g != nil && g.hasSnap {
		return g.snapMeta.Index, g.snapMeta.Term
	}
	return 0, 0
}

// lastLocked returns the group's LastIndex: the entry tail, or the snapshot
// index when no entries are retained. Caller must hold indexMu (read).
func lastLocked(g *groupState) uint64 {
	snapIdx, _ := snapBounds(g)
	if g == nil || len(g.ents) == 0 {
		return snapIdx
	}
	if l := g.last(); l > snapIdx {
		return l
	}
	return snapIdx
}

func (g *groupStorage) InitialState() (raftpb.HardState, raftpb.ConfState, error) {
	w := g.store.w
	w.indexMu.RLock()
	defer w.indexMu.RUnlock()
	gs := w.groups[g.groupID]
	if gs == nil {
		return raftpb.HardState{}, raftpb.ConfState{}, nil
	}
	hs := gs.hs // zero value when never persisted
	if gs.hasCS {
		return hs, gs.cs, nil
	}
	// Mirror etcd MemoryStorage semantics: with no explicitly-persisted
	// ConfState record (the Store never writes one), the membership as of
	// the last persisted snapshot is authoritative. Without this fallback
	// a group restarting from a compacted log — the bootstrap conf-change
	// entries truncated away — comes back with zero voters and can never
	// elect a leader again.
	if gs.hasSnap {
		return hs, gs.snapMeta.ConfState, nil
	}
	return hs, raftpb.ConfState{}, nil
}

func (g *groupStorage) FirstIndex() (uint64, error) {
	w := g.store.w
	w.indexMu.RLock()
	defer w.indexMu.RUnlock()
	snapIdx, _ := snapBounds(w.groups[g.groupID])
	return snapIdx + 1, nil
}

func (g *groupStorage) LastIndex() (uint64, error) {
	w := g.store.w
	w.indexMu.RLock()
	defer w.indexMu.RUnlock()
	return lastLocked(w.groups[g.groupID]), nil
}

func (g *groupStorage) Term(i uint64) (uint64, error) {
	w := g.store.w
	w.indexMu.RLock()
	defer w.indexMu.RUnlock()
	gs := w.groups[g.groupID]
	snapIdx, snapTerm := snapBounds(gs)
	if i == snapIdx {
		return snapTerm, nil
	}
	if i < snapIdx+1 {
		return 0, raft.ErrCompacted
	}
	if i > lastLocked(gs) {
		return 0, raft.ErrUnavailable
	}
	// In range above the snapshot: the entry must be retained (the index
	// keeps [base..last] contiguous with base above the snapshot floor). A
	// miss here is the runtime split-brain signature of minor-issues.md #9 —
	// entry positions inside the visible range with no retained entry and no
	// covering snapshot — so trip the wire before answering.
	if len(gs.ents) == 0 || i < gs.base {
		w.tripSplitBrain("Term", g.groupID, i, gs)
		return 0, raft.ErrUnavailable
	}
	return gs.ents[i-gs.base].term, nil
}

func (g *groupStorage) Entries(lo, hi, maxSize uint64) ([]raftpb.Entry, error) {
	w := g.store.w
	w.indexMu.RLock()
	defer w.indexMu.RUnlock()
	gs := w.groups[g.groupID]
	snapIdx, _ := snapBounds(gs)
	if lo < snapIdx+1 {
		return nil, raft.ErrCompacted
	}
	if hi > lastLocked(gs)+1 {
		return nil, raft.ErrUnavailable
	}
	if lo >= hi {
		return nil, nil
	}
	if gs == nil || len(gs.ents) == 0 || lo < gs.base {
		// Same split-brain guard as Term: unreachable on a healthy index
		// (the bounds checks above cover every in-contract miss).
		w.tripSplitBrain("Entries", g.groupID, lo, gs)
		return nil, raft.ErrUnavailable
	}

	// Select under the maxSize budget first (loc.n is the marshaled size,
	// identical to raftpb.Entry.Size()), honouring etcd's contract that at
	// least one entry is always returned even if it exceeds maxSize.
	sel := make([]entryLoc, 0, hi-lo)
	var size uint64
	for i := lo; i < hi; i++ {
		l := gs.ents[i-gs.base]
		if len(sel) > 0 && size+uint64(l.n) > maxSize {
			break
		}
		sel = append(sel, l)
		size += uint64(l.n)
	}

	// Read in coalesced runs: entries written in one record are adjacent on
	// disk, separated only by their fixed frame headers.
	ents := make([]raftpb.Entry, len(sel))
	for start := 0; start < len(sel); {
		end := start + 1
		for end < len(sel) &&
			sel[end].seg == sel[start].seg &&
			sel[end].off == sel[end-1].off+int64(sel[end-1].n)+entryFrameSize {
			end++
		}
		run := make([]byte, sel[end-1].off+int64(sel[end-1].n)-sel[start].off)
		if _, err := sel[start].seg.f.ReadAt(run, sel[start].off); err != nil {
			return nil, fmt.Errorf("sharedlog: Entries: read segment %s: %w", sel[start].seg.path, err)
		}
		for j := start; j < end; j++ {
			rel := sel[j].off - sel[start].off
			if err := ents[j].Unmarshal(run[rel : rel+int64(sel[j].n)]); err != nil {
				return nil, fmt.Errorf("sharedlog: Entries: unmarshal entry at index %d: %w", lo+uint64(j), err)
			}
		}
		start = end
	}
	return ents, nil
}

// Snapshot never returns an empty snapshot with a nil error, and never a hard
// error: etcd's maybeSendSnapshot panics on the former ("need non-empty
// snapshot") and on any error other than ErrSnapshotTemporarilyUnavailable —
// both killed the Ready loop live (minor-issues.md #9). "Nothing servable
// right now" — no snapshot persisted, or the payload momentarily unreadable —
// is answered with the bare sentinel (etcd matches it with ==): the leader
// logs, leaves the follower probing, and retries.
func (g *groupStorage) Snapshot() (raftpb.Snapshot, error) {
	w := g.store.w
	w.indexMu.RLock()
	defer w.indexMu.RUnlock()
	gs := w.groups[g.groupID]
	if gs == nil || !gs.hasSnap {
		return raftpb.Snapshot{}, raft.ErrSnapshotTemporarilyUnavailable
	}
	raw := make([]byte, gs.snapLoc.n)
	if _, err := gs.snapLoc.seg.f.ReadAt(raw, gs.snapLoc.off); err != nil {
		// Loud on every attempt: a transient read failure self-heals on the
		// retry; a persistent one keeps logging and is quarantined by boot
		// validation on the next start.
		w.log.Errorf("sharedlog: Snapshot: group %d: read segment %s: %v — reporting snapshot temporarily unavailable",
			g.groupID, gs.snapLoc.seg.path, err)
		return raftpb.Snapshot{}, raft.ErrSnapshotTemporarilyUnavailable
	}
	var snap raftpb.Snapshot
	if err := snap.Unmarshal(raw); err != nil {
		w.log.Errorf("sharedlog: Snapshot: group %d: unmarshal payload: %v — reporting snapshot temporarily unavailable",
			g.groupID, err)
		return raftpb.Snapshot{}, raft.ErrSnapshotTemporarilyUnavailable
	}
	return snap, nil
}

// tripSplitBrain records a read that hit the index split-brain guard: a
// position inside the visible range with no retained entry and no covering
// snapshot. Healthy indexes never get here — a trip means compaction outlived
// its authorizing snapshot record (the panic state of minor-issues.md #9,
// made retryable by Snapshot's sentinel above) and is worth an operator's
// attention. Caller holds indexMu (read).
func (w *wal) tripSplitBrain(op string, groupID, i uint64, g *groupState) {
	splitBrainReads.Inc()
	if !w.splitBrainLog.allow() {
		return
	}
	snapIdx, _ := snapBounds(g)
	base, n := uint64(0), 0
	if g != nil {
		base, n = g.base, len(g.ents)
	}
	w.log.Errorf("sharedlog: %s(%d) for group %d: position inside the visible range has no retained entry and no covering snapshot (base=%d, retained=%d, snapshot=%d) — split-brain guard tripped, answering ErrUnavailable",
		op, i, groupID, base, n, snapIdx)
}

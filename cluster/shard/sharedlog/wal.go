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

// Package sharedlog implements the node-wide shared raft log: one segmented
// append-only WAL carrying entries, HardState, ConfState and snapshot
// metadata for every shard raft group on the node, behind a group-commit
// batcher — one fsync covers every group's writes in a flush.
//
// # Layout
//
// The store is a directory of fixed-name segment files (%016d.wal, monotonic
// sequence numbers, contiguous). Appends go to the highest-numbered (active)
// segment; a flush is one sequential write plus one fsync. A full in-memory
// index (per group: unmarshaled HardState/ConfState/snapshot metadata plus a
// contiguous slice of entry payload locations) is rebuilt by replaying the
// segments in order at Open; every raft.Storage read except Entries and
// Snapshot payloads is answered from RAM.
//
// # Durability model
//
// The previous bbolt engine gave whole-batch transaction atomicity. The WAL
// gives two weaker-looking guarantees that together preserve every invariant
// etcd/raft checks on restart:
//
//   - Record atomicity: one GroupWrite is one CRC-framed record, so a torn
//     tail drops whole GroupWrites — it can never persist a snapshot or
//     entries without the HardState riding the same GroupWrite.
//   - Prefix durability: replay stops at the first torn record, keeping a
//     FIFO prefix of the un-fsynced region.
//
// The keystone: the torn region is exactly the last un-fsynced flush;
// records within it are FIFO in emission order; therefore any surviving
// HardState's commit refers only to entries persisted before it in that
// region or already durable from prior flushes. raft emits commit
// monotonically, never above entries it has already emitted for storage,
// and never truncates below a commit it has announced — so every FIFO
// prefix is a state etcd/raft accepts. Writes lost at the tear were never
// acknowledged (responses are durability-gated), making the loss
// crash-equivalent: the leader re-probes and re-sends.
//
// # Compaction and reclamation
//
// Compact is an in-memory index operation: the durable compaction floor is
// the group's persisted snapshot metadata, and replay prunes entries at or
// below it, so a crash re-derives every in-contract compaction. Segments
// whose live-byte account (maintained exactly, decrement-on-supersede)
// reaches zero are deleted oldest-first. Groups that go idle would pin
// their last records' segments forever, so a mostly-dead oldest segment is
// rewritten: every group with a live record in it has its ENTIRE live entry
// range (plus victim-resident singleton states) copied forward as a
// recTypeWriteCopy record. Copies are built from the per-entry index — never
// by re-reading whole records — so a superseded entry can never be
// resurrected over its replacement, and each copy is a self-contained
// contiguous snapshot of the range, because at replay it may be the only
// surviving source. Copy records apply as a merge (establish, prepend,
// relocate, extend — never truncate), so replaying one cannot delete
// entries appended between the rewrite and the replay.
package sharedlog

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

const (
	defaultSegmentMaxBytes = 64 << 20

	// Rewrite policy: only when more than rewriteMinSegments segments have
	// piled up AND the oldest carries at most rewriteMaxLiveBytes of live
	// data is it rewritten forward. The accepted trade: up to
	// ~rewriteMinSegments*SegmentMaxBytes (~512MiB at defaults) of
	// mostly-dead segments can accumulate before a rewrite fires, in
	// exchange for copy churn bounded at 4MiB and zero copying on the
	// common path (active groups self-clean via snapshot-cadence
	// compaction; only idle-group residue needs rewriting).
	rewriteMinSegments  = 8
	rewriteMaxLiveBytes = 4 << 20
)

// segment is one WAL file. size and the file itself are guarded by writeMu;
// live and membership in wal.segs are guarded by indexMu.
type segment struct {
	seq  uint64
	path string
	f    *os.File
	size int64
	live int64
}

// recRef attributes n live bytes to a segment for reclamation accounting.
type recRef struct {
	seg *segment
	n   int64
}

// entryLoc locates one live entry's marshaled payload. term rides along so
// Term(i) never touches disk.
type entryLoc struct {
	seg  *segment
	off  int64
	n    uint32
	term uint64
}

func (l entryLoc) ref() recRef { return recRef{l.seg, entryFrameSize + int64(l.n)} }

// groupState is one group's in-memory index: singleton states unmarshaled,
// entries as a contiguous [base..base+len) slice of payload locations —
// contiguity is a raft invariant (truncate-then-append).
type groupState struct {
	hasHS bool
	hs    raftpb.HardState
	hsRef recRef

	hasCS bool
	cs    raftpb.ConfState
	csRef recRef

	hasSnap  bool
	snapMeta raftpb.SnapshotMetadata
	snapLoc  entryLoc

	base uint64
	ents []entryLoc
}

// last returns the group's highest entry index; only meaningful when
// len(ents) > 0.
func (g *groupState) last() uint64 { return g.base + uint64(len(g.ents)) - 1 }

type wal struct {
	dir    string
	dirF   *os.File
	log    logrus.FieldLogger
	segMax int64

	// writeMu serializes every segment-file mutator (flush, DeleteGroup,
	// rotation, reclamation, close); indexMu guards the index and the
	// segment list. Lock order: writeMu before indexMu, never inverted.
	// indexMu is never held across a flush fsync; it IS held (read) across
	// Entries/Snapshot preads, which is what makes oldest-segment deletion
	// safe without per-segment refcounts: a segment is only deleted once no
	// index location references it, and no reader can be mid-pread while
	// the deleter holds the write lock.
	writeMu sync.Mutex
	indexMu sync.RWMutex

	segs   []*segment // ascending seq; segs[len(segs)-1] is the active tail
	groups map[uint64]*groupState

	// poisoned quarantines groups that failed boot validation (see
	// validateGroups): openWAL succeeds, healthy groups serve, but a poisoned
	// group's Store refuses to start until the group is dropped (a tombstone
	// clears the mark) or the operator intervenes. Guarded by indexMu;
	// populated only at open and cleared only by tombstones.
	poisoned map[uint64]string

	// splitBrainLog rate-limits the runtime split-brain tripwire's Errorf
	// (see tripSplitBrain) — a stuck follower probes every few ticks.
	splitBrainLog errLimiter

	scratch []byte // batch encode buffer; writeMu-guarded
}

// errLimiter allows one event per interval — the sharedlog-local equivalent
// of the shard package's logLimiter for guard-path Errorf lines.
type errLimiter struct {
	mu   sync.Mutex
	last time.Time
}

func (l *errLimiter) allow() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if time.Since(l.last) < time.Second {
		return false
	}
	l.last = time.Now()
	return true
}

// openWAL opens or creates the WAL directory and rebuilds the index by
// replaying every segment in sequence order.
func openWAL(dir string, segMax int64, log logrus.FieldLogger) (_ *wal, err error) {
	if fi, statErr := os.Stat(dir); statErr == nil && !fi.IsDir() {
		return nil, fmt.Errorf("%s is a regular file — the WAL needs a directory (a legacy bbolt shared log must be moved aside)", dir)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("mkdir %s: %w", dir, err)
	}
	dirF, err := os.Open(dir)
	if err != nil {
		return nil, fmt.Errorf("open dir %s: %w", dir, err)
	}

	w := &wal{
		dir: dir, dirF: dirF, log: log, segMax: segMax,
		groups:   make(map[uint64]*groupState),
		poisoned: make(map[uint64]string),
	}
	defer func() {
		if err != nil {
			_ = w.closeFiles()
		}
	}()

	seqs, err := w.listSegments()
	if err != nil {
		return nil, err
	}
	if len(seqs) == 0 {
		seg, err := w.createSegment(1)
		if err != nil {
			return nil, err
		}
		w.segs = append(w.segs, seg)
		if err := w.syncDir(); err != nil {
			return nil, err
		}
	} else {
		for i, seq := range seqs {
			if err := w.replaySegment(seq, i == len(seqs)-1); err != nil {
				return nil, err
			}
		}
	}

	// Boot validation: quarantine any group whose rebuilt index violates the
	// servability invariant before anything can serve (or compact) it.
	w.validateGroups()

	// Reclaim any fully-dead prefix left by a pre-crash deletion or made
	// dead by replay-side snapshot pruning.
	w.writeMu.Lock()
	w.reclaim()
	w.writeMu.Unlock()

	segmentsGauge.Set(float64(len(w.segs)))
	return w, nil
}

// listSegments returns the segment sequence numbers present in dir, sorted
// ascending, and enforces contiguity — reclamation only ever removes the
// oldest segment, so a gap means the log was externally damaged.
func (w *wal) listSegments() ([]uint64, error) {
	entries, err := os.ReadDir(w.dir)
	if err != nil {
		return nil, fmt.Errorf("read dir %s: %w", w.dir, err)
	}
	var seqs []uint64
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || len(name) != 20 || !strings.HasSuffix(name, ".wal") {
			w.log.Warnf("ignoring foreign file %s in WAL directory %s", name, w.dir)
			continue
		}
		seq, err := strconv.ParseUint(name[:16], 10, 64)
		if err != nil {
			w.log.Warnf("ignoring foreign file %s in WAL directory %s", name, w.dir)
			continue
		}
		seqs = append(seqs, seq)
	}
	sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })
	for i := 1; i < len(seqs); i++ {
		if seqs[i] != seqs[i-1]+1 {
			return nil, fmt.Errorf("segment sequence gap: %d.wal is followed by %d.wal — a middle segment is missing, the log is damaged", seqs[i-1], seqs[i])
		}
	}
	return seqs, nil
}

func segFileName(seq uint64) string { return fmt.Sprintf("%016d.wal", seq) }

// createSegment creates and syncs a fresh segment file (header only). The
// caller appends it to w.segs and syncs the directory.
func (w *wal) createSegment(seq uint64) (*segment, error) {
	path := filepath.Join(w.dir, segFileName(seq))
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return nil, fmt.Errorf("create segment %s: %w", path, err)
	}
	hdr := make([]byte, segHeaderSize)
	binary.LittleEndian.PutUint32(hdr, segMagic)
	hdr[4] = segVersion
	_, err = f.WriteAt(hdr, 0)
	if err == nil {
		err = f.Sync()
	}
	if err != nil {
		_ = f.Close()
		_ = os.Remove(path)
		return nil, fmt.Errorf("init segment %s: %w", path, err)
	}
	return &segment{seq: seq, path: path, f: f, size: segHeaderSize}, nil
}

func (w *wal) syncDir() error {
	if err := w.dirF.Sync(); err != nil {
		return fmt.Errorf("fsync WAL dir %s: %w", w.dir, err)
	}
	return nil
}

// replaySegment folds one segment into the index. Torn records — a short,
// zero-length, overrunning or checksum-failing frame — are permitted only in
// the last segment, where they are the un-fsynced tail of the crashed
// process and get truncated away (nothing in them was ever acknowledged:
// rotation fully syncs a segment before its successor exists). Anywhere
// else they are corruption of acknowledged data and fail the open. A
// CRC-valid record that fails structural decode is a format violation and
// fails the open regardless of position.
func (w *wal) replaySegment(seq uint64, last bool) error {
	path := filepath.Join(w.dir, segFileName(seq))
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return fmt.Errorf("open segment %s: %w", path, err)
	}
	seg := &segment{seq: seq, path: path, f: f}

	st, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return fmt.Errorf("stat segment %s: %w", path, err)
	}
	size := st.Size()

	hdrOK := false
	if size >= segHeaderSize {
		hdr := make([]byte, segHeaderSize)
		if _, err := f.ReadAt(hdr, 0); err != nil {
			_ = f.Close()
			return fmt.Errorf("read segment header %s: %w", path, err)
		}
		hdrOK = binary.LittleEndian.Uint32(hdr) == segMagic && hdr[4] == segVersion
	}
	if !hdrOK {
		if !last {
			_ = f.Close()
			return fmt.Errorf("segment %s: invalid header on a non-tail segment — the log is damaged", path)
		}
		// Crash during rotation: the file exists but its header never became
		// durable. Nothing in it was acknowledged; reset it to empty.
		w.log.Warnf("segment %s has a short or invalid header (crash during rotation); resetting it to an empty segment", path)
		if err := f.Truncate(0); err != nil {
			_ = f.Close()
			return fmt.Errorf("reset segment %s: %w", path, err)
		}
		hdr := make([]byte, segHeaderSize)
		binary.LittleEndian.PutUint32(hdr, segMagic)
		hdr[4] = segVersion
		_, werr := f.WriteAt(hdr, 0)
		if werr == nil {
			werr = f.Sync()
		}
		if werr != nil {
			_ = f.Close()
			return fmt.Errorf("reset segment %s: %w", path, werr)
		}
		seg.size = segHeaderSize
		w.segs = append(w.segs, seg)
		return nil
	}

	pos := int64(segHeaderSize)
	rd := io.NewSectionReader(f, pos, size-pos)

	torn := func(reason string) error {
		if !last {
			_ = f.Close()
			return fmt.Errorf("segment %s: %s at offset %d — corruption outside the tail segment, refusing to open", path, reason, pos)
		}
		w.log.Warnf("truncating torn tail of segment %s at offset %d: %s", path, pos, reason)
		terr := f.Truncate(pos)
		if terr == nil {
			terr = f.Sync()
		}
		if terr != nil {
			_ = f.Close()
			return fmt.Errorf("truncate torn tail of %s: %w", path, terr)
		}
		return nil
	}

	var body []byte
	for {
		var hdr [recHeaderSize]byte
		if _, err := io.ReadFull(rd, hdr[:]); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			if errors.Is(err, io.ErrUnexpectedEOF) {
				if terr := torn("short record header"); terr != nil {
					return terr
				}
				break
			}
			_ = f.Close()
			return fmt.Errorf("read segment %s: %w", path, err)
		}
		bl := binary.LittleEndian.Uint32(hdr[:4])
		crc := binary.LittleEndian.Uint32(hdr[4:])
		if bl == 0 {
			if terr := torn("zero-length record"); terr != nil {
				return terr
			}
			break
		}
		if int64(bl) > size-pos-recHeaderSize {
			if terr := torn(fmt.Sprintf("record length %d overruns the file", bl)); terr != nil {
				return terr
			}
			break
		}
		if cap(body) < int(bl) {
			body = make([]byte, bl)
		}
		body = body[:bl]
		if _, err := io.ReadFull(rd, body); err != nil {
			if terr := torn("short record body"); terr != nil {
				return terr
			}
			break
		}
		if crc32.Checksum(body, castagnoli) != crc {
			if terr := torn("record checksum mismatch"); terr != nil {
				return terr
			}
			break
		}
		typ, d, err := decodeRecord(body, seg, pos+recHeaderSize)
		if err != nil {
			_ = f.Close()
			return fmt.Errorf("segment %s: malformed record at offset %d: %w", path, pos, err)
		}
		// openWAL is single-threaded; the apply functions expect their
		// caller to hold indexMu at runtime.
		if typ == recTypeTombstone {
			w.applyTombstone(d.groupID)
		} else {
			w.applyWrite(&d)
		}
		pos += recHeaderSize + int64(bl)
	}

	seg.size = pos
	w.segs = append(w.segs, seg)
	return nil
}

// rotate seals the active segment (already fully synced — every flush
// syncs) and installs a fresh one. Crash windows: a created-but-unsynced
// file is reset at replay (invalid header); the directory entry is durable
// before any record in the new segment can be acknowledged.
func (w *wal) rotate() error {
	seg, err := w.createSegment(w.segs[len(w.segs)-1].seq + 1)
	if err != nil {
		return err
	}
	if err := w.syncDir(); err != nil {
		_ = seg.f.Close()
		return err
	}
	w.indexMu.Lock()
	w.segs = append(w.segs, seg)
	segmentsGauge.Set(float64(len(w.segs)))
	w.indexMu.Unlock()
	return nil
}

// writeAndSync appends buf (whole records) to the active segment, rotating
// first when the cap would be exceeded, and fsyncs. Returns the segment and
// the offset buf landed at. The active segment's size only advances on full
// success, so a failed or partial write is overwritten by the next attempt
// and truncated as a torn tail after a crash. writeMu must be held.
func (w *wal) writeAndSync(buf []byte) (*segment, int64, error) {
	active := w.segs[len(w.segs)-1]
	if active.size > segHeaderSize && active.size+int64(len(buf)) > w.segMax {
		if err := w.rotate(); err != nil {
			return nil, 0, err
		}
		active = w.segs[len(w.segs)-1]
	}
	base := active.size
	if _, err := active.f.WriteAt(buf, base); err != nil {
		return nil, 0, fmt.Errorf("write segment %s: %w", active.path, err)
	}
	if err := active.f.Sync(); err != nil {
		return nil, 0, fmt.Errorf("fsync segment %s: %w", active.path, err)
	}
	active.size += int64(len(buf))
	return active, base, nil
}

// writeBatch persists one flush: every GroupWrite encoded as one composite
// record, one sequential write, one fsync, then the index updates published
// in submission order. Any encode error fails the whole batch before
// anything is written, matching the previous engine's all-or-nothing
// transaction behavior for invalid input.
func (w *wal) writeBatch(writes []*GroupWrite) error {
	w.writeMu.Lock()
	defer w.writeMu.Unlock()

	buf := w.scratch[:0]
	decs := make([]decodedWrite, 0, len(writes))
	var batchCommit map[uint64]uint64
	for _, gw := range writes {
		w.warnUncoveredSnapshot(gw, batchCommit)
		if gw.HardState != nil {
			if batchCommit == nil {
				batchCommit = make(map[uint64]uint64)
			}
			batchCommit[gw.GroupID] = gw.HardState.Commit
		}
		var (
			d       decodedWrite
			emitted bool
			err     error
		)
		buf, d, emitted, err = encodeGroupWrite(buf, gw)
		if err != nil {
			w.scratch = buf[:0]
			return err
		}
		if emitted {
			decs = append(decs, d)
		}
	}
	w.scratch = buf
	if len(decs) == 0 {
		return nil
	}

	seg, base, err := w.writeAndSync(buf)
	if err != nil {
		return err
	}

	w.indexMu.Lock()
	for i := range decs {
		decs[i].rebase(seg, base)
		w.applyWrite(&decs[i])
	}
	w.indexMu.Unlock()

	w.reclaim()
	return nil
}

// warnUncoveredSnapshot flags the one write shape whose torn-tail loss the
// composite-record atomicity cannot excuse: a snapshot above the group's
// durable commit with no HardState in the same GroupWrite. If the covering
// commit only exists in a lost companion record, a restart can see the
// snapshot with a commit below it and panic inside etcd/raft. Production
// snapshot installs always carry their HardState in the same GroupWrite;
// this guards against upstream refactors breaking that. It cannot be an
// error: snapshot-only writes at or below the durable commit are the local
// snapshot-persist path, and test fixtures legitimately write bare
// snapshots.
func (w *wal) warnUncoveredSnapshot(gw *GroupWrite, batchCommit map[uint64]uint64) {
	if gw.Snapshot == nil || raft.IsEmptySnap(*gw.Snapshot) || gw.HardState != nil {
		return
	}
	commit, ok := batchCommit[gw.GroupID]
	if !ok {
		w.indexMu.RLock()
		if g := w.groups[gw.GroupID]; g != nil {
			commit = g.hs.Commit
		}
		w.indexMu.RUnlock()
	}
	if gw.Snapshot.Metadata.Index > commit {
		w.log.Warnf("group %d: persisting a snapshot at index %d above the durable commit %d without a HardState in the same write — a torn tail could strand the snapshot uncovered and panic the group on restart",
			gw.GroupID, gw.Snapshot.Metadata.Index, commit)
	}
}

// applyWrite folds one composite record into the index — the single
// semantics shared by the runtime write path, replay, and rewrite. The
// caller must hold indexMu (write) or be the single-threaded opener.
func (w *wal) applyWrite(d *decodedWrite) {
	g := w.groups[d.groupID]
	if g == nil {
		g = &groupState{}
		w.groups[d.groupID] = g
	}
	if len(d.entries) > 0 {
		if d.copyRec {
			w.patchEntries(g, d)
		} else {
			w.appendEntries(g, d)
		}
	}
	if d.hasHS {
		if g.hasHS {
			w.debit(g.hsRef)
		}
		g.hasHS, g.hs = true, d.hs
		g.hsRef = recRef{d.seg, 4 + int64(d.hsN)}
		w.credit(g.hsRef)
	}
	if d.hasCS {
		if g.hasCS {
			w.debit(g.csRef)
		}
		g.hasCS, g.cs = true, d.cs
		g.csRef = recRef{d.seg, 4 + int64(d.csN)}
		w.credit(g.csRef)
	}
	if d.hasSnap {
		if g.hasSnap {
			w.debit(recRef{g.snapLoc.seg, 4 + int64(g.snapLoc.n)})
		}
		g.hasSnap, g.snapMeta = true, d.snapMeta
		g.snapLoc = entryLoc{seg: d.seg, off: d.snapOff, n: d.snapN}
		w.credit(recRef{d.seg, 4 + int64(d.snapN)})
		// The persisted snapshot is the durable compaction floor: replay
		// always prunes below it, so prune eagerly here too — runtime and
		// restart states stay identical, and the explicit Compact that
		// follows every production snapshot becomes a no-op.
		w.compactGroup(g, d.snapMeta.Index+1)
	}
}

// appendEntries applies truncate-then-append: any existing entry at or above
// the incoming first index is superseded.
func (w *wal) appendEntries(g *groupState, d *decodedWrite) {
	first := d.entries[0].index
	switch {
	case len(g.ents) == 0:
		g.base = first
	case first <= g.base:
		w.dropEntriesFrom(g, 0)
		g.base = first
	case first <= g.last():
		w.dropEntriesFrom(g, int(first-g.base))
	case first > g.last()+1:
		// A gap is outside raft's contract; treat the new range as
		// authoritative rather than serving a hole.
		w.log.Warnf("entry append at index %d leaves a gap above %d; replacing the range", first, g.last())
		w.dropEntriesFrom(g, 0)
		g.base = first
	}
	for _, e := range d.entries {
		loc := entryLoc{seg: d.seg, off: e.off, n: e.n, term: e.term}
		g.ents = append(g.ents, loc)
		w.credit(loc.ref())
	}
}

// patchEntries applies a rewrite copy record's entries. A copy carries the
// group's ENTIRE live entry range as of rewrite time — a contiguous,
// self-contained snapshot — because at replay the copy may be the only
// surviving source: the records it replaced can live in segments deleted
// after the rewrite. The merge can therefore re-establish the range from
// nothing, prepend a portion that survives nowhere else, relocate
// overlapping entries (same logical values: nothing written before the
// rewrite can supersede an entry that was live AT the rewrite), and extend —
// but it never truncates: entries appended after the rewrite replay later
// and must win, and entries present now but absent from the copy were
// appended between the copy and this position.
func (w *wal) patchEntries(g *groupState, d *decodedWrite) {
	c0 := d.entries[0].index
	cLast := d.entries[len(d.entries)-1].index
	if cLast-c0+1 != uint64(len(d.entries)) {
		// The writer only ever produces contiguous copies; a CRC-valid
		// non-contiguous one is a writer bug, not disk corruption.
		panic(fmt.Sprintf("sharedlog: copy record for group %d is not contiguous ([%d..%d] with %d entries)",
			d.groupID, c0, cLast, len(d.entries)))
	}
	if len(g.ents) != 0 && (c0 > g.last()+1 || cLast+1 < g.base) {
		// The copy neither overlaps nor abuts the current range — a hole no
		// contiguous index can hold, impossible for a valid history. The
		// copy is the complete live range of its time; let it win.
		w.log.Warnf("copy record for group %d range [%d..%d] is disjoint from the current range [%d..%d]; replacing the range",
			d.groupID, c0, cLast, g.base, g.last())
		w.dropEntriesFrom(g, 0)
	}
	if len(g.ents) == 0 {
		g.base = c0
		for _, e := range d.entries {
			loc := entryLoc{seg: d.seg, off: e.off, n: e.n, term: e.term}
			g.ents = append(g.ents, loc)
			w.credit(loc.ref())
		}
		return
	}
	newBase := g.base
	if c0 < newBase {
		newBase = c0
	}
	newLast := g.last()
	if cLast > newLast {
		newLast = cLast
	}
	merged := make([]entryLoc, 0, newLast-newBase+1)
	for idx := newBase; idx <= newLast; idx++ {
		if idx >= c0 && idx <= cLast {
			e := d.entries[idx-c0]
			loc := entryLoc{seg: d.seg, off: e.off, n: e.n, term: e.term}
			if idx >= g.base && idx <= g.last() {
				w.debit(g.ents[idx-g.base].ref())
			}
			w.credit(loc.ref())
			merged = append(merged, loc)
		} else {
			// Outside the copy, inside the current range (the union of two
			// overlapping contiguous ranges has no other gaps).
			merged = append(merged, g.ents[idx-g.base])
		}
	}
	g.base = newBase
	g.ents = merged
}

// dropEntriesFrom debits and discards g.ents[k:].
func (w *wal) dropEntriesFrom(g *groupState, k int) {
	for _, l := range g.ents[k:] {
		w.debit(l.ref())
	}
	g.ents = g.ents[:k]
}

// compactGroup drops entries below upTo. In-memory only; see the package
// doc for why this needs no durable record.
func (w *wal) compactGroup(g *groupState, upTo uint64) {
	if len(g.ents) == 0 || upTo <= g.base {
		return
	}
	k := upTo - g.base
	if k > uint64(len(g.ents)) {
		k = uint64(len(g.ents))
	}
	for _, l := range g.ents[:k] {
		w.debit(l.ref())
	}
	g.ents = g.ents[k:]
	g.base += k
}

func (w *wal) applyTombstone(groupID uint64) {
	// A tombstone is the sanctioned way out of quarantine: dropping the
	// group discards the damaged state a poisoning described.
	if _, ok := w.poisoned[groupID]; ok {
		delete(w.poisoned, groupID)
		poisonedGroupsGauge.Set(float64(len(w.poisoned)))
	}
	g := w.groups[groupID]
	if g == nil {
		return
	}
	w.dropEntriesFrom(g, 0)
	if g.hasHS {
		w.debit(g.hsRef)
	}
	if g.hasCS {
		w.debit(g.csRef)
	}
	if g.hasSnap {
		w.debit(recRef{g.snapLoc.seg, 4 + int64(g.snapLoc.n)})
	}
	delete(w.groups, groupID)
}

// validateGroups checks every rebuilt group against the servability
// invariant — entry bounds must never be visible without the snapshot that
// authorized them being durable, readable, and abutting — and quarantines
// violators (minor-issues.md #9: a violating group's leader either panicked
// etcd pre-W-A or strands followers in an unhealable retry loop). Poisoning
// is per group by design: one damaged group on a node hosting a thousand
// healthy ones must not brick the node — openWAL succeeds, healthy groups
// serve, and the poisoned group's Store refuses to Start with a named error.
// Runs single-threaded at open, before anything can read or compact.
func (w *wal) validateGroups() {
	for gid, g := range w.groups {
		if reason := w.validateGroup(g); reason != "" {
			w.poisoned[gid] = reason
			w.log.Errorf("sharedlog: group %d failed boot validation and is POISONED (its store will refuse to start): %s", gid, reason)
		}
	}
	poisonedGroupsGauge.Set(float64(len(w.poisoned)))
}

// validateGroup returns "" for a healthy group or the poisoning reason.
func (w *wal) validateGroup(g *groupState) string {
	if !g.hasSnap {
		// No snapshot: the log must reach back to its beginning. Entries
		// starting above 1 mean a compaction outlived the snapshot record
		// that authorized it (e.g. tail-segment damage destroyed the record
		// after older segments were already reclaimed).
		if len(g.ents) > 0 && g.base != 1 {
			return fmt.Sprintf("entries [%d..%d] with no snapshot record — compaction outlived its authorizing snapshot", g.base, g.last())
		}
		return ""
	}
	raw := make([]byte, g.snapLoc.n)
	if _, err := g.snapLoc.seg.f.ReadAt(raw, g.snapLoc.off); err != nil {
		return fmt.Sprintf("snapshot payload at %s+%d unreadable: %v", g.snapLoc.seg.path, g.snapLoc.off, err)
	}
	var snap raftpb.Snapshot
	if err := snap.Unmarshal(raw); err != nil {
		return fmt.Sprintf("snapshot payload undecodable: %v", err)
	}
	if snap.Metadata.Index != g.snapMeta.Index || snap.Metadata.Term != g.snapMeta.Term {
		return fmt.Sprintf("snapshot payload metadata (%d/t%d) diverges from the indexed metadata (%d/t%d)",
			snap.Metadata.Index, snap.Metadata.Term, g.snapMeta.Index, g.snapMeta.Term)
	}
	if len(g.ents) > 0 && g.base != g.snapMeta.Index+1 {
		return fmt.Sprintf("entries [%d..%d] do not abut the snapshot at %d — a hole no follower can be served across",
			g.base, g.last(), g.snapMeta.Index)
	}
	return ""
}

// poisonedReason reports whether boot validation quarantined the group, and
// why.
func (w *wal) poisonedReason(groupID uint64) (string, bool) {
	w.indexMu.RLock()
	defer w.indexMu.RUnlock()
	reason, ok := w.poisoned[groupID]
	return reason, ok
}

// poisonedCount returns the number of currently quarantined groups.
func (w *wal) poisonedCount() int {
	w.indexMu.RLock()
	defer w.indexMu.RUnlock()
	return len(w.poisoned)
}

func (w *wal) credit(r recRef) { r.seg.live += r.n }

func (w *wal) debit(r recRef) {
	r.seg.live -= r.n
	if r.seg.live < 0 {
		panic(fmt.Sprintf("sharedlog: segment %s live-byte account went negative (%d) — reclamation accounting bug",
			r.seg.path, r.seg.live))
	}
}

// reclaim deletes fully-dead oldest segments and, when the rewrite policy
// fires, copies the oldest segment's few live records forward so it can be
// deleted too. Only ever removes the oldest segment — that invariant is what
// lets tombstones and superseding records be dropped safely: by the time a
// record's segment is oldest, everything it superseded is already gone.
// writeMu must be held.
func (w *wal) reclaim() {
	var removed []*segment
	var victim *segment
	w.indexMu.Lock()
	for len(w.segs) > 1 && w.segs[0].live == 0 {
		removed = append(removed, w.segs[0])
		w.segs = w.segs[1:]
	}
	if len(w.segs) > rewriteMinSegments && w.segs[0].live > 0 && w.segs[0].live <= rewriteMaxLiveBytes {
		victim = w.segs[0]
	}
	segmentsGauge.Set(float64(len(w.segs)))
	w.indexMu.Unlock()

	for _, s := range removed {
		_ = s.f.Close()
		if err := os.Remove(s.path); err != nil {
			w.log.Warnf("failed to remove dead segment %s: %v", s.path, err)
		}
	}
	if len(removed) > 0 {
		if err := w.syncDir(); err != nil {
			w.log.Warnf("failed to sync WAL dir after segment removal: %v", err)
		}
	}
	if victim != nil {
		w.rewriteSegment(victim)
	}
}

// rewriteSegment relocates every record pinning the victim segment into the
// active segment, then deletes the victim. For each group with any live
// record in the victim it copies the group's ENTIRE live entry range (plus
// whichever singleton states sit in the victim), gathered from the per-entry
// index — never by re-reading whole records — so superseded entries are
// never copied and can never be resurrected over their replacements, and so
// each copy record is a self-contained contiguous snapshot that replay can
// re-establish the range from even after the source segments are gone (see
// patchEntries). Total copy volume is budgeted: a group with a large live
// range is about to compact it anyway (snapshot cadence), so skipping is
// backoff, not starvation. Ordering: copies are durable (fsync) before the
// victim is removed, so a crash between the two merely replays duplicates,
// which the merge absorbs. Best-effort: any failure leaves the victim in
// place for the next trigger. writeMu must be held — it excludes every
// index mutator, which is what makes the gather-then-apply split safe.
func (w *wal) rewriteSegment(victim *segment) {
	buf := w.scratch[:0]
	var decs []decodedWrite

	w.indexMu.RLock()
	for gid, g := range w.groups {
		touches := (g.hasHS && g.hsRef.seg == victim) ||
			(g.hasCS && g.csRef.seg == victim) ||
			(g.hasSnap && g.snapLoc.seg == victim)
		if !touches {
			for _, l := range g.ents {
				if l.seg == victim {
					touches = true
					break
				}
			}
		}
		if !touches {
			continue
		}
		var ents []rawEntry
		hasVictimEntries := false
		for _, l := range g.ents {
			if l.seg == victim {
				hasVictimEntries = true
				break
			}
		}
		if hasVictimEntries {
			ents = make([]rawEntry, 0, len(g.ents))
			for i, l := range g.ents {
				data := make([]byte, l.n)
				if _, err := l.seg.f.ReadAt(data, l.off); err != nil {
					w.indexMu.RUnlock()
					w.scratch = buf[:0]
					w.log.Warnf("rewrite of segment %s aborted: read entry at index %d: %v", victim.path, g.base+uint64(i), err)
					return
				}
				ents = append(ents, rawEntry{index: g.base + uint64(i), term: l.term, data: data})
			}
		}
		var hsP *raftpb.HardState
		var csP *raftpb.ConfState
		var snapMetaP *raftpb.SnapshotMetadata
		var hsRaw, csRaw, snapRaw []byte
		var err error
		if g.hasHS && g.hsRef.seg == victim {
			if hsRaw, err = g.hs.Marshal(); err == nil {
				hsP = &g.hs
			}
		}
		if err == nil && g.hasCS && g.csRef.seg == victim {
			if csRaw, err = g.cs.Marshal(); err == nil {
				csP = &g.cs
			}
		}
		if err == nil && g.hasSnap && g.snapLoc.seg == victim {
			snapRaw = make([]byte, g.snapLoc.n)
			if _, err = victim.f.ReadAt(snapRaw, g.snapLoc.off); err == nil {
				snapMetaP = &g.snapMeta
			}
		}
		if err != nil {
			w.indexMu.RUnlock()
			w.scratch = buf[:0]
			w.log.Warnf("rewrite of segment %s aborted for group %d: %v", victim.path, gid, err)
			return
		}
		if len(ents) == 0 && hsRaw == nil && csRaw == nil && snapRaw == nil {
			continue
		}
		var d decodedWrite
		buf, d = appendWriteRecord(buf, recTypeWriteCopy, gid, ents, hsP, csP, snapMetaP, hsRaw, csRaw, snapRaw)
		decs = append(decs, d)
		if int64(len(buf)) > rewriteMaxLiveBytes {
			w.indexMu.RUnlock()
			w.scratch = buf[:0]
			w.log.Debugf("skipping rewrite of segment %s: full live-range copies exceed the %d-byte budget; retrying after compaction shrinks them",
				victim.path, rewriteMaxLiveBytes)
			return
		}
	}
	w.indexMu.RUnlock()
	w.scratch = buf

	if len(decs) > 0 {
		seg, base, err := w.writeAndSync(buf)
		if err != nil {
			w.log.Warnf("rewrite of segment %s aborted: %v", victim.path, err)
			return
		}
		w.indexMu.Lock()
		for i := range decs {
			decs[i].rebase(seg, base)
			w.applyWrite(&decs[i])
		}
		w.indexMu.Unlock()
	}

	w.indexMu.Lock()
	if victim.live != 0 {
		w.indexMu.Unlock()
		panic(fmt.Sprintf("sharedlog: segment %s still has %d live bytes after rewrite — reclamation accounting bug",
			victim.path, victim.live))
	}
	if w.segs[0] != victim {
		w.indexMu.Unlock()
		panic(fmt.Sprintf("sharedlog: rewrite victim %s is no longer the oldest segment", victim.path))
	}
	w.segs = w.segs[1:]
	segmentsGauge.Set(float64(len(w.segs)))
	w.indexMu.Unlock()

	_ = victim.f.Close()
	if err := os.Remove(victim.path); err != nil {
		w.log.Warnf("failed to remove rewritten segment %s: %v", victim.path, err)
	}
	if err := w.syncDir(); err != nil {
		w.log.Warnf("failed to sync WAL dir after segment rewrite: %v", err)
	}
}

// compact drops entries below upTo from the group's index. In-memory only:
// the durable floor is the persisted snapshot metadata (see package doc).
func (w *wal) compact(groupID, upTo uint64) {
	w.writeMu.Lock()
	defer w.writeMu.Unlock()
	w.indexMu.Lock()
	if g := w.groups[groupID]; g != nil {
		w.compactGroup(g, upTo)
	}
	w.indexMu.Unlock()
	w.reclaim()
}

// deleteGroup purges a group: a durable (individually fsynced) tombstone
// record, then the index drop. The tombstone must be durable — without it a
// crash would resurrect the group from its surviving records, the ghost
// OnShardDropped exists to prevent.
func (w *wal) deleteGroup(groupID uint64) error {
	w.writeMu.Lock()
	defer w.writeMu.Unlock()

	w.indexMu.RLock()
	_, exists := w.groups[groupID]
	w.indexMu.RUnlock()
	if !exists {
		// No live records anywhere in the log — nothing to purge and
		// nothing a crash could resurrect.
		return nil
	}

	buf := appendTombstoneRecord(w.scratch[:0], groupID)
	w.scratch = buf
	if _, _, err := w.writeAndSync(buf); err != nil {
		return fmt.Errorf("write tombstone: %w", err)
	}

	w.indexMu.Lock()
	w.applyTombstone(groupID)
	w.indexMu.Unlock()

	w.reclaim()
	return nil
}

func (w *wal) hasGroup(groupID uint64) bool {
	w.indexMu.RLock()
	defer w.indexMu.RUnlock()
	g := w.groups[groupID]
	return g != nil && g.hasHS
}

func (w *wal) close() error {
	w.writeMu.Lock()
	defer w.writeMu.Unlock()
	w.indexMu.Lock()
	defer w.indexMu.Unlock()
	return w.closeFiles()
}

func (w *wal) closeFiles() error {
	var errs []error
	for _, s := range w.segs {
		if err := s.f.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close segment %s: %w", s.path, err))
		}
	}
	w.segs = nil
	if w.dirF != nil {
		if err := w.dirF.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close WAL dir: %w", err))
		}
		w.dirF = nil
	}
	return errors.Join(errs...)
}

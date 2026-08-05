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

package shard

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/klauspost/compress/s2"
	"github.com/sirupsen/logrus"
	shardproto "github.com/weaviate/weaviate/cluster/shard/proto"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/objects"
	"google.golang.org/protobuf/proto"
)

// schemaFenceTimeout bounds one apply-side schema-fence wait: how long a
// replica waits for its local schema FSM to reach the version a write command
// carries before classifying the wait as a park (the apply worker retries and
// re-fences with backoff — the fence is never a give-up point). The
// coordinator already waited to this version before routing, so the local
// wait is ordinarily just schema-replication lag.
const schemaFenceTimeout = 5 * time.Second

// shard defines the operations that can be performed on a shard.
// This interface is implemented by the actual shard in adapters/repos/db.
//
// Error contract at this boundary (the FSM's single dispatch chokepoint):
// write methods mark errors that are deterministic — produced purely by the
// operation's own content against the replicated schema, identical on every
// replica — via entities/errors.Deterministic. The FSM skips those
// identically everywhere (counted and logged); every UNMARKED error is
// treated as environmental and parks the apply worker at the failing entry
// (indefinite retry with backoff). Misclassification therefore never loses
// acknowledged writes: an untagged deterministic error parks loudly instead
// of skipping.
type shard interface {
	// PutObject stores an object in the shard.
	PutObject(ctx context.Context, obj *storobj.Object) error

	// DeleteObject deletes an object from the shard by UUID.
	DeleteObject(ctx context.Context, id strfmt.UUID, deletionTime time.Time) error

	// MergeObject applies a partial update to an existing object.
	MergeObject(ctx context.Context, merge objects.MergeDocument) error

	// PutObjectBatch stores multiple objects in the shard.
	PutObjectBatch(ctx context.Context, objects []*storobj.Object) []error

	// DeleteObjectBatch deletes multiple objects from the shard.
	DeleteObjectBatch(ctx context.Context, uuids []strfmt.UUID, deletionTime time.Time, dryRun bool) objects.BatchSimpleObjects

	// AddReferencesBatch adds cross-references in batch.
	AddReferencesBatch(ctx context.Context, refs objects.BatchReferences) []error

	// FlushForSnapshot makes every durable sink of the shard durable on disk
	// — LSM memtables to fsynced segments, async vector queues' chunk
	// backlog, vector-index commit logs, the property-length tracker —
	// before a RAFT snapshot truncates the log. An error aborts the
	// snapshot: compaction must never outrun sink durability, because the
	// discarded entries are the only way to re-materialize un-durable
	// writes after a crash.
	FlushForSnapshot(ctx context.Context) error

	// DurableRaftFloor returns the highest raft applied index whose
	// materialization is durable in flushed LSM segments across all of the
	// shard's buckets, or MaxUint64 when no bucket holds un-flushed writes.
	// The snapshot index is capped at this value: log compaction must never
	// discard entries whose only materialization is in un-flushed memtables.
	// Callers must read the applied index BEFORE calling this and cap that
	// pre-read value (see lsmkv.(*Store).DurableRaftFloor).
	DurableRaftFloor() uint64

	// ReadOnlyErr reports whether the shard currently refuses writes
	// (read-only, e.g. the resource-pressure guardrail), with the full
	// operator-facing reason. The leader consults it at admission
	// (Store.Apply) to reject new writes before proposing — pressure lasts
	// minutes and a retry against the same leader buys nothing, so the
	// client must see the reason. Backed by the same check 2PC's prepare
	// gate uses.
	ReadOnlyErr() error

	// WaitForSchemaVersion blocks until this node's schema FSM has caught up
	// to version — the apply-side half of the schema fence. Every write
	// command carries the schema version its coordinator observed; waiting
	// to it before materializing keeps the analyzed schema at least as new
	// as the one the write was admitted under.
	WaitForSchemaVersion(ctx context.Context, version uint64) error

	// ClassPresent reports whether the shard's class is (still) present in
	// the local schema. Consulted by the fence only at or past a write's
	// stamped version — there, absence deterministically means the class was
	// dropped after admission — and by the leader at admission to reject
	// honestly during a drop window.
	ClassPresent() bool

	// CreateTransferSnapshot creates a hardlink snapshot of all shard files
	// for out-of-band state transfer. Returns snapshot metadata including the
	// staging directory path. Caller must call ReleaseTransferSnapshot when done.
	CreateTransferSnapshot(ctx context.Context) (TransferSnapshot, error)

	// ReleaseTransferSnapshot deletes the staging directory for a snapshot.
	ReleaseTransferSnapshot(snapshotID string) error

	// Name returns the shard name.
	Name() string
}

// TransferSnapshot holds metadata for a hardlink snapshot created for
// out-of-band state transfer.
type TransferSnapshot struct {
	ID    string             // unique snapshot identifier
	Dir   string             // staging directory path containing hardlinks
	Files []TransferFileInfo // file list with sizes and checksums
}

// TransferFileInfo describes a single file within a transfer snapshot.
type TransferFileInfo struct {
	Name  string // relative path within staging dir
	Size  int64
	CRC32 uint32
}

// StateTransferer handles downloading shard data from a leader when a
// follower needs a full state transfer (e.g. after falling too far behind).
type StateTransferer interface {
	TransferState(ctx context.Context, className, shardName string) error
}

// FSM is the per-shard command dispatcher. The Store's apply worker — one
// goroutine per Store, fed committed RAFT log entries in log order by the
// Ready loop — hands them to DispatchBatch; the FSM applies each command to
// the underlying shard. With etcd/raft the FSM is no longer a library
// interface — it is a plain dispatcher invoked single-threaded from the
// apply worker.
type FSM struct {
	className string
	shardName string
	nodeID    string
	log       logrus.FieldLogger

	// shard is the actual shard implementation that processes commands.
	// It's set via SetShard after the FSM is created.
	shard shard
	mu    sync.RWMutex

	// stateTransferer handles downloading shard data from the leader when
	// Restore() detects a foreign snapshot. Set via SetStateTransferer.
	stateTransferer StateTransferer

	// lastAppliedIndex tracks the last RAFT log index that was applied.
	// This is used for catch-up detection and snapshot consistency.
	lastAppliedIndex atomic.Uint64

	// indexMu and indexCond are used by WaitForIndex to allow callers to
	// block until the FSM has applied up to a target log index.
	indexMu   sync.Mutex
	indexCond *sync.Cond
}

// NewFSM creates a new FSM for a shard's RAFT cluster.
func NewFSM(className, shardName, nodeID string, log logrus.FieldLogger) *FSM {
	f := &FSM{
		className: className,
		shardName: shardName,
		nodeID:    nodeID,
		log: log.WithFields(logrus.Fields{
			"component": "shard_raft_fsm",
			"class":     className,
			"shard":     shardName,
		}),
	}
	f.indexCond = sync.NewCond(&f.indexMu)
	return f
}

// SetShard sets the shard operator that will process commands.
// This must be called before the RAFT cluster starts processing logs.
func (f *FSM) SetShard(shard shard) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.shard = shard
}

// getShard returns the current shard operator, or nil if not set.
func (f *FSM) getShard() shard {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.shard
}

// SetStateTransferer sets the state transferrer used by Restore() to download
// shard data from the leader when a foreign snapshot is detected.
func (f *FSM) SetStateTransferer(st StateTransferer) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.stateTransferer = st
}

// setApplied records the last applied RAFT log index and wakes WaitForIndex
// waiters. The Store's apply worker calls it directly for entries that carry
// no command (empty leader entries, conf changes); DispatchBatch calls it for
// command entries, only ever behind materialization.
//
// The store and broadcast happen under indexMu: sync.Cond.Broadcast does not
// acquire the mutex itself, so a bare broadcast could land between a waiter's
// condition check and its wait registration (both under the lock) and be
// lost — if it was the final broadcast, the waiter would strand until its
// context deadline.
func (f *FSM) setApplied(index uint64) {
	f.indexMu.Lock()
	f.lastAppliedIndex.Store(index)
	f.indexCond.Broadcast()
	f.indexMu.Unlock()
}

// decodeRequest unmarshals one command payload and decompresses its
// sub-command if the replicator compressed it (uncompressed entries pass
// through unchanged — backwards compatibility during rolling upgrades). A
// failure returns a nil request and the error Response the caller must
// surface for the entry.
func (f *FSM) decodeRequest(payload []byte, index uint64) (*shardproto.ApplyRequest, Response) {
	var req shardproto.ApplyRequest
	if err := proto.Unmarshal(payload, &req); err != nil {
		f.log.Errorf("failed to unmarshal command: %v", err)
		return nil, Response{Version: index, Error: fmt.Errorf("unmarshal command: %w", err)}
	}
	if req.Compressed {
		decompressed, err := s2.Decode(nil, req.SubCommand)
		if err != nil {
			f.log.Errorf("failed to decompress sub_command: %v", err)
			return nil, Response{Version: index, Error: fmt.Errorf("decompress: %w", err)}
		}
		req.SubCommand = decompressed
	}
	return &req, Response{}
}

// applyCommand runs one decoded single-unit command against the shard. Every
// command runs under the LWW replay guard: RAFT apply is at-least-once
// (server-side retries can double-propose a command that already committed, a
// restart re-delivers the committed suffix, and a parked entry re-runs in
// full on every retry), so a (re)applied put or delete strictly older than
// the locally stored state is dropped by the shard instead of clobbering a
// newer same-UUID write. The comparison is deterministic on identical replica
// state, so replicas stay consistent — and it aligns the RAFT path with the
// 2PC write path's LWW semantics.
//
// The returned error is the entry's PARK error: non-nil means the entry did
// not fully materialize for an environmental reason and must be retried in
// full (the caller must not advance the applied index over it). Deterministic
// failures — explicitly marked at the shard boundary, plus decode failures
// and fence-established drop-after-admission — are skipped identically on
// every replica: counted, logged, and folded into a nil return.
func (f *FSM) applyCommand(shard shard, req *shardproto.ApplyRequest, index uint64) (Response, error) {
	ctx := objects.WithLWWReplayGuard(context.Background())
	var parkErr error
	switch req.Type {
	case shardproto.ApplyRequest_TYPE_PUT_OBJECT:
		parkErr = f.putObject(ctx, shard, req, index)
	case shardproto.ApplyRequest_TYPE_DELETE_OBJECT:
		parkErr = f.deleteObject(ctx, shard, req, index)
	case shardproto.ApplyRequest_TYPE_MERGE_OBJECT:
		parkErr = f.mergeObject(ctx, shard, req, index)
	case shardproto.ApplyRequest_TYPE_PUT_OBJECTS_BATCH:
		// Unreachable: DispatchBatch — the only dispatch entry point — routes
		// put-batches through the merged window. Parking (never skipping) a
		// batch that somehow lands here keeps the miswiring loud and lossless.
		parkErr = fmt.Errorf("put-objects-batch reached the single-command path")
	case shardproto.ApplyRequest_TYPE_DELETE_OBJECTS_BATCH:
		parkErr = f.deleteObjectsBatch(ctx, shard, req, index)
	case shardproto.ApplyRequest_TYPE_ADD_REFERENCES:
		parkErr = f.addReferences(ctx, shard, req, index)
	default:
		// Unknown command types are deterministic by construction (the same
		// entry bytes reach every replica): skipped identically, as before.
		f.log.WithField("type", req.Type).Error("unknown command type")
		f.skipDeterministic("unknown_command", index, fmt.Errorf("unknown command type: %v", req.Type))
	}
	return Response{Version: index}, parkErr
}

// skipDeterministic accounts one deterministically-failed materialization
// unit (an item or a whole entry): identical on every replica, so the apply
// path skips it and advances — counted and rate-limit-logged, never silent.
func (f *FSM) skipDeterministic(op string, index uint64, err error) {
	shardRaftApplySkipped.WithLabelValues(f.className, f.shardName, skipReasonDeterministic).Inc()
	if applySkipLog.Allow(f.className + "/" + f.shardName) {
		f.log.WithFields(logrus.Fields{"op": op, "index": index}).
			Errorf("apply: deterministic error, skipped identically on every replica: %v", err)
	}
}

// abandonDropped accounts one entry abandoned by the schema fence: the local
// schema is at or past the version the write carried and the class is absent,
// so the class was dropped after the write was admitted. Every replica
// reaches the same verdict; the entry is skipped and the applied index
// advances.
func (f *FSM) abandonDropped(index uint64) {
	shardRaftApplySkipped.WithLabelValues(f.className, f.shardName, skipReasonClassDropped).Inc()
	if applySkipLog.Allow(f.className + "/" + f.shardName) {
		f.log.WithField("index", index).
			Warn("apply: class dropped after admission, entry abandoned deterministically")
	}
}

// classifyOpErr folds one shard-operation result into the entry outcome: nil
// passes through, an explicitly-marked deterministic error is skipped
// (counted+logged, nil), anything else parks the entry.
func (f *FSM) classifyOpErr(op string, index uint64, err error) error {
	if err == nil {
		return nil
	}
	if enterrors.IsDeterministic(err) {
		f.skipDeterministic(op, index, err)
		return nil
	}
	return err
}

// fenceSchema gates one entry's materialization on the schema version the
// write carries. version 0 is the legacy passthrough (no stamp, no fence).
// Otherwise the local schema must reach the stamped version first: a wait
// failure parks the entry (the retry re-fences); at or past the stamp with
// the class absent, the write was admitted before a drop — abandon=true, the
// deterministic skip every replica agrees on. No error-string logic anywhere.
func (f *FSM) fenceSchema(sh shard, version uint64) (abandon bool, parkErr error) {
	if version == 0 {
		return false, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), schemaFenceTimeout)
	defer cancel()
	if err := sh.WaitForSchemaVersion(ctx, version); err != nil {
		return false, fmt.Errorf("schema fence: waiting for schema version %d: %w", version, err)
	}
	return !sh.ClassPresent(), nil
}

// applySkipLog rate-limits the deterministic-skip and fence-abandon log lines
// per shard: a poisoned batch or a dropped class's backlog would otherwise
// emit one line per item.
var applySkipLog = newLogLimiter(time.Second)

// fsmCmd is one committed EntryNormal handed to DispatchBatch: payload is the
// command body with the request-ID prefix already stripped — nil for empty
// leader entries and malformed entries, which materialize as no-ops — and
// index is the entry's RAFT log index.
type fsmCmd struct {
	payload []byte
	index   uint64
}

// defaultApplyCoalesceBytes caps the object payload of one merged put window:
// half the MaxCommittedSizePerReady pipeline quota, so MsgStorageApplyResp
// acks release progressively — while one window materializes, up to a
// window's worth of newly committed entries flows in behind it
// (double-buffering) instead of the whole quota parking behind one
// mega-round.
const defaultApplyCoalesceBytes = defaultMaxCommittedSizePerReady / 2

// putWindow accumulates a run of consecutive PUT_OBJECTS_BATCH commands (plus
// no-op and decode-failure members, which write nothing) into one merged
// shard round. ranges maps merged object offsets back to their source log
// index for per-object error attribution.
type putWindow struct {
	active bool
	from   int // cmd index of the first member
	to     int // cmd index of the last member
	last   uint64
	objs   []*storobj.Object
	uuids  map[strfmt.UUID]struct{}
	bytes  int
	resps  []Response
	ranges []putWindowRange
}

type putWindowRange struct {
	startOff int
	logIndex uint64
	cmdIdx   int
}

// add admits one member. objs is nil for members that write nothing.
func (w *putWindow) add(cmdIdx int, index uint64, resp Response, objs []*storobj.Object, size int) {
	if !w.active {
		w.active = true
		w.from = cmdIdx
	}
	w.to = cmdIdx
	w.last = index
	w.resps = append(w.resps, resp)
	if len(objs) > 0 {
		w.ranges = append(w.ranges, putWindowRange{startOff: len(w.objs), logIndex: index, cmdIdx: cmdIdx})
		w.objs = append(w.objs, objs...)
		w.bytes += size
		if w.uuids == nil {
			w.uuids = make(map[strfmt.UUID]struct{}, len(objs))
		}
		for _, o := range objs {
			w.uuids[o.Object.ID] = struct{}{}
		}
	}
}

// blocks reports whether admitting an entry with these objects requires
// flushing first: a UUID already present in the window (cross-entry
// duplicates must be adjudicated by the LWW replay guard in log order — the
// shard batcher's keep-last dedupe would otherwise make the outcome depend on
// how this replica happened to partition the backlog: replica divergence), or
// the window byte cap.
func (w *putWindow) blocks(objs []*storobj.Object, size int) bool {
	if !w.active {
		return false
	}
	if w.bytes > 0 && w.bytes+size > defaultApplyCoalesceBytes {
		return true
	}
	for _, o := range objs {
		if _, dup := w.uuids[o.Object.ID]; dup {
			return true
		}
	}
	return false
}

// logIndexOf attributes a merged object offset to its source log index.
func (w *putWindow) logIndexOf(off int) uint64 {
	idx := uint64(0)
	for _, r := range w.ranges {
		if r.startOff > off {
			break
		}
		idx = r.logIndex
	}
	return idx
}

// rangeAt attributes a merged object offset to its owning member: cmd index,
// log index, and the member's first merged offset.
func (w *putWindow) rangeAt(off int) putWindowRange {
	r := w.ranges[0]
	for _, cand := range w.ranges {
		if cand.startOff > off {
			break
		}
		r = cand
	}
	return r
}

func (w *putWindow) reset() {
	*w = putWindow{}
}

// entryPark reports materialization stopping at one committed entry for an
// environmental reason: everything before cmd fully materialized (the applied
// index covers exactly that prefix), the entry at cmd and everything after
// did not run and must be re-dispatched — the parked entry IN FULL (the LWW
// replay guard makes re-applying its already-landed items harmless; no
// sub-entry bookkeeping exists — Decision C in the storage-error taxonomy).
type entryPark struct {
	cmd   int    // index into the cmds slice DispatchBatch was given
	index uint64 // the parked entry's raft log index
	err   error  // the environmental error that parked it
}

// DispatchBatch applies a run of committed command entries in log order,
// merging consecutive PUT_OBJECTS_BATCH commands into fewer, larger shard
// rounds — one PutObjectBatch call materializes many entries, amortizing the
// per-round LSM overhead (WAL walks, queue flushes, tracker rewrites, fan-out
// barriers) that made one-round-per-entry the apply lane's bottleneck. All
// other command types dispatch singly, in order. Windows split where merging
// could diverge from sequential apply: when an entry writes a UUID the window
// already contains (see putWindow.blocks) and at the window byte cap.
//
// The applied watermark advances only over fully materialized entries. On a
// completed unit — a merged window or a single command — it advances to the
// unit's last index. On an environmental failure it advances exactly to the
// last complete entry BEFORE the failing one and DispatchBatch returns an
// entryPark: writes are never discarded, so the parked entry and everything
// after wait for the apply worker's retry (which re-dispatches from the
// parked entry; the LWW replay guard absorbs the re-run). Deterministic
// failures — explicitly marked at the shard boundary, decode failures, and
// fence-established drop-after-admission — never park: they are skipped
// identically on every replica, counted and logged.
//
// Every entry carrying a schema version is fenced before materializing: the
// local schema must reach the stamped version (wait failure = park), and at
// or past it a missing class abandons the entry deterministically (the class
// was dropped after admission). Version 0 passes through unfenced.
//
// onUnit is invoked after each unit materializes, with the cmd-index range it
// covered and one Response per cmd; returning false aborts the run (store
// shutdown), reported as ok=false. It is invoked single-threaded from the
// Store's apply worker; the shard reference is read once per run.
func (f *FSM) DispatchBatch(cmds []fsmCmd, onUnit func(from, to int, resps []Response) bool) (parked *entryPark, ok bool) {
	sh := f.getShard()
	ctx := objects.WithLWWReplayGuard(context.Background())

	var w putWindow
	flushWindow := func() (*entryPark, bool) {
		if !w.active {
			return nil, true
		}
		var park *entryPark
		if len(w.objs) > 0 {
			var pOK bool
			park, pOK = f.materializeWindow(ctx, sh, &w)
			if !pOK {
				// The window parked at its first member: nothing landed, no
				// unit completed. (pOK is only false alongside a park.)
				return park, true
			}
		}
		if park != nil {
			// A prefix of the window landed: applied advances exactly to the
			// last complete entry, its responses deliver, the rest waits.
			prefix := park.cmd - w.from
			f.setApplied(w.resps[prefix-1].Version)
			f.observeApplyUnit(prefix)
			if !onUnit(w.from, park.cmd-1, w.resps[:prefix]) {
				return nil, false
			}
			return park, true
		}
		f.setApplied(w.last)
		f.observeApplyUnit(w.to - w.from + 1)
		unitOK := onUnit(w.from, w.to, w.resps)
		w.reset()
		if !unitOK {
			return nil, false
		}
		return nil, true
	}

	for i := range cmds {
		payload, index := cmds[i].payload, cmds[i].index

		// Empty and malformed entries write nothing: they ride the current
		// window so their applied-index bookkeeping can never run ahead of an
		// unmaterialized earlier entry.
		if payload == nil {
			w.add(i, index, Response{Version: index}, nil, 0)
			continue
		}
		if sh == nil {
			// Shard-not-set: surface per entry, one unit each.
			if park, flushOK := flushWindow(); !flushOK {
				return nil, false
			} else if park != nil {
				return park, true
			}
			f.log.Error("shard not set, cannot apply log entry")
			f.setApplied(index)
			f.observeApplyUnit(1)
			if !onUnit(i, i, []Response{{Version: index, Error: fmt.Errorf("shard not set")}}) {
				return nil, false
			}
			continue
		}
		req, errResp := f.decodeRequest(payload, index)
		if req == nil {
			// Decode failures write nothing — a window member carrying its
			// error Response.
			w.add(i, index, errResp, nil, 0)
			continue
		}
		if req.Type == shardproto.ApplyRequest_TYPE_PUT_OBJECTS_BATCH {
			objs, size, ver, decOK := f.decodePutObjectsBatch(req)
			if !decOK {
				// Deserialize failure: deterministic (same bytes everywhere),
				// counted at decode — a no-op member.
				w.add(i, index, Response{Version: index}, nil, 0)
				continue
			}
			abandon, ferr := f.fenceSchema(sh, ver)
			if ferr != nil {
				// The fence wait failed: flush the window (earlier entries
				// land) and park at this entry — the retry re-fences.
				if park, flushOK := flushWindow(); !flushOK {
					return nil, false
				} else if park != nil {
					return park, true
				}
				return &entryPark{cmd: i, index: index, err: ferr}, true
			}
			if abandon {
				f.abandonDropped(index)
				w.add(i, index, Response{Version: index}, nil, 0)
				continue
			}
			if w.blocks(objs, size) {
				if park, flushOK := flushWindow(); !flushOK {
					return nil, false
				} else if park != nil {
					return park, true
				}
			}
			w.add(i, index, Response{Version: index}, objs, size)
			continue
		}

		// Any other command type is its own unit, dispatched in order.
		if park, flushOK := flushWindow(); !flushOK {
			return nil, false
		} else if park != nil {
			return park, true
		}
		resp, parkErr := f.applyCommand(sh, req, index)
		if parkErr != nil {
			return &entryPark{cmd: i, index: index, err: parkErr}, true
		}
		f.setApplied(index)
		f.observeApplyUnit(1)
		if !onUnit(i, i, []Response{resp}) {
			return nil, false
		}
	}
	return flushWindow()
}

// materializeWindow runs one merged window's shard round and classifies its
// per-item outcomes. Returns (nil, true) when every item landed or failed
// deterministically (all skips counted); (park, true) when a strict prefix of
// the window's members completed and the member at park.cmd must wait —
// deterministic skips are counted only for that completed prefix, because the
// parked member re-runs in full; (park, false) when the FIRST member parked
// and nothing about the window may be recorded.
//
// Attribution: an error slice shorter than the object count is the legacy
// whole-batch shape (e.g. shard-level read-only collapses to one element) —
// its first non-nil error covers every object.
func (f *FSM) materializeWindow(ctx context.Context, sh shard, w *putWindow) (*entryPark, bool) {
	errs := sh.PutObjectBatch(ctx, w.objs)

	var wholeBatch error
	if len(errs) != len(w.objs) {
		for _, e := range errs {
			if e != nil {
				wholeBatch = e
				break
			}
		}
	}
	itemErr := func(off int) error {
		if wholeBatch != nil {
			return wholeBatch
		}
		return errs[off]
	}

	// First environmental failure, in merged (= log) order, parks its entry.
	parkOff := -1
	var parkErr error
	for off := range w.objs {
		err := itemErr(off)
		if err == nil || enterrors.IsDeterministic(err) {
			continue
		}
		parkOff, parkErr = off, err
		break
	}

	// Deterministic skips are final only for members that fully completed:
	// all of them when nothing parked, otherwise only offsets before the
	// parked member's first offset (the parked member re-runs in full).
	countThrough := len(w.objs)
	var park *entryPark
	if parkOff >= 0 {
		r := w.rangeAt(parkOff)
		countThrough = r.startOff
		park = &entryPark{cmd: r.cmdIdx, index: r.logIndex, err: parkErr}
	}
	for off := 0; off < countThrough; off++ {
		if err := itemErr(off); err != nil {
			f.skipDeterministic("put_objects_batch", w.logIndexOf(off), err)
		}
	}

	if park != nil && park.cmd == w.from {
		return park, false
	}
	return park, true
}

// observeApplyUnit records one materialization unit's entry count — the apply
// lane's coalescing signal.
func (f *FSM) observeApplyUnit(entries int) {
	shardRaftApplyWindowEntries.WithLabelValues(f.className, f.shardName).Observe(float64(entries))
}

// decodePutObjectsBatch deserializes a put-batch command's objects, returning
// ok=false — with the failure counted as a deterministic skip (identical
// bytes fail identically on every replica) — when the sub-command or any
// object is undecodable. size is the serialized payload size, the window
// byte-cap measure; version is the schema version the coordinator stamped.
func (f *FSM) decodePutObjectsBatch(req *shardproto.ApplyRequest) (objs []*storobj.Object, size int, version uint64, ok bool) {
	var subreq shardproto.PutObjectsBatchRequest
	if err := proto.Unmarshal(req.SubCommand, &subreq); err != nil {
		f.skipDeterministic("put_objects_batch_unmarshal", 0, err)
		return nil, 0, 0, false
	}
	objs = make([]*storobj.Object, len(subreq.Objects))
	for i, raw := range subreq.Objects {
		obj, err := storobj.FromBinaryNetwork(raw)
		if err != nil {
			f.skipDeterministic("put_objects_batch_deserialize", 0, fmt.Errorf("object %d: %w", i, err))
			return nil, 0, 0, false
		}
		objs[i] = obj
		size += len(raw)
	}
	return objs, size, subreq.SchemaVersion, true
}

// putObject applies a PUT_OBJECT command to the shard. The returned error is
// the entry's park error (see applyCommand).
func (f *FSM) putObject(ctx context.Context, shard shard, req *shardproto.ApplyRequest, index uint64) error {
	var subreq shardproto.PutObjectRequest
	if err := proto.Unmarshal(req.SubCommand, &subreq); err != nil {
		f.skipDeterministic("put_object_unmarshal", index, err)
		return nil
	}

	obj, err := storobj.FromBinaryNetwork(subreq.Object)
	if err != nil {
		f.skipDeterministic("put_object_deserialize", index, err)
		return nil
	}

	if abandon, ferr := f.fenceSchema(shard, subreq.SchemaVersion); ferr != nil {
		return ferr
	} else if abandon {
		f.abandonDropped(index)
		return nil
	}

	return f.classifyOpErr("put_object", index, shard.PutObject(ctx, obj))
}

// deleteObject applies a DELETE_OBJECT command to the shard. The returned
// error is the entry's park error (see applyCommand).
func (f *FSM) deleteObject(ctx context.Context, shard shard, req *shardproto.ApplyRequest, index uint64) error {
	var subreq shardproto.DeleteObjectRequest
	if err := proto.Unmarshal(req.SubCommand, &subreq); err != nil {
		f.skipDeterministic("delete_object_unmarshal", index, err)
		return nil
	}

	id := strfmt.UUID(subreq.Id)

	var deletionTime time.Time
	if subreq.DeletionTimeUnix != 0 {
		deletionTime = time.Unix(0, subreq.DeletionTimeUnix)
	}

	if abandon, ferr := f.fenceSchema(shard, subreq.SchemaVersion); ferr != nil {
		return ferr
	} else if abandon {
		f.abandonDropped(index)
		return nil
	}

	return f.classifyOpErr("delete_object", index, shard.DeleteObject(ctx, id, deletionTime))
}

// mergeObject applies a MERGE_OBJECT command to the shard. The returned error
// is the entry's park error (see applyCommand).
func (f *FSM) mergeObject(ctx context.Context, shard shard, req *shardproto.ApplyRequest, index uint64) error {
	var subreq shardproto.MergeObjectRequest
	if err := proto.Unmarshal(req.SubCommand, &subreq); err != nil {
		f.skipDeterministic("merge_object_unmarshal", index, err)
		return nil
	}

	var doc objects.MergeDocument
	if err := json.Unmarshal(subreq.MergeDocumentJson, &doc); err != nil {
		f.skipDeterministic("merge_object_unmarshal", index, err)
		return nil
	}

	if abandon, ferr := f.fenceSchema(shard, subreq.SchemaVersion); ferr != nil {
		return ferr
	} else if abandon {
		f.abandonDropped(index)
		return nil
	}

	return f.classifyOpErr("merge_object", index, shard.MergeObject(ctx, doc))
}

// deleteObjectsBatch applies a DELETE_OBJECTS_BATCH command to the shard. The
// first environmental per-item failure parks the whole entry (it re-runs in
// full; the LWW replay guard absorbs re-applied deletes); deterministic
// per-item failures are counted and skipped. The returned error is the
// entry's park error (see applyCommand).
func (f *FSM) deleteObjectsBatch(ctx context.Context, shard shard, req *shardproto.ApplyRequest, index uint64) error {
	var subreq shardproto.DeleteObjectsBatchRequest
	if err := proto.Unmarshal(req.SubCommand, &subreq); err != nil {
		f.skipDeterministic("delete_objects_batch_unmarshal", index, err)
		return nil
	}

	uuids := make([]strfmt.UUID, len(subreq.Uuids))
	for i, id := range subreq.Uuids {
		uuids[i] = strfmt.UUID(id)
	}

	var deletionTime time.Time
	if subreq.DeletionTimeUnix != 0 {
		deletionTime = time.Unix(0, subreq.DeletionTimeUnix)
	}

	if abandon, ferr := f.fenceSchema(shard, subreq.SchemaVersion); ferr != nil {
		return ferr
	} else if abandon {
		f.abandonDropped(index)
		return nil
	}

	results := shard.DeleteObjectBatch(ctx, uuids, deletionTime, subreq.DryRun)
	for i, r := range results {
		if r.Err == nil {
			continue
		}
		if parkErr := f.classifyOpErr("delete_objects_batch", index, fmt.Errorf("item %d: %w", i, r.Err)); parkErr != nil {
			return parkErr
		}
	}
	return nil
}

// addReferences applies an ADD_REFERENCES command to the shard. Per-item
// classification mirrors deleteObjectsBatch. The returned error is the
// entry's park error (see applyCommand).
func (f *FSM) addReferences(ctx context.Context, shard shard, req *shardproto.ApplyRequest, index uint64) error {
	var subreq shardproto.AddReferencesRequest
	if err := proto.Unmarshal(req.SubCommand, &subreq); err != nil {
		f.skipDeterministic("add_references_unmarshal", index, err)
		return nil
	}

	var refs objects.BatchReferences
	if err := json.Unmarshal(subreq.ReferencesJson, &refs); err != nil {
		f.skipDeterministic("add_references_unmarshal", index, err)
		return nil
	}

	if abandon, ferr := f.fenceSchema(shard, subreq.SchemaVersion); ferr != nil {
		return ferr
	} else if abandon {
		f.abandonDropped(index)
		return nil
	}

	errs := shard.AddReferencesBatch(ctx, refs)
	for i, err := range errs {
		if err == nil {
			continue
		}
		if parkErr := f.classifyOpErr("add_references", index, fmt.Errorf("item %d: %w", i, err)); parkErr != nil {
			return parkErr
		}
	}
	return nil
}

// SnapshotMetadata returns a snapshot of the FSM's current identity and applied
// index. The Store uses it to build a SnapshotRequest for the Snapshotter pool.
func (f *FSM) SnapshotMetadata() shardSnapshotData {
	return shardSnapshotData{
		ClassName:        f.className,
		ShardName:        f.shardName,
		NodeID:           f.nodeID,
		LastAppliedIndex: f.lastAppliedIndex.Load(),
	}
}

// RestoreFromSnapshot restores FSM state from a RAFT snapshot's metadata. The
// Store's Ready loop calls it when etcd/raft delivers a non-empty rd.Snapshot.
// If the snapshot was created by a different node (foreign snapshot), it
// triggers an out-of-band state transfer to download shard data from the
// current leader. The StateTransferer determines the leader dynamically — we
// don't use meta.NodeID for leader determination since the leader may have
// changed between snapshot creation and restore.
func (f *FSM) RestoreFromSnapshot(meta shardSnapshotData) error {
	// Verify snapshot is for the correct shard
	if meta.ClassName != f.className || meta.ShardName != f.shardName {
		return fmt.Errorf("snapshot class/shard mismatch: expected %s/%s, got %s/%s",
			f.className, f.shardName, meta.ClassName, meta.ShardName)
	}

	// Trigger state transfer if this is a foreign snapshot (from another node).
	f.mu.RLock()
	st := f.stateTransferer
	f.mu.RUnlock()

	if meta.NodeID != f.nodeID && st != nil {
		f.log.WithFields(logrus.Fields{
			"snapshot_node_id": meta.NodeID,
			"local_node_id":    f.nodeID,
		}).Info("foreign snapshot detected, initiating state transfer")

		ctx := context.Background()
		if err := st.TransferState(ctx, f.className, f.shardName); err != nil {
			return fmt.Errorf("state transfer for %s/%s: %w", f.className, f.shardName, err)
		}
	}

	f.setApplied(meta.LastAppliedIndex)
	f.log.WithField("lastAppliedIndex", meta.LastAppliedIndex).Info("restored from snapshot")

	return nil
}

// LastAppliedIndex returns the last RAFT log index that was applied.
func (f *FSM) LastAppliedIndex() uint64 {
	return f.lastAppliedIndex.Load()
}

// WaitForIndex blocks until the FSM has applied at least targetIndex, or the
// context is cancelled. This is used by followers to ensure their local state
// has caught up to the leader before performing a local read.
func (f *FSM) WaitForIndex(ctx context.Context, targetIndex uint64) error {
	// Fast path: already caught up.
	if f.lastAppliedIndex.Load() >= targetIndex {
		return nil
	}

	// Spawn a goroutine that broadcasts on context cancellation so that
	// the cond.Wait loop can observe ctx.Done.
	stopWaker := make(chan struct{})
	defer close(stopWaker)
	enterrors.GoWrapper(func() {
		select {
		case <-ctx.Done():
			// Broadcast under indexMu so the wakeup cannot land between the
			// waiter's ctx check and its wait registration and be lost (the
			// same lost-wakeup shape setApplied guards against).
			f.indexMu.Lock()
			f.indexCond.Broadcast()
			f.indexMu.Unlock()
		case <-stopWaker:
		}
	}, f.log)

	if err := func() error {
		f.indexMu.Lock()
		defer f.indexMu.Unlock()
		for f.lastAppliedIndex.Load() < targetIndex {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			f.indexCond.Wait()
		}
		return nil
	}(); err != nil {
		return err
	}

	return nil
}

// shardSnapshotData is the JSON-serializable snapshot data structure. It is
// the payload of raftpb.Snapshot.Data and the content of each .snap file the
// Snapshotter writes.
type shardSnapshotData struct {
	ClassName        string `json:"class_name"`
	ShardName        string `json:"shard_name"`
	NodeID           string `json:"node_id"`
	LastAppliedIndex uint64 `json:"last_applied_index"`
}

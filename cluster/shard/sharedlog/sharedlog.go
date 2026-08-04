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
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

const (
	defaultBatchMaxWait = time.Millisecond
	defaultBatchMaxSize = 64
)

var ErrStoreClosed = errors.New("sharedlog: store is closed")

type Options struct {
	Path         string
	BatchMaxWait time.Duration
	BatchMaxSize int
	Logger       logrus.FieldLogger

	// SegmentMaxBytes caps a WAL segment file; the writer rotates when a
	// batch would exceed it (a single oversized batch is still written
	// whole). 0 means the 64MiB default. Test hook — production uses the
	// default.
	SegmentMaxBytes int64

	// BeforeFlush / AfterFlush are test instrumentation hooks bracketing each
	// batch commit (one WAL batch write = one fsync). Nil in production.
	BeforeFlush func()
	AfterFlush  func()
}

func (o *Options) applyDefaults() {
	if o.BatchMaxWait <= 0 {
		o.BatchMaxWait = defaultBatchMaxWait
	}
	if o.BatchMaxSize <= 0 {
		o.BatchMaxSize = defaultBatchMaxSize
	}
	if o.SegmentMaxBytes <= 0 {
		o.SegmentMaxBytes = defaultSegmentMaxBytes
	}
}

// GroupWrite is a single group's contribution to a batch. All fields
// are applied atomically with the rest of the batch.
type GroupWrite struct {
	GroupID uint64

	// Entries: any existing entries with index >= Entries[0].Index are
	// deleted first, so a new leader can overwrite a follower's tail.
	Entries []raftpb.Entry

	HardState *raftpb.HardState
	ConfState *raftpb.ConfState
	Snapshot  *raftpb.Snapshot
}

type batchReq struct {
	write GroupWrite
	done  chan error
}

// Store is safe for concurrent use; all writes funnel through a single
// batcher goroutine that performs one WAL batch write (one fsync) per
// batch regardless of how many groups contributed.
type Store struct {
	opts Options
	log  logrus.FieldLogger
	w    *wal

	reqCh          chan *batchReq
	shutdown       chan struct{}
	batcherStopped chan struct{}

	closeMu  sync.Mutex
	closed   bool
	inflight atomic.Int64

	// lastSlowFlushLog rate-limits the slow-flush WARN; touched only by the
	// batcher goroutine.
	lastSlowFlushLog time.Time
}

// Open opens or creates the WAL directory at Options.Path, rebuilds the
// in-memory index from the segments, and starts the batcher goroutine.
// Call Close to flush and shut down.
func Open(opts Options) (*Store, error) {
	if opts.Path == "" {
		return nil, fmt.Errorf("sharedlog: Options.Path is required")
	}
	if opts.Logger == nil {
		return nil, fmt.Errorf("sharedlog: Options.Logger is required")
	}
	opts.applyDefaults()

	log := opts.Logger.WithField("component", "sharedlog")
	w, err := openWAL(opts.Path, opts.SegmentMaxBytes, log)
	if err != nil {
		return nil, fmt.Errorf("sharedlog: open WAL at %s: %w", opts.Path, err)
	}

	s := &Store{
		opts:           opts,
		log:            log,
		w:              w,
		reqCh:          make(chan *batchReq, opts.BatchMaxSize*2),
		shutdown:       make(chan struct{}),
		batcherStopped: make(chan struct{}),
	}

	enterrors.GoWrapper(s.batcherLoop, s.log)
	return s, nil
}

// Close drains in-flight Appends, stops the batcher, and closes the
// WAL files. Safe to call multiple times.
func (s *Store) Close() error {
	s.closeMu.Lock()
	if s.closed {
		s.closeMu.Unlock()
		return nil
	}
	s.closed = true
	s.closeMu.Unlock()

	// The batcher must keep running until inflight Appends finish, or
	// they'd hang waiting on req.done.
	for s.inflight.Load() > 0 {
		time.Sleep(time.Millisecond)
	}

	close(s.shutdown)
	<-s.batcherStopped

	if err := s.w.close(); err != nil {
		return fmt.Errorf("sharedlog: close WAL: %w", err)
	}
	return nil
}

// Append blocks until the containing batch has been fsynced.
//
// If ctx is cancelled before the batch completes, the underlying write
// may still complete in the background (at-least-once on cancel) —
// callers needing strict at-most-once must serialise their own retries.
func (s *Store) Append(ctx context.Context, w GroupWrite) error {
	done, err := s.AppendAsync(ctx, w)
	if err != nil {
		return err
	}
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// AppendAsync queues one group's write for the batcher and returns a
// buffered channel that resolves with the flush result once the containing
// batch has been fsynced — the pipelining half of Append: a caller can keep
// submitting while earlier writes await their covering fsync, so every write
// arriving during a flush rides the next flush together (one fsync covers
// them all). Results resolve in submission order per caller goroutine (the
// batcher is single-goroutine and FIFO).
//
// ctx bounds only the submission (a full request queue); once queued, the
// write always completes and its channel always resolves — an abandoned
// channel cannot wedge Close, because the store's inflight accounting is
// released by the batcher when the result is delivered, not by the reader.
func (s *Store) AppendAsync(ctx context.Context, w GroupWrite) (<-chan error, error) {
	s.closeMu.Lock()
	if s.closed {
		s.closeMu.Unlock()
		return nil, ErrStoreClosed
	}
	s.inflight.Add(1)
	s.closeMu.Unlock()

	req := &batchReq{write: w, done: make(chan error, 1)}
	select {
	case s.reqCh <- req:
		return req.done, nil
	case <-ctx.Done():
		s.inflight.Add(-1)
		return nil, ctx.Err()
	}
}

func (s *Store) Storage(groupID uint64) raft.Storage {
	return &groupStorage{store: s, groupID: groupID}
}

// HasGroup returns true only if a HardState has been persisted for the
// group; entries alone do not count, so callers can use this to gate
// the bootstrap-vs-restart decision.
func (s *Store) HasGroup(groupID uint64) (bool, error) {
	return s.w.hasGroup(groupID), nil
}

// PoisonedReason reports whether boot validation quarantined the group (its
// rebuilt index violated the snapshot-servability invariant) and the reason.
// A poisoned group's Store must refuse to start; DeleteGroup clears the
// quarantine along with the damaged state.
func (s *Store) PoisonedReason(groupID uint64) (string, bool) {
	return s.w.poisonedReason(groupID)
}

// PoisonedGroupCount returns the number of groups currently quarantined by
// boot validation, for operator-facing stats.
func (s *Store) PoisonedGroupCount() int {
	return s.w.poisonedCount()
}

// Compact drops entries with index < idx for this group. Idempotent. Caller
// must ensure a snapshot at >= idx-1 has been written first (etcd/raft's
// standard compaction invariant); otherwise FirstIndex will not reflect the
// new lower bound. Compact is an in-memory index operation: the persisted
// snapshot metadata is the durable compaction floor and replay prunes below
// it on restart, so an out-of-contract compaction (no covering snapshot)
// additionally does not survive a restart.
func (s *Store) Compact(groupID, idx uint64) error {
	s.w.compact(groupID, idx)
	return nil
}

// DeleteGroup removes every record for the group via a durably fsynced
// tombstone. Idempotent.
//
// The caller must have stopped the group's Ready loop first: after the
// barrier below, a still-running loop could re-Append state and resurrect
// the group.
func (s *Store) DeleteGroup(groupID uint64) error {
	s.closeMu.Lock()
	if s.closed {
		s.closeMu.Unlock()
		return ErrStoreClosed
	}
	// Two inflight slots: the flush barrier below (released by the batcher
	// at result delivery, like any request) and the delete itself.
	s.inflight.Add(2)
	s.closeMu.Unlock()
	defer s.inflight.Add(-1)

	// Flush barrier: a stopped Ready loop abandons its tickets, but writes
	// it already submitted still sit in the batcher pipeline. Without
	// draining them first, a final append could re-apply the group AFTER
	// the tombstone — and its record would land after the tombstone on
	// disk, resurrecting the group across a restart as well. The barrier
	// rides the batcher FIFO, so everything submitted before this call is
	// applied before the tombstone is written.
	barrier := &batchReq{done: make(chan error, 1)}
	s.reqCh <- barrier
	<-barrier.done

	if err := s.w.deleteGroup(groupID); err != nil {
		return fmt.Errorf("sharedlog: DeleteGroup: %w", err)
	}
	return nil
}

func (s *Store) batcherLoop() {
	defer close(s.batcherStopped)

	pending := make([]*batchReq, 0, s.opts.BatchMaxSize)
	flushPending := func() {
		if len(pending) > 0 {
			s.flush(pending)
			pending = pending[:0]
		}
	}

	ticker := time.NewTicker(s.opts.BatchMaxWait)
	defer ticker.Stop()

	for {
		select {
		case <-s.shutdown:
			// Close waits for inflight=0 before signalling, so reqCh
			// should be empty; drain anyway in case of a races we
			// haven't proven absent.
		drainLoop:
			for {
				select {
				case req := <-s.reqCh:
					pending = append(pending, req)
				default:
					break drainLoop
				}
			}
			flushPending()
			return

		case req := <-s.reqCh:
			pending = append(pending, req)
			if len(pending) >= s.opts.BatchMaxSize {
				flushPending()
			}

		case <-ticker.C:
			flushPending()
		}
	}
}

func (s *Store) flush(reqs []*batchReq) {
	if s.opts.BeforeFlush != nil {
		s.opts.BeforeFlush()
	}
	start := time.Now()
	writes := make([]*GroupWrite, len(reqs))
	for i := range reqs {
		writes[i] = &reqs[i].write
	}
	err := s.w.writeBatch(writes)
	dur := time.Since(start)
	flushSeconds.Observe(dur.Seconds())
	flushBatchSize.Observe(float64(len(reqs)))
	if dur > slowFlushThreshold && time.Since(s.lastSlowFlushLog) >= time.Second {
		// Batcher is single-goroutine, so the plain field is race-free.
		s.lastSlowFlushLog = time.Now()
		s.log.Warnf("shared raft log flush took %s for a batch of %d group writes", dur, len(reqs))
	}
	if s.opts.AfterFlush != nil {
		s.opts.AfterFlush()
	}
	for _, r := range reqs {
		r.done <- err
		// Inflight is held from submission to result delivery (see
		// AppendAsync): releasing it here rather than at the waiter keeps
		// Close's drain independent of whether anyone still reads done.
		s.inflight.Add(-1)
	}
}

// slowFlushThreshold mirrors the Ready-loop stall budget: a flush beyond it
// delays every group in the batch by that much before their acks can leave.
const slowFlushThreshold = 100 * time.Millisecond

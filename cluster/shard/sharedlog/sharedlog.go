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
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"go.etcd.io/bbolt"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

const (
	bucketEntries   = "entries"
	bucketState     = "state"
	bucketConfState = "confstate"
	bucketSnapMeta  = "snapmeta"

	defaultBatchMaxWait = time.Millisecond
	defaultBatchMaxSize = 64
)

var ErrStoreClosed = errors.New("sharedlog: store is closed")

type Options struct {
	Path         string
	BatchMaxWait time.Duration
	BatchMaxSize int
	Logger       logrus.FieldLogger
}

func (o *Options) applyDefaults() {
	if o.BatchMaxWait <= 0 {
		o.BatchMaxWait = defaultBatchMaxWait
	}
	if o.BatchMaxSize <= 0 {
		o.BatchMaxSize = defaultBatchMaxSize
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
// batcher goroutine that performs one bbolt commit (one fsync) per
// batch regardless of how many groups contributed.
type Store struct {
	opts Options
	log  logrus.FieldLogger
	db   *bbolt.DB

	reqCh          chan *batchReq
	shutdown       chan struct{}
	batcherStopped chan struct{}

	closeMu  sync.Mutex
	closed   bool
	inflight atomic.Int64
}

// Open opens or creates the bbolt file and starts the batcher goroutine.
// Call Close to flush and shut down.
func Open(opts Options) (*Store, error) {
	if opts.Path == "" {
		return nil, fmt.Errorf("sharedlog: Options.Path is required")
	}
	if opts.Logger == nil {
		return nil, fmt.Errorf("sharedlog: Options.Logger is required")
	}
	opts.applyDefaults()

	if err := os.MkdirAll(filepath.Dir(opts.Path), 0o755); err != nil {
		return nil, fmt.Errorf("sharedlog: mkdir %s: %w", filepath.Dir(opts.Path), err)
	}

	db, err := bbolt.Open(opts.Path, 0o600, &bbolt.Options{
		Timeout:         time.Second,
		InitialMmapSize: 16 * 1024 * 1024,
	})
	if err != nil {
		return nil, fmt.Errorf("sharedlog: open bbolt at %s: %w", opts.Path, err)
	}

	if err := db.Update(func(tx *bbolt.Tx) error {
		for _, name := range []string{bucketEntries, bucketState, bucketConfState, bucketSnapMeta} {
			if _, err := tx.CreateBucketIfNotExists([]byte(name)); err != nil {
				return fmt.Errorf("create bucket %s: %w", name, err)
			}
		}
		return nil
	}); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("sharedlog: init buckets: %w", err)
	}

	s := &Store{
		opts:           opts,
		log:            opts.Logger.WithField("component", "sharedlog"),
		db:             db,
		reqCh:          make(chan *batchReq, opts.BatchMaxSize*2),
		shutdown:       make(chan struct{}),
		batcherStopped: make(chan struct{}),
	}

	enterrors.GoWrapper(s.batcherLoop, s.log)
	return s, nil
}

// Close drains in-flight Appends, stops the batcher, and closes the
// bbolt file. Safe to call multiple times.
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

	if err := s.db.Close(); err != nil {
		return fmt.Errorf("sharedlog: close bbolt: %w", err)
	}
	return nil
}

// Append blocks until the containing batch has been fsynced.
//
// If ctx is cancelled before the batch completes, the underlying write
// may still complete in the background (at-least-once on cancel) —
// callers needing strict at-most-once must serialise their own retries.
func (s *Store) Append(ctx context.Context, w GroupWrite) error {
	s.closeMu.Lock()
	if s.closed {
		s.closeMu.Unlock()
		return ErrStoreClosed
	}
	s.inflight.Add(1)
	s.closeMu.Unlock()
	defer s.inflight.Add(-1)

	req := &batchReq{write: w, done: make(chan error, 1)}
	select {
	case s.reqCh <- req:
	case <-ctx.Done():
		return ctx.Err()
	}

	select {
	case err := <-req.done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Store) Storage(groupID uint64) raft.Storage {
	return &groupStorage{store: s, groupID: groupID}
}

// HasGroup returns true only if a HardState has been persisted for the
// group; entries alone do not count, so callers can use this to gate
// the bootstrap-vs-restart decision.
func (s *Store) HasGroup(groupID uint64) (bool, error) {
	var present bool
	err := s.db.View(func(tx *bbolt.Tx) error {
		present = tx.Bucket([]byte(bucketState)).Get(encodeGroupKey(groupID)) != nil
		return nil
	})
	if err != nil {
		return false, fmt.Errorf("sharedlog: HasGroup: %w", err)
	}
	return present, nil
}

// Compact deletes entries with index < idx. Idempotent. Caller must
// ensure a snapshot at >= idx-1 has been written first (etcd/raft's
// standard compaction invariant); otherwise FirstIndex will not reflect
// the new lower bound.
func (s *Store) Compact(groupID, idx uint64) error {
	err := s.db.Update(func(tx *bbolt.Tx) error {
		prefix := encodeGroupKey(groupID)
		c := tx.Bucket([]byte(bucketEntries)).Cursor()
		for k, _ := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, _ = c.Next() {
			if decodeEntryIndex(k) >= idx {
				break
			}
			if err := c.Delete(); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("sharedlog: Compact: %w", err)
	}
	return nil
}

// DeleteGroup removes every record for the group. Idempotent.
func (s *Store) DeleteGroup(groupID uint64) error {
	key := encodeGroupKey(groupID)
	err := s.db.Update(func(tx *bbolt.Tx) error {
		eb := tx.Bucket([]byte(bucketEntries))
		c := eb.Cursor()
		for k, _ := c.Seek(key); k != nil && bytes.HasPrefix(k, key); k, _ = c.Next() {
			if err := c.Delete(); err != nil {
				return err
			}
		}
		for _, b := range []string{bucketState, bucketConfState, bucketSnapMeta} {
			if err := tx.Bucket([]byte(b)).Delete(key); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
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
	err := s.db.Update(func(tx *bbolt.Tx) error {
		for _, r := range reqs {
			if e := applyWrite(tx, &r.write); e != nil {
				return e
			}
		}
		return nil
	})
	for _, r := range reqs {
		r.done <- err
	}
}

func applyWrite(tx *bbolt.Tx, w *GroupWrite) error {
	key := encodeGroupKey(w.GroupID)

	if len(w.Entries) > 0 {
		eb := tx.Bucket([]byte(bucketEntries))
		firstIdx := w.Entries[0].Index
		// Erase any prior entries at or above the new first index so a
		// leader-change can overwrite a follower's stale tail.
		c := eb.Cursor()
		startKey := encodeEntryKey(w.GroupID, firstIdx)
		for k, _ := c.Seek(startKey); k != nil && bytes.HasPrefix(k, key); k, _ = c.Next() {
			if err := c.Delete(); err != nil {
				return fmt.Errorf("truncate entries: %w", err)
			}
		}
		for i := range w.Entries {
			data, err := w.Entries[i].Marshal()
			if err != nil {
				return fmt.Errorf("marshal entry: %w", err)
			}
			if err := eb.Put(encodeEntryKey(w.GroupID, w.Entries[i].Index), data); err != nil {
				return fmt.Errorf("put entry: %w", err)
			}
		}
	}

	if w.HardState != nil {
		data, err := w.HardState.Marshal()
		if err != nil {
			return fmt.Errorf("marshal hardstate: %w", err)
		}
		if err := tx.Bucket([]byte(bucketState)).Put(key, data); err != nil {
			return fmt.Errorf("put hardstate: %w", err)
		}
	}

	if w.ConfState != nil {
		data, err := w.ConfState.Marshal()
		if err != nil {
			return fmt.Errorf("marshal confstate: %w", err)
		}
		if err := tx.Bucket([]byte(bucketConfState)).Put(key, data); err != nil {
			return fmt.Errorf("put confstate: %w", err)
		}
	}

	if w.Snapshot != nil && !raft.IsEmptySnap(*w.Snapshot) {
		data, err := w.Snapshot.Marshal()
		if err != nil {
			return fmt.Errorf("marshal snapshot: %w", err)
		}
		if err := tx.Bucket([]byte(bucketSnapMeta)).Put(key, data); err != nil {
			return fmt.Errorf("put snapshot: %w", err)
		}
	}

	return nil
}

// encodeGroupKey uses big-endian so bbolt's lexicographic sort matches
// numeric order — a cursor scan over one group's prefix walks its
// entries in index order.
func encodeGroupKey(groupID uint64) []byte {
	k := make([]byte, 8)
	binary.BigEndian.PutUint64(k, groupID)
	return k
}

func encodeEntryKey(groupID, index uint64) []byte {
	k := make([]byte, 16)
	binary.BigEndian.PutUint64(k[:8], groupID)
	binary.BigEndian.PutUint64(k[8:], index)
	return k
}

func decodeEntryIndex(k []byte) uint64 {
	return binary.BigEndian.Uint64(k[8:])
}

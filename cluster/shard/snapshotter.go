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
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/sirupsen/logrus"
	enterrors "github.com/weaviate/weaviate/entities/errors"
)

var (
	// ErrSnapshotterBusy means every worker is occupied and the queue is
	// full. Snapshots are advisory, so the Store simply retries on a later
	// tick rather than blocking the Ready loop.
	ErrSnapshotterBusy = errors.New("shard: snapshotter queue is full")

	// ErrSnapshotterClosed means Submit was called after Close.
	ErrSnapshotterClosed = errors.New("shard: snapshotter is closed")
)

const snapshotFileSuffix = ".snap"

// memtableFlusher is the one method of the shard interface the snapshotter
// needs. The concrete shard (adapters/repos/db) satisfies it structurally.
type memtableFlusher interface {
	FlushMemtables(ctx context.Context) error
}

// SnapshotRequest is one shard's snapshot job. The Store builds it on the
// Ready loop and submits it; the heavy work then happens on a worker.
type SnapshotRequest struct {
	GroupID      uint64
	ClassName    string
	ShardName    string
	NodeID       string
	AppliedIndex uint64 // the snapshot covers entries up to and including this
	Flusher      memtableFlusher
	Result       chan<- SnapshotResult // worker sends exactly one result here
}

// SnapshotResult carries the outcome back to the Store. On success Metadata
// holds the marshalled snapshot data the Store puts into raftpb.Snapshot.Data
// before calling CreateSnapshot/Compact on the Ready loop.
type SnapshotResult struct {
	GroupID  uint64
	Index    uint64
	Metadata []byte
	Path     string
	Err      error
}

type SnapshotterOptions struct {
	RootDataPath string // snapshots land under <RootDataPath>/raft-snapshots/
	Workers      int    // default: maxConcurrentSnapshots
	Retain       int    // default: nRetainedSnapshots
	Logger       logrus.FieldLogger
}

// Snapshotter runs the slow part of RAFT snapshotting — flushing memtables
// and writing snapshot metadata — on a bounded pool of workers, off the
// Store's single-threaded Ready loop. It deliberately does no raft-storage
// bookkeeping (CreateSnapshot/Compact): mutating etcd's MemoryStorage from a
// worker would race the Ready loop, so the Store does that itself once it
// receives the result.
type Snapshotter struct {
	log    logrus.FieldLogger
	root   string
	retain int

	reqCh chan SnapshotRequest

	closeOnce sync.Once
	closed    chan struct{}
	wg        sync.WaitGroup
}

func NewSnapshotter(opts SnapshotterOptions) *Snapshotter {
	workers := opts.Workers
	if workers <= 0 {
		workers = maxConcurrentSnapshots
	}
	retain := opts.Retain
	if retain <= 0 {
		retain = nRetainedSnapshots
	}

	s := &Snapshotter{
		log:    opts.Logger.WithField("component", "shard_snapshotter"),
		root:   opts.RootDataPath,
		retain: retain,
		// Buffer == workers: a full buffer plus busy workers is the
		// saturation point at which Submit reports ErrSnapshotterBusy.
		reqCh:  make(chan SnapshotRequest, workers),
		closed: make(chan struct{}),
	}
	for i := 0; i < workers; i++ {
		s.wg.Add(1)
		enterrors.GoWrapper(s.worker, s.log)
	}
	return s
}

// Submit queues a snapshot job. It never blocks: if the pool is saturated it
// returns ErrSnapshotterBusy and the caller is expected to retry later.
func (s *Snapshotter) Submit(req SnapshotRequest) error {
	select {
	case <-s.closed:
		return ErrSnapshotterClosed
	default:
	}
	select {
	case s.reqCh <- req:
		return nil
	case <-s.closed:
		return ErrSnapshotterClosed
	default:
		return ErrSnapshotterBusy
	}
}

// Close stops accepting new jobs, lets in-flight and already-queued jobs
// finish, then stops the workers. Idempotent.
func (s *Snapshotter) Close() error {
	s.closeOnce.Do(func() { close(s.closed) })
	s.wg.Wait()
	return nil
}

func (s *Snapshotter) worker() {
	defer s.wg.Done()
	for {
		select {
		case req := <-s.reqCh:
			s.process(req)
		case <-s.closed:
			// Drain whatever is still queued so no submitted job is lost.
			for {
				select {
				case req := <-s.reqCh:
					s.process(req)
				default:
					return
				}
			}
		}
	}
}

func (s *Snapshotter) process(req SnapshotRequest) {
	path, metadata, err := s.createSnapshot(req)
	res := SnapshotResult{
		GroupID:  req.GroupID,
		Index:    req.AppliedIndex,
		Metadata: metadata,
		Path:     path,
		Err:      err,
	}
	if req.Result == nil {
		if err != nil {
			s.log.WithField("group", req.GroupID).WithError(err).Error("snapshot failed, no result channel")
		}
		return
	}
	req.Result <- res
}

func (s *Snapshotter) createSnapshot(req SnapshotRequest) (path string, metadata []byte, err error) {
	if err := req.Flusher.FlushMemtables(context.Background()); err != nil {
		return "", nil, fmt.Errorf("flush memtables: %w", err)
	}

	data, err := json.Marshal(&shardSnapshotData{
		ClassName:        req.ClassName,
		ShardName:        req.ShardName,
		NodeID:           req.NodeID,
		LastAppliedIndex: req.AppliedIndex,
	})
	if err != nil {
		return "", nil, fmt.Errorf("marshal snapshot metadata: %w", err)
	}

	dir := filepath.Join(s.root, "raft-snapshots", req.ClassName, req.ShardName)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", nil, fmt.Errorf("mkdir %s: %w", dir, err)
	}
	path = filepath.Join(dir, snapshotFileName(req.AppliedIndex))
	if err := writeFileAtomic(path, data); err != nil {
		return "", nil, fmt.Errorf("write snapshot %s: %w", path, err)
	}

	// Pruning failure is non-fatal — the snapshot itself is durable.
	if err := s.prune(dir); err != nil {
		s.log.WithField("dir", dir).WithError(err).Warn("prune old snapshots")
	}
	return path, data, nil
}

// prune keeps the s.retain highest-indexed snapshot files in dir and deletes
// the rest, replacing hashicorp FileSnapshotStore's automatic retention.
func (s *Snapshotter) prune(dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	var indices []uint64
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), snapshotFileSuffix) {
			continue
		}
		idx, err := strconv.ParseUint(strings.TrimSuffix(e.Name(), snapshotFileSuffix), 10, 64)
		if err != nil {
			continue // not a snapshot file we wrote; leave it untouched
		}
		indices = append(indices, idx)
	}
	if len(indices) <= s.retain {
		return nil
	}
	sort.Slice(indices, func(i, j int) bool { return indices[i] > indices[j] })
	for _, idx := range indices[s.retain:] {
		if err := os.Remove(filepath.Join(dir, snapshotFileName(idx))); err != nil {
			return err
		}
	}
	return nil
}

// snapshotFileName zero-pads the index so lexical filename order matches
// numeric index order — pruning then just sorts directory entries.
func snapshotFileName(index uint64) string {
	return fmt.Sprintf("%020d%s", index, snapshotFileSuffix)
}

// writeFileAtomic writes data to a temp file, fsyncs it, and renames it into
// place so a crash mid-write never leaves a torn snapshot.
func writeFileAtomic(path string, data []byte) error {
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, path)
}

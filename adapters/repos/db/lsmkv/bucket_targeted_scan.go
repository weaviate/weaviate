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

package lsmkv

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"slices"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	enterrors "github.com/weaviate/weaviate/entities/errors"
	entlsmkv "github.com/weaviate/weaviate/entities/lsmkv"
)

// TargetedScanEntry exposes one live replace-strategy entry for targeted reads.
// Callers inspect Peek to decide which byte range of the value they need, so large
// values are never read whole unless requested. The entry and every slice it hands
// out are valid only until the callback returns.
type TargetedScanEntry struct {
	ValueSize uint64
	// Peek holds the first min(peekSize, ValueSize) bytes of the value.
	Peek []byte

	seg        Segment // nil for memtable entries
	valueStart uint64
	value      []byte // memtable entries only
	buf        []byte // grow-only scratch for ReadRange
}

// ReadRange returns value[from:to); to == 0 means ValueSize. The returned slice is
// backed by a per-entry scratch buffer: it is invalidated by the next ReadRange call
// as well as by the callback returning.
func (e *TargetedScanEntry) ReadRange(from, to uint64) ([]byte, error) {
	if to == 0 {
		to = e.ValueSize
	}
	if from > to || to > e.ValueSize {
		return nil, fmt.Errorf("read range [%d,%d) out of value bounds %d", from, to, e.ValueSize)
	}
	if e.seg == nil {
		return e.value[from:to], nil
	}

	need := to - from
	if uint64(cap(e.buf)) < need {
		e.buf = make([]byte, need)
	}
	b := e.buf[:need]
	r, err := e.seg.newNodeReader(nodeOffset{start: e.valueStart + from, end: e.valueStart + to}, "TargetedScanRange")
	if err != nil {
		return nil, err
	}
	defer r.Release()
	if _, err := io.ReadFull(r, b); err != nil {
		return nil, errors.Wrap(err, "targeted scan: read value range")
	}
	return b, nil
}

// ScanTargetedReplace visits every live entry of every memtable and disk segment,
// WITHOUT merging across them: a key superseded in a newer segment is still visited
// in every older segment holding it, ordering is not newest-wins, and a tombstone
// only hides the entry in its own segment. Callers must be able to identify rows by
// value content and tolerate stale versions. fn must be safe for concurrent use; a
// non-nil error aborts the scan.
func (b *Bucket) ScanTargetedReplace(ctx context.Context, peekSize, parallel int,
	fn func(e *TargetedScanEntry) error, logger logrus.FieldLogger,
) error {
	MustBeExpectedStrategy(b.strategy, StrategyReplace)

	if peekSize < 1 {
		return fmt.Errorf("targeted scan: peek size must be positive, got %d", peekSize)
	}
	if parallel < 1 {
		parallel = 1
	}

	b.flushLock.RLock()
	// memtable cursors flatten shallow copies at init, so they stay valid after the
	// lock is released
	inMem := []innerCursorReplace{b.active.newCursor()}
	if b.flushing != nil {
		inMem = append(inMem, b.flushing.newCursor())
	}
	segments, release := b.disk.getConsistentViewOfSegments()
	b.flushLock.RUnlock()
	defer release()

	for _, c := range inMem {
		if err := scanTargetedMemtable(ctx, c, peekSize, fn); err != nil {
			return err
		}
	}

	tasks := buildTargetedScanTasks(segments, parallel)
	if len(tasks) == 0 {
		return nil
	}

	scanCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	workers := parallel
	if workers > len(tasks) {
		workers = len(tasks)
	}
	taskCh := make(chan targetedScanTask)

	var (
		wg       sync.WaitGroup
		firstErr atomic.Pointer[error]
	)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		enterrors.GoWrapper(func() {
			defer wg.Done()
			entry := TargetedScanEntry{buf: make([]byte, 0)}
			head := make([]byte, 9+peekSize)
			for task := range taskCh {
				err := scanTargetedSegmentRange(scanCtx, task, peekSize, &entry, head, fn)
				if err != nil {
					e := err
					if firstErr.CompareAndSwap(nil, &e) {
						cancel()
					}
					break
				}
			}
			for range taskCh { // drain so the sender never blocks
			}
		}, logger)
	}

	for _, t := range tasks {
		taskCh <- t
	}
	close(taskCh)
	wg.Wait()

	if e := firstErr.Load(); e != nil {
		return *e
	}
	return nil
}

type targetedScanTask struct {
	seg        Segment
	start, end []byte // key range [start,end); nil = open-ended
}

// buildTargetedScanTasks splits each segment into enough key ranges that the total
// task count reaches the requested parallelism even when few segments exist.
func buildTargetedScanTasks(segments []Segment, parallel int) []targetedScanTask {
	if len(segments) == 0 {
		return nil
	}
	rangesPerSeg := (parallel + len(segments) - 1) / len(segments)

	var tasks []targetedScanTask
	for _, seg := range segments {
		seeds := [][]byte{}
		if rangesPerSeg > 1 {
			// quantileKeys yields BFS order; ranges need sorted, unique bounds or they
			// overlap and nodes get visited twice
			seeds = seg.quantileKeys(rangesPerSeg - 1)
			sort.Slice(seeds, func(i, j int) bool { return bytes.Compare(seeds[i], seeds[j]) < 0 })
			seeds = slices.CompactFunc(seeds, bytes.Equal)
		}
		if len(seeds) == 0 {
			tasks = append(tasks, targetedScanTask{seg: seg})
			continue
		}
		tasks = append(tasks, targetedScanTask{seg: seg, end: seeds[0]})
		for i := 0; i < len(seeds)-1; i++ {
			tasks = append(tasks, targetedScanTask{seg: seg, start: seeds[i], end: seeds[i+1]})
		}
		tasks = append(tasks, targetedScanTask{seg: seg, start: seeds[len(seeds)-1]})
	}
	return tasks
}

func scanTargetedMemtable(ctx context.Context, c innerCursorReplace, peekSize int,
	fn func(e *TargetedScanEntry) error,
) error {
	entry := TargetedScanEntry{}
	const checkContextEveryN = 1024
	n := 0
	k, v, err := c.first()
	for {
		if err != nil {
			if errors.Is(err, entlsmkv.NotFound) {
				return nil
			}
			if !errors.Is(err, entlsmkv.Deleted) {
				return err
			}
			// tombstone: skip
		} else if k == nil {
			return nil
		} else if len(v) > 0 {
			entry.ValueSize = uint64(len(v))
			entry.Peek = v[:min(peekSize, len(v))]
			entry.seg = nil
			entry.value = v
			if err := fn(&entry); err != nil {
				return err
			}
		}

		n++
		if n%checkContextEveryN == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		k, v, err = c.next()
	}
}

func scanTargetedSegmentRange(ctx context.Context, task targetedScanTask, peekSize int,
	entry *TargetedScanEntry, head []byte, fn func(e *TargetedScanEntry) error,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	const checkContextEveryN = 1024
	rows := 0
	return task.seg.scanNodeRanges(task.start, task.end, func(n segmentNodeRange) error {
		rows++
		if rows%checkContextEveryN == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}

		// one read covers the 9-byte node header (tombstone + value length) plus the
		// value prefix
		headEnd := n.Start + uint64(9+peekSize)
		if headEnd > n.End {
			headEnd = n.End
		}
		r, err := task.seg.newNodeReader(nodeOffset{start: n.Start, end: headEnd}, "TargetedScanPeek")
		if err != nil {
			return err
		}
		if _, err := io.ReadFull(r, head[:9]); err != nil {
			r.Release()
			return errors.Wrap(err, "targeted scan: read node header")
		}
		if head[0] != 0 { // tombstone: nothing to serve from this segment
			r.Release()
			return nil
		}
		valueLen := binary.LittleEndian.Uint64(head[1:9])
		if valueLen == 0 {
			r.Release()
			return nil
		}
		peekLen := uint64(peekSize)
		if peekLen > valueLen {
			peekLen = valueLen
		}
		if _, err := io.ReadFull(r, head[9:9+peekLen]); err != nil {
			r.Release()
			return errors.Wrap(err, "targeted scan: read value peek")
		}
		r.Release()

		entry.ValueSize = valueLen
		entry.Peek = head[9 : 9+peekLen]
		entry.seg = task.seg
		entry.valueStart = n.Start + 9
		entry.value = nil
		return fn(entry)
	})
}

// segmentNodeRange is the byte range of one node within a segment, in key order.
type segmentNodeRange struct {
	Start, End uint64
}

func (s *segment) scanNodeRanges(start, end []byte, fn func(n segmentNodeRange) error) error {
	node, err := s.index.Seek(start)
	for {
		if err != nil {
			if errors.Is(err, entlsmkv.NotFound) {
				return nil
			}
			return err
		}
		if len(end) > 0 && bytes.Compare(node.Key, end) >= 0 {
			return nil
		}
		if err := fn(segmentNodeRange{Start: node.Start, End: node.End}); err != nil {
			return err
		}
		node, err = s.index.Next(node.Key)
	}
}

func (s *lazySegment) scanNodeRanges(start, end []byte, fn func(n segmentNodeRange) error) error {
	s.mustLoad()
	return s.segment.scanNodeRanges(start, end, fn)
}

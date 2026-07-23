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

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	entlsmkv "github.com/weaviate/weaviate/entities/lsmkv"
)

// TargetedScanEntry is one live entry served by ScanTargetedReplace. The entry
// and every slice it hands out are valid only until the callback returns.
type TargetedScanEntry struct {
	ValueSize uint64
	// Peek holds the first min(peekSize, ValueSize) bytes of the value.
	Peek []byte

	seg        *segment // nil for memtable entries
	valueStart uint64
	value      []byte // memtable entries only
	buf        []byte // grow-only scratch for pread-mode ReadRange
}

// ReadRange returns value[from:to); to == 0 means ValueSize. The returned slice may
// be backed by a per-entry scratch buffer: it is invalidated by the next ReadRange
// call as well as by the callback returning.
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
	if e.seg.readFromMemory {
		return e.seg.contents[e.valueStart+from : e.valueStart+to], nil
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

// ScanTargetedReplace visits every live entry with merged-cursor visibility but
// no merge: segments are scanned independently in parallel, and a row is served
// only when no newer segment or memtable holds its key — probed from in-memory
// bloom filters and indexes before any value bytes are read. fn must be safe for
// concurrent use; a non-nil error aborts the scan.
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
	// memtable cursors flatten node pointers at init; concurrent same-key updates
	// reassign value slices afterwards — the same exposure Bucket.Cursor has
	inMem := []innerCursorReplace{b.active.newCursor()}
	if b.flushing != nil {
		inMem = append(inMem, b.flushing.newCursor())
	}
	segments, release := b.disk.getConsistentViewOfSegments()
	b.flushLock.RUnlock()
	defer release()

	// inMem[0] is the active memtable (newest); a flushing memtable follows
	var hideSets []map[string]struct{}
	for _, c := range inMem {
		collect := map[string]struct{}{}
		if err := scanTargetedMemtable(ctx, c, peekSize, hideSets, collect, fn); err != nil {
			return err
		}
		hideSets = append(hideSets, collect)
	}

	tasks := buildTargetedScanTasks(segments, parallel)
	if len(tasks) == 0 {
		return nil
	}

	// worker panics become errors + a context cancel, so a failing task cannot
	// leave this function blocked holding segment refs
	eg, egCtx := enterrors.NewErrorGroupWithContextWrapper(logger, ctx)
	eg.SetLimit(parallel)
	for _, task := range tasks {
		eg.Go(func() error {
			var entry TargetedScanEntry
			head := make([]byte, 9+peekSize)
			return scanTargetedSegmentRange(egCtx, task, peekSize, hideSets, &entry, head, fn)
		})
	}
	return eg.Wait()
}

type targetedScanTask struct {
	seg        *segment
	start, end []byte // key range [start,end); nil = open-ended
	// newer holds the segments written after seg, newest first; a key present in
	// any of them hides seg's row
	newer []*segment
}

// buildTargetedScanTasks splits each segment into enough key ranges that the total
// task count reaches the requested parallelism even when few segments exist.
func buildTargetedScanTasks(segments []Segment, parallel int) []targetedScanTask {
	if len(segments) == 0 {
		return nil
	}
	rangesPerSeg := (parallel + len(segments) - 1) / len(segments)

	underlying := make([]*segment, len(segments))
	for i, s := range segments {
		underlying[i] = s.underlyingSegment()
	}

	var tasks []targetedScanTask
	for segIdx, seg := range underlying {
		// segments arrive oldest to newest; probe newest first
		var newer []*segment
		for j := len(underlying) - 1; j > segIdx; j-- {
			newer = append(newer, underlying[j])
		}
		seeds := [][]byte{}
		if rangesPerSeg > 1 {
			// quantileKeys yields BFS order; ranges need sorted, unique bounds or they
			// overlap and nodes get visited twice
			seeds = seg.quantileKeys(rangesPerSeg - 1)
			slices.SortFunc(seeds, bytes.Compare)
			seeds = slices.CompactFunc(seeds, bytes.Equal)
		}
		if len(seeds) == 0 {
			tasks = append(tasks, targetedScanTask{seg: seg, newer: newer})
			continue
		}
		tasks = append(tasks, targetedScanTask{seg: seg, end: seeds[0], newer: newer})
		for i := 0; i < len(seeds)-1; i++ {
			tasks = append(tasks, targetedScanTask{seg: seg, start: seeds[i], end: seeds[i+1], newer: newer})
		}
		tasks = append(tasks, targetedScanTask{seg: seg, start: seeds[len(seeds)-1], newer: newer})
	}
	return tasks
}

// collect receives every key the memtable holds — tombstones included, they hide
// older versions; hideSets suppresses rows superseded by newer memtables.
func scanTargetedMemtable(ctx context.Context, c innerCursorReplace, peekSize int,
	hideSets []map[string]struct{}, collect map[string]struct{},
	fn func(e *TargetedScanEntry) error,
) error {
	hidden := func(k []byte) bool {
		for _, s := range hideSets {
			if _, ok := s[string(k)]; ok {
				return true
			}
		}
		return false
	}

	entry := TargetedScanEntry{}
	const checkContextEveryN = 1024
	n := 0
	k, v, err := c.first()
	for {
		serve := false
		if err != nil {
			if errors.Is(err, entlsmkv.NotFound) {
				return nil
			}
			if !errors.Is(err, entlsmkv.Deleted) {
				return err
			}
			// tombstone: not served, but it still hides older versions
		} else if len(v) > 0 {
			serve = true
		}

		if collect != nil && k != nil {
			collect[string(k)] = struct{}{}
		}
		if serve && k != nil && hidden(k) {
			serve = false
		}
		if serve {
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
	hideSets []map[string]struct{}, entry *TargetedScanEntry, head []byte,
	fn func(e *TargetedScanEntry) error,
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
		if n.End-n.Start < 9 {
			return fmt.Errorf("targeted scan: node at %d smaller than its header", n.Start)
		}

		for _, s := range hideSets {
			if _, ok := s[string(n.Key)]; ok {
				return nil
			}
		}
		for _, newer := range task.newer {
			if newer.hasKeyReplace(n.Key) {
				return nil
			}
		}

		var peek []byte
		var valueLen uint64
		if task.seg.readFromMemory {
			node := task.seg.contents[n.Start:n.End]
			if node[0] != 0 { // tombstone: nothing to serve from this segment
				return nil
			}
			valueLen = binary.LittleEndian.Uint64(node[1:9])
			if valueLen == 0 {
				return nil
			}
			if err := checkNodeValueLen(valueLen, n); err != nil {
				return err
			}
			peekLen := uint64(peekSize)
			if peekLen > valueLen {
				peekLen = valueLen
			}
			peek = node[9 : 9+peekLen]
		} else {
			// one buffered read covers the 9-byte node header (tombstone + value
			// length) plus the value prefix
			headEnd := n.Start + uint64(9+peekSize)
			if headEnd > n.End {
				headEnd = n.End
			}
			r, err := task.seg.newNodeReader(nodeOffset{start: n.Start, end: headEnd}, "TargetedScanPeek")
			if err != nil {
				return err
			}
			defer r.Release()

			if _, err := io.ReadFull(r, head[:9]); err != nil {
				return errors.Wrap(err, "targeted scan: read node header")
			}
			if head[0] != 0 { // tombstone: nothing to serve from this segment
				return nil
			}
			valueLen = binary.LittleEndian.Uint64(head[1:9])
			if valueLen == 0 {
				return nil
			}
			if err := checkNodeValueLen(valueLen, n); err != nil {
				return err
			}
			peekLen := uint64(peekSize)
			if peekLen > valueLen {
				peekLen = valueLen
			}
			if _, err := io.ReadFull(r, head[9:9+peekLen]); err != nil {
				return errors.Wrap(err, "targeted scan: read value peek")
			}
			peek = head[9 : 9+peekLen]
		}

		entry.ValueSize = valueLen
		entry.Peek = peek
		entry.seg = task.seg
		entry.valueStart = n.Start + 9
		entry.value = nil
		return fn(entry)
	})
}

// checkNodeValueLen rejects a value length that extends past its node — corruption
// that would otherwise size a huge allocation or slice out of bounds downstream.
func checkNodeValueLen(valueLen uint64, n segmentNodeRange) error {
	if valueLen > n.End-n.Start-9 {
		return fmt.Errorf("targeted scan: node at %d: value length %d exceeds node size %d",
			n.Start, valueLen, n.End-n.Start)
	}
	return nil
}

// segmentNodeRange is one node within a segment; Key comes from the in-memory
// index, available before any value bytes are read.
type segmentNodeRange struct {
	Key        []byte
	Start, End uint64
}

// scanNodeRanges visits the primary index over key range [start,end) — nil bounds
// (not merely empty) are open-ended — in key order, yielding each node's byte range
// without reading any value bytes.
func (s *segment) scanNodeRanges(start, end []byte, fn func(n segmentNodeRange) error) error {
	node, err := s.index.Seek(start)
	for {
		if err != nil {
			if errors.Is(err, entlsmkv.NotFound) {
				return nil
			}
			return err
		}
		if end != nil && bytes.Compare(node.Key, end) >= 0 {
			return nil
		}
		if err := fn(segmentNodeRange{Key: node.Key, Start: node.Start, End: node.End}); err != nil {
			return err
		}
		node, err = s.index.Next(node.Key)
	}
}

func (s *segment) underlyingSegment() *segment {
	return s
}

// hasKeyReplace: does the segment hold an entry (live or tombstoned) for key,
// answered from the in-memory bloom filter and index only.
func (s *segment) hasKeyReplace(key []byte) bool {
	if s.strategy != segmentindex.StrategyReplace {
		return false
	}
	if s.useBloomFilter && !s.bloomFilter.Test(key) {
		return false
	}
	_, err := s.index.Get(key)
	return err == nil
}

func (s *lazySegment) underlyingSegment() *segment {
	s.mustLoad()
	return s.segment
}

// EstimatedEntrySize is the average on-disk bytes per net entry across flushed
// segments (file size over net additions); 0 with no flushed entries or when the
// bucket does not track net additions (see WithCalcCountNetAdditions).
func (b *Bucket) EstimatedEntrySize() int64 {
	segments, release := b.disk.getConsistentViewOfSegments()
	defer release()

	var size, count int64
	for _, seg := range segments {
		size += seg.Size()
		count += int64(seg.getCountNetAdditions())
	}
	if count <= 0 {
		return 0
	}
	return size / count
}

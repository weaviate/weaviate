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
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	enterrors "github.com/weaviate/weaviate/entities/errors"
	entlsmkv "github.com/weaviate/weaviate/entities/lsmkv"
)

// TargetedScanEntry is one live entry served by ScanTargetedReplace. The entry
// and every slice it hands out are valid only until the callback returns.
type TargetedScanEntry struct {
	// Key comes from the index walk, so it costs nothing to carry.
	Key       []byte
	ValueSize uint64
	// Peek holds the first min(peekSize, ValueSize) bytes of the value. Peek stays
	// valid across ReadRange calls: they read into a separate scratch buffer.
	Peek []byte

	seg        Segment // nil for memtable entries
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
	return e.seg.readRange(nodeOffset{start: e.valueStart + from, end: e.valueStart + to},
		"TargetedScanRange", &e.buf)
}

// maxScanParallelism bounds both the worker goroutines and the per-segment task
// fan-out, so an oversized caller value cannot turn into a task per index node.
const maxScanParallelism = 256

// ScanTargetedReplace visits every live entry with merged-cursor visibility but
// no merge: segments are scanned independently in parallel, and a row is served
// only when no newer segment or memtable holds its key — probed from in-memory
// bloom filters and indexes before any value bytes are read. fn must be safe for
// concurrent use; a non-nil error aborts the scan.
//
// parallel applies to segments only: memtable rows are served serially, before
// any segment task starts.
func (b *Bucket) ScanTargetedReplace(ctx context.Context, peekSize, parallel int,
	fn func(e *TargetedScanEntry) error, logger logrus.FieldLogger,
) error {
	MustBeExpectedStrategy(b.strategy, StrategyReplace)

	if peekSize < 1 {
		return fmt.Errorf("targeted scan: peek size must be positive, got %d", peekSize)
	}
	parallel = min(max(parallel, 1), maxScanParallelism)
	if err := ctx.Err(); err != nil {
		return err
	}

	inMem, segments, release := b.targetedScanSnapshot()
	defer release()

	// inMem runs newest first, so each memtable's keys hide the ones after it and
	// every segment. The oldest one's keys have no such reader when there are no
	// segments, so they are not collected.
	var hideSets []map[string]struct{}
	for i, c := range inMem {
		var collect map[string]struct{}
		if i < len(inMem)-1 || len(segments) > 0 {
			collect = map[string]struct{}{}
		}
		if err := scanTargetedMemtable(ctx, c, peekSize, hideSets, collect, fn); err != nil {
			return err
		}
		if collect != nil {
			hideSets = append(hideSets, collect)
		}
	}

	tasks := buildTargetedScanTasks(segments, parallel)
	if len(tasks) == 0 {
		return ctx.Err()
	}

	// worker panics become errors + a context cancel, so a failing task cannot
	// leave this function blocked holding segment refs
	eg, egCtx := enterrors.NewErrorGroupWithContextWrapper(logger, ctx)
	eg.SetLimit(parallel)
	for _, task := range tasks {
		eg.Go(func() error {
			return scanTargetedSegmentRange(egCtx, task, peekSize, hideSets, fn)
		})
	}
	if err := eg.Wait(); err != nil {
		return err
	}
	return ctx.Err()
}

// targetedScanSnapshot grabs the (active, flushing, segments) triple under one
// flushLock hold — the same point-in-time snapshot Bucket.Cursor takes. The
// deferred unlock releases this function's own hold if acquiring the segment
// view panics (a lazy segment's load error).
func (b *Bucket) targetedScanSnapshot() ([]innerCursorReplace, []Segment, func()) {
	b.flushLock.RLock()
	defer b.flushLock.RUnlock()

	// memtable cursors flatten shallow node copies under the memtable's read lock,
	// so a concurrent same-key update reassigns the original node's fields, not the
	// cursor's — the entries stay a point-in-time snapshot
	inMem := []innerCursorReplace{b.active.newCursor()}
	if b.flushing != nil {
		inMem = append(inMem, b.flushing.newCursor())
	}
	segments, release := b.disk.getConsistentViewOfSegments()
	return inMem, segments, release
}

type targetedScanTask struct {
	seg      Segment
	from, to int // byte range over seg's index, for scanIndexNodes
	// newer holds the segments written after seg, newest first; a key present in
	// any of them hides seg's row
	newer []Segment
}

// buildTargetedScanTasks splits each segment's index into byte ranges, sized by
// that segment's share of the total index bytes so one large segment does not end
// up walked by a single worker.
func buildTargetedScanTasks(segments []Segment, parallel int) []targetedScanTask {
	if len(segments) == 0 {
		return nil
	}

	var totalIndexSize int64
	for _, seg := range segments {
		totalIndexSize += int64(seg.indexSize())
	}

	var tasks []targetedScanTask
	for segIdx, seg := range segments {
		// segments arrive oldest to newest; probe newest first
		var newer []Segment
		for j := len(segments) - 1; j > segIdx; j-- {
			newer = append(newer, segments[j])
		}

		parts := 1
		if totalIndexSize > 0 {
			share := float64(parallel) * float64(seg.indexSize()) / float64(totalIndexSize)
			if rounded := int(math.Round(share)); rounded > 1 {
				parts = rounded
			}
		}

		for _, r := range seg.indexNodeSplits(parts) {
			tasks = append(tasks, targetedScanTask{seg: seg, from: r[0], to: r[1], newer: newer})
		}
	}
	return tasks
}

// collect, when non-nil, receives every key the memtable holds — tombstones
// included, they hide older versions; hideSets suppresses rows superseded by
// newer memtables.
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
		} else {
			// a zero-length value is a live entry (Put accepts nil and []byte{}),
			// which Bucket.Cursor serves — only the tombstone flag means deleted
			serve = true
		}

		if collect != nil {
			collect[string(k)] = struct{}{}
		}
		if serve && hidden(k) {
			serve = false
		}
		if serve {
			entry.Key = k
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
	hideSets []map[string]struct{},
	fn func(e *TargetedScanEntry) error,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	var entry TargetedScanEntry
	var head []byte // pread-mode scratch for the header+peek read; unused in mmap mode

	const checkContextEveryN = 1024
	rows := 0
	return task.seg.scanIndexNodes(task.from, task.to, func(n segmentNodeRange) error {
		rows++
		if rows%checkContextEveryN == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		for _, s := range hideSets {
			if _, ok := s[string(n.Key)]; ok {
				return nil
			}
		}
		for _, newer := range task.newer {
			has, err := newer.existsKey(n.Key)
			if err != nil {
				return err
			}
			if has {
				return nil
			}
		}

		// one read covers the 9-byte node header (tombstone + value length) plus
		// the value prefix
		headEnd := n.Start + 9 + uint64(peekSize)
		if headEnd > n.End {
			headEnd = n.End
		}
		node, err := task.seg.readRange(nodeOffset{start: n.Start, end: headEnd}, "TargetedScanPeek", &head)
		if err != nil {
			return err
		}
		if node[0] != 0 { // tombstone: nothing to serve from this segment
			return nil
		}
		valueLen := binary.LittleEndian.Uint64(node[1:9])
		if err := checkNodeValueLen(valueLen, n); err != nil {
			return err
		}
		peekLen := uint64(peekSize)
		if peekLen > valueLen {
			peekLen = valueLen
		}

		entry.Key = n.Key
		entry.ValueSize = valueLen
		entry.Peek = node[9 : 9+peekLen]
		entry.seg = task.seg
		entry.valueStart = n.Start + 9
		entry.value = nil
		return fn(&entry)
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
// index, available before any value bytes are read. Key is a subslice of the
// index buffer, valid only for the duration of the yielding callback.
type segmentNodeRange struct {
	Key        []byte
	Start, End uint64
}

// scanIndexNodes visits the primary index nodes packed in byte range [from,to) —
// on-disk order, not key order — yielding each node's byte range without reading
// any value bytes.
//
// Ranges are checked against the segment's data bounds, which stops a corrupt
// offset from sizing a read, but not against the row they claim to describe:
// offsets pointing at another live row pass, and that row's bytes are served
// under this node's key. Detecting that needs the value's trailing primary key,
// which this deliberately does not read. Callers that cannot tolerate it should
// run with checksum validation on.
func (s *segment) scanIndexNodes(from, to int, fn func(n segmentNodeRange) error) error {
	return s.index.ForEachNodeInRange(from, to, func(key []byte, start, end uint64) error {
		// ordered so the subtraction cannot wrap on end < start
		if end <= start || end-start < 9 || start < s.dataStartPos || end > s.dataEndPos {
			return fmt.Errorf("targeted scan: node [%d,%d) outside data bounds [%d,%d) or smaller than its header",
				start, end, s.dataStartPos, s.dataEndPos)
		}
		return fn(segmentNodeRange{Key: key, Start: start, End: end})
	})
}

func (s *segment) indexNodeSplits(parts int) [][2]int {
	return s.index.SplitNodeRanges(parts)
}

// readRange serves the segment bytes in [offset.start, offset.end). In mmap mode
// the result is a zero-copy slice of the segment contents; otherwise *buf is
// grown as needed, filled via a single pread, and the result aliases it.
func (s *segment) readRange(offset nodeOffset, operation string, buf *[]byte) ([]byte, error) {
	if s.readFromMemory {
		return s.contents[offset.start:offset.end], nil
	}

	need := offset.end - offset.start
	if uint64(cap(*buf)) < need {
		*buf = make([]byte, need)
	}
	b := (*buf)[:need]
	r, err := s.newNodeReader(offset, operation)
	if err != nil {
		return nil, err
	}
	defer r.Release()
	if _, err := io.ReadFull(r, b); err != nil {
		return nil, errors.Wrap(err, "targeted scan: read range")
	}
	return b, nil
}

// EstimatedEntrySize is the average on-disk bytes per net entry across flushed
// segments (payload size over net additions — the index tree is excluded); 0 with
// no flushed entries or when the bucket does not track net additions (see
// WithCalcCountNetAdditions).
func (b *Bucket) EstimatedEntrySize() int64 {
	segments, release := b.disk.getConsistentViewOfSegments()
	defer release()

	var size, count int64
	for _, seg := range segments {
		size += int64(seg.payloadSize())
		count += int64(seg.getCountNetAdditions())
	}
	if count <= 0 {
		return 0
	}
	return size / count
}

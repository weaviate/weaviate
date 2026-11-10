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
	"errors"
	"fmt"
	"time"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/entities/lsmkv"
	"github.com/weaviate/weaviate/entities/storobj"
)

// ValueRangePicker chooses the byte range of a value that GetBySecondaryRange
// returns. It sees only head, the first bytes of the value: as many as the
// caller asked for, or the whole value when the value is shorter. A picker
// that cannot find its range in head returns need, a lower bound on the
// number of bytes of the value that would be enough. The bucket then reads
// that much (at most the whole value) and calls the picker again, under the
// same lookup. A need at or below len(head) means the range is final.
type ValueRangePicker func(head []byte) (offset, length, need uint64, err error)

// maxHeadRounds bounds how often a picker may ask for a longer head. The
// multi-vector locator is the one picker, and the bound is the most heads it
// asks for on a well-formed object (storobj.MultiVectorLocatorRounds).
const maxHeadRounds = storobj.MultiVectorLocatorRounds

// GetBySecondaryRange returns bytes [offset, offset+length) of the value
// stored under the secondary key seckey. pick chooses the range from the
// value's first head bytes (ValueRangePicker). The method exists for a
// multi-vector index that reads one token of an object of many tokens.
// Through GetBySecondary, that read copies and decodes the whole object. The lookup
// is the same as in GetBySecondary: the memtables, then the segments newest
// first, with bloom filter, secondary index, tombstones and the primary-key
// recheck in the newer segments. A segment reads the node's prefix together
// with the head, then the range, then the node's tail (the primary key and
// the secondary keys), each with a positioned read. It reads the value whole
// only when the picker needs the whole value as its head, and a tombstone
// node whole. A memtable or an
// in-memory segment slices the value. The range is returned in buffer when
// it fits, as GetBySecondaryWithBuffer does. An absent or deleted key
// returns nil, buffer, nil.
func (b *Bucket) GetBySecondaryRange(ctx context.Context, pos int, seckey []byte, head uint64, pick ValueRangePicker, buffer []byte) ([]byte, []byte, error) {
	v, allocBuf, err := b.getBySecondaryRange(ctx, pos, seckey, head, pick, buffer)
	if err != nil && lsmkv.IsDeletedOrNotFound(err) {
		return nil, buffer, nil
	}
	return v, allocBuf, err
}

func (b *Bucket) getBySecondaryRange(ctx context.Context, pos int, seckey []byte, head uint64, pick ValueRangePicker, buffer []byte) ([]byte, []byte, error) {
	if pos >= int(b.secondaryIndices) {
		return nil, nil, fmt.Errorf("no secondary index at pos %d", pos)
	}

	beforeAll := time.Now()
	beforeView := time.Now()
	view := b.GetConsistentView()
	defer view.ReleaseView()
	viewTook := time.Since(beforeView)
	memtables, count := viewMemtables(view)

	var memtablesTook [2]time.Duration
	for i := range count {
		beforeMemtable := time.Now()
		k, v, err := b.getBySecondaryFromMemtable(pos, seckey, memtables[i], memtableNames[i])
		memtablesTook[i] = time.Since(beforeMemtable)
		if err == nil {
			if i == 1 {
				// found in the flushing memtable. As in getBySecondaryCore, the
				// active memtable decides whether a newer version or a deletion
				// supersedes it.
				err = memtables[0].exists(k)
				if err == nil {
					return nil, nil, lsmkv.NotFound
				} else if errors.Is(err, lsmkv.Deleted) {
					return nil, nil, err
				} else if !errors.Is(err, lsmkv.NotFound) {
					return nil, nil, fmt.Errorf("Bucket::getBySecondaryRange() %q: %w", memtableNames[0], err)
				}
			}
			return rangeOfValue(v, head, pick, buffer)
		}
		if errors.Is(err, lsmkv.Deleted) {
			return nil, nil, err
		}
		if !errors.Is(err, lsmkv.NotFound) {
			return nil, nil, fmt.Errorf("Bucket::getBySecondaryRange() %q: %w", memtableNames[i], err)
		}
	}

	beforeSegments := time.Now()
	priKey, v, allocBuf, secSegIndex, err := b.getBySecondaryRangeFromSegmentGroup(pos, seckey, head, pick, buffer, view.Disk)
	if err != nil {
		return nil, nil, err
	}
	segmentsTook := time.Since(beforeSegments)

	// as in getBySecondaryCore: a later version of the document in a newer
	// segment or a memtable makes this one stale
	beforeReCheck := time.Now()
	err = b.existsWithConsistentViewUpTo(priKey, secSegIndex+1, view)
	if err == nil {
		return nil, nil, lsmkv.NotFound
	}
	if !errors.Is(err, lsmkv.NotFound) {
		return nil, nil, err
	}
	recheckTook := time.Since(beforeReCheck)

	helpers.AnnotateSlowQueryLogAppend(ctx, "lsm_get_by_secondary_range", BucketSlowLogEntry{
		View:             viewTook,
		ActiveMemtable:   memtablesTook[0],
		FlushingMemtable: memtablesTook[1],
		Segments:         segmentsTook,
		Recheck:          recheckTook,
		Total:            time.Since(beforeAll),
	})

	return v, allocBuf, nil
}

// getBySecondaryRangeFromSegmentGroup is getBySecondaryFromSegmentGroup for a
// range of the value: the same metrics, under the operation
// "getbysecondaryrange".
func (b *Bucket) getBySecondaryRangeFromSegmentGroup(pos int, seckey []byte, head uint64, pick ValueRangePicker, buffer []byte, segments []Segment,
) (k, v []byte, buf []byte, segmentId int, err error) {
	op := "getbysecondaryrange"
	component := "segment_group"

	start := time.Now()
	b.metrics.IncBucketReadOpCountByComponent(op, component)
	b.metrics.IncBucketReadOpOngoingByComponent(op, component)

	defer func() {
		b.metrics.DecBucketReadOpOngoingByComponent(op, component)
		if err != nil && !lsmkv.IsDeletedOrNotFound(err) {
			b.metrics.IncBucketReadOpFailureCountByComponent(op, component)
			return
		}
		b.metrics.ObserveBucketReadOpDurationByComponent(op, component, time.Since(start))
	}()

	return b.disk.getBySecondaryRangeWithSegmentList(pos, seckey, head, pick, buffer, segments)
}

// rangeOfValue applies pick to a value that is whole in memory and copies
// the chosen range into buffer.
func rangeOfValue(v []byte, head uint64, pick ValueRangePicker, buffer []byte) ([]byte, []byte, error) {
	offset, length, err := pickRange(uint64(len(v)), head, pick, func(n uint64) ([]byte, error) {
		return v[:n], nil
	})
	if err != nil {
		return nil, nil, err
	}
	// a nil buffer is replaced even for an empty range, because a nil
	// result means absent
	if buffer == nil || uint64(cap(buffer)) < length {
		buffer = make([]byte, length)
	}
	out := buffer[:length:length]
	copy(out, v[offset:offset+length])
	return out, buffer, nil
}

// pickRange runs the picker over heads of growing length until the picker
// names its range, then checks that range against the value's length.
// readHead returns the value's first n bytes, with n at most valueLength.
func pickRange(valueLength, head uint64, pick ValueRangePicker, readHead func(n uint64) ([]byte, error)) (uint64, uint64, error) {
	// the builtin min is not used: the package defines its own min over ints
	n := head
	if valueLength < n {
		n = valueLength
	}
	for round := 1; ; round++ {
		h, err := readHead(n)
		if err != nil {
			return 0, 0, err
		}
		offset, length, need, err := pick(h)
		if err != nil {
			return 0, 0, err
		}
		if need <= uint64(len(h)) {
			if end := offset + length; end < offset || end > valueLength {
				return 0, 0, fmt.Errorf("value range [%d, %d) outside the value of %d bytes", offset, end, valueLength)
			}
			return offset, length, nil
		}
		// need is a lower bound from a picker that cannot see the end of
		// the value. A request past the end reads the whole value once. A
		// picker that still asks for more after that has a truncated value.
		if n == valueLength {
			return 0, 0, fmt.Errorf("value head of %d bytes asked, the value has %d", need, valueLength)
		}
		if round == maxHeadRounds {
			return 0, 0, fmt.Errorf("value head still short after %d reads (%d bytes asked)", round, need)
		}
		n = need
		if valueLength < n {
			n = valueLength
		}
	}
}

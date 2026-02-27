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

//go:build linux

package lsmkv

import (
	"fmt"
	"sync"
	"syscall"

	"github.com/weaviate/weaviate/usecases/iouring"
)

// ringPoolSize is the fixed capacity of each pooled io_uring ring.
// Must be a power of two; 4096 covers the vast majority of batch sizes.
const ringPoolSize uint32 = 4096

// ringPool holds reusable io_uring rings to avoid setup overhead per call.
var ringPool = sync.Pool{
	New: func() any {
		r, err := iouring.NewRing(ringPoolSize)
		if err != nil {
			// Ring creation can fail (e.g. kernel < 5.1, RLIMIT_MEMLOCK).
			// Return nil; callers check for nil and fall back to ReadAt.
			return (*iouring.Ring)(nil)
		}
		return r
	},
}

// batchPread reads the data for each pread-backed secondaryNodePos using
// io_uring, writing results back into the corresponding entry of results.
// Positions that are not pread-backed (inMemoryData != nil or deleted) are
// left untouched.
//
// results must already be allocated to len(positions). On success each
// pread-backed position has its inMemoryData field populated with a fresh
// slice containing the read bytes.
func batchPread(positions []secondaryNodePos, results [][]byte) error {
	// Collect all ops that need disk reads.
	type opIndex struct {
		posIdx int
		buf    []byte
	}

	ops := make([]iouring.ReadOp, 0, len(positions))
	opMap := make([]opIndex, 0, len(positions))

	for i, p := range positions {
		if p.inMemoryData != nil || p.deleted || (p.fd == 0 && p.length == 0) {
			continue
		}
		buf := make([]byte, p.length)
		ops = append(ops, iouring.ReadOp{
			Fd:       p.fd,
			Offset:   p.offset,
			Buf:      buf,
			UserData: uint64(len(opMap)),
		})
		opMap = append(opMap, opIndex{posIdx: i, buf: buf})
	}

	if len(ops) == 0 {
		return nil
	}

	// Borrow a ring from the pool.
	r, _ := ringPool.Get().(*iouring.Ring)
	if r == nil {
		// io_uring unavailable; fall back to sequential ReadAt.
		return batchPreadFallback(positions, results)
	}
	defer ringPool.Put(r)

	// If the batch exceeds the ring capacity, process it in chunks.
	capacity := int(ringPoolSize)
	for start := 0; start < len(ops); start += capacity {
		end := start + capacity
		if end > len(ops) {
			end = len(ops)
		}
		chunk := ops[start:end]

		res, err := r.SubmitAndWaitAll(chunk)
		if err != nil {
			return fmt.Errorf("io_uring SubmitAndWaitAll: %w", err)
		}

		for _, cr := range res {
			if cr.Res < 0 {
				return fmt.Errorf("io_uring read error: errno %d", -cr.Res)
			}
			// cr.UserData holds the global opMap index (set during op construction).
			// Do NOT add start — that would double-count the chunk offset.
			oi := opMap[int(cr.UserData)]
			if int(cr.Res) != len(oi.buf) {
				return fmt.Errorf("short read: got %d, want %d", cr.Res, len(oi.buf))
			}
			results[oi.posIdx] = oi.buf
		}
	}

	return nil
}

// batchPreadFallback is used when io_uring is unavailable at runtime.
// It reads each position sequentially using pread.
func batchPreadFallback(positions []secondaryNodePos, results [][]byte) error {
	for i, p := range positions {
		if p.inMemoryData != nil || p.deleted || (p.fd == 0 && p.length == 0) {
			continue
		}
		buf := make([]byte, p.length)
		n, err := syscall.Pread(p.fd, buf, p.offset)
		if err != nil {
			return fmt.Errorf("pread fd=%d offset=%d: %w", p.fd, p.offset, err)
		}
		if n != int(p.length) {
			return fmt.Errorf("short pread fd=%d: got %d, want %d", p.fd, n, p.length)
		}
		results[i] = buf
	}
	return nil
}

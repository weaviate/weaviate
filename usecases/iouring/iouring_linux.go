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

// Package iouring provides a minimal io_uring wrapper for batched pread
// operations. Only the IORING_OP_READ opcode is supported; the intent is to
// replace N individual blocking pread64 syscalls with a single
// io_uring_enter(submit+wait) round-trip.
//
// The implementation uses raw Linux syscalls (425/426) and unsafe pointer
// arithmetic to map the shared ring buffers, following the same pattern as
// usecases/mmap/mmap_unix.go.
//
// NOTE: The current Go garbage collector is non-moving, so pinning caller
// buffers before passing their addresses to the kernel is not strictly
// necessary. This assumption must be revisited if Go ever adopts a
// compacting/moving GC.
package iouring

import (
	"fmt"
	"sync/atomic"
	"syscall"
	"unsafe"
)

// Linux syscall numbers (x86_64 and arm64 share the same numbers here).
const (
	sysIOURingSetup = 425
	sysIOURingEnter = 426
)

// io_uring constants from linux/io_uring.h.
const (
	opRead         uint8  = 22 // IORING_OP_READ
	enterGetEvents uint32 = 1  // IORING_ENTER_GETEVENTS
	featSingleMMap uint32 = 1  // IORING_FEAT_SINGLE_MMAP

	offSQRing uint64 = 0x0        // IORING_OFF_SQ_RING
	offCQRing uint64 = 0x8000000  // IORING_OFF_CQ_RING
	offSQEs   uint64 = 0x10000000 // IORING_OFF_SQES
)

// ioUringParams mirrors struct io_uring_params (linux/io_uring.h). Total: 120 bytes.
type ioUringParams struct {
	sqEntries    uint32
	cqEntries    uint32
	flags        uint32
	sqThreadCPU  uint32
	sqThreadIdle uint32
	features     uint32
	wqFd         uint32
	resv         [3]uint32
	sqOff        sqRingOffsets
	cqOff        cqRingOffsets
}

// sqRingOffsets mirrors struct io_sqring_offsets (40 bytes).
type sqRingOffsets struct {
	head        uint32
	tail        uint32
	ringMask    uint32
	ringEntries uint32
	flags       uint32
	dropped     uint32
	array       uint32
	resv1       uint32
	resv2       uint64
}

// cqRingOffsets mirrors struct io_cqring_offsets (40 bytes).
type cqRingOffsets struct {
	head        uint32
	tail        uint32
	ringMask    uint32
	ringEntries uint32
	overflow    uint32
	cqes        uint32
	flags       uint32
	resv1       uint32
	userAddr    uint64
}

// ioUringSQE mirrors struct io_uring_sqe (64 bytes). Only the fields needed
// for IORING_OP_READ are named; the rest are padding to maintain layout.
type ioUringSQE struct {
	opcode   uint8
	flags    uint8
	ioprio   uint16
	fd       int32
	off      uint64 // file offset
	addr     uint64 // buffer address (userspace pointer)
	length   uint32
	rwFlags  uint32
	userData uint64
	bufIndex uint16
	pad0     uint16
	pad1     int32
	addr3    uint64
	pad2     uint64
}

// ioUringCQE mirrors struct io_uring_cqe (16 bytes).
type ioUringCQE struct {
	userData uint64
	res      int32
	flags    uint32
}

// ReadOp describes a single pread operation to submit to the ring.
type ReadOp struct {
	Fd       int
	Offset   int64
	Buf      []byte // destination; must be non-empty
	UserData uint64 // echoed back in the completion
}

// Result is the completion result for one ReadOp.
type Result struct {
	UserData uint64
	Res      int32 // bytes read (≥0), or negative errno on error
}

// Ring is a minimal io_uring ring supporting only batched pread operations.
// It is not safe for concurrent use from multiple goroutines.
type Ring struct {
	fd      int
	entries uint32 // actual SQ size returned by the kernel

	// SQ ring region
	sqMem   []byte
	sqHead  *uint32
	sqTail  *uint32
	sqMask  *uint32
	sqArray uintptr // base address of the uint32 index array

	// CQ ring region (may alias sqMem when IORING_FEAT_SINGLE_MMAP)
	cqMem  []byte
	cqHead *uint32
	cqTail *uint32
	cqMask *uint32
	cqBase uintptr // base address of the CQE array

	// SQE array region
	sqesMem  []byte
	sqesBase uintptr // base address of the SQE structs
}

// NewRing creates an io_uring ring. entries must be a power of two; the kernel
// may round it up. The ring must be closed with Close when no longer needed.
func NewRing(entries uint32) (*Ring, error) {
	var p ioUringParams
	rfd, _, errno := syscall.Syscall(sysIOURingSetup, uintptr(entries),
		uintptr(unsafe.Pointer(&p)), 0)
	if errno != 0 {
		return nil, fmt.Errorf("io_uring_setup: %w", errno)
	}
	ringFd := int(rfd)

	r := &Ring{fd: ringFd, entries: p.sqEntries}

	sqRingSize := int(p.sqOff.array + p.sqEntries*4)
	cqRingSize := int(p.cqOff.cqes + p.cqEntries*16)
	sqesSize := int(unsafe.Sizeof(ioUringSQE{})) * int(p.sqEntries)

	// mmap SQ ring
	sqMem, err := mmapRing(ringFd, sqRingSize, offSQRing)
	if err != nil {
		_ = syscall.Close(ringFd)
		return nil, fmt.Errorf("mmap SQ ring: %w", err)
	}
	r.sqMem = sqMem
	r.sqHead = (*uint32)(unsafe.Pointer(&sqMem[p.sqOff.head]))
	r.sqTail = (*uint32)(unsafe.Pointer(&sqMem[p.sqOff.tail]))
	r.sqMask = (*uint32)(unsafe.Pointer(&sqMem[p.sqOff.ringMask]))
	r.sqArray = uintptr(unsafe.Pointer(&sqMem[p.sqOff.array]))

	// mmap CQ ring (may share with SQ ring)
	if p.features&featSingleMMap != 0 {
		r.cqMem = sqMem
	} else {
		cqMem, err := mmapRing(ringFd, cqRingSize, offCQRing)
		if err != nil {
			_ = unmapRing(sqMem)
			_ = syscall.Close(ringFd)
			return nil, fmt.Errorf("mmap CQ ring: %w", err)
		}
		r.cqMem = cqMem
	}
	r.cqHead = (*uint32)(unsafe.Pointer(&r.cqMem[p.cqOff.head]))
	r.cqTail = (*uint32)(unsafe.Pointer(&r.cqMem[p.cqOff.tail]))
	r.cqMask = (*uint32)(unsafe.Pointer(&r.cqMem[p.cqOff.ringMask]))
	r.cqBase = uintptr(unsafe.Pointer(&r.cqMem[p.cqOff.cqes]))

	// mmap SQE array
	sqesMem, err := mmapRing(ringFd, sqesSize, offSQEs)
	if err != nil {
		_ = unmapRing(sqMem)
		if &r.cqMem[0] != &sqMem[0] {
			_ = unmapRing(r.cqMem)
		}
		_ = syscall.Close(ringFd)
		return nil, fmt.Errorf("mmap SQEs: %w", err)
	}
	r.sqesMem = sqesMem
	r.sqesBase = uintptr(unsafe.Pointer(&sqesMem[0]))

	return r, nil
}

// Close releases the ring's resources.
func (r *Ring) Close() error {
	_ = unmapRing(r.sqesMem)
	if len(r.cqMem) > 0 && &r.cqMem[0] != &r.sqMem[0] {
		_ = unmapRing(r.cqMem)
	}
	_ = unmapRing(r.sqMem)
	return syscall.Close(r.fd)
}

// SubmitAndWaitAll submits all ops to the ring and blocks until all
// completions are received. len(ops) must not exceed the ring's entry count.
//
// The caller must ensure the Buf slices in each ReadOp remain valid (i.e.
// are not garbage-collected) for the duration of this call. Since Go's GC is
// currently non-moving this is guaranteed as long as the ops slice is live on
// the caller's stack or heap.
func (r *Ring) SubmitAndWaitAll(ops []ReadOp) ([]Result, error) {
	n := uint32(len(ops))
	if n == 0 {
		return nil, nil
	}
	if n > r.entries {
		return nil, fmt.Errorf("too many ops: %d > ring capacity %d", n, r.entries)
	}
	for i := range ops {
		if len(ops[i].Buf) == 0 {
			return nil, fmt.Errorf("op %d has empty Buf", i)
		}
	}

	sqTail := atomic.LoadUint32(r.sqTail)
	mask := atomic.LoadUint32(r.sqMask)
	sqeSize := uintptr(unsafe.Sizeof(ioUringSQE{}))

	for i, op := range ops {
		idx := (sqTail + uint32(i)) & mask

		sqePtr := (*ioUringSQE)(unsafe.Pointer(r.sqesBase + uintptr(idx)*sqeSize))
		*sqePtr = ioUringSQE{} // zero all fields
		sqePtr.opcode = opRead
		sqePtr.fd = int32(op.Fd)
		sqePtr.off = uint64(op.Offset)
		sqePtr.addr = uint64(uintptr(unsafe.Pointer(&op.Buf[0])))
		sqePtr.length = uint32(len(op.Buf))
		sqePtr.userData = op.UserData

		// sq_array[idx] = idx — default 1-to-1 mapping
		arrayElem := (*uint32)(unsafe.Pointer(r.sqArray + uintptr(idx)*4))
		atomic.StoreUint32(arrayElem, idx)
	}

	// Publish the new SQ tail (memory-release so kernel sees the SQEs we wrote).
	atomic.StoreUint32(r.sqTail, sqTail+n)

	// Submit n SQEs and wait for n completions in one syscall.
	_, _, errno := syscall.Syscall6(sysIOURingEnter,
		uintptr(r.fd),
		uintptr(n),
		uintptr(n),
		uintptr(enterGetEvents),
		0, 0,
	)
	if errno != 0 {
		return nil, fmt.Errorf("io_uring_enter: %w", errno)
	}

	// Drain completions.
	results := make([]Result, 0, n)
	cqHead := atomic.LoadUint32(r.cqHead)
	cqTail := atomic.LoadUint32(r.cqTail) // kernel has updated this
	cqMask := atomic.LoadUint32(r.cqMask)
	cqeSize := uintptr(unsafe.Sizeof(ioUringCQE{}))

	for cqHead != cqTail {
		idx := cqHead & cqMask
		cqePtr := (*ioUringCQE)(unsafe.Pointer(r.cqBase + uintptr(idx)*cqeSize))
		results = append(results, Result{
			UserData: cqePtr.userData,
			Res:      cqePtr.res,
		})
		cqHead++
	}
	atomic.StoreUint32(r.cqHead, cqHead)

	if len(results) != int(n) {
		return nil, fmt.Errorf("incomplete: got %d results, expected %d", len(results), n)
	}

	return results, nil
}

// mmapRing maps a region of the io_uring file descriptor.
func mmapRing(fd, size int, offset uint64) ([]byte, error) {
	addr, _, errno := syscall.Syscall6(
		syscall.SYS_MMAP, 0, uintptr(size),
		syscall.PROT_READ|syscall.PROT_WRITE,
		syscall.MAP_SHARED,
		uintptr(fd), uintptr(offset),
	)
	if errno != 0 {
		return nil, errno
	}
	return unsafe.Slice((*byte)(unsafe.Pointer(addr)), size), nil
}

// unmapRing releases a mmap'd ring region.
func unmapRing(b []byte) error {
	if len(b) == 0 {
		return nil
	}
	_, _, errno := syscall.Syscall(syscall.SYS_MUNMAP,
		uintptr(unsafe.Pointer(&b[0])), uintptr(len(b)), 0)
	if errno != 0 {
		return errno
	}
	return nil
}

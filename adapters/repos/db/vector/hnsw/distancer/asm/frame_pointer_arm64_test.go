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

//go:build !noasm && arm64

package asm

import (
	"os"
	"os/exec"
	"runtime"
	"runtime/pprof"
	"strings"
	"testing"
	"time"
	"unsafe"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/cpu"
)

// The kernels of this package that are generated from C must leave the stack
// of the Go function that calls them as they found it.
//
// A C compiler opens a function by pushing the frame pointer and the link
// register below the stack pointer (stp x29, x30, [sp, #-16]!). Go on arm64
// keeps the frame pointer a function saved in the 8 bytes right below its
// own stack pointer. A kernel that runs on the stack of its caller, as these
// do, and keeps that push writes the link register over the saved frame
// pointer of the Go function calling it. Nothing reads that slot in a normal
// run, so every distance comes out right. The block profile, the mutex
// profile and the execution tracer walk a stack by its frame pointers: the
// walk reaches a return address where a frame pointer should be, follows it
// into the code, takes the instructions there for the next frame pointer and
// faults. The process dies with "fatal error: unexpected signal during
// runtime execution" in runtime.fpTracebackPartialExpand.
//
// A server sets those profiles with GO_BLOCK_PROFILE_RATE and
// GO_MUTEX_PROFILE_FRACTION. With either on, on arm64, any goroutine that
// had computed a distance and then blocked or waited for a lock could take
// the node down.
//
// The check runs every kernel in a process of its own, because what it
// catches cannot be recovered from: the kernel, called by a function that
// then blocks with the block profile on.

const framePointerKernelEnv = "ASM_FRAME_POINTER_KERNEL"

// framePointerDims is the length the results are checked at, and
// framePointerOddDims one that leaves a tail after any block of the kernels:
// some of them only set their frame up on the way to the tail.
const (
	framePointerDims    = 1536
	framePointerOddDims = 1543
)

var (
	framePointerFloatsA = make([]float32, framePointerOddDims)
	framePointerFloatsB = make([]float32, framePointerOddDims)
	framePointerBytesA  = make([]uint8, framePointerOddDims)
	framePointerBytesB  = make([]uint8, framePointerOddDims)
	framePointerWordsA  = make([]uint64, framePointerOddDims/64+1)
	framePointerWordsB  = make([]uint64, framePointerOddDims/64+1)
)

func init() {
	for i := range framePointerFloatsA {
		framePointerFloatsA[i], framePointerFloatsB[i] = float32(i%7), float32(i%5)
		framePointerBytesA[i], framePointerBytesB[i] = uint8(i%7), uint8(i%5)
	}
	for i := range framePointerWordsA {
		framePointerWordsA[i], framePointerWordsB[i] = uint64(i)*0x9e3779b97f4a7c15, uint64(i)*0xbf58476d1ce4e5b9
	}
}

// framePointerBlock blocks until ch is closed. With the block profile on,
// waking up records the event with the stack of this goroutine, walked by
// its frame pointers.
//
//go:noinline
func framePointerBlock(ch chan struct{}) {
	<-ch
}

// Every function below calls one kernel as its wrapper does and then blocks.
// The kernel must be called by the function that blocks: a function in
// between would return over the damage, with its own saved frame pointer.

//go:noinline
func framePointerL2Neon(ch chan struct{}, n int) float64 {
	var res float32
	l := n
	l2_neon(unsafe.Pointer(&framePointerFloatsA[0]), unsafe.Pointer(&framePointerFloatsB[0]), unsafe.Pointer(&res), unsafe.Pointer(&l))
	framePointerBlock(ch)
	return float64(res)
}

//go:noinline
func framePointerDotNeon(ch chan struct{}, n int) float64 {
	var res float32
	l := n
	dot_neon(unsafe.Pointer(&framePointerFloatsA[0]), unsafe.Pointer(&framePointerFloatsB[0]), unsafe.Pointer(&res), unsafe.Pointer(&l))
	framePointerBlock(ch)
	return float64(res)
}

//go:noinline
func framePointerHamming(ch chan struct{}, n int) float64 {
	var res float32
	l := n
	hamming(unsafe.Pointer(&framePointerFloatsA[0]), unsafe.Pointer(&framePointerFloatsB[0]), unsafe.Pointer(&res), unsafe.Pointer(&l))
	framePointerBlock(ch)
	return float64(res)
}

//go:noinline
func framePointerHammingBitwise(ch chan struct{}, n int) float64 {
	var res uint64
	l := n
	hamming_bitwise(unsafe.Pointer(&framePointerWordsA[0]), unsafe.Pointer(&framePointerWordsB[0]), unsafe.Pointer(&res), unsafe.Pointer(&l))
	framePointerBlock(ch)
	return float64(res)
}

//go:noinline
func framePointerDotByte(ch chan struct{}, n int) float64 {
	var res uint32
	l := n
	dot_byte_256(unsafe.Pointer(&framePointerBytesA[0]), unsafe.Pointer(&framePointerBytesB[0]), unsafe.Pointer(&res), unsafe.Pointer(&l))
	framePointerBlock(ch)
	return float64(res)
}

//go:noinline
func framePointerL2Byte(ch chan struct{}, n int) float64 {
	var res uint32
	l := n
	l2_neon_byte_256(unsafe.Pointer(&framePointerBytesA[0]), unsafe.Pointer(&framePointerBytesB[0]), unsafe.Pointer(&res), unsafe.Pointer(&l))
	framePointerBlock(ch)
	return float64(res)
}

//go:noinline
func framePointerDotFloatByte(ch chan struct{}, n int) float64 {
	var res float32
	l := n
	dot_float_byte_neon(unsafe.Pointer(&framePointerFloatsA[0]), unsafe.Pointer(&framePointerBytesB[0]), unsafe.Pointer(&res), unsafe.Pointer(&l))
	framePointerBlock(ch)
	return float64(res)
}

//go:noinline
func framePointerL2SVE(ch chan struct{}, n int) float64 {
	var res float32
	l := n
	l2_sve(unsafe.Pointer(&framePointerFloatsA[0]), unsafe.Pointer(&framePointerFloatsB[0]), unsafe.Pointer(&res), unsafe.Pointer(&l))
	framePointerBlock(ch)
	return float64(res)
}

//go:noinline
func framePointerDotSVE(ch chan struct{}, n int) float64 {
	var res float32
	l := n
	dot_sve(unsafe.Pointer(&framePointerFloatsA[0]), unsafe.Pointer(&framePointerFloatsB[0]), unsafe.Pointer(&res), unsafe.Pointer(&l))
	framePointerBlock(ch)
	return float64(res)
}

// framePointerKernel is one kernel under the check: how it is run, what it
// has to return for the inputs above, and whether this machine can run it.
type framePointerKernel struct {
	name string
	run  func(ch chan struct{}, n int) float64
	// words tells that the kernel counts its length in words of 64 bits.
	words     bool
	want      float64
	available bool
}

func framePointerKernels() []framePointerKernel {
	var l2, dot, differ, dotBytes, l2Bytes, dotFloatByte float64
	for i := 0; i < framePointerDims; i++ {
		a, b := float64(framePointerFloatsA[i]), float64(framePointerFloatsB[i])
		l2 += (a - b) * (a - b)
		dot += a * b
		if a != b {
			differ++
		}
		ba, bb := float64(framePointerBytesA[i]), float64(framePointerBytesB[i])
		dotBytes += ba * bb
		l2Bytes += (ba - bb) * (ba - bb)
		dotFloatByte += a * bb
	}
	var bits float64
	for i := 0; i < framePointerDims/64; i++ {
		for x := framePointerWordsA[i] ^ framePointerWordsB[i]; x != 0; x &= x - 1 {
			bits++
		}
	}
	return []framePointerKernel{
		{name: "l2_neon", run: framePointerL2Neon, want: l2, available: true},
		{name: "dot_neon", run: framePointerDotNeon, want: dot, available: true},
		{name: "hamming", run: framePointerHamming, want: differ, available: true},
		{name: "hamming_bitwise", run: framePointerHammingBitwise, words: true, want: bits, available: true},
		{name: "dot_byte_256", run: framePointerDotByte, want: dotBytes, available: true},
		{name: "l2_neon_byte_256", run: framePointerL2Byte, want: l2Bytes, available: true},
		{name: "dot_float_byte_neon", run: framePointerDotFloatByte, want: dotFloatByte, available: true},
		{name: "l2_sve", run: framePointerL2SVE, want: l2, available: cpu.ARM64.HasSVE},
		{name: "dot_sve", run: framePointerDotSVE, want: dot, available: cpu.ARM64.HasSVE},
	}
}

func TestKernelsKeepTheFramePointer(t *testing.T) {
	if name := os.Getenv(framePointerKernelEnv); name != "" {
		framePointerChild(t, name)
		return
	}
	for _, kernel := range framePointerKernels() {
		t.Run(kernel.name, func(t *testing.T) {
			if !kernel.available {
				t.Skip("this machine cannot run the kernel")
			}
			cmd := exec.Command(os.Args[0], "-test.run=^TestKernelsKeepTheFramePointer$", "-test.count=1")
			cmd.Env = append(os.Environ(), framePointerKernelEnv+"="+kernel.name)
			out, err := cmd.CombinedOutput()
			// What a crash prints is the dump of every goroutine: its first
			// lines say what happened.
			head := strings.Join(strings.SplitN(string(out), "\n", 8)[:min(7, strings.Count(string(out), "\n")+1)], "\n")
			require.NoErrorf(t, err, "after %s the stack of its caller cannot be walked by frame pointers:\n%s", kernel.name, head)
			require.Contains(t, string(out), "PASS")
		})
	}
}

// framePointerChild runs one kernel and blocks after it, with the block
// profile recording every event. It runs in a process of its own.
func framePointerChild(t *testing.T, name string) {
	var kernel *framePointerKernel
	for _, k := range framePointerKernels() {
		if k.name == name {
			kernel = &k
		}
	}
	require.NotNilf(t, kernel, "no kernel %q", name)
	runtime.SetBlockProfileRate(1)
	defer runtime.SetBlockProfileRate(0)
	lengths := []int{framePointerDims, framePointerOddDims}
	if kernel.words {
		lengths = []int{framePointerDims / 64, framePointerOddDims / 64}
	}
	for i := 0; i < 10; i++ {
		for at, n := range lengths {
			ch := make(chan struct{})
			timer := time.AfterFunc(2*time.Millisecond, func() { close(ch) })
			got := kernel.run(ch, n)
			timer.Stop()
			if at == 0 {
				// The kernel ran its assembly, over all the input. What a
				// kernel does with a tail is its wrapper's business.
				require.InDelta(t, kernel.want, got, 1e-3*kernel.want, "the result of %s", name)
			}
		}
	}
	// The waits were recorded, each with a walk of this stack.
	require.Positive(t, pprof.Lookup("block").Count(), "the block profile recorded nothing")
}

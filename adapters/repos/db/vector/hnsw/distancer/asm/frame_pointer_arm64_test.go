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
	"math"
	"os"
	"os/exec"
	"runtime"
	"runtime/pprof"
	"runtime/trace"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/cpu"
)

// Every arm64 kernel of this package must leave the stack of the Go function
// that calls it as it found it.
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
// The kernels generated from C with goat had that push; it is taken out by
// hand in each of them, and generating one anew brings it back. The kernels
// written by hand touch neither the stack nor the frame pointer, and are
// checked so that it stays that way.
//
// The check runs every kernel in a process of its own, because what it
// catches cannot be recovered from: the kernel, called by a function that
// then waits, under each of the three walks in turn.

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
	// Codes of 4 bits, two to a byte, both nibbles set.
	framePointerPackedA = make([]uint8, framePointerOddDims)
	framePointerPackedB = make([]uint8, framePointerOddDims)
)

func init() {
	for i := range framePointerFloatsA {
		framePointerFloatsA[i], framePointerFloatsB[i] = float32(i%7), float32(i%5)
		framePointerBytesA[i], framePointerBytesB[i] = uint8(i%7), uint8(i%5)
		framePointerPackedA[i], framePointerPackedB[i] = uint8(i*37+11), uint8(i*91+5)
	}
	for i := range framePointerWordsA {
		framePointerWordsA[i], framePointerWordsB[i] = uint64(i)*0x9e3779b97f4a7c15, uint64(i)*0xbf58476d1ce4e5b9
	}
}

// Every function below calls one kernel as its wrapper does and then waits,
// in a way that one of the walks records with the stack of this goroutine.
// The kernel must be called by the function that waits: a function in
// between would return over the damage, with its own saved frame pointer.

//go:noinline
func framePointerL2Neon(wait func(), n int) float64 {
	var res float32
	l := n
	l2_neon(unsafe.Pointer(&framePointerFloatsA[0]), unsafe.Pointer(&framePointerFloatsB[0]), unsafe.Pointer(&res), unsafe.Pointer(&l))
	wait()
	return float64(res)
}

//go:noinline
func framePointerDotNeon(wait func(), n int) float64 {
	var res float32
	l := n
	dot_neon(unsafe.Pointer(&framePointerFloatsA[0]), unsafe.Pointer(&framePointerFloatsB[0]), unsafe.Pointer(&res), unsafe.Pointer(&l))
	wait()
	return float64(res)
}

//go:noinline
func framePointerHamming(wait func(), n int) float64 {
	var res float32
	l := n
	hamming(unsafe.Pointer(&framePointerFloatsA[0]), unsafe.Pointer(&framePointerFloatsB[0]), unsafe.Pointer(&res), unsafe.Pointer(&l))
	wait()
	return float64(res)
}

//go:noinline
func framePointerHammingBitwise(wait func(), n int) float64 {
	var res uint64
	l := n
	hamming_bitwise(unsafe.Pointer(&framePointerWordsA[0]), unsafe.Pointer(&framePointerWordsB[0]), unsafe.Pointer(&res), unsafe.Pointer(&l))
	wait()
	return float64(res)
}

//go:noinline
func framePointerDotByte(wait func(), n int) float64 {
	var res uint32
	l := n
	dot_byte_256(unsafe.Pointer(&framePointerBytesA[0]), unsafe.Pointer(&framePointerBytesB[0]), unsafe.Pointer(&res), unsafe.Pointer(&l))
	wait()
	return float64(res)
}

//go:noinline
func framePointerL2Byte(wait func(), n int) float64 {
	var res uint32
	l := n
	l2_neon_byte_256(unsafe.Pointer(&framePointerBytesA[0]), unsafe.Pointer(&framePointerBytesB[0]), unsafe.Pointer(&res), unsafe.Pointer(&l))
	wait()
	return float64(res)
}

//go:noinline
func framePointerDotFloatByte(wait func(), n int) float64 {
	var res float32
	l := n
	dot_float_byte_neon(unsafe.Pointer(&framePointerFloatsA[0]), unsafe.Pointer(&framePointerBytesB[0]), unsafe.Pointer(&res), unsafe.Pointer(&l))
	wait()
	return float64(res)
}

//go:noinline
func framePointerL2SVE(wait func(), n int) float64 {
	var res float32
	l := n
	l2_sve(unsafe.Pointer(&framePointerFloatsA[0]), unsafe.Pointer(&framePointerFloatsB[0]), unsafe.Pointer(&res), unsafe.Pointer(&l))
	wait()
	return float64(res)
}

//go:noinline
func framePointerDotSVE(wait func(), n int) float64 {
	var res float32
	l := n
	dot_sve(unsafe.Pointer(&framePointerFloatsA[0]), unsafe.Pointer(&framePointerFloatsB[0]), unsafe.Pointer(&res), unsafe.Pointer(&l))
	wait()
	return float64(res)
}

// The kernels written by hand.

//go:noinline
func framePointerDotByteUDOT(wait func(), n int) float64 {
	res := dotByteUDOTAsm(&framePointerBytesA[0], &framePointerBytesB[0], n)
	wait()
	return float64(res)
}

//go:noinline
func framePointerL2ByteUDOT(wait func(), n int) float64 {
	res := l2ByteUDOTAsm(&framePointerBytesA[0], &framePointerBytesB[0], n)
	wait()
	return float64(res)
}

//go:noinline
func framePointerDotByteNibbleUDOT(wait func(), n int) float64 {
	res := dotByteNibbleUDOTAsm(&framePointerBytesA[0], &framePointerPackedB[0], n)
	wait()
	return float64(res)
}

//go:noinline
func framePointerDotByteNibbleUADALP(wait func(), n int) float64 {
	res := dotByteNibbleUADALPAsm(&framePointerBytesA[0], &framePointerPackedB[0], n)
	wait()
	return float64(res)
}

//go:noinline
func framePointerDotNibbleNibbleUDOT(wait func(), n int) float64 {
	res := dotNibbleNibbleUDOTAsm(&framePointerPackedA[0], &framePointerPackedB[0], n)
	wait()
	return float64(res)
}

//go:noinline
func framePointerDotNibbleNibbleUADALP(wait func(), n int) float64 {
	res := dotNibbleNibbleUADALPAsm(&framePointerPackedA[0], &framePointerPackedB[0], n)
	wait()
	return float64(res)
}

//go:noinline
func framePointerPrefetch(wait func(), _ int) float64 {
	Prefetch(uintptr(unsafe.Pointer(&framePointerFloatsA[0])))
	wait()
	return 0
}

//go:noinline
func framePointerPrefetchN(wait func(), n int) float64 {
	PrefetchN(uintptr(unsafe.Pointer(&framePointerFloatsA[0])), 4*n)
	wait()
	return 0
}

//go:noinline
func framePointerSVEVectorLength(wait func(), _ int) float64 {
	res := SVEVectorLengthBytes()
	wait()
	return float64(res)
}

// framePointerKernel is one kernel under the check: how it is run, what it
// has to return for the inputs above, and whether this machine can run it.
type framePointerKernel struct {
	name string
	run  func(wait func(), n int) float64
	// unit is how many dimensions one unit of the length of the kernel
	// covers: 64 for a word of bits, 2 for a byte of two codes of 4 bits.
	// Zero is one.
	unit int
	// want is what run returns at framePointerDims dimensions; a prefetch
	// returns nothing and wants zero. check, when set, judges the result
	// instead.
	want      float64
	check     func(got float64) bool
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
	// Byte j of a packed code holds dimension j in its low nibble and
	// dimension j+half in its high one.
	var dotByteNibble, dotNibbleNibble float64
	half := framePointerDims / 2
	for j := 0; j < half; j++ {
		pa, pb := framePointerPackedA[j], framePointerPackedB[j]
		dotByteNibble += float64(framePointerBytesA[j])*float64(pb&0x0f) + float64(framePointerBytesA[j+half])*float64(pb>>4)
		dotNibbleNibble += float64(pa&0x0f)*float64(pb&0x0f) + float64(pa>>4)*float64(pb>>4)
	}
	return []framePointerKernel{
		// Generated from C with goat.
		{name: "l2_neon", run: framePointerL2Neon, want: l2, available: true},
		{name: "dot_neon", run: framePointerDotNeon, want: dot, available: true},
		{name: "hamming", run: framePointerHamming, want: differ, available: true},
		{name: "hamming_bitwise", run: framePointerHammingBitwise, unit: 64, want: bits, available: true},
		{name: "dot_byte_256", run: framePointerDotByte, want: dotBytes, available: true},
		{name: "l2_neon_byte_256", run: framePointerL2Byte, want: l2Bytes, available: true},
		{name: "dot_float_byte_neon", run: framePointerDotFloatByte, want: dotFloatByte, available: true},
		{name: "l2_sve", run: framePointerL2SVE, want: l2, available: cpu.ARM64.HasSVE},
		{name: "dot_sve", run: framePointerDotSVE, want: dot, available: cpu.ARM64.HasSVE},
		// Written by hand, each behind the feature its callers check.
		{name: "dotByteUDOTAsm", run: framePointerDotByteUDOT, want: dotBytes, available: cpu.ARM64.HasASIMDDP},
		{name: "l2ByteUDOTAsm", run: framePointerL2ByteUDOT, want: l2Bytes, available: cpu.ARM64.HasASIMDDP},
		{name: "dotByteNibbleUDOTAsm", run: framePointerDotByteNibbleUDOT, unit: 2, want: dotByteNibble, available: cpu.ARM64.HasASIMDDP},
		{name: "dotByteNibbleUADALPAsm", run: framePointerDotByteNibbleUADALP, unit: 2, want: dotByteNibble, available: cpu.ARM64.HasASIMD},
		{name: "dotNibbleNibbleUDOTAsm", run: framePointerDotNibbleNibbleUDOT, unit: 2, want: dotNibbleNibble, available: cpu.ARM64.HasASIMDDP},
		{name: "dotNibbleNibbleUADALPAsm", run: framePointerDotNibbleNibbleUADALP, unit: 2, want: dotNibbleNibble, available: cpu.ARM64.HasASIMD},
		{name: "Prefetch", run: framePointerPrefetch, available: true},
		{name: "PrefetchN", run: framePointerPrefetchN, available: true},
		{name: "SVEVectorLengthBytes", run: framePointerSVEVectorLength, check: func(got float64) bool {
			// A multiple of 128 bits, from 128 to 2048.
			return got >= 16 && got <= 256 && math.Mod(got, 16) == 0
		}, available: cpu.ARM64.HasSVE},
	}
}

// framePointerWalk is one of the walks of a stack by frame pointers that the
// runtime makes while a server runs: start turns it on and gives back how to
// turn it off and see it recorded something; wait is what the function that
// called the kernel does next, made fresh for each call, and done what to do
// after that function returned.
type framePointerWalk struct {
	name  string
	start func(t *testing.T) (stop func())
	wait  func() (wait, done func())
}

// framePointerChannelWait blocks on a channel until a timer closes it.
func framePointerChannelWait() (wait, done func()) {
	ch := make(chan struct{})
	timer := time.AfterFunc(2*time.Millisecond, func() { close(ch) })
	return func() { <-ch }, func() { timer.Stop() }
}

// framePointerCountingWriter counts the bytes of a trace.
type framePointerCountingWriter struct{ n atomic.Int64 }

func (w *framePointerCountingWriter) Write(p []byte) (int, error) {
	w.n.Add(int64(len(p)))
	return len(p), nil
}

var framePointerWalks = []framePointerWalk{
	{
		// GO_BLOCK_PROFILE_RATE: the wait is recorded when it ends.
		name: "block",
		start: func(t *testing.T) func() {
			runtime.SetBlockProfileRate(1)
			return func() {
				runtime.SetBlockProfileRate(0)
				require.Positive(t, pprof.Lookup("block").Count(), "the block profile recorded nothing")
			}
		},
		wait: framePointerChannelWait,
	},
	{
		// GO_MUTEX_PROFILE_FRACTION: the contention is recorded by the
		// goroutine that unlocks, with its own stack.
		name: "mutex",
		start: func(t *testing.T) func() {
			runtime.SetMutexProfileFraction(1)
			return func() {
				runtime.SetMutexProfileFraction(0)
				require.Positive(t, pprof.Lookup("mutex").Count(), "the mutex profile recorded nothing")
			}
		},
		wait: func() (wait, done func()) {
			var mu sync.Mutex
			mu.Lock()
			acquired := make(chan struct{})
			time.AfterFunc(0, func() {
				mu.Lock()
				close(acquired)
				mu.Unlock()
			})
			// Time for the other goroutine to queue on the mutex.
			time.Sleep(2 * time.Millisecond)
			return mu.Unlock, func() { <-acquired }
		},
	},
	{
		// The execution tracer, as /debug/pprof/trace starts it: the wait is
		// recorded when it begins.
		name: "trace",
		start: func(t *testing.T) func() {
			var w framePointerCountingWriter
			require.NoError(t, trace.Start(&w))
			return func() {
				trace.Stop()
				require.Positive(t, w.n.Load(), "the tracer wrote nothing")
			}
		},
		wait: framePointerChannelWait,
	},
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
			for _, walk := range framePointerWalks {
				t.Run(walk.name, func(t *testing.T) {
					cmd := exec.Command(os.Args[0], "-test.run=^TestKernelsKeepTheFramePointer$", "-test.count=1")
					cmd.Env = append(os.Environ(), framePointerKernelEnv+"="+kernel.name+"/"+walk.name)
					out, err := cmd.CombinedOutput()
					// What a crash prints is the dump of every goroutine: its
					// first lines say what happened and where.
					lines := strings.SplitN(string(out), "\n", 10)
					head := strings.Join(lines[:min(9, len(lines))], "\n")
					require.NoErrorf(t, err, "after %s the stack of its caller cannot be walked by frame pointers (%s):\n%s", kernel.name, walk.name, head)
					require.Contains(t, string(out), "PASS")
				})
			}
		})
	}
}

// framePointerChild runs one kernel and waits after it, with one walk
// recording every wait. It runs in a process of its own.
func framePointerChild(t *testing.T, name string) {
	kernelName, walkName, _ := strings.Cut(name, "/")
	var kernel *framePointerKernel
	for _, k := range framePointerKernels() {
		if k.name == kernelName {
			kernel = &k
		}
	}
	require.NotNilf(t, kernel, "no kernel %q", kernelName)
	var walk *framePointerWalk
	for i := range framePointerWalks {
		if framePointerWalks[i].name == walkName {
			walk = &framePointerWalks[i]
		}
	}
	require.NotNilf(t, walk, "no walk %q", walkName)

	stop := walk.start(t)
	unit := max(kernel.unit, 1)
	lengths := []int{framePointerDims / unit, framePointerOddDims / unit}
	for i := 0; i < 10; i++ {
		for at, n := range lengths {
			wait, done := walk.wait()
			got := kernel.run(wait, n)
			done()
			if at != 0 {
				// What a kernel does with a tail is its wrapper's business.
				continue
			}
			// The kernel ran its assembly, over all the input.
			if kernel.check != nil {
				require.Truef(t, kernel.check(got), "the result of %s: %v", kernelName, got)
			} else {
				require.InDelta(t, kernel.want, got, 1e-3*kernel.want, "the result of %s", kernelName)
			}
		}
	}
	// The waits were recorded, each with a walk of this stack.
	stop()
}

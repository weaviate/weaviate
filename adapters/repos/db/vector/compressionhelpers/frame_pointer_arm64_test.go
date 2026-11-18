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

//go:build arm64

package compressionhelpers

import (
	"os"
	"os/exec"
	"runtime"
	"runtime/pprof"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// The arm64 kernels of this package must leave the stack of the Go function
// that calls them as they found it, like those of distancer/asm
// (TestKernelsKeepTheFramePointer there has the whole story). Go on arm64
// keeps the frame pointer a function saved in the 8 bytes right below its
// stack pointer, and a kernel has no frame of its own: one write below the
// stack pointer and the next walk of the stack by frame pointers, which the
// block profile, the mutex profile and the tracer do, takes the process down.
//
// These kernels are written by hand and touch neither the stack nor the
// frame pointer. The check pins that: every kernel in a process of its own,
// called by a function that then blocks with the block profile on.

const framePointerKernelEnv = "COMPRESSIONHELPERS_FRAME_POINTER_KERNEL"

//go:noinline
func framePointerBlock(ch chan struct{}) {
	<-ch
}

// The kernel must be called by the function that blocks: a function in
// between would return over the damage, with its own saved frame pointer.

//go:noinline
func framePointerWeightedHamming(ch chan struct{}, q [5][]uint64, x []uint64) int {
	got := weightedHammingAsm(&q[0][0], &q[1][0], &q[2][0], &q[3][0], &q[4][0], &x[0], len(x)/weightedHammingChunkWords)
	framePointerBlock(ch)
	return got
}

//go:noinline
func framePointerRQ4QuantCorr(ch chan struct{}, xs []float32, codes []int32) (float32, int32, int32) {
	sumXC, sumC, sumC2 := rq4QuantCorrAsm(&xs[0], &codes[0], len(xs), 2, 0.5)
	framePointerBlock(ch)
	return sumXC, sumC, sumC2
}

//go:noinline
func framePointerRQ4MinMaxSum(ch chan struct{}, xs []float32) (float32, float32, float32) {
	minV, maxV, sum := rq4MinMaxSumAsm(&xs[0], len(xs))
	framePointerBlock(ch)
	return minV, maxV, sum
}

func TestKernelsKeepTheFramePointer(t *testing.T) {
	// 24 words is a code of 1536 bits, and 12 chunks of the kernel.
	var q [5][]uint64
	x := make([]uint64, 24)
	for plane := range q {
		q[plane] = make([]uint64, len(x))
		for i := range x {
			q[plane][i] = uint64(i+1) * 0x9e3779b97f4a7c15 * uint64(plane+1)
		}
	}
	for i := range x {
		x[i] = uint64(i+1) * 0xbf58476d1ce4e5b9
	}
	xs := make([]float32, 256)
	for i := range xs {
		xs[i] = float32(i%16) / 4
	}
	kernels := []struct {
		name string
		run  func(t *testing.T, ch chan struct{})
	}{
		{name: "weightedHammingAsm", run: func(t *testing.T, ch chan struct{}) {
			got := framePointerWeightedHamming(ch, q, x)
			require.Equal(t, weightedHammingGo(q[0], q[1], q[2], q[3], q[4], x), got)
		}},
		{name: "rq4QuantCorrAsm", run: func(t *testing.T, ch chan struct{}) {
			codes := make([]int32, len(xs))
			_, sumC, _ := framePointerRQ4QuantCorr(ch, xs, codes)
			total := int32(0)
			for _, c := range codes {
				total += c
			}
			require.Equal(t, total, sumC, "the kernel wrote its codes")
			require.Positive(t, sumC)
		}},
		{name: "rq4MinMaxSumAsm", run: func(t *testing.T, ch chan struct{}) {
			minV, maxV, sum := framePointerRQ4MinMaxSum(ch, xs)
			require.Equal(t, float32(0), minV)
			require.Equal(t, float32(15)/4, maxV)
			require.InDelta(t, float32(16*(0+15)*16/2)/4, sum, 1e-3)
		}},
	}

	if name := os.Getenv(framePointerKernelEnv); name != "" {
		// The child: one kernel, then a blocking wait that the block profile
		// records with a walk of this stack.
		runtime.SetBlockProfileRate(1)
		defer runtime.SetBlockProfileRate(0)
		ran := false
		for _, kernel := range kernels {
			if kernel.name != name {
				continue
			}
			ran = true
			for i := 0; i < 10; i++ {
				ch := make(chan struct{})
				timer := time.AfterFunc(2*time.Millisecond, func() { close(ch) })
				kernel.run(t, ch)
				timer.Stop()
			}
		}
		require.Truef(t, ran, "no kernel %q", name)
		require.Positive(t, pprof.Lookup("block").Count(), "the block profile recorded nothing")
		return
	}
	for _, kernel := range kernels {
		t.Run(kernel.name, func(t *testing.T) {
			cmd := exec.Command(os.Args[0], "-test.run=^TestKernelsKeepTheFramePointer$", "-test.count=1")
			cmd.Env = append(os.Environ(), framePointerKernelEnv+"="+kernel.name)
			out, err := cmd.CombinedOutput()
			head := strings.Join(strings.SplitN(string(out), "\n", 8)[:min(7, strings.Count(string(out), "\n")+1)], "\n")
			require.NoErrorf(t, err, "after %s the stack of its caller cannot be walked by frame pointers:\n%s", kernel.name, head)
			require.Contains(t, string(out), "PASS")
		})
	}
}

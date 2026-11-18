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
	"golang.org/x/sys/cpu"
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
// called by a function that then blocks with the block profile on. That one
// walk is enough to see the damage: the block profile, the mutex profile and
// the tracer all read the same slot, and distancer/asm runs each of them.

const framePointerKernelEnv = "COMPRESSIONHELPERS_FRAME_POINTER_KERNEL"

//go:noinline
func framePointerBlock(ch chan struct{}) {
	<-ch
}

// The kernel must be called by the function that blocks: a function in
// between would return over the damage, with its own saved frame pointer.

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
	// A multiple of 16, as the kernels take.
	xs := make([]float32, 256)
	for i := range xs {
		xs[i] = float32(i%16) / 4
	}
	kernels := []struct {
		name      string
		run       func(t *testing.T, ch chan struct{})
		available bool
	}{
		{name: "rq4QuantCorrAsm", available: cpu.ARM64.HasASIMD, run: func(t *testing.T, ch chan struct{}) {
			codes := make([]int32, len(xs))
			_, sumC, sumC2 := framePointerRQ4QuantCorr(ch, xs, codes)
			wantCodes := make([]int32, len(xs))
			_, wantC, wantC2 := rq4QuantCorrGo(wantCodes, xs, 2, 0.5)
			require.Equal(t, wantCodes, codes)
			require.Equal(t, wantC, sumC)
			require.Equal(t, wantC2, sumC2)
			require.Positive(t, sumC)
		}},
		{name: "rq4MinMaxSumAsm", available: cpu.ARM64.HasASIMD, run: func(t *testing.T, ch chan struct{}) {
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
			if !kernel.available {
				t.Skip("this machine cannot run the kernel")
			}
			cmd := exec.Command(os.Args[0], "-test.run=^TestKernelsKeepTheFramePointer$", "-test.count=1")
			cmd.Env = append(os.Environ(), framePointerKernelEnv+"="+kernel.name)
			out, err := cmd.CombinedOutput()
			lines := strings.SplitN(string(out), "\n", 10)
			head := strings.Join(lines[:min(9, len(lines))], "\n")
			require.NoErrorf(t, err, "after %s the stack of its caller cannot be walked by frame pointers:\n%s", kernel.name, head)
			require.Contains(t, string(out), "PASS")
		})
	}
}

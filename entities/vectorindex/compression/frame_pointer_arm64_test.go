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

package compression

import (
	"math"
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
// that calls them as they found it, like those of
// adapters/repos/db/vector/hnsw/distancer/asm (TestKernelsKeepTheFramePointer
// there has the whole story). Go on arm64 keeps the frame pointer a function
// saved in the 8 bytes right below its stack pointer, and a kernel has no
// frame of its own: one write below the stack pointer and the next walk of
// the stack by frame pointers, which the block profile, the mutex profile
// and the tracer do, takes the process down.
//
// These kernels are written by hand and touch neither the stack nor the
// frame pointer. The check pins that: every kernel in a process of its own,
// called by a function that then blocks with the block profile on. That one
// walk is enough to see the damage: the block profile, the mutex profile and
// the tracer all read the same slot, and distancer/asm runs each of them.

const framePointerKernelEnv = "COMPRESSION_FRAME_POINTER_KERNEL"

//go:noinline
func framePointerBlock(ch chan struct{}) {
	<-ch
}

// The kernel must be called by the function that blocks: a function in
// between would return over the damage, with its own saved frame pointer.

//go:noinline
func framePointerFWHT64(ch chan struct{}, x []float32) {
	fwht64NEON(&x[0])
	framePointerBlock(ch)
}

//go:noinline
func framePointerFWHT256(ch chan struct{}, x []float32) {
	fwht256NEON(&x[0])
	framePointerBlock(ch)
}

func TestKernelsKeepTheFramePointer(t *testing.T) {
	// ASIMD is part of ARMv8-A: the callers of these kernels check nothing.
	kernels := []struct {
		name      string
		n         int
		run       func(ch chan struct{}, x []float32)
		reference func(x []float32)
	}{
		{name: "fwht64NEON", n: 64, run: framePointerFWHT64, reference: fastWalshHadamardTransform64Go},
		{name: "fwht256NEON", n: 256, run: framePointerFWHT256, reference: fastWalshHadamardTransform256Go},
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
				x := make([]float32, kernel.n)
				for j := range x {
					x[j] = float32((j*7+i)%13) - 6
				}
				want := make([]float32, kernel.n)
				copy(want, x)
				kernel.reference(want)

				ch := make(chan struct{})
				timer := time.AfterFunc(2*time.Millisecond, func() { close(ch) })
				kernel.run(ch, x)
				timer.Stop()
				for j := range x {
					require.Equal(t, math.Float32bits(want[j]), math.Float32bits(x[j]), "%s at %d", name, j)
				}
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
			lines := strings.SplitN(string(out), "\n", 10)
			head := strings.Join(lines[:min(9, len(lines))], "\n")
			require.NoErrorf(t, err, "after %s the stack of its caller cannot be walked by frame pointers:\n%s", kernel.name, head)
			require.Contains(t, string(out), "PASS")
		})
	}
}

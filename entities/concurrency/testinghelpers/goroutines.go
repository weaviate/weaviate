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

// Package testinghelpers provides shared assertions for concurrency-budget
// tests: verifying hot read paths don't fan out more goroutines than budgeted.
package testinghelpers

import (
	"bytes"
	"context"
	"fmt"
	"runtime/pprof"
	"strconv"
	"sync"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ceilingLabel is set on AssertGoroutineCeiling's workers and inherited by
// every goroutine they start, so other goroutines in the process are not counted.
const ceilingLabel = "goroutine-ceiling"

// AssertGoroutineCeiling runs do from numWorkers goroutines for runFor and asserts that
// they and their descendants never exceed numWorkers*maxPerWorker + noiseSlack. Each
// worker counts toward its own maxPerWorker. noiseSlack covers joined children not yet exited.
func AssertGoroutineCeiling(t require.TestingT, numWorkers, maxPerWorker, noiseSlack int,
	runFor time.Duration, do func() error,
) {
	if h, ok := t.(interface{ Helper() }); ok {
		h.Helper()
	}

	stop := make(chan struct{})
	samplerDone := make(chan struct{})
	// written only by the sampler, read after samplerDone closes
	var (
		maxSeen   int
		sampleErr error
	)

	go func() {
		defer close(samplerDone)
		ticker := time.NewTicker(1 * time.Millisecond)
		defer ticker.Stop()
		var buf bytes.Buffer
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				n, err := countLabelled(&buf)
				if err != nil {
					sampleErr = err
					return
				}
				maxSeen = max(maxSeen, n)
			}
		}
	}()

	var (
		mu       sync.Mutex
		firstErr error
	)
	// abort stops remaining workers early once one fails.
	abort := make(chan struct{})
	var wg sync.WaitGroup
	deadline := time.Now().Add(runFor)
	labels := pprof.Labels(ceilingLabel, "counted")
	for range numWorkers {
		wg.Add(1)
		go pprof.Do(context.Background(), labels, func(context.Context) {
			defer wg.Done()
			for time.Now().Before(deadline) {
				select {
				case <-abort:
					return
				default:
				}
				if err := do(); err != nil {
					mu.Lock()
					if firstErr == nil {
						firstErr = err
						close(abort)
					}
					mu.Unlock()
					return
				}
			}
		})
	}
	wg.Wait()
	close(stop)
	<-samplerDone

	require.NoError(t, firstErr)
	require.NoError(t, sampleErr)

	ceiling := numWorkers*maxPerWorker + noiseSlack
	assert.LessOrEqualf(t, maxSeen, ceiling,
		"goroutines started by do peaked at %d, above workers(%d)*perWorker(%d)+noise(%d)=%d; "+
			"the concurrency budget must bound the fan-out",
		maxSeen, numWorkers, maxPerWorker, noiseSlack, ceiling)
}

// countLabelled counts live goroutines carrying ceilingLabel. It reads the
// goroutine profile, a consistent snapshot, because runtime.NumGoroutine
// counts the whole process and can overshoot while goroutines exit.
func countLabelled(buf *bytes.Buffer) (int, error) {
	buf.Reset()
	if err := pprof.Lookup("goroutine").WriteTo(buf, 1); err != nil {
		return 0, err
	}
	// debug=1 prints each stack as "<count> @ <pcs>", then its labels if any.
	marker := []byte(strconv.Quote(ceilingLabel) + ":")
	total, count := 0, 0
	for line := range bytes.Lines(buf.Bytes()) {
		if bytes.HasPrefix(line, []byte("# labels: ")) {
			if bytes.Contains(line, marker) {
				total += count
			}
			continue
		}
		if c, _, ok := bytes.Cut(line, []byte(" @ ")); ok {
			var err error
			if count, err = strconv.Atoi(string(c)); err != nil {
				return 0, fmt.Errorf("parse goroutine profile line %q: %w", line, err)
			}
		}
	}
	return total, nil
}

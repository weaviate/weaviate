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
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// AssertGoroutineCeiling runs do from numWorkers goroutines for runFor and
// asserts peak live goroutines stay within baseline + numWorkers*maxPerWorker
// + noiseSlack, where maxPerWorker bounds the goroutines one in-flight do
// call may legitimately hold.
func AssertGoroutineCeiling(t *testing.T, numWorkers, maxPerWorker, noiseSlack int,
	runFor time.Duration, do func() error,
) {
	t.Helper()

	stop := make(chan struct{})
	samplerDone := make(chan struct{})
	var maxSeen int // written only by the sampler, read after samplerDone closes

	go func() {
		defer close(samplerDone)
		ticker := time.NewTicker(1 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				if n := runtime.NumGoroutine(); n > maxSeen {
					maxSeen = n
				}
			}
		}
	}()

	// let the sampler settle before capturing the baseline
	time.Sleep(5 * time.Millisecond)
	base := runtime.NumGoroutine()

	firstErr := make(chan error, 1)
	var wg sync.WaitGroup
	deadline := time.Now().Add(runFor)
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for time.Now().Before(deadline) {
				if err := do(); err != nil {
					select {
					case firstErr <- err:
					default:
					}
					return
				}
			}
		}()
	}
	wg.Wait()
	close(stop)
	<-samplerDone

	select {
	case err := <-firstErr:
		require.NoError(t, err)
	default:
	}

	ceiling := base + numWorkers*maxPerWorker + noiseSlack
	assert.LessOrEqualf(t, maxSeen, ceiling,
		"live goroutines peaked at %d, above base(%d)+workers(%d)*perWorker(%d)+noise(%d)=%d; "+
			"the concurrency budget must bound the fan-out",
		maxSeen, base, numWorkers, maxPerWorker, noiseSlack, ceiling)
}

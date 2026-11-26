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

package testinghelpers

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type recordingT struct {
	mu     sync.Mutex
	failed bool
}

func (r *recordingT) Errorf(string, ...any) { r.fail() }
func (r *recordingT) FailNow()              { r.fail() }

func (r *recordingT) fail() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.failed = true
}

// spawnAndJoin starts n goroutines that each hold for d, and waits for them.
func spawnAndJoin(n int, d time.Duration) {
	var wg sync.WaitGroup
	for range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			time.Sleep(d)
		}()
	}
	wg.Wait()
}

func TestAssertGoroutineCeiling(t *testing.T) {
	const (
		numWorkers = 4
		// one child per worker may still be exiting when the next do starts
		noiseSlack = numWorkers
		runFor     = 50 * time.Millisecond
	)

	tests := []struct {
		name string
		// setup returns what each worker runs, and a func releasing what setup started.
		setup    func() (do func() error, cleanup func())
		perW     int
		wantFail bool
	}{
		{
			name: "children within the per-worker bound pass",
			setup: func() (func() error, func()) {
				return func() error { spawnAndJoin(1, time.Millisecond); return nil }, func() {}
			},
			perW: 2,
		},
		{
			name: "children beyond the per-worker bound fail",
			setup: func() (func() error, func()) {
				return func() error { spawnAndJoin(8, 2*time.Millisecond); return nil }, func() {}
			},
			perW:     1,
			wantFail: true,
		},
		{
			name: "goroutines started outside do are not counted",
			setup: func() (func() error, func()) {
				start, release := make(chan struct{}), make(chan struct{})
				var once sync.Once
				// unrelated goroutines start while the workers run
				go func() {
					<-start
					for range 50 {
						go func() { <-release }()
					}
				}()
				do := func() error {
					once.Do(func() { close(start) })
					time.Sleep(time.Millisecond)
					return nil
				}
				return do, func() { close(release) }
			},
			perW: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			do, cleanup := tt.setup()
			t.Cleanup(cleanup)

			rec := &recordingT{}
			AssertGoroutineCeiling(rec, numWorkers, tt.perW, noiseSlack, runFor, do)
			assert.Equal(t, tt.wantFail, rec.failed)
		})
	}
}

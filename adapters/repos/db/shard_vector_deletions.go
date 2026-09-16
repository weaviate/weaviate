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

package db

import (
	"context"
	"sync"
)

// vectorDeletions gates a drop's file removal against a transfer halt, the
// way compaction is paused: paused, a drop leaves its files and hands its
// name over for the resume. The zero value is ready to use.
type vectorDeletions struct {
	mu       sync.Mutex
	paused   bool
	inFlight int
	idle     chan struct{} // closed when inFlight reaches zero, for a waiting Pause
	deferred []string
}

// Enter is called by a drop before it removes anything. Not paused: the
// deletion is counted in and runs now, leave counts it out. Paused: the
// name is queued and nothing runs.
func (d *vectorDeletions) Enter(name string) (now bool, leave func()) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.paused {
		d.deferred = append(d.deferred, name)
		return false, func() {}
	}
	d.inFlight++
	return true, d.leave
}

func (d *vectorDeletions) leave() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.inFlight--
	if d.inFlight == 0 && d.idle != nil {
		close(d.idle)
		d.idle = nil
	}
}

// Pause is called by the halt before it lists anything: from here on drops
// defer, and the deletions already running finish first.
func (d *vectorDeletions) Pause(ctx context.Context) error {
	d.mu.Lock()
	d.paused = true
	if d.inFlight == 0 {
		d.mu.Unlock()
		return nil
	}
	if d.idle == nil {
		d.idle = make(chan struct{})
	}
	idle := d.idle
	d.mu.Unlock()

	select {
	case <-idle:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Resume is called by the last resume: drops delete right away again, and
// the names deferred meanwhile are returned for the deletion job.
func (d *vectorDeletions) Resume() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.paused = false
	names := d.deferred
	d.deferred = nil
	return names
}

// running reports the deletions counted in; tests poll it.
func (d *vectorDeletions) running() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.inFlight
}

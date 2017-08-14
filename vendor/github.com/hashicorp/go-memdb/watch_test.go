package memdb

import (
	"fmt"
	"testing"
	"time"
)

// testWatch makes a bunch of watch channels based on the given size and fires
// the one at the given fire index to make sure it's detected (or a timeout
// occurs if the fire index isn't hit).
func testWatch(size, fire int) error {
	shouldTimeout := true
	ws := NewWatchSet()
	for i := 0; i < size; i++ {
		watchCh := make(chan struct{})
		ws.Add(watchCh)
		if fire == i {
			close(watchCh)
			shouldTimeout = false
		}
	}

	timeoutCh := make(chan time.Time)
	doneCh := make(chan bool, 1)
	go func() {
		doneCh <- ws.Watch(timeoutCh)
	}()

	if shouldTimeout {
		select {
		case <-doneCh:
			return fmt.Errorf("should not trigger")
		default:
		}

		close(timeoutCh)
		select {
		case didTimeout := <-doneCh:
			if !didTimeout {
				return fmt.Errorf("should have timed out")
			}
		case <-time.After(10 * time.Second):
			return fmt.Errorf("should have timed out")
		}
	} else {
		select {
		case didTimeout := <-doneCh:
			if didTimeout {
				return fmt.Errorf("should not have timed out")
			}
		case <-time.After(10 * time.Second):
			return fmt.Errorf("should have triggered")
		}
		close(timeoutCh)
	}
	return nil
}

func TestWatch(t *testing.T) {
	// Sweep through a bunch of chunks to hit the various cases of dividing
	// the work into watchFew calls.
	for size := 0; size < 3*aFew; size++ {
		// Fire each possible channel slot.
		for fire := 0; fire < size; fire++ {
			if err := testWatch(size, fire); err != nil {
				t.Fatalf("err %d %d: %v", size, fire, err)
			}
		}

		// Run a timeout case as well.
		fire := -1
		if err := testWatch(size, fire); err != nil {
			t.Fatalf("err %d %d: %v", size, fire, err)
		}
	}
}

func TestWatch_AddWithLimit(t *testing.T) {
	// Make sure nil doesn't crash.
	{
		var ws WatchSet
		ch := make(chan struct{})
		ws.AddWithLimit(10, ch, ch)
	}

	// Run a case where we trigger a channel that should be in
	// there.
	{
		ws := NewWatchSet()
		inCh := make(chan struct{})
		altCh := make(chan struct{})
		ws.AddWithLimit(1, inCh, altCh)

		nopeCh := make(chan struct{})
		ws.AddWithLimit(1, nopeCh, altCh)

		close(inCh)
		didTimeout := ws.Watch(time.After(1 * time.Second))
		if didTimeout {
			t.Fatalf("bad")
		}
	}

	// Run a case where we trigger the alt channel that should have
	// been added.
	{
		ws := NewWatchSet()
		inCh := make(chan struct{})
		altCh := make(chan struct{})
		ws.AddWithLimit(1, inCh, altCh)

		nopeCh := make(chan struct{})
		ws.AddWithLimit(1, nopeCh, altCh)

		close(altCh)
		didTimeout := ws.Watch(time.After(1 * time.Second))
		if didTimeout {
			t.Fatalf("bad")
		}
	}

	// Run a case where we trigger the nope channel that should not have
	// been added.
	{
		ws := NewWatchSet()
		inCh := make(chan struct{})
		altCh := make(chan struct{})
		ws.AddWithLimit(1, inCh, altCh)

		nopeCh := make(chan struct{})
		ws.AddWithLimit(1, nopeCh, altCh)

		close(nopeCh)
		didTimeout := ws.Watch(time.After(1 * time.Second))
		if !didTimeout {
			t.Fatalf("bad")
		}
	}
}

func BenchmarkWatch(b *testing.B) {
	ws := NewWatchSet()
	for i := 0; i < 1024; i++ {
		watchCh := make(chan struct{})
		ws.Add(watchCh)
	}

	timeoutCh := make(chan time.Time)
	close(timeoutCh)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ws.Watch(timeoutCh)
	}
}

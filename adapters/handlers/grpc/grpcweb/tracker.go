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

package grpcweb

import (
	"net/http"
	"sync"
)

// Tracker counts in-flight grpc-web requests so shutdown can wait for them
// before stopping the gRPC server they run on. The zero value is ready to use.
type Tracker struct {
	// mu guards all fields. A request is counted before Drain starts, or it is
	// refused. So inFlight never grows once draining is set.
	mu       sync.Mutex
	inFlight int
	draining bool
	// zero is created by Drain and closed exactly once, when draining is set
	// and inFlight is 0.
	zero chan struct{}
}

// wrap serves h while not draining and refuses with 503 once Drain has started.
func (t *Tracker) wrap(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !t.enter() {
			http.Error(w, "server is shutting down", http.StatusServiceUnavailable)
			return
		}
		defer t.leave()
		h.ServeHTTP(w, r)
	})
}

func (t *Tracker) enter() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.draining {
		return false
	}
	t.inFlight++
	return true
}

func (t *Tracker) leave() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.inFlight--
	if t.draining && t.inFlight == 0 {
		close(t.zero)
	}
}

// Drain refuses new requests and waits for the in-flight ones. It returns true
// once none are in flight, or false if stop closes first.
func (t *Tracker) Drain(stop <-chan struct{}) bool {
	zero := func() chan struct{} {
		t.mu.Lock()
		defer t.mu.Unlock()
		if !t.draining {
			t.draining = true
			t.zero = make(chan struct{})
			if t.inFlight == 0 {
				close(t.zero)
			}
		}
		return t.zero
	}()

	select {
	case <-zero:
		return true
	case <-stop:
		return false
	}
}

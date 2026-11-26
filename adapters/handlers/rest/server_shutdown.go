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

package rest

import (
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// ServeAndShutdown runs Serve, closes servers still busy past GracefulTimeout and waits
// up to GracefulTimeout more for their handlers to return. It then runs ServerShutdown
// once, which the generated handleShutdown skips after a timeout. Call it after ConfigureAPI.
func (s *Server) ServeAndShutdown() error {
	serverShutdown := sync.OnceFunc(s.api.ServerShutdown)
	s.api.ServerShutdown = serverShutdown

	var tracked trackedServers
	defer tracked.track()()

	if err := s.Serve(); err != nil {
		return err
	}
	if n := tracked.closeAndWait(s.GracefulTimeout, s.Logf); n > 0 {
		s.Logf("%d HTTP handlers still running %s after their servers closed", n, s.GracefulTimeout)
	}
	serverShutdown()
	return nil
}

// trackedServers records the servers Serve configures and counts their running handlers.
type trackedServers struct {
	servers []*http.Server
	running atomic.Int64
}

// track swaps in a wrapper for configureServer, the only hook into the
// servers the generated Serve creates, and returns a func that puts it back.
func (t *trackedServers) track() (restore func()) {
	original := configureServer
	configureServer = func(hs *http.Server, scheme, addr string) {
		original(hs, scheme, addr)
		// Count handlers so closeAndWait can wait for them.
		inner := hs.Handler
		hs.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			t.running.Add(1)
			defer t.running.Add(-1)
			inner.ServeHTTP(w, r)
		})
		t.servers = append(t.servers, hs)
	}
	return func() { configureServer = original }
}

// closeAndWait closes every server and waits up to timeout for their handlers, since one
// woken by a failed read may still be using the database. It returns how many still run.
func (t *trackedServers) closeAndWait(timeout time.Duration, logf func(string, ...any)) int64 {
	for _, hs := range t.servers {
		if err := hs.Close(); err != nil {
			logf("HTTP server Close: %v", err)
		}
	}
	// A handler can start after Close, which rules out a sync.WaitGroup.
	deadline := time.Now().Add(timeout)
	for {
		n := t.running.Load()
		if n == 0 || !time.Now().Before(deadline) {
			return n
		}
		time.Sleep(10 * time.Millisecond)
	}
}

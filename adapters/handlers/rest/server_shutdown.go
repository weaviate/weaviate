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
// exactly once, which the generated handleShutdown would skip. Call it after ConfigureAPI.
func (s *Server) ServeAndShutdown() error {
	serverShutdown := sync.OnceFunc(s.api.ServerShutdown)
	s.api.ServerShutdown = serverShutdown

	// Serve calls configureServer for every listener before it starts serving.
	var servers []*http.Server
	var running atomic.Int64
	configure := configureServer
	configureServer = func(hs *http.Server, scheme, addr string) {
		configure(hs, scheme, addr)
		next := hs.Handler
		hs.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			running.Add(1)
			defer running.Add(-1)
			next.ServeHTTP(w, r)
		})
		servers = append(servers, hs)
	}
	defer func() { configureServer = configure }()

	if err := s.Serve(); err != nil {
		return err
	}
	for _, hs := range servers {
		if err := hs.Close(); err != nil {
			s.Logf("HTTP server Close: %v", err)
		}
	}
	// Close does not wait for handlers, and one woken by a failed read may still
	// be using the database.
	if n := waitForHandlers(&running, s.GracefulTimeout); n > 0 {
		s.Logf("%d HTTP handlers still running %s after their servers closed", n, s.GracefulTimeout)
	}
	serverShutdown()
	return nil
}

// waitForHandlers polls like http.Server.Shutdown does, because a handler can
// still start after Close, which rules out a sync.WaitGroup.
func waitForHandlers(running *atomic.Int64, timeout time.Duration) int64 {
	deadline := time.Now().Add(timeout)
	for {
		n := running.Load()
		if n == 0 || !time.Now().Before(deadline) {
			return n
		}
		time.Sleep(10 * time.Millisecond)
	}
}

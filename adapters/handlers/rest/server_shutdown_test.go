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
	"fmt"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/handlers/rest/operations"
)

// ServerShutdown leaves the cluster and closes the database, so it must run
// exactly once, also when a request outlives GracefulTimeout.
func TestServeAndShutdown(t *testing.T) {
	tests := []struct {
		name                 string
		requestOutlivesDrain bool
	}{
		{name: "drained in time", requestOutlivesDrain: false},
		{name: "request outlives the drain", requestOutlivesDrain: true},
	}

	// configureAPI sets configureServer, and Serve calls it for every listener.
	prev := configureServer
	configureServer = func(*http.Server, string, string) {}
	t.Cleanup(func() { configureServer = prev })

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var serverShutdownCalls atomic.Int32
			s := NewServer(&operations.WeaviateAPI{
				PreServerShutdown: func() {},
				ServerShutdown:    func() { serverShutdownCalls.Add(1) },
			})
			s.EnabledListeners = []string{schemeHTTP}
			s.Host = "127.0.0.1"
			s.GracefulTimeout = 50 * time.Millisecond

			entered := make(chan struct{})
			release := make(chan struct{})
			t.Cleanup(func() { close(release) })
			s.SetHandler(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
				close(entered)
				<-release
			}))
			require.NoError(t, s.Listen())

			served := make(chan error, 1)
			go func() { served <- s.ServeAndShutdown() }()

			if tt.requestOutlivesDrain {
				go func() {
					resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/", s.Port))
					if err == nil {
						resp.Body.Close()
					}
				}()
				<-entered
			}

			require.NoError(t, s.Shutdown())
			require.NoError(t, <-served)
			require.Equal(t, int32(1), serverShutdownCalls.Load())
		})
	}
}

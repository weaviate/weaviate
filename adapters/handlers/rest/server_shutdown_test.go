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
	"io"
	"net"
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
		// stallBody sends part of a body and stops, so the handler is blocked reading it.
		stallBody bool
	}{
		{name: "drained in time", requestOutlivesDrain: false},
		{name: "request outlives the drain", requestOutlivesDrain: true},
		{name: "client stalls mid-body", stallBody: true},
	}

	// configureAPI sets configureServer, and Serve calls it for every listener.
	prev := configureServer
	configureServer = func(*http.Server, string, string) {}
	t.Cleanup(func() { configureServer = prev })

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var serverShutdownCalls atomic.Int32
			var handlerReturned, returnedBeforeServerShutdown atomic.Bool
			s := NewServer(&operations.WeaviateAPI{
				PreServerShutdown: func() {},
				ServerShutdown: func() {
					serverShutdownCalls.Add(1)
					returnedBeforeServerShutdown.Store(handlerReturned.Load())
				},
			})
			s.EnabledListeners = []string{schemeHTTP}
			s.Host = "127.0.0.1"
			s.GracefulTimeout = 200 * time.Millisecond

			entered := make(chan struct{})
			release := make(chan struct{})
			t.Cleanup(func() { close(release) })
			readErr := make(chan error, 1)
			s.SetHandler(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
				close(entered)
				if tt.stallBody {
					defer handlerReturned.Store(true)
					_, err := io.ReadAll(r.Body)
					readErr <- err
					// Without the wait for handlers, ServerShutdown starts within this window.
					time.Sleep(20 * time.Millisecond)
					return
				}
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
			if tt.stallBody {
				conn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", s.Port))
				require.NoError(t, err)
				t.Cleanup(func() { conn.Close() })
				_, err = fmt.Fprint(conn, "POST / HTTP/1.1\r\nHost: test\r\nContent-Length: 64\r\n\r\npartial")
				require.NoError(t, err)
				<-entered
			}

			require.NoError(t, s.Shutdown())
			require.NoError(t, <-served)
			require.Equal(t, int32(1), serverShutdownCalls.Load())

			if tt.stallBody {
				select {
				case err := <-readErr:
					require.Error(t, err, "closing the server must fail the stalled body read")
					require.True(t, returnedBeforeServerShutdown.Load(), "the woken handler must return before ServerShutdown")
				case <-time.After(5 * time.Second):
					t.Fatal("handler still blocked reading the stalled body")
				}
			}
		})
	}
}

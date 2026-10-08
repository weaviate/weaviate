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

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/handlers/rest/operations"
	"github.com/weaviate/weaviate/entities/shutdownreport"
)

// ServerShutdown leaves the cluster and closes the database, so it must run
// exactly once, also when a request outlives GracefulTimeout.
func TestServeAndShutdown(t *testing.T) {
	tests := []struct {
		name                 string
		requestOutlivesDrain bool
		stallBody            bool
		wantForced           bool
		wantStillRunning     int64
	}{
		{name: "drained in time", requestOutlivesDrain: false},
		{name: "request outlives the drain", requestOutlivesDrain: true, wantForced: true, wantStillRunning: 1},
		// The woken handler returns within the wait, so a forced close can leave none running.
		{name: "client stalls mid-body", stallBody: true, wantForced: true},
	}

	// configureAPI sets configureServer, and Serve calls it for every listener.
	prev := configureServer
	configureServer = func(*http.Server, string, string) {}
	t.Cleanup(func() { configureServer = prev })

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			prevReport := shutdownReport
			shutdownReport = shutdownreport.New("node-1")
			t.Cleanup(func() { shutdownReport = prevReport })

			var serverShutdownCalls atomic.Int32
			var handlerReturned, returnedBeforeServerShutdown atomic.Bool
			var summary logrus.Fields
			s := NewServer(&operations.WeaviateAPI{
				PreServerShutdown: func() {},
				ServerShutdown: func() {
					serverShutdownCalls.Add(1)
					returnedBeforeServerShutdown.Store(handlerReturned.Load())
					// The production ServerShutdown logs the report as its last step.
					summary = logReport(shutdownReport)
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

			shutdownReport.Begin()
			require.NoError(t, s.Shutdown())
			require.NoError(t, <-served)
			require.Equal(t, int32(1), serverShutdownCalls.Load())

			require.NotNil(t, summary, "summary line")
			assert.Equal(t, tt.wantForced, summary["rest_forced_close"])
			assert.Equal(t, tt.wantStillRunning, summary["rest_handlers_still_running"])

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

// logReport logs r and returns the summary line's fields.
func logReport(r *shutdownreport.Report) logrus.Fields {
	logger, hook := test.NewNullLogger()
	r.Log(logger, shutdownreport.DefaultThresholds())
	for _, e := range hook.AllEntries() {
		if e.Message == "shutdown finished" {
			return e.Data
		}
	}
	return nil
}

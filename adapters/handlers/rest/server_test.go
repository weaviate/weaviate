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
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/handlers/rest/operations"
	enterrors "github.com/weaviate/weaviate/entities/errors"
)

// A request still running at GracefulTimeout must not skip ServerShutdown.
func TestHandleShutdownRunsServerShutdown(t *testing.T) {
	const bodyLen = 64
	tests := []struct {
		name            string
		gracefulTimeout time.Duration
		// stallBody stops the client mid-body instead of sending the rest after shutdown starts.
		stallBody       bool
		wantReadErr     bool
		wantShutdownLog bool
	}{
		{name: "client stalls mid-body", gracefulTimeout: 100 * time.Millisecond, stallBody: true, wantReadErr: true, wantShutdownLog: true},
		{name: "request finishes before the deadline", gracefulTimeout: 5 * time.Second},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, _ := logrustest.NewNullLogger()
			var serverShutdowns atomic.Int32
			var logMu sync.Mutex
			var logs []string
			api := &operations.WeaviateAPI{
				PreServerShutdown: func() {},
				ServerShutdown:    func() { serverShutdowns.Add(1) },
				Logger: func(f string, args ...interface{}) {
					logMu.Lock()
					defer logMu.Unlock()
					logs = append(logs, fmt.Sprintf(f, args...))
				},
			}
			s := NewServer(api)
			s.GracefulTimeout = tt.gracefulTimeout

			entered := make(chan struct{})
			release := make(chan struct{})
			readErr := make(chan error, 1)
			srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				close(entered)
				<-release
				_, err := io.ReadAll(r.Body)
				readErr <- err
			})}
			t.Cleanup(func() { srv.Close() })
			ln, err := net.Listen("tcp", "127.0.0.1:0")
			require.NoError(t, err)
			enterrors.GoWrapper(func() { srv.Serve(ln) }, logger)

			conn, err := net.Dial("tcp", ln.Addr().String())
			require.NoError(t, err)
			t.Cleanup(func() { conn.Close() })
			_, err = fmt.Fprintf(conn, "POST / HTTP/1.1\r\nHost: test\r\nContent-Length: %d\r\n\r\n", bodyLen)
			require.NoError(t, err)
			waitOrFail(t, entered, "handler never started")

			if tt.stallBody {
				_, err = conn.Write([]byte("partial"))
				require.NoError(t, err)
				close(release)
			}

			wg := &sync.WaitGroup{}
			wg.Add(1)
			servers := []*http.Server{srv}
			enterrors.GoWrapper(func() { s.handleShutdown(wg, &servers) }, logger)
			require.NoError(t, s.Shutdown())

			if !tt.stallBody {
				_, err = conn.Write([]byte(strings.Repeat("x", bodyLen)))
				require.NoError(t, err)
				close(release)
			}

			handled := make(chan struct{})
			enterrors.GoWrapper(func() { wg.Wait(); close(handled) }, logger)
			waitOrFail(t, handled, "handleShutdown did not return")
			require.Equal(t, int32(1), serverShutdowns.Load(), "ServerShutdown calls")

			select {
			case err := <-readErr:
				if tt.wantReadErr {
					require.Error(t, err, "the stalled body read must fail once its connection is closed")
				} else {
					require.NoError(t, err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("handler still blocked reading the body")
			}

			logMu.Lock()
			defer logMu.Unlock()
			assert.Equal(t, tt.wantShutdownLog, strings.Contains(strings.Join(logs, "\n"), "HTTP server Shutdown"), "logs: %q", logs)
		})
	}
}

func waitOrFail(t *testing.T, ch <-chan struct{}, msg string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatal(msg)
	}
}

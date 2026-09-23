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
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
	"syscall"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeRequestsTotal struct {
	userErrors   int
	serverErrors int
}

func (f *fakeRequestsTotal) logError(className string, err error) {}

func (f *fakeRequestsTotal) logOk(className string) {}

func (f *fakeRequestsTotal) logUserError(className string) { f.userErrors++ }

func (f *fakeRequestsTotal) logServerError(className string, err error) { f.serverErrors++ }

func TestHandlePanics(t *testing.T) {
	tests := []struct {
		name                 string
		panicValue           any
		expectedUserErrors   int
		expectedServerErrors int
		expectedMessage      string
		expectedStack        bool
	}{
		{
			name:               "client closed the connection",
			panicValue:         &net.OpError{Op: "write", Err: syscall.EPIPE},
			expectedUserErrors: 1,
			expectedMessage:    "broken pipe",
		},
		{
			name:               "client reset the connection",
			panicValue:         &net.OpError{Op: "write", Err: syscall.ECONNRESET},
			expectedUserErrors: 1,
			expectedMessage:    "broken pipe",
		},
		{
			name:               "connection deadline exceeded",
			panicValue:         &net.OpError{Op: "write", Err: os.ErrDeadlineExceeded},
			expectedUserErrors: 1,
			expectedMessage:    "i/o timeout",
		},
		{
			name:                 "error we do not handle explicitly",
			panicValue:           errors.New("something went wrong"),
			expectedServerErrors: 1,
			expectedMessage:      "something went wrong",
			expectedStack:        true,
		},
		{
			name:                 "panic value that is not an error",
			panicValue:           "something went wrong",
			expectedServerErrors: 1,
			expectedMessage:      "something went wrong",
			expectedStack:        true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, hook := test.NewNullLogger()
			metric := &fakeRequestsTotal{}
			req, err := http.NewRequest("POST", "/v1/graphql", nil)
			require.NoError(t, err)

			rec := httptest.NewRecorder()
			pw := &panicResponseWriter{ResponseWriter: rec}
			require.NotPanics(t, func() {
				defer handlePanics(logger, metric, req, pw)
				panic(tt.panicValue)
			})

			// a server-side panic still answers; a vanished peer gets nothing
			if tt.expectedServerErrors > 0 {
				assert.Equal(t, http.StatusInternalServerError, rec.Code)
				assert.Contains(t, rec.Body.String(), "internal server error")
			} else {
				assert.Equal(t, http.StatusOK, rec.Code)
				assert.Empty(t, rec.Body.String())
			}

			assert.Equal(t, tt.expectedUserErrors, metric.userErrors)
			assert.Equal(t, tt.expectedServerErrors, metric.serverErrors)

			var messages, actions []string
			for _, entry := range hook.AllEntries() {
				messages = append(messages, entry.Message)
				if action, ok := entry.Data["action"].(string); ok {
					actions = append(actions, action)
				}
			}
			assert.Contains(t, messages, tt.expectedMessage)
			assert.Equal(t, tt.expectedStack, slices.Contains(actions, "print_stack"))
		})
	}
}

func TestHandlePanicsWithoutPanic(t *testing.T) {
	logger, hook := test.NewNullLogger()
	metric := &fakeRequestsTotal{}
	req, err := http.NewRequest("POST", "/v1/graphql", nil)
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	func() {
		defer handlePanics(logger, metric, req, &panicResponseWriter{ResponseWriter: rec})
	}()
	assert.Empty(t, rec.Body.String())

	assert.Zero(t, metric.userErrors)
	assert.Zero(t, metric.serverErrors)
	assert.Empty(t, hook.AllEntries())
}

// A panic below the middleware must not surface as net/http's implicit
// empty 200; once the handler has written, the response is left alone.
func TestCatchPanicsMiddlewareResponse(t *testing.T) {
	tests := []struct {
		name       string
		handler    http.HandlerFunc
		wantStatus int
		wantBody   string
	}{
		{
			name:       "panic before any write answers 500",
			handler:    func(w http.ResponseWriter, r *http.Request) { panic(errors.New("boom")) },
			wantStatus: http.StatusInternalServerError,
			wantBody:   `{"error":[{"message":"internal server error; details are in the server log"}]}`,
		},
		{
			name: "panic after the handler wrote keeps the handler's response",
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusAccepted)
				w.Write([]byte("partial"))
				panic("boom")
			},
			wantStatus: http.StatusAccepted,
			wantBody:   "partial",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, _ := test.NewNullLogger()
			rec := httptest.NewRecorder()
			req, err := http.NewRequest("POST", "/v1/search/Movie/bm25", nil)
			require.NoError(t, err)

			makeCatchPanics(logger, &fakeRequestsTotal{})(tt.handler).ServeHTTP(rec, req)

			assert.Equal(t, tt.wantStatus, rec.Code)
			assert.Equal(t, tt.wantBody, rec.Body.String())
		})
	}
}

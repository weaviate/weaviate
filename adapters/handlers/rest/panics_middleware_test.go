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

			require.NotPanics(t, func() {
				defer handlePanics(logger, metric, req)
				panic(tt.panicValue)
			})

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

	func() {
		defer handlePanics(logger, metric, req)
	}()

	assert.Zero(t, metric.userErrors)
	assert.Zero(t, metric.serverErrors)
	assert.Empty(t, hook.AllEntries())
}

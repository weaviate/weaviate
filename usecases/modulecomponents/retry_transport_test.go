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

package modulecomponents

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptrace"
	"syscall"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/usecases/monitoring"
)

func TestRetryTransport(t *testing.T) {
	connReset := &net.OpError{Op: "read", Err: syscall.ECONNRESET}
	certErr := errors.New("x509: certificate signed by unknown authority")

	tests := []struct {
		name string
		// errs[i] is returned for attempt i+1; a nil entry (or running past
		// the end of the slice) responds 200.
		errs []error
		// reusedConns[i] tells attempt i+1 whether its connection came out of
		// the pool; attempts past the end of the slice use a pooled one.
		reusedConns      []bool
		body             io.Reader
		bodyCannotResend bool
		cancelCtx        bool
		wantAttempts     int
		wantErr          error
	}{
		{
			name:         "no retry when the first attempt succeeds",
			body:         bytes.NewReader([]byte(`{"text":"hello"}`)),
			wantAttempts: 1,
		},
		{
			name:         "retries a bare EOF",
			errs:         []error{io.EOF},
			body:         bytes.NewReader([]byte(`{"text":"hello"}`)),
			wantAttempts: 2,
		},
		{
			name:         "retries an unexpected EOF",
			errs:         []error{io.ErrUnexpectedEOF},
			body:         bytes.NewReader([]byte(`{"text":"hello"}`)),
			wantAttempts: 2,
		},
		{
			name:         "retries a connection reset",
			errs:         []error{connReset},
			body:         bytes.NewReader([]byte(`{"text":"hello"}`)),
			wantAttempts: 2,
		},
		{
			name:         "retries a broken pipe",
			errs:         []error{syscall.EPIPE},
			body:         bytes.NewReader([]byte(`{"text":"hello"}`)),
			wantAttempts: 2,
		},
		{
			name:         "retries a request without a body",
			errs:         []error{io.EOF},
			wantAttempts: 2,
		},
		{
			name:         "retries twice before giving up",
			errs:         []error{io.EOF, connReset, io.EOF},
			body:         bytes.NewReader([]byte(`{"text":"hello"}`)),
			wantAttempts: 3,
			wantErr:      io.EOF,
		},
		{
			name:         "recovers on the last retry",
			errs:         []error{io.EOF, io.EOF},
			body:         bytes.NewReader([]byte(`{"text":"hello"}`)),
			wantAttempts: 3,
		},
		{
			name:         "does not retry an error unrelated to the connection",
			errs:         []error{certErr},
			body:         bytes.NewReader([]byte(`{"text":"hello"}`)),
			wantAttempts: 1,
			wantErr:      certErr,
		},
		{
			name:         "does not retry a connection that did not come out of the pool",
			errs:         []error{io.EOF},
			reusedConns:  []bool{false},
			body:         bytes.NewReader([]byte(`{"text":"hello"}`)),
			wantAttempts: 1,
			wantErr:      io.EOF,
		},
		{
			name:         "stops once a resend dials a fresh connection",
			errs:         []error{io.EOF, io.EOF},
			reusedConns:  []bool{true, false},
			body:         bytes.NewReader([]byte(`{"text":"hello"}`)),
			wantAttempts: 2,
			wantErr:      io.EOF,
		},
		{
			name:             "does not retry a body it cannot resend",
			errs:             []error{io.EOF},
			body:             bytes.NewReader([]byte(`{"text":"hello"}`)),
			bodyCannotResend: true,
			wantAttempts:     1,
			wantErr:          io.EOF,
		},
		{
			name:         "does not retry a cancelled request",
			errs:         []error{io.EOF},
			body:         bytes.NewReader([]byte(`{"text":"hello"}`)),
			cancelCtx:    true,
			wantAttempts: 1,
			wantErr:      context.Canceled,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			base := &fakeRoundTripper{errs: test.errs, reusedConns: test.reusedConns}
			resends := testutil.ToFloat64(monitoring.GetMetrics().ModuleExternalRequestResends)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if test.cancelCtx {
				cancel()
			}

			req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://localhost/vectors", test.body)
			require.NoError(t, err)
			if test.bodyCannotResend {
				req.GetBody = nil
			}

			res, err := (&retryTransport{base: base}).RoundTrip(req)
			if res != nil {
				defer res.Body.Close()
			}

			if test.wantErr != nil {
				require.ErrorIs(t, err, test.wantErr)
				assert.Nil(t, res)
			} else {
				require.NoError(t, err)
				require.NotNil(t, res)
				assert.Equal(t, http.StatusOK, res.StatusCode)
			}
			assert.Equal(t, test.wantAttempts, base.attempts)
			assert.Equal(t, float64(test.wantAttempts-1),
				testutil.ToFloat64(monitoring.GetMetrics().ModuleExternalRequestResends)-resends)

			// A retried attempt must not send different input than the caller passed.
			if test.body != nil {
				require.Len(t, base.bodies, test.wantAttempts)
				for _, body := range base.bodies {
					assert.Equal(t, `{"text":"hello"}`, body)
				}
			}
		})
	}
}

type fakeRoundTripper struct {
	errs        []error
	reusedConns []bool
	attempts    int
	bodies      []string
}

func (f *fakeRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	f.attempts++
	if trace := httptrace.ContextClientTrace(req.Context()); trace != nil {
		trace.GotConn(httptrace.GotConnInfo{Reused: f.connReused()})
	}
	if req.Body != nil {
		body, err := io.ReadAll(req.Body)
		if err != nil {
			return nil, err
		}
		req.Body.Close()
		f.bodies = append(f.bodies, string(body))
	}

	if i := f.attempts - 1; i < len(f.errs) && f.errs[i] != nil {
		return nil, f.errs[i]
	}
	return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody}, nil
}

func (f *fakeRoundTripper) connReused() bool {
	if i := f.attempts - 1; i < len(f.reusedConns) {
		return f.reusedConns[i]
	}
	return true
}

func TestJittered(t *testing.T) {
	const delay = 100 * time.Millisecond

	seen := map[time.Duration]bool{}
	for range 100 {
		got := jittered(delay)
		require.GreaterOrEqual(t, got, delay)
		require.Less(t, got, 2*delay)
		seen[got] = true
	}

	// Without a spread every connection dropped at the same moment is resent
	// at the same moment.
	assert.Greater(t, len(seen), 1)
}

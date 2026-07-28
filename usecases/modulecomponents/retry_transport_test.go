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
	"syscall"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRetryTransport(t *testing.T) {
	connReset := &net.OpError{Op: "read", Err: syscall.ECONNRESET}
	certErr := errors.New("x509: certificate signed by unknown authority")

	tests := []struct {
		name string
		// errs[i] is returned for attempt i+1; a nil entry (or running past
		// the end of the slice) responds 200.
		errs             []error
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
			base := &fakeRoundTripper{errs: test.errs}
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
	errs     []error
	attempts int
	bodies   []string
}

func (f *fakeRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	f.attempts++
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

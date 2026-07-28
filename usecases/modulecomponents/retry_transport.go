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
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"syscall"
	"time"
)

// retryDelays holds the pause before each retry, so its length is the number of
// retries. The first retry runs immediately: the broken connection is already
// out of the pool.
var retryDelays = []time.Duration{0, 100 * time.Millisecond}

// retryTransport resends a request whose connection broke before any response
// arrived. net/http resends a POST only when none of its bytes reached the wire,
// so a server closing a pooled connection mid-request fails the call. A resent
// request may be processed twice by the origin.
//
// timeout covers all attempts and the response body read. http.Client.Timeout
// cannot be used: with a wrapped transport it cancels through Request.Cancel, so
// a timed-out call can report "request canceled" instead of the deadline.
type retryTransport struct {
	base    http.RoundTripper
	timeout time.Duration
}

func (t *retryTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if t.timeout <= 0 {
		return t.roundTripWithRetries(req)
	}

	ctx, cancel := context.WithTimeout(req.Context(), t.timeout)
	res, err := t.roundTripWithRetries(req.WithContext(ctx))
	if err != nil {
		cancel()
		return nil, err
	}
	res.Body = &cancelOnClose{ReadCloser: res.Body, cancel: cancel}
	return res, nil
}

func (t *retryTransport) roundTripWithRetries(req *http.Request) (*http.Response, error) {
	canResend := req.Body == nil || req.GetBody != nil

	attempt := req
	for i := 0; ; i++ {
		res, err := t.base.RoundTrip(attempt)
		if err == nil || !canResend || !isBrokenConnError(err) {
			return res, err
		}
		if i >= len(retryDelays) {
			// Without the count a caller cannot tell the request was resent.
			return nil, fmt.Errorf("after %d attempts: %w", i+1, err)
		}

		// A deadline landing in the pause is what ended the call, not the
		// connection error before it.
		if waitErr := waitBeforeRetry(req.Context(), retryDelays[i]); waitErr != nil {
			return nil, waitErr
		}

		// The previous attempt consumed and closed the body, so take a fresh one.
		attempt = req.Clone(req.Context())
		if req.GetBody != nil {
			body, bodyErr := req.GetBody()
			if bodyErr != nil {
				return nil, bodyErr
			}
			attempt.Body = body
		}
	}
}

// isBrokenConnError reports whether the connection died before a response could
// be read. net/http wraps these, hence errors.Is.
func isBrokenConnError(err error) bool {
	return errors.Is(err, io.EOF) ||
		errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, syscall.EPIPE)
}

func waitBeforeRetry(ctx context.Context, delay time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if delay == 0 {
		return nil
	}

	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// cancelOnClose keeps the timeout in force until the caller closes the body.
type cancelOnClose struct {
	io.ReadCloser
	cancel context.CancelFunc
}

func (b *cancelOnClose) Close() error {
	err := b.ReadCloser.Close()
	b.cancel()
	return err
}

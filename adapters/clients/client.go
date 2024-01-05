//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package clients

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"time"
)

type retryClient struct {
	client *http.Client
	*retryer
}

func (c *retryClient) doWithCustomMarshaller(timeout time.Duration,
	req *http.Request, body []byte, decode func([]byte) error,
) (err error) {
	ctx, cancel := context.WithTimeout(req.Context(), timeout)
	defer cancel()
	try := func(ctx context.Context) (bool, error) {
		if body != nil {
			req.Body = io.NopCloser(bytes.NewReader(body))
		}
		res, err := c.client.Do(req)
		if err != nil {
			return ctx.Err() == nil, fmt.Errorf("connect: %w", err)
		}

		respBody, err := io.ReadAll(res.Body)
		if err != nil {
			return shouldRetry(res.StatusCode), fmt.Errorf("read response: %w", err)
		}
		defer res.Body.Close()

		if code := res.StatusCode; code != http.StatusOK {
			return shouldRetry(code), fmt.Errorf("status code: %v, error: %s", code, respBody)
		}

		if err := decode(respBody); err != nil {
			return false, fmt.Errorf("unmarshal response: %w", err)
		}

		return false, nil
	}
	return c.retry(ctx, 9, try)
}

type retryer struct {
	minBackOff  time.Duration
	maxBackOff  time.Duration
	timeoutUnit time.Duration
}

func newRetryer() *retryer {
	return &retryer{
		minBackOff:  time.Millisecond * 250,
		maxBackOff:  time.Second * 30,
		timeoutUnit: time.Second, // used by unit tests
	}
}

func (r *retryer) retry(ctx context.Context, n int, work func(context.Context) (bool, error)) error {
	delay := r.minBackOff
	for {
		keepTrying, err := work(ctx)
		if !keepTrying || n < 1 || err == nil {
			return err
		}

		n--
		if delay = backOff(delay); delay > r.maxBackOff {
			delay = r.maxBackOff
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return fmt.Errorf("%v: %w", err, ctx.Err())
		case <-timer.C:
		}
		timer.Stop()
	}
}

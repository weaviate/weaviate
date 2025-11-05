//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package clients

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

type retryClient struct {
	client *http.Client
	*retryer
}

func (c *retryClient) doWithCustomMarshaller(timeout time.Duration,
	req *http.Request, data []byte, decode func([]byte) error, success func(code int) bool, numRetries int,
) (err error) {
	ctx, cancel := context.WithTimeout(req.Context(), timeout)
	defer cancel()
	req = req.WithContext(ctx)
	try := func(ctx context.Context) (b bool, e error) {
		if data != nil {
			req.Body = io.NopCloser(bytes.NewReader(data))
		}
		res, err := c.client.Do(req)
		if err != nil {
			return false, fmt.Errorf("connect: %w", err)
		}
		defer res.Body.Close()

		respBody, err := io.ReadAll(res.Body)
		if err != nil {
			return shouldRetry(res.StatusCode), fmt.Errorf("read response: %w", err)
		}

		if code := res.StatusCode; !success(code) {
			return shouldRetry(code), fmt.Errorf("status code: %v, error: %s", code, respBody)
		}

		if err := decode(respBody); err != nil {
			return false, fmt.Errorf("unmarshal response: %w", err)
		}

		return false, nil
	}
	return c.retry(ctx, numRetries, try)
}

func (c *retryClient) do(timeout time.Duration, req *http.Request, body []byte, resp interface{}, success func(code int) bool) (code int, err error) {
	ctx, cancel := context.WithTimeout(req.Context(), timeout)
	defer cancel()
	req = req.WithContext(ctx)
	try := func(ctx context.Context) (bool, error) {
		if body != nil {
			req.Body = io.NopCloser(bytes.NewReader(body))
		}
		res, err := c.client.Do(req)
		if err != nil {
			return false, fmt.Errorf("connect: %w", err)
		}
		defer res.Body.Close()

		if code = res.StatusCode; !success(code) {
			b, _ := io.ReadAll(res.Body)
			return shouldRetry(code), fmt.Errorf("status code: %v, error: %s", code, b)
		}
		if resp != nil {
			if err := json.NewDecoder(res.Body).Decode(resp); err != nil {
				return false, fmt.Errorf("decode response: %w", err)
			}
		}
		return false, nil
	}
	return code, c.retry(ctx, 9, try)
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

// n is the number of retries, work will always be called at least once.
func (r *retryer) retry(ctx context.Context, n int, work func(context.Context) (bool, error)) error {
	ctx, span := otel.Tracer("weaviate-search").Start(ctx, "retryer.retry",
		trace.WithSpanKind(trace.SpanKindInternal),
	)
	defer span.End()

	delay := r.minBackOff
	for {
		loopCtx, span := otel.Tracer("weaviate-search").Start(ctx, "retryer.retry.loop",
			trace.WithSpanKind(trace.SpanKindInternal),
			trace.WithAttributes(
				attribute.Float64("n", float64(n)),
			),
		)
		keepTrying, err := work(loopCtx)
		if !keepTrying || n < 1 || err == nil {
			span.End()
			return err
		}

		n--
		if delay = backOff(delay); delay > r.maxBackOff {
			delay = r.maxBackOff
		}
		span.AddEvent("backoff-retry", trace.WithAttributes(
			attribute.String("delay", delay.String()),
		))
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			span.End()
			return fmt.Errorf("%w: %w", err, ctx.Err())
		case <-timer.C:
		}
		timer.Stop()
		span.End()
	}
}

func successCode(code int) bool {
	return code >= http.StatusOK && code <= http.StatusIMUsed
}

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
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/sirupsen/logrus"
)

type retryClient struct {
	client       *http.Client
	nodeResolver nodeResolver
	*retryer
}

func (c *retryClient) doWithCustomMarshaller(timeout time.Duration,
	req *http.Request, data []byte, decode func([]byte) error, success func(code int) bool, numRetries int, owner string,
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
			if owner != "" {
				newHost, ok := c.nodeResolver.NodeHostname(owner)
				if ok {
					logrus.WithFields(logrus.Fields{
						"action":  "resolve",
						"owner":   owner,
						"newHost": newHost,
						"oldHost": req.Host,
					}).Error("connect: %w", err)
					req.Host = newHost
					return true, fmt.Errorf("connect: %w", err)
				}
			}
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

func (c *retryClient) do(timeout time.Duration, req *http.Request, body []byte, resp interface{}, numRetries int, success func(code int) bool, owner string) (code int, err error) {
	ctx, cancel := context.WithTimeout(req.Context(), timeout)
	defer cancel()
	req = req.WithContext(ctx)
	try := func(ctx context.Context) (bool, error) {
		if body != nil {
			req.Body = io.NopCloser(bytes.NewReader(body))
		}
		res, err := c.client.Do(req)
		if err != nil {
			if owner != "" {
				newHost, ok := c.nodeResolver.NodeHostname(owner)
				if ok {
					logrus.WithFields(logrus.Fields{
						"action":  "resolve",
						"owner":   owner,
						"newHost": newHost,
						"oldHost": req.Host,
					}).Error("connect: %w", err)
					req.Host = newHost
					return true, fmt.Errorf("connect: %w", err)
				}
			}

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
	return code, c.retry(ctx, numRetries, try)
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
		timeoutUnit: time.Second,
	}
}

// n is the number of retries, work will always be called at least once.
func (r *retryer) retry(ctx context.Context, n int, work func(context.Context) (bool, error)) error {
	return backoff.Retry(func() error {
		keepTrying, err := work(ctx)
		if err != nil && !keepTrying {
			return backoff.Permanent(err)
		}
		return err
	}, backoff.WithContext(
		backoff.WithMaxRetries(
			backoff.NewExponentialBackOff(
				backoff.WithInitialInterval(r.minBackOff),
				backoff.WithMaxInterval(r.maxBackOff),
				backoff.WithMultiplier(1.5),
			),
			uint64(n)), ctx))
}

func successCode(code int) bool {
	return code >= http.StatusOK && code <= http.StatusIMUsed
}

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

	"github.com/sirupsen/logrus"
)

type retryClient struct {
	client *http.Client
	*retryer
	log logrus.FieldLogger
}

func (c *retryClient) doWithCustomMarshaller(timeout time.Duration,
	req *http.Request, body []byte, decode func([]byte) error, numRetries int,
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
	return c.retry(ctx, numRetries, try, c.logEntryForReq(req.Method, req.URL.String()))
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
func (r *retryer) retry(ctx context.Context, n int, work func(context.Context) (bool, error), retryLogEntry *logrus.Entry) error {
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
		if retryLogEntry != nil {
			retryLogEntry.WithFields(logrus.Fields{
				"numberOfRetriesLeft": n,
				"delay":               delay,
			}).WithError(err).Debug("retrying after work errored")
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

// logEntryForReq returns a log entry with the given method/url if the retryClient's logger
// is not nil. If the retryClient's logger is nil, returns nil.
func (c *retryClient) logEntryForReq(method, url string) *logrus.Entry {
	if c.log == nil {
		return nil
	}
	return c.log.WithFields(logrus.Fields{
		"method": method,
		"url":    url,
	})
}

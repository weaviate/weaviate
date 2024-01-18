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

package client

import (
	"context"
	"net/http"
	"time"

	"github.com/pkg/errors"
)

func (c *client) WaitForStartup(initCtx context.Context,
	interval time.Duration,
) error {
	t := time.NewTicker(interval)
	defer t.Stop()
	expired := initCtx.Done()
	var lastErr error
	for {
		select {
		case <-t.C:
			lastErr = c.checkReady(initCtx)
			if lastErr == nil {
				return nil
			}
			c.logger.
				WithField("action", "reranktransformers_remote_wait_for_startup").
				WithError(lastErr).Warnf("reranktransformers remote service not ready")
		case <-expired:
			return errors.Wrapf(lastErr, "init context expired before remote was ready")
		}
	}
}

func (c *client) checkReady(initCtx context.Context) error {
	// spawn a new context (derived on the overall context) which is used to
	// consider an individual request timed out
	requestCtx, cancel := context.WithTimeout(initCtx, 500*time.Millisecond)
	defer cancel()

	req, err := http.NewRequestWithContext(requestCtx, http.MethodGet,
		c.url("/.well-known/ready"), nil)
	if err != nil {
		return errors.Wrap(err, "create check ready request")
	}

	res, err := c.httpClient.Do(req)
	if err != nil {
		return errors.Wrap(err, "send check ready request")
	}

	defer res.Body.Close()
	if res.StatusCode > 299 {
		return errors.Errorf("not ready: status %d", res.StatusCode)
	}

	return nil
}

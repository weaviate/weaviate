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

package ollama

import (
	"context"
	"time"

	"github.com/pkg/errors"
)

func (c *ollama) WaitForStartup(initCtx context.Context,
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
				WithField("action", "generativeollama_remote_wait_for_startup").
				WithError(lastErr).Warnf("generativeollama remote service not ready")
		case <-expired:
			return errors.Wrapf(lastErr, "init context expired before remote was ready")
		}
	}
}

// So the Ollama images don't have a /.well-known/ready endpoint
// Looking for what this might be called.

func (c *ollama) checkReady(initCtx context.Context) error {
	// spawn a new context (derived on the overall context) which is used to
	// consider an individual request timed out

	/* An API to check status would be `/api/show`` */

	/*
		// THIS IS HOW STARTUP WORKS IN OTHER LOCAL MODULES SUCH AS RERANKER-TRANSFORMERS

		requestCtx, cancel := context.WithTimeout(initCtx, 500*time.Millisecond)
		defer cancel()

		req, err := http.NewRequestWithContext(requestCtx, http.MethodGet,
			c.getOllamaUrl("/.well-known/ready"), nil)
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
	*/

	return nil
}

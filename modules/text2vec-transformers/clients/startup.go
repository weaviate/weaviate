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
	"context"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"
)

func (v *vectorizer) WaitForStartup(initCtx context.Context,
	interval time.Duration,
) error {
	endpoints := map[string]string{}
	if v.originPassage != v.originQuery {
		endpoints["passage"] = v.urlPassage("/.well-known/ready")
		endpoints["query"] = v.urlQuery("/.well-known/ready")
	} else {
		endpoints[""] = v.urlPassage("/.well-known/ready")
	}

	ch := make(chan error, len(endpoints))
	var wg sync.WaitGroup
	for serviceName, endpoint := range endpoints {
		wg.Add(1)
		go func(serviceName string, endpoint string) {
			defer wg.Done()
			if err := v.waitFor(initCtx, interval, endpoint, serviceName); err != nil {
				ch <- err
			}
		}(serviceName, endpoint)
	}
	wg.Wait()
	close(ch)

	if len(ch) > 0 {
		var errs []string
		for err := range ch {
			errs = append(errs, err.Error())
		}
		return errors.Errorf(strings.Join(errs, ", "))
	}
	return nil
}

func (v *vectorizer) waitFor(initCtx context.Context, interval time.Duration, endpoint string, serviceName string) error {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	expired := initCtx.Done()
	var lastErr error
	prefix := ""
	if serviceName != "" {
		prefix = "[" + serviceName + "] "
	}

	for {
		select {
		case <-ticker.C:
			lastErr = v.checkReady(initCtx, endpoint, serviceName)
			if lastErr == nil {
				return nil
			}
			v.logger.
				WithField("action", "transformer_remote_wait_for_startup").
				WithError(lastErr).Warnf("%stransformer remote inference service not ready", prefix)
		case <-expired:
			return errors.Wrapf(lastErr, "%sinit context expired before remote was ready", prefix)
		}
	}
}

func (v *vectorizer) checkReady(initCtx context.Context, endpoint string, serviceName string) error {
	// spawn a new context (derived on the overall context) which is used to
	// consider an individual request timed out
	// due to parent timeout being superior over request's one, request can be cancelled by parent timeout
	// resulting in "send check ready request" even if service is responding with non 2xx http code
	requestCtx, cancel := context.WithTimeout(initCtx, 500*time.Millisecond)
	defer cancel()

	req, err := http.NewRequestWithContext(requestCtx, http.MethodGet, endpoint, nil)
	if err != nil {
		return errors.Wrap(err, "create check ready request")
	}

	res, err := v.httpClient.Do(req)
	if err != nil {
		return errors.Wrap(err, "send check ready request")
	}

	defer res.Body.Close()
	if res.StatusCode > 299 {
		return errors.Errorf("not ready: status %d", res.StatusCode)
	}

	return nil
}

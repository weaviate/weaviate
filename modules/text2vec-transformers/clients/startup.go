//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package clients

import (
	"context"
	"net/http"
	"sync"
	"time"

	"github.com/pkg/errors"
)

type checkReadyFn func() error

func (v *vectorizer) WaitForStartup(initCtx context.Context,
	interval time.Duration) error {
	t := time.Tick(interval)
	expired := initCtx.Done()

	var lastErr error
	checkReadyFns := v.allCheckReadyFns(initCtx)
	initalLen := len(checkReadyFns)
	for {
		select {
		case <-t:
			switch len(checkReadyFns) {
			case 0:
				return errors.Errorf("no checkReady callback configured")
			case 1:
				for name, checkReady := range checkReadyFns {
					lastErr = checkReady()
					if lastErr != nil && initalLen != 1 {
						lastErr = errors.Errorf("[%s] %v", name, lastErr.Error())
					}
				}
			default:
				checkReadyFns, lastErr = v.checkReadyAll(checkReadyFns)
			}

			if lastErr == nil {
				return nil
			}
			v.logger.
				WithField("action", "transformer_remote_wait_for_startup").
				WithError(lastErr).Warnf("transformer remote inference service not ready")
		case <-expired:
			return errors.Wrapf(lastErr, "init context expired before remote was ready")
		}
	}
}

func (v *vectorizer) allCheckReadyFns(initCtx context.Context) map[string]checkReadyFn {
	if v.originPassage != v.originQuery {
		return map[string]checkReadyFn{
			"passage": func() error { return v.checkReady(initCtx, v.urlPassage) },
			"query":   func() error { return v.checkReady(initCtx, v.urlQuery) },
		}
	}
	return map[string]checkReadyFn{
		"common": func() error { return v.checkReady(initCtx, v.urlPassage) },
	}
}

// check all callbacks for ready, returns ones that are not ready and combined error
func (v *vectorizer) checkReadyAll(checkReadyFns map[string]checkReadyFn) (map[string]checkReadyFn, error) {
	type nameErr struct {
		name string
		err  error
	}

	var wg sync.WaitGroup
	ch := make(chan nameErr, len(checkReadyFns))

	for name, checkReady := range checkReadyFns {
		wg.Add(1)
		go func(name string, checkReady checkReadyFn) {
			defer wg.Done()
			if err := checkReady(); err != nil {
				ch <- nameErr{name, err}
			}
		}(name, checkReady)
	}
	wg.Wait()
	close(ch)

	checkReadyAgainFns := map[string]checkReadyFn{}
	if len(ch) > 0 {
		ne := <-ch
		combinedErr := errors.Errorf("[%s] %v", ne.name, ne.err.Error())
		checkReadyAgainFns[ne.name] = checkReadyFns[ne.name]
		for ne := range ch {
			combinedErr = errors.Wrapf(combinedErr, "[%s] %v", ne.name, ne.err.Error())
			checkReadyAgainFns[ne.name] = checkReadyFns[ne.name]
		}
		return checkReadyAgainFns, combinedErr
	}
	return checkReadyAgainFns, nil
}

func (v *vectorizer) checkReady(initCtx context.Context, url func(string) string) error {
	// spawn a new context (derived on the overall context) which is used to
	// consider an individual request timed out
	requestCtx, cancel := context.WithTimeout(initCtx, 500*time.Millisecond)
	defer cancel()

	req, err := http.NewRequestWithContext(requestCtx, http.MethodGet,
		url("/.well-known/ready"), nil)
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

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
	"strings"
	"sync"
	"time"

	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/usecases/modulecomponents/clients/transformers"

	"github.com/pkg/errors"
)

func (v *vectorizer) WaitForStartup(initCtx context.Context,
	interval time.Duration,
) error {
	endpoints := map[string]string{}
	if v.originPassage != v.originQuery {
		endpoints["passage"] = v.urlBuilder.GetPassageURL("/.well-known/ready", transformers.VectorizationConfig{})
		endpoints["query"] = v.urlBuilder.GetQueryURL("/.well-known/ready", transformers.VectorizationConfig{})
	} else {
		endpoints[""] = v.urlBuilder.GetPassageURL("/.well-known/ready", transformers.VectorizationConfig{})
	}

	ch := make(chan error, len(endpoints))
	var wg sync.WaitGroup
	for serviceName, endpoint := range endpoints {
		serviceName, endpoint := serviceName, endpoint
		wg.Add(1)
		enterrors.GoWrapper(func() {
			defer wg.Done()
			if err := v.waitFor(initCtx, interval, endpoint, serviceName); err != nil {
				ch <- err
			}
		}, v.logger)
	}
	wg.Wait()
	close(ch)

	if len(ch) > 0 {
		var errs []string
		for err := range ch {
			errs = append(errs, err.Error())
		}
		return errors.New(strings.Join(errs, ", "))
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
			lastErr = v.client.CheckReady(initCtx, endpoint)
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

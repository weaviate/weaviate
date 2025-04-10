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
	"fmt"
	"time"

	"github.com/pkg/errors"
)

func (v *vectorizer) WaitForStartup(initCtx context.Context,
	interval time.Duration,
) error {
	wellKnownReadyEndpoint := fmt.Sprintf("%s/.well-known/ready", v.url)
	t := time.NewTicker(interval)
	defer t.Stop()
	expired := initCtx.Done()
	var lastErr error
	for {
		select {
		case <-t.C:
			lastErr = v.client.CheckReady(initCtx, wellKnownReadyEndpoint)
			if lastErr == nil {
				return nil
			}
			v.logger.
				WithField("action", "text2vec_model2vec_remote_wait_for_startup").
				WithError(lastErr).Warnf("text2vev-model2vec inference service not ready")
		case <-expired:
			return errors.Wrapf(lastErr, "init context expired before remote was ready")
		}
	}
}

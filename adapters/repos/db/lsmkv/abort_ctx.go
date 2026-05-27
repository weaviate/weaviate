//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package lsmkv

import (
	"context"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	enterrors "github.com/weaviate/weaviate/entities/errors"
)

// ctxFromShouldAbort bridges a cyclemanager ShouldAbortCallback to a context
// that cancels the moment shouldAbort first returns true. The compaction and
// flush hot loops take a ctx, while the cyclemanager only offers a polled
// callback; a 50ms watcher closes the gap. The watcher exits when the returned
// cancel runs, so callers must defer it. shouldAbort may be nil.
func ctxFromShouldAbort(shouldAbort cyclemanager.ShouldAbortCallback, logger logrus.FieldLogger) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	if shouldAbort == nil {
		return ctx, cancel
	}
	if shouldAbort() {
		cancel()
		return ctx, cancel
	}
	enterrors.GoWrapper(func() {
		t := time.NewTicker(50 * time.Millisecond)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				if shouldAbort() {
					cancel()
					return
				}
			}
		}
	}, logger)
	return ctx, cancel
}

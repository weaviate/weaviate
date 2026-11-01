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

// ctxFromShouldAbort adapts a cyclemanager ShouldAbortCallback into a ctx for
// the compaction/flush loops: a 50ms watcher cancels once shouldAbort fires.
// Callers must defer the returned cancel — it also stops the watcher.
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

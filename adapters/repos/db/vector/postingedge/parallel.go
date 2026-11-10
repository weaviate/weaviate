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

package postingedge

import (
	"context"
	"runtime"
	"sync/atomic"

	"github.com/sirupsen/logrus"

	enterrors "github.com/weaviate/weaviate/entities/errors"
)

// parallelChunks runs fn over [0, n) in chunks, on one goroutine per
// available CPU. Chunks are handed out dynamically, so uneven chunks do not
// leave workers idle. fn receives the worker number to index per-worker
// scratch. The first error, or a cancelled ctx, stops the remaining chunks.
func parallelChunks(ctx context.Context, logger logrus.FieldLogger, n, chunk int,
	fn func(worker, start, end int) error,
) error {
	if n <= 0 {
		return nil
	}
	workers := min(parallelWorkers(), (n+chunk-1)/chunk)

	var next atomic.Int64
	eg, ctx := enterrors.NewErrorGroupWithContextWrapper(logger, ctx)
	for worker := range workers {
		eg.Go(func() error {
			for {
				if err := ctx.Err(); err != nil {
					return err
				}
				start := int(next.Add(int64(chunk))) - chunk
				if start >= n {
					return nil
				}
				if err := fn(worker, start, min(start+chunk, n)); err != nil {
					return err
				}
			}
		})
	}
	return eg.Wait()
}

// parallelWorkers is the number of workers parallelChunks may use, for
// callers that size per-worker scratch.
func parallelWorkers() int {
	return runtime.GOMAXPROCS(0)
}

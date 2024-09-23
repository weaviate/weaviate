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

package db

import (
	"context"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/storobj"
)

type job struct {
	object  *storobj.Object
	status  objectInsertStatus
	index   int
	ctx     context.Context
	batcher *objectsBatcher

	// async only
	indexer batchIndexer
	ids     []uint64
	deleted []uint64
	vectors [][]float32
	done    func()
}

type AsyncWorker struct {
	logger        logrus.FieldLogger
	retryInterval time.Duration
	ch            chan job
}

func NewAsyncWorker(ch chan job, logger logrus.FieldLogger, retryInterval time.Duration) *AsyncWorker {
	return &AsyncWorker{
		logger:        logger,
		retryInterval: retryInterval,
		ch:            ch,
	}
}

func (a *AsyncWorker) Run() {
	for job := range a.ch {
		stop := a.do(job)

		if stop {
			return
		}
	}
}

func (a *AsyncWorker) do(job job) (stop bool) {
	defer job.done()

	if len(job.ids) > 0 {
		return a.withRetry(job.ctx, func() error {
			return job.indexer.AddBatch(job.ctx, job.ids, job.vectors)
		})
	}

	if len(job.deleted) > 0 {
		stop = a.withRetry(job.ctx, func() error {
			return job.indexer.Delete(job.deleted...)
		})
		if stop {
			return
		}
	}

	return false
}

func (a *AsyncWorker) withRetry(ctx context.Context, fn func() error) bool {
	for {
		err := fn()
		if err == nil {
			return false
		}

		if errors.Is(err, context.Canceled) {
			a.logger.WithError(err).Debug("skipping indexing batch due to context cancellation")
			return true
		}

		a.logger.WithError(err).Infof("failed to index or delete vectors, retrying in %s", a.retryInterval.String())

		t := time.NewTimer(a.retryInterval)
		select {
		case <-ctx.Done():
			// drain the timer
			if !t.Stop() {
				<-t.C
			}

			return true
		case <-t.C:
		}
	}
}

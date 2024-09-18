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
		stop := func() bool {
			defer job.done()

			for {
				err := job.indexer.AddBatch(job.ctx, job.ids, job.vectors)
				if err == nil {
					return false
				}

				if errors.Is(err, context.Canceled) {
					a.logger.WithError(err).Debug("skipping indexing batch due to context cancellation")
					return true
				}

				a.logger.WithError(err).Infof("failed to index vectors, retrying in %s", a.retryInterval.String())

				t := time.NewTimer(a.retryInterval)
				select {
				case <-job.ctx.Done():
					// drain the timer
					if !t.Stop() {
						<-t.C
					}

					return true
				case <-t.C:
				}
			}
		}()

		if stop {
			return
		}
	}
}

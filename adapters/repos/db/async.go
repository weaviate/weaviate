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

func asyncWorker(ch chan job, logger logrus.FieldLogger, retryInterval time.Duration) {
	for job := range ch {
		stop := func() bool {
			defer job.done()

			for {
				err := job.indexer.AddBatch(job.ctx, job.ids, job.vectors)
				if err == nil {
					return false
				}

				if errors.Is(err, context.Canceled) {
					logger.WithError(err).Debug("skipping indexing batch due to context cancellation")
					return true
				}

				logger.WithError(err).Infof("failed to index vectors, retrying in %s", retryInterval.String())

				t := time.NewTimer(retryInterval)
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

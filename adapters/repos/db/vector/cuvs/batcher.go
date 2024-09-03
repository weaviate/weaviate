package cuvs_index

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
)

type batchRequest struct {
	vector []float32
	k      int
	allow  helpers.AllowList
	result chan batchResult
}

type batchResult struct {
	ids       []uint64
	distances []float32
	err       error
}

type SearchBatcher struct {
	index           *cuvs_index
	batchSize       int
	maxWaitTime     time.Duration
	requestChan     chan batchRequest
	mu              sync.Mutex
	pendingRequests []batchRequest
	timer           *time.Timer
}

func NewSearchBatcher(index *cuvs_index, batchSize int, maxWaitTime time.Duration) *SearchBatcher {
	sb := &SearchBatcher{
		index:       index,
		batchSize:   batchSize,
		maxWaitTime: maxWaitTime,
		requestChan: make(chan batchRequest),
	}
	go sb.run()
	return sb
}

func (sb *SearchBatcher) run() {
	for {
		select {
		case req := <-sb.requestChan:
			sb.mu.Lock()
			sb.pendingRequests = append(sb.pendingRequests, req)
			// println("append")
			// println(len(sb.pendingRequests))
			if len(sb.pendingRequests) == 1 {
				sb.timer = time.AfterFunc(sb.maxWaitTime, func() {
					sb.mu.Lock()
					fmt.Println("timeout, batching with batch size", len(sb.pendingRequests))
					sb.processBatch()
					sb.mu.Unlock()
				})
			}
			if len(sb.pendingRequests) >= sb.batchSize {
				if sb.timer != nil {
					sb.timer.Stop()
				}
				// print "batching with batch size"
				fmt.Println("max batch, batching with batch size", len(sb.pendingRequests))
				sb.processBatch()
			}
			sb.mu.Unlock()
		}
	}
}
func (sb *SearchBatcher) processBatch() {
	// println("got here")
	if len(sb.pendingRequests) == 0 {
		return
	}

	batch := sb.pendingRequests
	sb.pendingRequests = nil
	sb.timer = nil

	go func() {
		vectors := make([][]float32, len(batch))
		for i, req := range batch {
			vectors[i] = req.vector
		}

		ids, distances, err := sb.index.SearchByVectorBatch(vectors, batch[0].k, batch[0].allow)

		for i, req := range batch {
			if err != nil {
				req.result <- batchResult{err: err}
			} else {
				// start := i * batch[0].k
				// end := start + batch[0].k
				req.result <- batchResult{
					ids:       ids[i],
					distances: distances[i],
				}
			}
		}
	}()
}

func (sb *SearchBatcher) SearchByVector(ctx context.Context, vector []float32, k int, allow helpers.AllowList) ([]uint64, []float32, error) {
	resultChan := make(chan batchResult, 1)
	req := batchRequest{
		vector: vector,
		k:      k,
		allow:  allow,
		result: resultChan,
	}

	select {
	case sb.requestChan <- req:
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	}

	select {
	case result := <-resultChan:
		return result.ids, result.distances, result.err
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	}
}

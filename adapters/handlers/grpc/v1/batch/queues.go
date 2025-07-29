//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package batch

import (
	"sync"

	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
)

type errorsObject struct {
	Errors []*pb.BatchError
	Stop   bool
}

type (
	writeQueue chan *pb.BatchSendRequest
	readQueue  chan errorsObject
	readQueues map[string]readQueue
)

// NewBatchWriteQueue creates a buffered channel to store objects for batch writing.
//
// The buffer size can be adjusted based on expected load and performance requirements
// to optimize throughput and resource usage. But is required so that there is a small buffer
// that can be quickly flushed in the event of a shutdown.
func NewBatchWriteQueue() writeQueue {
	return make(chan *pb.BatchSendRequest, 10)
}

func NewBatchReadQueues() *ReadQueues {
	return &ReadQueues{
		queues: make(readQueues),
	}
}

func NewBatchReadQueue() readQueue {
	return make(readQueue)
}

func NewStopObject() errorsObject {
	return errorsObject{
		Errors: nil,
		Stop:   true,
	}
}

func NewErrorsObject(errs []*pb.BatchError) errorsObject {
	return errorsObject{
		Errors: errs,
	}
}

type ReadQueues struct {
	lock   sync.RWMutex
	queues readQueues
}

// Get retrieves the read queue for the given stream ID.
func (r *ReadQueues) Get(streamId string) (readQueue, bool) {
	r.lock.RLock()
	defer r.lock.RUnlock()
	queue, ok := r.queues[streamId]
	if !ok {
		return nil, false
	}
	return queue, true
}

// Delete removes the read queue for the given stream ID.
func (r *ReadQueues) Delete(streamId string) {
	r.lock.Lock()
	defer r.lock.Unlock()
	close(r.queues[streamId])
	delete(r.queues, streamId)
}

func (r *ReadQueues) Close() {
	r.lock.Lock()
	defer r.lock.Unlock()
	for _, queue := range r.queues {
		close(queue)
	}
}

// Make initializes a read queue for the given stream ID if it does not already exist.
func (r *ReadQueues) Make(streamId string) {
	r.lock.Lock()
	defer r.lock.Unlock()
	if _, ok := r.queues[streamId]; !ok {
		r.queues[streamId] = make(readQueue)
	}
}

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
	"context"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
)

type Scheduler struct {
	logger        logrus.FieldLogger
	writeQueues   *WriteQueues
	internalQueue internalQueue
}

func NewScheduler(writeQueues *WriteQueues, internalQueue internalQueue, logger logrus.FieldLogger) *Scheduler {
	return &Scheduler{
		logger:        logger,
		writeQueues:   writeQueues,
		internalQueue: internalQueue,
	}
}

func (s *Scheduler) Loop(ctx context.Context) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			s.logger.Info("shutting down scheduler loop")
			s.loop(s.drain)
			// Close the internal queue so that the workers can exit once they've drained the queue
			close(s.internalQueue)
			return
		case <-ticker.C:
			s.loop(s.schedule)
		}
	}
}

func (s *Scheduler) loop(op func(streamId string, wq *WriteQueue)) {
	for _, uuid := range s.writeQueues.Uuids() {
		wq, ok := s.writeQueues.Get(uuid)
		if !ok {
			continue
		}
		op(uuid, wq)
	}
}

func (s *Scheduler) drain(streamId string, wq *WriteQueue) {
	objs := make([]*pb.BatchObject, 0, 1000)
	refs := make([]*pb.BatchReference, 0, 1000)
	for obj := range wq.queue {
		if obj.Object != nil {
			objs = append(objs, obj.Object)
		}
		if obj.Reference != nil {
			refs = append(refs, obj.Reference)
		}
		if len(objs) >= 1000 || len(refs) >= 1000 || obj.Stop {
			req := newProcessRequest(objs, refs, streamId, obj.Stop, wq)
			s.internalQueue <- req
			// Reset the queues
			objs = make([]*pb.BatchObject, 0, 1000)
			refs = make([]*pb.BatchReference, 0, 1000)
		}
	}
	if len(objs) > 0 || len(refs) > 0 {
		req := newProcessRequest(objs, refs, streamId, false, wq)
		s.internalQueue <- req
	}
}

func (s *Scheduler) schedule(streamId string, wq *WriteQueue) {
	objs, refs, stop := s.pull(wq.queue, 1000)
	req := newProcessRequest(objs, refs, streamId, false, wq)
	if (req.Objects != nil && len(req.Objects.Values) > 0) || (req.References != nil && len(req.References.Values) > 0) {
		s.internalQueue <- req
	}
	if stop {
		s.internalQueue <- newProcessRequest(nil, nil, streamId, true, wq)
	}
}

func (s *Scheduler) pull(queue writeQueue, max int) ([]*pb.BatchObject, []*pb.BatchReference, bool) {
	objs := make([]*pb.BatchObject, 0, max)
	refs := make([]*pb.BatchReference, 0, max)
	for i := 0; i < max && len(queue) > 0; i++ {
		select {
		case obj, ok := <-queue:
			if !ok {
				// channel is closed
				return objs, refs, false
			}
			if obj.Object != nil {
				objs = append(objs, obj.Object)
			}
			if obj.Reference != nil {
				refs = append(refs, obj.Reference)
			}
			if obj.Stop {
				return objs, refs, true
			}
		default:
			return objs, refs, false
		}
	}
	return objs, refs, false
}

func newProcessRequest(objs []*pb.BatchObject, refs []*pb.BatchReference, streamId string, stop bool, wq *WriteQueue) *ProcessRequest {
	req := &ProcessRequest{
		StreamId: streamId,
		Stop:     stop,
	}
	if len(objs) > 0 {
		req.Objects = &SendObjects{
			Values:           objs,
			ConsistencyLevel: wq.consistencyLevel,
		}
	}
	if len(refs) > 0 {
		req.References = &SendReferences{
			Values:           refs,
			ConsistencyLevel: wq.consistencyLevel,
		}
	}
	return req
}

func StartScheduler(ctx context.Context, wg *sync.WaitGroup, writeQueues *WriteQueues, internalQueue internalQueue, logger logrus.FieldLogger) {
	scheduler := NewScheduler(writeQueues, internalQueue, logger)
	wg.Add(1)
	enterrors.GoWrapper(func() {
		defer wg.Done()
		scheduler.Loop(ctx)
	}, logger)
}

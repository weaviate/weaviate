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
	for {
		select {
		case <-ctx.Done():
			s.logger.Info("shutting down scheduler loop")
			return
		default:
			s.writeQueues.queues.Range(func(key, value any) bool {
				streamId, ok := key.(string)
				if !ok {
					s.logger.WithField("key", key).Error("expected string key in write queues")
					return true // continue iteration
				}
				wq, ok := value.(*WriteQueue)
				if !ok {
					s.logger.WithField("value", value).Error("expected WriteQueue value in write queues")
					return true // continue iteration
				}
				if len(wq.queue) == 0 {
					return true // continue iteration if queue is empty
				}
				return s.add(ctx, streamId, wq)
			})
			time.Sleep(time.Millisecond * 100)
		}
	}
}

func (s *Scheduler) pull(ctx context.Context, queue writeQueue, max int) ([]*pb.BatchObject, []*pb.BatchReference, bool) {
	objs := make([]*pb.BatchObject, 0, max)
	refs := make([]*pb.BatchReference, 0, max)
	for i := 0; i < max && len(queue) > 0; i++ {
		select {
		case <-ctx.Done():
			s.logger.Info("shutting down scheduler loop due to grpc shutdown")
			return objs, refs, false // stop iteration
		case obj := <-queue:
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
		}
	}
	return objs, refs, false
}

func (s *Scheduler) add(ctx context.Context, streamId string, wq *WriteQueue) bool {
	batchSize := wq.BatchSize()
	if batchSize == 0 {
		s.logger.WithField("streamId", streamId).Debug("no batch size set, skipping")
		return true // continue iteration
	}
	objs, refs, stop := s.pull(ctx, wq.queue, batchSize)
	if len(objs) > 0 {
		s.internalQueue <- &ProcessRequest{
			StreamId: streamId,
			Objects: &SendObjects{
				Values:           objs,
				ConsistencyLevel: wq.consistencyLevel,
				Index:            wq.objIndex,
			},
		}
		wq.objIndex += int32(len(objs))
		s.logger.WithFields(logrus.Fields{
			"streamId": streamId,
			"count":    len(objs),
		}).Debug("scheduled batch write request")
	}
	if len(refs) > 0 {
		s.internalQueue <- &ProcessRequest{
			StreamId: streamId,
			References: &SendReferences{
				Values:           refs,
				ConsistencyLevel: wq.consistencyLevel,
				Index:            wq.refIndex,
			},
		}
		wq.refIndex += int32(len(refs))
		s.logger.WithFields(logrus.Fields{
			"streamId": streamId,
			"count":    len(refs),
		}).Debug("scheduled batch reference request")
	}
	if stop {
		s.internalQueue <- &ProcessRequest{
			StreamId: streamId,
			Stop:     true,
		}
	}
	time.Sleep(time.Millisecond * 5)
	return true
}

func StartScheduler(ctx context.Context, wg *sync.WaitGroup, writeQueues *WriteQueues, internalQueue internalQueue, logger logrus.FieldLogger) {
	scheduler := NewScheduler(writeQueues, internalQueue, logger)
	wg.Add(1)
	enterrors.GoWrapper(func() {
		scheduler.Loop(ctx)
		wg.Done()
	}, logger)
}

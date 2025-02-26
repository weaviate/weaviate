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

package batch

import (
	"context"
	"fmt"
	"os"

	"github.com/sirupsen/logrus"
	entcfg "github.com/weaviate/weaviate/entities/config"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
)

type batcher interface {
	BatchObjects(ctx context.Context, req *pb.BatchObjectsRequest) (*pb.BatchObjectsReply, error)
}

type StreamHandler struct {
	asyncEnabled bool
	batcher      batcher
	logger       logrus.FieldLogger
}

func NewStreamHandler(batcher batcher, logger logrus.FieldLogger) *StreamHandler {
	return &StreamHandler{
		asyncEnabled: entcfg.Enabled(os.Getenv("ASYNC_INDEXING")),
		batcher:      batcher,
		logger:       logger,
	}
}

func (h *StreamHandler) Stream(stream pb.Weaviate_BatchServer) error {
	first, err := stream.Recv()
	if err != nil {
		return fmt.Errorf("receive first batch object: %w", err)
	}
	init := first.GetInit()
	if init == nil {
		return fmt.Errorf("first object must be init object")
	}

	send := func(objects []queueObject, errors chan errorsObject) error {
		firstIndex := objects[0].Index
		objs := make([]*pb.BatchObject, len(objects))
		for i, obj := range objects {
			objs[i] = obj.Object
		}
		reply, err := h.batcher.BatchObjects(stream.Context(), &pb.BatchObjectsRequest{Objects: objs, ConsistencyLevel: init.ConsistencyLevel})
		if err != nil {
			return err
		}
		if len(reply.GetErrors()) > 0 {
			for _, err := range reply.GetErrors() {
				if err == nil {
					continue
				}
				err.Index += int32(firstIndex)
			}
			errors <- errorsObject{Errors: reply.GetErrors()}
		}
		return nil
	}

	batcher := func(queue chan queueObject, errors chan errorsObject) error {
		objects := make([]queueObject, 0, 1000)
		for {
			select {
			case <-stream.Context().Done():
				return nil
			case obj := <-queue:
				if obj.IsSentinel() {
					if len(objects) > 0 {
						if err := send(objects, errors); err != nil {
							return err
						}
					}
					errors <- errorsObject{Shutdown: true}
					return nil
				}
				if obj.IsObject() {
					objects = append(objects, obj)
				}
				if len(objects) == 1000 {
					if err := send(objects, errors); err != nil {
						return err
					}
					objects = objects[:0]
				}
			}
		}
	}

	replier := func(errors chan errorsObject, concurrency int) error {
		haveShutdown := 0
		for {
			select {
			case <-stream.Context().Done():
				return nil
			case errs := <-errors:
				if errs.Shutdown {
					haveShutdown += 1
				}
				for _, err := range errs.Errors {
					if innerErr := stream.Send(&pb.BatchError{
						Index: err.Index,
						Error: err.Error,
					}); innerErr != nil {
						return innerErr
					}
				}
			}
			if haveShutdown == concurrency {
				return nil
			}
		}
	}

	errors := make(chan errorsObject)
	queue := make(chan queueObject)
	defer func() {
		close(queue)
		close(errors)
	}()

	concurrency := 2
	if init.Concurrency != nil {
		concurrency = int(init.GetConcurrency())
	}
	eg := enterrors.NewErrorGroupWrapper(h.logger)
	for range concurrency {
		eg.Go(func() error { return batcher(queue, errors) })
	}
	eg.Go(func() error { return replier(errors, concurrency) })

	index := 0
	for {
		select {
		case <-stream.Context().Done():
			return nil
		default:
		}
		req, err := stream.Recv()
		if err != nil {
			stream.Send(&pb.BatchError{
				Index: int32(index),
				Error: err.Error(),
			})
			index++
			continue
		}

		sentinel := req.GetSentinel()
		if sentinel != nil {
			for range concurrency {
				queue <- queueObject{Sentinel: sentinel}
			}
			break
		}

		object := req.GetObject()
		if object == nil {
			stream.Send(&pb.BatchError{
				Index: int32(index),
				Error: fmt.Errorf("object must be message object, got %T", req).Error(),
			})
			index++
			continue
		}
		queue <- queueObject{Index: index, Object: object}
		index++
	}
	eg.Wait()
	return nil
}

type queueObject struct {
	Index    int
	Object   *pb.BatchObject
	Sentinel *pb.BatchStop
}

func (o queueObject) IsSentinel() bool {
	return o.Sentinel != nil
}

func (o queueObject) IsObject() bool {
	return o.Object != nil
}

type errorsObject struct {
	Errors   []*pb.BatchObjectsReply_BatchError
	Shutdown bool
}

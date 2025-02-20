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
		asyncEnabled: asyncEnabled(),
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

	objects := make([]*pb.BatchObject, 0, 1000)

	send := func(sem chan struct{}, objects []*pb.BatchObject, index int) error {
		sem <- struct{}{}        // acquire the semaphore
		defer func() { <-sem }() // release the semaphore

		reply, err := h.batcher.BatchObjects(stream.Context(), &pb.BatchObjectsRequest{Objects: objects, ConsistencyLevel: init.ConsistencyLevel})
		if err != nil {
			return err
		}
		for _, err := range reply.GetErrors() {
			err.Index += int32(index - len(objects))
		}
		if err := stream.Send(reply); err != nil {
			return err
		}
		return nil
	}

	concurrency := 2
	if init.Concurrency != nil {
		concurrency = int(init.GetConcurrency())
	}
	sem := make(chan struct{}, concurrency)

	index := 0
	eg := enterrors.NewErrorGroupWrapper(h.logger)
	for {
		req, err := stream.Recv()
		if err != nil {
			return err
		}

		sentinel := req.GetSentinel()
		if sentinel != nil {
			eg.Go(func() error { return send(sem, objects, index) })
			break
		}

		object := req.GetObject()
		if object == nil {
			return fmt.Errorf("object must be message object, got %T", req)
		}

		index += 1
		objects = append(objects, object)
		if h.asyncEnabled && len(objects) == 1000 {
			eg.Go(func() error { return send(sem, objects, index) })
			objects = objects[:0] // clear while maintaining capacity
			continue
		}
		if !h.asyncEnabled {
		}
	}
	eg.Wait()
	return nil
}

func asyncEnabled() bool {
	return entcfg.Enabled(os.Getenv("ASYNC_INDEXING"))
}

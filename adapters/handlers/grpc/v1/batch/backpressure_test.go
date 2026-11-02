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

package batch

import (
	"context"
	"errors"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/handlers/grpc/v1/batch/mocks"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/versioned"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/memwatch"
)

// A held receiver blocks drain until its hold ends, so Start clamps a
// configured hold to the shutdown grace period.
func TestStartBackpressure(t *testing.T) {
	start := func(opts ...Option) *StreamHandler {
		handler, _ := Start(nil, nil, nil, nil, nil, 0, logrus.New(), false, opts...)
		return handler
	}

	t.Run("a hold longer than the grace period is clamped", func(t *testing.T) {
		handler := start(WithStreamConfig(config.NewBatchStream(nil, nil, nil, new(1000), nil)))
		require.Equal(t, int(SHUTDOWN_GRACE_PERIOD/time.Second), handler.config.HoldSeconds())
	})

	t.Run("a zero hold is kept", func(t *testing.T) {
		handler := start(WithStreamConfig(config.NewBatchStream(nil, nil, nil, new(0), nil)))
		require.Zero(t, handler.config.HoldSeconds())
	})
}

func TestEnqueueReleasesReservation(t *testing.T) {
	const (
		size      = 4096
		className = "TestClass"
	)

	// Maybe on both lookups: the namespace subtest fails before either runs.
	newSchemaManager := func(lookupErr error) *mocks.MockschemaManager {
		schemaManager := mocks.NewMockschemaManager(t)
		schemaManager.EXPECT().ResolveAlias(mock.Anything).Return("").Maybe()
		classes := map[string]versioned.Class{className: {Class: &models.Class{Class: className}}}
		if lookupErr != nil {
			classes = nil
		}
		schemaManager.EXPECT().GetCachedClassNoAuth(mock.Anything, mock.Anything).Return(classes, lookupErr).Maybe()
		return schemaManager
	}

	newHandler := func(schemaManager schemaManager, queue processingQueue, namespacesEnabled bool) *StreamHandler {
		shuttingDownCtx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		return NewStreamHandler(nil, nil, shuttingDownCtx, cancel, &sync.WaitGroup{}, &sync.WaitGroup{},
			NewReportingQueues(), queue, nil, logrus.New(), schemaManager, namespacesEnabled,
			memwatch.NewDummyMonitor(), config.BatchStream{})
	}

	call := func(h *StreamHandler, principal *models.Principal, collection string, wg *sync.WaitGroup) error {
		_, err := h.tryAdmit(size)
		require.NoError(t, err)
		stream := mocks.NewMockWeaviate_BatchStreamServer[pb.BatchStreamRequest, pb.BatchStreamReply](t)
		stream.EXPECT().Send(mock.Anything).Return(nil).Maybe()
		objs := []*pb.BatchObject{{Collection: collection, Uuid: "5f8e0d34-1c6a-4a1e-9f0c-6f9b6e0f0a11"}}
		return h.enqueue(context.Background(), stream, h.logger.WithField("streamId", "stream"),
			principal, "stream", nil, wg, objs, nil, size)
	}

	t.Run("a namespace resolution error releases the reservation", func(t *testing.T) {
		h := newHandler(newSchemaManager(nil), NewProcessingQueue(), true)

		err := call(h, &models.Principal{Namespace: "customer1"}, "customer2:TestClass", &sync.WaitGroup{})

		require.Error(t, err)
		require.Zero(t, h.memInFlight.Load())
	})

	t.Run("a class lookup error releases the reservation", func(t *testing.T) {
		h := newHandler(newSchemaManager(errors.New("schema unavailable")), NewProcessingQueue(), false)

		err := call(h, &models.Principal{}, className, &sync.WaitGroup{})

		require.Error(t, err)
		require.Zero(t, h.memInFlight.Load())
	})

	t.Run("a panic at the queue send releases the reservation once and balances the wait group", func(t *testing.T) {
		queue := NewProcessingQueue()
		close(queue)
		h := newHandler(newSchemaManager(nil), queue, false)

		wg := &sync.WaitGroup{}
		require.Panics(t, func() { _ = call(h, &models.Principal{}, className, wg) })

		require.Zero(t, h.memInFlight.Load(), "the reservation is released once, by enqueue's guard alone")
		waited := make(chan struct{})
		go func() {
			wg.Wait()
			close(waited)
		}()
		select {
		case <-waited:
		case <-time.After(5 * time.Second):
			t.Fatal("push's guard did not release the wait group after the panic")
		}
	})
}

func TestAckDelay(t *testing.T) {
	cfg := config.NewBatchStream(new(0.9), new(0.5), new(time.Second), nil, nil)

	t.Run("zero at and below the engage ratio", func(t *testing.T) {
		for _, ratio := range []float64{0, 0.1, 0.49, 0.5} {
			require.Zero(t, ackDelay(cfg, ratio), "ratio %v", ratio)
		}
	})

	t.Run("max delay at and above the gate ratio", func(t *testing.T) {
		for _, ratio := range []float64{0.9, 0.95, 1} {
			require.Equal(t, cfg.MaxAckDelay(), ackDelay(cfg, ratio), "ratio %v", ratio)
		}
	})

	t.Run("increasing between engage and gate", func(t *testing.T) {
		previous := time.Duration(0)
		for ratio := 0.5; ratio < 0.9; ratio += 0.01 {
			delay := ackDelay(cfg, ratio)
			require.GreaterOrEqual(t, delay, previous, "ratio %v", ratio)
			previous = delay
		}
		require.Positive(t, previous, "the curve must rise before the gate")
	})

	t.Run("convex: the midpoint delay is below the linear interpolation", func(t *testing.T) {
		midpoint := (cfg.EngageRatio() + cfg.GateRatio()) / 2
		require.Less(t, ackDelay(cfg, midpoint), cfg.MaxAckDelay()/2)
	})

	t.Run("zero everywhere when gate is at or below engage", func(t *testing.T) {
		off := config.NewBatchStream(new(0.5), new(0.9), new(time.Second), nil, nil)
		for _, ratio := range []float64{0, 0.5, 0.7, 0.9, 1} {
			require.Zero(t, ackDelay(off, ratio), "ratio %v", ratio)
		}
	})

	t.Run("a zero max delay is no delay at any ratio", func(t *testing.T) {
		off := config.NewBatchStream(new(0.5), new(0.9), new(time.Duration(0)), nil, nil)
		for _, ratio := range []float64{0, 0.7, 0.9, 1} {
			require.Zero(t, ackDelay(off, ratio), "ratio %v", ratio)
		}
	})

	t.Run("NaN and out-of-range ratios are clamped", func(t *testing.T) {
		require.Zero(t, ackDelay(cfg, math.NaN()))
		require.Zero(t, ackDelay(cfg, -1))
		require.Equal(t, cfg.MaxAckDelay(), ackDelay(cfg, 2))
	})
}

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
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/versioned"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/memwatch"
)

// A held receiver blocks drain until its hold ends, so Start clamps a
// configured hold to the shutdown grace period.
func TestStartBackpressure(t *testing.T) {
	// The nil arguments are enough to reach the wiring under test. Zero workers
	// means Start launches no goroutines.
	start := func(opts ...Option) *StreamHandler {
		handler, _ := Start(nil, nil, nil, nil, nil, 0, logrus.New(), false, opts...)
		return handler
	}

	t.Run("a hold longer than the grace period is clamped", func(t *testing.T) {
		handler := start(WithBackpressure(config.BatchStream{HoldSeconds: 1000}))
		require.Equal(t, int(SHUTDOWN_GRACE_PERIOD/time.Second), handler.config.HoldSeconds)
	})
}

// stubSchemaManager answers the two lookups enqueue makes.
type stubSchemaManager struct{ lookupErr error }

func (s stubSchemaManager) GetCachedClassNoAuth(_ context.Context, names ...string) (map[string]versioned.Class, error) {
	if s.lookupErr != nil {
		return nil, s.lookupErr
	}
	classes := make(map[string]versioned.Class, len(names))
	for _, name := range names {
		classes[name] = versioned.Class{Class: &models.Class{Class: name}}
	}
	return classes, nil
}

func (s stubSchemaManager) ResolveAlias(string) string { return "" }

// stubStream swallows the Results message a rejected batch sends. enqueue
// touches no other stream method.
type stubStream struct {
	pb.Weaviate_BatchStreamServer
}

func (stubStream) Send(*pb.BatchStreamReply) error { return nil }

// The handler lives as long as the process. A leaked reservation would tighten
// the memory gate for every later stream until restart, and a double release
// would loosen it.
func TestEnqueueReleasesReservation(t *testing.T) {
	const size = 4096

	newHandler := func(schemaManager schemaManager, queue processingQueue, namespacesEnabled bool) *StreamHandler {
		shuttingDownCtx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		return NewStreamHandler(nil, nil, shuttingDownCtx, cancel, &sync.WaitGroup{}, &sync.WaitGroup{},
			NewReportingQueues(), queue, nil, logrus.New(), schemaManager, namespacesEnabled,
			memwatch.NewDummyMonitor(), config.BatchStream{})
	}

	// The test calls enqueue directly, so it must make the reservation itself.
	// It goes through the same admission check the receiver uses.
	call := func(h *StreamHandler, principal *models.Principal, collection string, wg *sync.WaitGroup) error {
		_, err := h.tryAdmit(size)
		require.NoError(t, err)
		objs := []*pb.BatchObject{{Collection: collection, Uuid: "5f8e0d34-1c6a-4a1e-9f0c-6f9b6e0f0a11"}}
		return h.enqueue(context.Background(), stubStream{}, h.logger.WithField("streamId", "stream"),
			principal, "stream", nil, wg, objs, nil, size)
	}

	t.Run("a namespace resolution error releases the reservation", func(t *testing.T) {
		h := newHandler(stubSchemaManager{}, NewProcessingQueue(), true)

		// a namespaced principal may not qualify a class name itself, so the
		// resolution fails before the schema manager is touched
		err := call(h, &models.Principal{Namespace: "customer1"}, "customer2:TestClass", &sync.WaitGroup{})

		require.Error(t, err)
		require.Zero(t, h.memInFlight.Load())
	})

	t.Run("a class lookup error releases the reservation", func(t *testing.T) {
		h := newHandler(stubSchemaManager{lookupErr: errors.New("schema unavailable")}, NewProcessingQueue(), false)

		err := call(h, &models.Principal{}, "TestClass", &sync.WaitGroup{})

		require.Error(t, err)
		require.Zero(t, h.memInFlight.Load())
	})

	t.Run("a panic at the queue send releases the reservation once and balances the wait group", func(t *testing.T) {
		// Closing the queue makes push's send panic after it has added to the
		// wait group. In production drain closes the queue only after every
		// receiver has returned, so this send cannot panic there.
		queue := NewProcessingQueue()
		close(queue)
		h := newHandler(stubSchemaManager{}, queue, false)

		wg := &sync.WaitGroup{}
		require.Panics(t, func() { _ = call(h, &models.Principal{}, "TestClass", wg) })

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
	cfg := config.BatchStream{EngageRatio: 0.5, GateRatio: 0.9, MaxAckDelay: time.Second}

	t.Run("zero at and below the engage ratio", func(t *testing.T) {
		for _, ratio := range []float64{0, 0.1, 0.49, 0.5} {
			require.Zero(t, ackDelay(cfg, ratio), "ratio %v", ratio)
		}
	})

	t.Run("max delay at and above the gate ratio", func(t *testing.T) {
		for _, ratio := range []float64{0.9, 0.95, 1} {
			require.Equal(t, cfg.MaxAckDelay, ackDelay(cfg, ratio), "ratio %v", ratio)
		}
	})

	t.Run("monotone between engage and gate", func(t *testing.T) {
		previous := time.Duration(0)
		for ratio := 0.5; ratio < 0.9; ratio += 0.01 {
			delay := ackDelay(cfg, ratio)
			require.GreaterOrEqual(t, delay, previous, "ratio %v", ratio)
			previous = delay
		}
		require.Positive(t, previous, "the curve must rise before the gate")
	})

	t.Run("convex: the midpoint delay is below the linear interpolation", func(t *testing.T) {
		midpoint := (cfg.EngageRatio + cfg.GateRatio) / 2
		require.Less(t, ackDelay(cfg, midpoint), cfg.MaxAckDelay/2)
	})

	t.Run("zero everywhere when gate is at or below engage", func(t *testing.T) {
		off := config.BatchStream{EngageRatio: 0.9, GateRatio: 0.5, MaxAckDelay: time.Second}
		for _, ratio := range []float64{0, 0.5, 0.7, 0.9, 1} {
			require.Zero(t, ackDelay(off, ratio), "ratio %v", ratio)
		}
	})

	t.Run("NaN and out-of-range ratios are clamped", func(t *testing.T) {
		require.Zero(t, ackDelay(cfg, math.NaN()))
		require.Zero(t, ackDelay(cfg, -1))
		require.Equal(t, cfg.MaxAckDelay, ackDelay(cfg, 2))
	})
}

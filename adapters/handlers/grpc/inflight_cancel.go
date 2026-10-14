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

package grpc

import (
	"context"
	"sync/atomic"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var errServerShuttingDown = status.Error(codes.Unavailable, "server is shutting down")

// InFlightCancel refuses unary gRPC calls once callsCtx is cancelled, and
// cancels those still running, so a graceful stop waits only for handlers that
// ignore it.
type InFlightCancel struct {
	callsCtx             context.Context
	unavailableResponses atomic.Int64
	running              atomic.Int64
	shuttingDown         atomic.Bool
}

func NewInFlightCancel(callsCtx context.Context) *InFlightCancel {
	return &InFlightCancel{callsCtx: callsCtx}
}

// StartShutdown makes CountRunningStreams add streams ending in codes.Unavailable to
// UnavailableResponses. The batch stream answers that code once its drain has begun.
func (c *InFlightCancel) StartShutdown() {
	c.shuttingDown.Store(true)
}

// UnavailableResponses returns how many calls were refused or cut short, and how
// many streams ended in codes.Unavailable after StartShutdown.
func (c *InFlightCancel) UnavailableResponses() int64 {
	return c.unavailableResponses.Load()
}

// CallsRunning returns how many unary calls and streams are being handled.
func (c *InFlightCancel) CallsRunning() int64 {
	return c.running.Load()
}

// CountRunningStreams counts streams in CallsRunning while their handler runs,
// and in UnavailableResponses when one ends in codes.Unavailable after StartShutdown.
func (c *InFlightCancel) CountRunningStreams() grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		c.running.Add(1)
		defer c.running.Add(-1)
		err := handler(srv, ss)
		if c.shuttingDown.Load() && status.Code(err) == codes.Unavailable {
			c.unavailableResponses.Add(1)
		}
		return err
	}
}

// UnavailableAfterCancel answers codes.Unavailable for a call still running when
// callsCtx is cancelled, and refuses one arriving after that without running it.
// Clients retry neither Canceled nor the per-object errors a cancelled BatchObjects replies with.
func (c *InFlightCancel) UnavailableAfterCancel() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if c.callsCtx.Err() != nil {
			c.unavailableResponses.Add(1)
			return nil, errServerShuttingDown
		}
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()
		stop := context.AfterFunc(c.callsCtx, func() {
			c.unavailableResponses.Add(1)
			cancel()
		})
		c.running.Add(1)
		defer c.running.Add(-1)
		resp, err := handler(ctx, req)
		if stop() {
			return resp, err
		}
		// Waiting lets the callback count this call before it returns. A client
		// cancel at the same instant can still let it return before being counted.
		<-ctx.Done()
		return nil, errServerShuttingDown
	}
}

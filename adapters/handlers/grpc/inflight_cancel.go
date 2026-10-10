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
}

func NewInFlightCancel(callsCtx context.Context) *InFlightCancel {
	return &InFlightCancel{callsCtx: callsCtx}
}

// UnavailableResponses returns how many calls were refused or cut short.
func (c *InFlightCancel) UnavailableResponses() int64 {
	return c.unavailableResponses.Load()
}

// UnavailableAfterCancel answers codes.Unavailable for a call the cancel cut
// short, and refuses one arriving after it without running it. Clients retry
// neither Canceled nor the per-object errors a cancelled BatchObjects replies with.
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

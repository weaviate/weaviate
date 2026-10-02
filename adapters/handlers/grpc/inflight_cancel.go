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

// InFlightCancel cancels the ctx of every unary call running through its
// interceptor when Cancel is called, so a graceful server stop waits only for
// handlers that ignore it.
type InFlightCancel struct {
	ctx      context.Context
	cancel   context.CancelFunc
	cutShort atomic.Int64
}

func NewInFlightCancel() *InFlightCancel {
	ctx, cancel := context.WithCancel(context.Background())
	return &InFlightCancel{ctx: ctx, cancel: cancel}
}

func (c *InFlightCancel) Cancel() {
	c.cancel()
}

// CutShort returns how many calls Cancel cut short or refused.
func (c *InFlightCancel) CutShort() int64 {
	return c.cutShort.Load()
}

// UnaryInterceptor answers codes.Unavailable for a call Cancel cut short, and
// refuses one arriving after Cancel without running it. Clients retry neither
// Canceled nor the per-object errors a cancelled BatchObjects replies with.
func (c *InFlightCancel) UnaryInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if c.ctx.Err() != nil {
			c.cutShort.Add(1)
			return nil, errServerShuttingDown
		}
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()
		stop := context.AfterFunc(c.ctx, func() {
			c.cutShort.Add(1)
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

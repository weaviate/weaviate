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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pbv1 "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
)

// afterFuncCountingCtx counts the context.AfterFunc registrations that have
// neither fired nor been stopped.
type afterFuncCountingCtx struct {
	context.Context
	live atomic.Int64
}

// Value hides the embedded cancelCtx, which context.AfterFunc would otherwise
// register with directly instead of calling AfterFunc below.
func (c *afterFuncCountingCtx) Value(any) any { return nil }

func (c *afterFuncCountingCtx) AfterFunc(f func()) func() bool {
	c.live.Add(1)
	stop := context.AfterFunc(c.Context, func() {
		c.live.Add(-1)
		f()
	})
	return func() bool {
		stopped := stop()
		if stopped {
			c.live.Add(-1)
		}
		return stopped
	}
}

func TestInFlightCancelUnaryInterceptor(t *testing.T) {
	okReply := &pbv1.BatchObjectsReply{Took: 1}
	handlerErr := status.Error(codes.InvalidArgument, "bad request")

	var handlerCancelled bool
	// waitForCancel bounds the wait so a missing cancel fails instead of hanging.
	waitForCancel := func(ctx context.Context) (any, error) {
		select {
		case <-ctx.Done():
			handlerCancelled = true
			return okReply, ctx.Err()
		case <-time.After(2 * time.Second):
			return okReply, nil
		}
	}

	cases := []struct {
		name                 string
		cancelBefore         bool
		handler              func(ctx context.Context, ic *InFlightCancel, callerCancel context.CancelFunc) (any, error)
		wantResp             any
		wantCode             codes.Code
		wantErr              error
		wantCutShort         int64
		wantHandlerCancelled bool
	}{
		{
			name: "cancel during the call returns Unavailable and drops the reply",
			handler: func(ctx context.Context, ic *InFlightCancel, _ context.CancelFunc) (any, error) {
				ic.Cancel()
				resp, _ := waitForCancel(ctx)
				return resp, nil
			},
			wantCode:             codes.Unavailable,
			wantCutShort:         1,
			wantHandlerCancelled: true,
		},
		{
			name:         "call arriving after cancel returns Unavailable",
			cancelBefore: true,
			handler: func(ctx context.Context, _ *InFlightCancel, _ context.CancelFunc) (any, error) {
				return waitForCancel(ctx)
			},
			wantCode:             codes.Unavailable,
			wantCutShort:         1,
			wantHandlerCancelled: true,
		},
		{
			name: "call finishing before cancel passes its reply through",
			handler: func(ctx context.Context, _ *InFlightCancel, _ context.CancelFunc) (any, error) {
				return okReply, nil
			},
			wantResp: okReply,
		},
		{
			name: "handler error is not remapped",
			handler: func(ctx context.Context, _ *InFlightCancel, _ context.CancelFunc) (any, error) {
				return nil, handlerErr
			},
			wantErr: handlerErr,
		},
		{
			name: "caller cancelling its own call is not remapped",
			handler: func(ctx context.Context, _ *InFlightCancel, callerCancel context.CancelFunc) (any, error) {
				callerCancel()
				return waitForCancel(ctx)
			},
			wantResp:             okReply,
			wantErr:              context.Canceled,
			wantHandlerCancelled: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			shutdownCtx, shutdownCancel := context.WithCancel(context.Background())
			t.Cleanup(shutdownCancel)
			countingCtx := &afterFuncCountingCtx{Context: shutdownCtx}
			ic := &InFlightCancel{ctx: countingCtx, cancel: shutdownCancel}
			handlerCancelled = false
			if tc.cancelBefore {
				ic.Cancel()
			}

			callerCtx, callerCancel := context.WithCancel(context.Background())
			t.Cleanup(callerCancel)
			resp, err := ic.UnaryInterceptor()(callerCtx, nil, &grpc.UnaryServerInfo{},
				func(ctx context.Context, _ any) (any, error) { return tc.handler(ctx, ic, callerCancel) })

			if tc.wantCode != codes.OK {
				require.Error(t, err)
				assert.Equal(t, tc.wantCode, status.Code(err))
			} else {
				assert.ErrorIs(t, err, tc.wantErr)
			}
			assert.Equal(t, tc.wantResp, resp)
			assert.Equal(t, tc.wantHandlerCancelled, handlerCancelled, "handler ctx cancellation")
			assert.Equal(t, int64(0), countingCtx.live.Load(), "AfterFunc registration outlived the call")

			ic.Cancel()
			assert.Equal(t, tc.wantCutShort, ic.CutShort(), "cut-short count")
		})
	}
}

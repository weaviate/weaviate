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
	"io"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
)

// fakeRecvStream implements only the methods recv uses. The embedded interface
// covers the rest; calling an unimplemented method panics.
type fakeRecvStream struct {
	pb.Weaviate_BatchStreamServer
	recvFn func() (*pb.BatchStreamRequest, error)
}

func (f *fakeRecvStream) Recv() (*pb.BatchStreamRequest, error) {
	return f.recvFn()
}

func recvGoroutines() int {
	buf := make([]byte, 1<<20)
	n := runtime.Stack(buf, true)
	return strings.Count(string(buf[:n]), "(*StreamHandler).recv.func")
}

// TestRecvGoroutineExitsWhenConsumerIsGone pins the leak from
// https://github.com/weaviate/weaviate/issues/12749: the goroutine spawned by recv
// blocked forever on its unbuffered channel sends when the receiver loop returned
// (for example via ctx.Done()) without draining them.
func TestRecvGoroutineExitsWhenConsumerIsGone(t *testing.T) {
	tests := []struct {
		name   string
		recvFn func() (*pb.BatchStreamRequest, error)
	}{
		{
			// recv is blocked sending the Recv error into errCh
			name:   "blocked on error send",
			recvFn: func() (*pb.BatchStreamRequest, error) { return nil, errors.New("stream broken") },
		},
		{
			// recv is blocked sending a request into reqCh
			name:   "blocked on request send",
			recvFn: func() (*pb.BatchStreamRequest, error) { return &pb.BatchStreamRequest{}, nil },
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Zero(t, recvGoroutines(), "leftover recv goroutines from a previous test")
			h := &StreamHandler{logger: logrus.New()}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			h.recv(ctx, &fakeRecvStream{recvFn: tt.recvFn})
			require.Eventually(t, func() bool { return recvGoroutines() == 1 }, 3*time.Second, 10*time.Millisecond,
				"recv goroutine did not start")

			// The consumer never reads reqCh/errCh; cancelling ctx is how the receiver
			// loop signals it is gone (it cancels its ctx on every exit path).
			cancel()
			require.Eventually(t, func() bool { return recvGoroutines() == 0 }, 3*time.Second, 10*time.Millisecond,
				"recv goroutine leaked after the consumer went away")
		})
	}
}

// TestRecvDeliversRequestsAndError verifies the normal path: requests then the
// terminating error are delivered, and the goroutine exits closing both channels.
func TestRecvDeliversRequestsAndError(t *testing.T) {
	require.Zero(t, recvGoroutines(), "leftover recv goroutines from a previous test")
	h := &StreamHandler{logger: logrus.New()}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	calls := 0
	stream := &fakeRecvStream{recvFn: func() (*pb.BatchStreamRequest, error) {
		calls++
		if calls == 1 {
			return &pb.BatchStreamRequest{}, nil
		}
		return nil, io.EOF
	}}

	reqCh, errCh := h.recv(ctx, stream)
	select {
	case req := <-reqCh:
		require.NotNil(t, req)
	case <-time.After(3 * time.Second):
		t.Fatal("request was not delivered")
	}
	select {
	case err := <-errCh:
		require.ErrorIs(t, err, io.EOF)
	case <-time.After(3 * time.Second):
		t.Fatal("error was not delivered")
	}

	_, open := <-reqCh
	require.False(t, open, "reqCh should be closed after recv exits")
	_, open = <-errCh
	require.False(t, open, "errCh should be closed after recv exits")
	require.Eventually(t, func() bool { return recvGoroutines() == 0 }, 3*time.Second, 10*time.Millisecond)
}

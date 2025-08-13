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

package grpcconn

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/test/bufconn"
)

const bufSize = 1024 * 1024

func newBufConnDialer(t *testing.T) (*bufconn.Listener, func(context.Context, string) (net.Conn, error)) {
	t.Helper()

	lis := bufconn.Listen(bufSize)

	dialer := func(ctx context.Context, _ string) (net.Conn, error) {
		return lis.DialContext(ctx)
	}

	return lis, dialer
}

func newTestManager(t *testing.T, timeout time.Duration, opts ...grpc.DialOption) (*ConnManager, func()) {
	t.Helper()

	reg := prometheus.NewPedanticRegistry()
	logger := logrus.New()

	m := NewConnManager(timeout, reg, logger, opts...)

	return m, func() { m.Close() }
}

func TestGetConn_CachesConnection(t *testing.T) {
	lis, dialer := newBufConnDialer(t)
	defer lis.Close()

	m, done := newTestManager(t, time.Minute,
		grpc.WithContextDialer(dialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	defer done()

	c1, err := m.GetConn("bufnet")
	if err != nil {
		t.Fatalf("first GetConn failed: %v", err)
	}

	c2, err := m.GetConn("bufnet")
	if err != nil {
		t.Fatalf("second GetConn failed: %v", err)
	}

	if c1 != c2 {
		t.Fatalf("expected cached connection; got different pointers")
	}
}

func TestCleanupIdleConnections_RemovesStaleOnes(t *testing.T) {
	lis, dialer := newBufConnDialer(t)
	defer lis.Close()

	// Tiny timeout so entries become stale quickly.
	m, done := newTestManager(t, 20*time.Millisecond,
		grpc.WithContextDialer(dialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	defer done()

	c1, err := m.GetConn("bufnet")
	if err != nil {
		t.Fatalf("GetConn failed: %v", err)
	}

	// Let it become stale.
	time.Sleep(50 * time.Millisecond)

	// Force a cleanup pass (no direct field access).
	m.CleanupIdleConnections()

	// The old connection should be closed by now.
	if s := c1.GetState(); s != connectivity.Shutdown {
		t.Fatalf("expected stale conn to be closed (Shutdown), got %v", s)
	}

	// A fresh GetConn should yield a new, distinct connection.
	c2, err := m.GetConn("bufnet")
	if err != nil {
		t.Fatalf("GetConn after cleanup failed: %v", err)
	}

	if c1 == c2 {
		t.Fatalf("expected a new connection after cleanup")
	}
}

func TestCleanupIdleConnections_RespectsRecentUse(t *testing.T) {
	lis, dialer := newBufConnDialer(t)
	defer lis.Close()

	m, done := newTestManager(t, 100*time.Millisecond,
		grpc.WithContextDialer(dialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	defer done()

	c1, err := m.GetConn("bufnet")
	if err != nil {
		t.Fatalf("GetConn failed: %v", err)
	}

	// Wait but not past the timeout, then "touch" the entry by calling GetConn again.
	time.Sleep(50 * time.Millisecond)
	c2, err := m.GetConn("bufnet")
	if err != nil {
		t.Fatalf("GetConn refresh failed: %v", err)
	}

	if c1 != c2 {
		t.Fatalf("expected same connection on refresh")
	}

	// Immediately run cleanup; lastUsed should have been refreshed, so it should NOT close.
	m.CleanupIdleConnections()

	if s := c1.GetState(); s == connectivity.Shutdown {
		t.Fatalf("did not expect conn to be closed after recent use")
	}
}

func TestClose_ClosesConnections(t *testing.T) {
	lis, dialer := newBufConnDialer(t)
	defer lis.Close()

	m, _ := newTestManager(t, time.Minute,
		grpc.WithContextDialer(dialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)

	c1, err := m.GetConn("bufnet")
	if err != nil {
		t.Fatalf("GetConn failed: %v", err)
	}

	err = m.CloseConn("bufnet")
	if err != nil {
		t.Fatalf("CloseConn failed: %v", err)
	}

	if s := c1.GetState(); s != connectivity.Shutdown {
		t.Fatalf("expected conn to be closed after Close(), got %v", s)
	}

	// Manager should still allow new dials (fresh connection instance).
	c2, err := m.GetConn("bufnet")
	if err != nil {
		t.Fatalf("GetConn after Close() failed: %v", err)
	}

	if c1 == c2 {
		t.Fatalf("expected a new connection after Close()")
	}

	m.Close()

	_, err = m.GetConn("bufnet")
	if err == nil {
		t.Fatalf("expected error after Close(), got nil")
	}
	if err.Error() != "connection manager is closed" {
		t.Fatalf("expected closed error, got %v", err)
	}

	err = m.CloseConn("bufnet")
	if err == nil {
		t.Fatalf("expected error on CloseConn after manager closed, got nil")
	}
	if err.Error() != "connection manager is closed" {
		t.Fatalf("expected closed error on CloseConn, got %v", err)
	}
}

func TestBasicAuthHeader(t *testing.T) {
	got := BasicAuthHeader("alice", "s3cr3t")
	want := "Basic YWxpY2U6czNjcjN0" // base64("alice:s3cr3t")
	if got != want {
		t.Fatalf("unexpected auth header; want %q, got %q", want, got)
	}
}

func TestBasicAuthUnaryInterceptor_SetsAuthorization(t *testing.T) {
	auth := BasicAuthHeader("bob", "pwd")
	interceptor := BasicAuthUnaryInterceptor(auth)

	ctx := metadata.NewOutgoingContext(context.Background(), metadata.Pairs("foo", "bar"))

	invoked := false
	invoker := func(ctx context.Context, method string, req, reply interface{},
		cc *grpc.ClientConn, opts ...grpc.CallOption,
	) error {
		invoked = true
		md, ok := metadata.FromOutgoingContext(ctx)
		if !ok {
			t.Fatalf("no outgoing metadata in context")
		}
		if v := md.Get("authorization"); len(v) != 1 || v[0] != auth {
			t.Fatalf("authorization header not set correctly, got %v", v)
		}
		if v := md.Get("foo"); len(v) != 1 || v[0] != "bar" {
			t.Fatalf("existing metadata not preserved, got %v", md)
		}
		return nil
	}

	if err := interceptor(ctx, "/pkg.Svc/Method", nil, nil, nil, invoker); err != nil {
		t.Fatalf("interceptor returned error: %v", err)
	}
	if !invoked {
		t.Fatalf("invoker was not called")
	}
}

type fakeClientStream struct{ ctx context.Context }

func (f *fakeClientStream) Header() (metadata.MD, error) { return nil, nil }
func (f *fakeClientStream) Trailer() metadata.MD         { return nil }
func (f *fakeClientStream) CloseSend() error             { return nil }
func (f *fakeClientStream) Context() context.Context     { return f.ctx }
func (f *fakeClientStream) SendMsg(m interface{}) error  { return nil }
func (f *fakeClientStream) RecvMsg(m interface{}) error  { return nil }

func TestBasicAuthStreamInterceptor_SetsAuthorization(t *testing.T) {
	auth := BasicAuthHeader("carol", "pw")
	interceptor := BasicAuthStreamInterceptor(auth)

	// Carry existing metadata to ensure it's preserved.
	ctx := metadata.NewOutgoingContext(context.Background(), metadata.Pairs("x", "y"))

	streamer := func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		// Validate metadata right where the call would be made.
		md, ok := metadata.FromOutgoingContext(ctx)
		if !ok {
			t.Fatalf("no outgoing metadata in context")
		}
		if v := md.Get("authorization"); len(v) != 1 || v[0] != auth {
			t.Fatalf("authorization header not set correctly, got %v", v)
		}
		if v := md.Get("x"); len(v) != 1 || v[0] != "y" {
			t.Fatalf("existing metadata not preserved, got %v", md)
		}
		return &fakeClientStream{ctx: ctx}, nil
	}

	desc := &grpc.StreamDesc{ServerStreams: true, ClientStreams: true}
	cs, err := interceptor(ctx, desc, nil, "/pkg.Svc/Stream", streamer)
	if err != nil {
		t.Fatalf("stream interceptor returned error: %v", err)
	}
	if cs == nil {
		t.Fatalf("expected non-nil ClientStream")
	}
}

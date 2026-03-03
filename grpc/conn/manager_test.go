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

package grpcconn

import (
	"context"
	"net"
	"sync"
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

func newTestManager(t *testing.T, maxConns int, timeout time.Duration, opts ...grpc.DialOption) (*ConnManager, func()) {
	t.Helper()

	reg := prometheus.NewPedanticRegistry()
	logger := logrus.New()

	m, err := NewConnManager(maxConns, timeout, reg, logger, opts...)
	if err != nil {
		t.Fatalf("NewConnManager failed: %v", err)
	}

	return m, func() { m.Close() }
}

func TestNewConnManager_Validation(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	logger := logrus.New()

	cases := []struct {
		name         string
		maxOpenConns int
		timeout      time.Duration
		wantErr      string
	}{
		{
			name:         "zero maxOpenConns",
			maxOpenConns: 0,
			timeout:      time.Minute,
			wantErr:      "grpcconn: maxOpenConns must be > 0, got 0",
		},
		{
			name:         "negative maxOpenConns",
			maxOpenConns: -1,
			timeout:      time.Minute,
			wantErr:      "grpcconn: maxOpenConns must be > 0, got -1",
		},
		{
			name:         "zero timeout",
			maxOpenConns: 10,
			timeout:      0,
			wantErr:      "grpcconn: timeout must be > 0, got 0s",
		},
		{
			name:         "negative timeout",
			maxOpenConns: 10,
			timeout:      -time.Second,
			wantErr:      "grpcconn: timeout must be > 0, got -1s",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewConnManager(tc.maxOpenConns, tc.timeout, reg, logger)
			if err == nil {
				t.Fatalf("expected error, got nil")
			}
			if err.Error() != tc.wantErr {
				t.Fatalf("expected error %q, got %q", tc.wantErr, err.Error())
			}
		})
	}
}

func TestGetConn_CachesConnection(t *testing.T) {
	lis, dialer := newBufConnDialer(t)
	defer lis.Close()

	m, done := newTestManager(t, 10, time.Minute,
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

func TestClose_ClosesConnections(t *testing.T) {
	lis, dialer := newBufConnDialer(t)
	defer lis.Close()

	m, _ := newTestManager(t, 10, time.Minute,
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

func TestGetConn_ConcurrentSingleFlight(t *testing.T) {
	lis, dialer := newBufConnDialer(t)
	defer lis.Close()

	m, done := newTestManager(t, 10, time.Minute,
		grpc.WithContextDialer(dialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	defer done()

	const n = 25
	conns := make([]*grpc.ClientConn, n)
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(idx int) {
			defer wg.Done()
			c, err := m.GetConn("bufnet")
			if err != nil {
				t.Errorf("GetConn failed: %v", err)
				return
			}
			conns[idx] = c
		}(i)
	}
	wg.Wait()

	for i := 1; i < n; i++ {
		if conns[i] != conns[0] {
			t.Fatalf("expected all goroutines to receive the same connection instance")
		}
	}
}

func TestGetConn_MaxConnsEvictsExpired(t *testing.T) {
	lis, dialer := newBufConnDialer(t)
	defer lis.Close()

	// Allow only one live connection; make entries expire quickly
	m, done := newTestManager(t, 1, 30*time.Millisecond,
		grpc.WithContextDialer(dialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	defer done()

	c1, err := m.GetConn("bufnet1")
	if err != nil {
		t.Fatalf("GetConn addr1 failed: %v", err)
	}

	// Let it become expired w.r.t. timeout
	time.Sleep(50 * time.Millisecond)

	c2, err := m.GetConn("bufnet2")
	if err != nil {
		t.Fatalf("GetConn addr2 failed: %v", err)
	}
	if c1 == c2 {
		t.Fatalf("expected a different connection after eviction due to expiration")
	}

	// The old connection should have been closed by the capacity eviction
	if s := c1.GetState(); s != connectivity.Shutdown {
		t.Fatalf("expected first connection to be Shutdown after eviction, got %v", s)
	}
}

func TestGetConn_AtCapacityNoExpired_Rejected(t *testing.T) {
	lis, dialer := newBufConnDialer(t)
	defer lis.Close()

	// Capacity 1, but use a large timeout so nothing is considered expired.
	m, done := newTestManager(t, 1, time.Minute,
		grpc.WithContextDialer(dialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	defer done()

	if _, err := m.GetConn("bufnet1"); err != nil {
		t.Fatalf("GetConn addr1 failed unexpectedly: %v", err)
	}

	_, err := m.GetConn("bufnet2")
	if err == nil {
		t.Fatalf("expected capacity rejection error, got nil")
	}
	if got := err.Error(); got != "connection limit reached and no expired connections available" {
		t.Fatalf("unexpected error: %q", got)
	}
}

func TestCloseConn_NotFound(t *testing.T) {
	lis, dialer := newBufConnDialer(t)
	defer lis.Close()

	m, done := newTestManager(t, 10, time.Minute,
		grpc.WithContextDialer(dialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	defer done()

	err := m.CloseConn("nonexistent")
	if err == nil {
		t.Fatalf("expected error when closing nonexistent address, got nil")
	}
}

func TestBasicAuthHeader_SpecialCharacters(t *testing.T) {
	got := BasicAuthHeader("al!ce", "p@ss:word")
	// base64("al!ce:p@ss:word")
	want := "Basic YWwhY2U6cEBzczp3b3Jk"
	if got != want {
		t.Fatalf("unexpected auth header; want %q, got %q", want, got)
	}
}

func TestBasicAuthUnaryInterceptor_NoExistingMetadata(t *testing.T) {
	auth := BasicAuthHeader("user", "pw")
	interceptor := BasicAuthUnaryInterceptor(auth)

	ctx := context.Background() // no existing metadata

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
		return nil
	}

	if err := interceptor(ctx, "/pkg.Svc/Method", nil, nil, nil, invoker); err != nil {
		t.Fatalf("interceptor returned error: %v", err)
	}
	if !invoked {
		t.Fatalf("invoker was not called")
	}
}

func TestBasicAuthStreamInterceptor_NoExistingMetadata(t *testing.T) {
	auth := BasicAuthHeader("user", "pw")
	interceptor := BasicAuthStreamInterceptor(auth)

	ctx := context.Background() // no existing metadata

	streamer := func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		md, ok := metadata.FromOutgoingContext(ctx)
		if !ok {
			t.Fatalf("no outgoing metadata in context")
		}
		if v := md.Get("authorization"); len(v) != 1 || v[0] != auth {
			t.Fatalf("authorization header not set correctly, got %v", v)
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

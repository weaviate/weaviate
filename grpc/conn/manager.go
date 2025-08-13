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
	"encoding/base64"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	enterrors "github.com/weaviate/weaviate/entities/errors"
)

type ConnManager struct {
	conns   map[string]*connEntry
	mu      sync.RWMutex
	opts    []grpc.DialOption
	timeout time.Duration

	metrics connMetrics

	stopCh chan struct{}
	wg     sync.WaitGroup

	closed bool
}

type connEntry struct {
	conn     *grpc.ClientConn
	lastUsed time.Time
}

func NewConnManager(timeout time.Duration, reg prometheus.Registerer, logger logrus.FieldLogger, opts ...grpc.DialOption) *ConnManager {
	m := &ConnManager{
		conns:   make(map[string]*connEntry),
		opts:    opts,
		timeout: timeout,
		metrics: newConnMetrics(reg),
		stopCh:  make(chan struct{}),
	}

	m.wg.Add(1)
	enterrors.GoWrapper(m.cleanupLoop, logger)

	return m
}

func (m *ConnManager) GetConn(addr string) (*grpc.ClientConn, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return nil, fmt.Errorf("connection manager is closed")
	}

	if entry, ok := m.conns[addr]; ok {
		entry.lastUsed = time.Now()
		m.metrics.connReuseTotal.Inc()
		return entry.conn, nil
	}

	conn, err := grpc.NewClient(addr, m.opts...)
	if err != nil {
		return nil, err
	}

	m.conns[addr] = &connEntry{
		conn:     conn,
		lastUsed: time.Now(),
	}

	m.metrics.connCreateTotal.Inc()
	m.metrics.connOpenGauge.Inc()

	return conn, nil
}

func (m *ConnManager) CloseConn(addr string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return fmt.Errorf("connection manager is closed")
	}

	if entry, ok := m.conns[addr]; ok {
		if err := entry.conn.Close(); err != nil {
			return fmt.Errorf("failed to close connection: %w", err)
		}

		delete(m.conns, addr)

		m.metrics.connCloseTotal.Inc()
		m.metrics.connOpenGauge.Dec()

		return nil
	}

	return fmt.Errorf("no connection found for address: %s", addr)
}

func (m *ConnManager) cleanupLoop() {
	ticker := time.NewTicker(time.Minute)
	defer func() {
		ticker.Stop()
		m.wg.Done()
	}()

	for {
		select {
		case <-ticker.C:
			m.CleanupIdleConnections()
		case <-m.stopCh:
			return
		}
	}
}

func (m *ConnManager) CleanupIdleConnections() {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return
	}

	now := time.Now()

	for addr, entry := range m.conns {
		if now.Sub(entry.lastUsed) > m.timeout {
			_ = entry.conn.Close()
			delete(m.conns, addr)

			m.metrics.connCloseTotal.Inc()
			m.metrics.connOpenGauge.Dec()
		}
	}
}

func (m *ConnManager) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return
	}

	close(m.stopCh)
	m.wg.Wait()

	for _, entry := range m.conns {
		_ = entry.conn.Close()

		m.metrics.connCloseTotal.Inc()
		m.metrics.connOpenGauge.Dec()
	}

	m.conns = make(map[string]*connEntry)

	m.closed = true
}

func BasicAuthHeader(username, password string) string {
	creds := fmt.Sprintf("%s:%s", username, password)
	return "Basic " + base64.StdEncoding.EncodeToString([]byte(creds))
}

func BasicAuthUnaryInterceptor(authHeader string) grpc.UnaryClientInterceptor {
	return func(
		ctx context.Context,
		method string,
		req, reply interface{},
		cc *grpc.ClientConn,
		invoker grpc.UnaryInvoker,
		opts ...grpc.CallOption,
	) error {
		md, ok := metadata.FromOutgoingContext(ctx)
		if !ok {
			md = metadata.New(nil)
		} else {
			md = md.Copy()
		}

		md.Set("authorization", authHeader)
		ctx = metadata.NewOutgoingContext(ctx, md)

		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

func BasicAuthStreamInterceptor(authHeader string) grpc.StreamClientInterceptor {
	return func(
		ctx context.Context,
		desc *grpc.StreamDesc,
		cc *grpc.ClientConn,
		method string,
		streamer grpc.Streamer,
		opts ...grpc.CallOption,
	) (grpc.ClientStream, error) {
		md, ok := metadata.FromOutgoingContext(ctx)
		if !ok {
			md = metadata.New(nil)
		} else {
			md = md.Copy()
		}
		md.Set("authorization", authHeader)
		ctx = metadata.NewOutgoingContext(ctx, md)

		return streamer(ctx, desc, cc, method, opts...)
	}
}

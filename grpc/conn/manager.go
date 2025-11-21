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
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"golang.org/x/sync/singleflight"
)

type ConnManager struct {
	mu sync.RWMutex

	maxOpenConns int
	timeout      time.Duration

	metrics connMetrics
	logger  logrus.FieldLogger

	conns    map[string]*connEntry
	grpcOpts []grpc.DialOption

	stopCh chan struct{}
	wg     sync.WaitGroup

	closed bool

	sf singleflight.Group
}

type connEntry struct {
	conn     *grpc.ClientConn
	lastUsed atomic.Int64 // unix nanos
}

func NewConnManager(maxOpenConns int, timeout time.Duration, reg prometheus.Registerer, logger logrus.FieldLogger, grpcOpts ...grpc.DialOption) *ConnManager {
	m := &ConnManager{
		maxOpenConns: maxOpenConns,
		timeout:      timeout,
		metrics:      newConnMetrics(reg),
		logger:       logger,
		conns:        make(map[string]*connEntry),
		grpcOpts:     grpcOpts,
		stopCh:       make(chan struct{}),
	}

	if timeout > 0 {
		m.wg.Add(1)
		enterrors.GoWrapper(m.cleanupLoop, logger)
	}

	return m
}

func (m *ConnManager) GetConn(addr string) (*grpc.ClientConn, error) {
	// try to reuse an existing connection without blocking other readers
	if conn, ok, err := m.reuseExistingConn(addr); err != nil || ok {
		return conn, err
	}

	// create the connection using singleflight to avoid duplicate dials
	v, err, _ := m.sf.Do(addr, func() (interface{}, error) {
		conn, err := grpc.NewClient(addr, m.grpcOpts...)
		if err != nil {
			return nil, err
		}

		err = m.storeNewConn(addr, conn)
		if err != nil {
			if cerr := conn.Close(); cerr != nil {
				m.logger.Warnf("failed to close connection after storeNewConn error: %v", cerr)
			}
			return nil, err
		}
		return conn, nil
	})
	if err != nil {
		return nil, err
	}

	return v.(*grpc.ClientConn), nil
}

func (m *ConnManager) reuseExistingConn(addr string) (*grpc.ClientConn, bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.closed {
		return nil, false, fmt.Errorf("connection manager is closed")
	}

	if entry, ok := m.conns[addr]; ok {
		entry.lastUsed.Store(time.Now().UnixNano())
		m.metrics.connReuseTotal.Inc()
		return entry.conn, true, nil
	}

	return nil, false, nil
}

func (m *ConnManager) storeNewConn(addr string, newConn *grpc.ClientConn) error {
	now := time.Now()

	var toClose *grpc.ClientConn

	defer func() {
		if toClose == nil {
			return
		}

		if err := toClose.Close(); err != nil {
			m.logger.Warnf("failed to close connection evicted due to max open conns: %v", err)
		} else {
			m.metrics.connCloseTotal.Inc()
			m.metrics.connOpenGauge.Dec()
			m.metrics.connEvictTotal.WithLabelValues("max_open_conns").Inc()
		}
	}()

	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return fmt.Errorf("connection manager is closed")
	}

	// if a connection for this address already exists, it fails because it's unexpected
	if _, ok := m.conns[addr]; ok {
		return fmt.Errorf("unexpected: connection for address %s already exists", addr)
	}

	if m.maxOpenConns > 0 && len(m.conns) >= m.maxOpenConns {
		// try to evict the oldest expired connection
		toClose = m.evictOldestExpiredLocked(now)
		if toClose == nil {
			m.metrics.connRejectTotal.WithLabelValues("no_expired_available").Inc()
			return fmt.Errorf("connection limit reached and no expired connections available")
		}
	}

	// accept and store the new connection
	entry := &connEntry{conn: newConn}
	entry.lastUsed.Store(now.UnixNano())
	m.conns[addr] = entry

	m.metrics.connCreateTotal.Inc()
	m.metrics.connOpenGauge.Inc()

	return nil
}

// evictOldestExpiredLocked removes and returns the oldest expired connection when called
// with m.mu held. If no expired connections are found, it returns nil.
func (m *ConnManager) evictOldestExpiredLocked(now time.Time) *grpc.ClientConn {
	if m.timeout <= 0 {
		return nil
	}

	var (
		oldestAddr  string
		oldestEntry *connEntry
		oldestTime  time.Time
	)

	for addr, entry := range m.conns {
		lu := time.Unix(0, entry.lastUsed.Load())
		if now.Sub(lu) <= m.timeout {
			continue
		}
		if oldestEntry == nil || lu.Before(oldestTime) {
			oldestAddr = addr
			oldestEntry = entry
			oldestTime = lu
		}
	}

	if oldestEntry == nil {
		return nil
	}

	delete(m.conns, oldestAddr)
	return oldestEntry.conn
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
	defer ticker.Stop()
	defer m.wg.Done()

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
	if m.timeout <= 0 {
		return
	}

	// snapshot stale entries under read lock
	m.mu.RLock()
	if m.closed {
		m.mu.RUnlock()
		return
	}

	type staleEntry struct {
		addr string
		ptr  *connEntry
	}

	var stale []staleEntry

	for addr, entry := range m.conns {
		if time.Since(time.Unix(0, entry.lastUsed.Load())) > m.timeout {
			stale = append(stale, staleEntry{addr: addr, ptr: entry})
		}
	}
	m.mu.RUnlock()

	if len(stale) == 0 {
		return
	}

	// delete the exact entries (defensive re-check) under write lock
	var toClose []*grpc.ClientConn
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return
	}
	for _, s := range stale {
		if cur, ok := m.conns[s.addr]; ok && cur == s.ptr {
			delete(m.conns, s.addr)
			toClose = append(toClose, s.ptr.conn)
		}
	}
	m.mu.Unlock()

	// close outside the lock
	for _, c := range toClose {
		err := c.Close()
		if err == nil {
			m.metrics.connCloseTotal.Inc()
			m.metrics.connOpenGauge.Dec()
			m.metrics.connEvictTotal.WithLabelValues("idle_cleanup").Inc()
		} else {
			m.logger.Warnf("failed to close idle connection: %v", err)
		}
	}
}

func (m *ConnManager) Close() {
	// Flip state and snapshot under lock
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return
	}
	m.closed = true

	toClose := make([]*grpc.ClientConn, 0, len(m.conns))
	for _, entry := range m.conns {
		toClose = append(toClose, entry.conn)
	}
	// clear map so new lookups fail immediately
	m.conns = make(map[string]*connEntry)

	stopCh := m.stopCh
	m.mu.Unlock()

	// stop background work and wait, without holding the mutex
	close(stopCh)
	m.wg.Wait()

	// close connections outside the lock
	for _, c := range toClose {
		if err := c.Close(); err != nil {
			m.logger.Warnf("failed to close connection: %v", err)
		} else {
			m.metrics.connCloseTotal.Inc()
			m.metrics.connOpenGauge.Dec()
		}
	}
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

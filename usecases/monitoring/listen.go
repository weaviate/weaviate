package monitoring

import (
	"net"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

type countingListener struct {
	net.Listener
	count prometheus.Gauge
}

func CountingListener(l net.Listener, g prometheus.Gauge) net.Listener {
	return &countingListener{Listener: l, count: g}
}

func (c *countingListener) Accept() (net.Conn, error) {
	conn, err := c.Listener.Accept()
	if err != nil {
		return nil, err
	}
	c.count.Inc()
	return &countingConn{Conn: conn, count: c.count}, nil
}

type countingConn struct {
	net.Conn
	count prometheus.Gauge
	once  sync.Once
}

func (c *countingConn) Close() error {
	err := c.Conn.Close()

	// Client can call `Close()` any number of times on a single connection. Make sure to decrement the counter only once.
	c.once.Do(func() {
		c.count.Dec()
	})

	return err
}

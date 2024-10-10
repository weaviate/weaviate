//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package monitoring

import (
	"errors"
	"net"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
)

type fakeListener struct {
	net.Listener
	acceptErr error
	closeErr  error
}

type fakeConn struct {
	net.Conn
	closeErr error
}

func (c *fakeConn) Close() error {
	return c.closeErr
}

func (c *fakeListener) Accept() (net.Conn, error) {
	return &fakeConn{closeErr: c.closeErr}, c.acceptErr
}

func TestCountingListener(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	g := promauto.With(reg).NewGauge(prometheus.GaugeOpts{
		Namespace: "test",
		Name:      "gauge",
	})

	fake := &fakeListener{}
	l := CountingListener(fake, g)
	assert.Equal(t, float64(0), testutil.ToFloat64(g))

	// Accepting connections should increment the gauge.
	c1, err := l.Accept()
	assert.NoError(t, err)
	assert.Equal(t, float64(1), testutil.ToFloat64(g))
	c2, err := l.Accept()
	assert.NoError(t, err)
	assert.Equal(t, float64(2), testutil.ToFloat64(g))

	// Closing connections should decrement the gauge.
	assert.NoError(t, c1.Close())
	assert.Equal(t, float64(1), testutil.ToFloat64(g))
	assert.NoError(t, c2.Close())
	assert.Equal(t, float64(0), testutil.ToFloat64(g))

	// Duplicate calls to Close should not decrement.
	assert.NoError(t, c1.Close())
	assert.Equal(t, float64(0), testutil.ToFloat64(g))

	// Accept errors should not cause an increment.
	fake.acceptErr = errors.New("accept")
	_, err = l.Accept()
	assert.Error(t, err)
	assert.Equal(t, float64(0), testutil.ToFloat64(g))

	// Close errors should still decrement.
	fake.acceptErr = nil
	fake.closeErr = errors.New("close")
	c3, err := l.Accept()
	assert.NoError(t, err)
	assert.Equal(t, float64(1), testutil.ToFloat64(g))
	assert.Error(t, c3.Close())
	assert.Equal(t, float64(0), testutil.ToFloat64(g))
}

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
	"io"
	"net/http"
	"strconv"

	"github.com/felixge/httpsnoop"
	"github.com/prometheus/client_golang/prometheus"
)

type InstrumentHandler struct {
	inflightRequests *prometheus.GaugeVec
	duration         *prometheus.HistogramVec

	// in bytes
	requestSize  *prometheus.HistogramVec
	responseSize *prometheus.HistogramVec

	next http.Handler
}

func InstrumentHTTP(
	next http.Handler,
	inflight *prometheus.GaugeVec,
	duration *prometheus.HistogramVec,
	requestSize *prometheus.HistogramVec,
	responseSize *prometheus.HistogramVec,
) *InstrumentHandler {
	return &InstrumentHandler{
		inflightRequests: inflight,
		next:             next,
		duration:         duration,
		requestSize:      requestSize,
		responseSize:     responseSize,
	}
}

func (i *InstrumentHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// TODO(kavi): make it into right static route
	// e.g: removing `/<class>/` from the path in `v1/schema`. Because that will make the label unbounded.
	route := r.URL.String()

	method := r.Method

	inflight := i.inflightRequests.WithLabelValues(method, route)
	inflight.Inc()
	defer inflight.Dec()

	origBody := r.Body
	defer func() {
		// We don't need `countingReadCloser` before this instrument handler
		r.Body = origBody
	}()

	cr := &countingReadCloser{
		r: r.Body,
	}
	r.Body = cr

	// This is where we run actual upstream http.Handler
	respWithMetrics := httpsnoop.CaptureMetricsFn(w, func(rw http.ResponseWriter) {
		i.next.ServeHTTP(rw, r)
	})

	i.requestSize.WithLabelValues(method, route).Observe(float64(cr.read))
	i.responseSize.WithLabelValues(method, route).Observe(float64(respWithMetrics.Written))

	labelValues := []string{
		method,
		route,
		strconv.Itoa(respWithMetrics.Code),
	}

	i.duration.WithLabelValues(labelValues...).Observe(respWithMetrics.Duration.Seconds())
}

type countingReadCloser struct {
	r    io.ReadCloser
	read int64
}

func (c *countingReadCloser) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	if n > 0 {
		c.read += int64(n)
	}
	return n, err
}

func (c *countingReadCloser) Close() error {
	return c.r.Close()
}

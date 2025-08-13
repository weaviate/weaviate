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
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type connMetrics struct {
	connCreateTotal prometheus.Counter
	connReuseTotal  prometheus.Counter
	connCloseTotal  prometheus.Counter
	connOpenGauge   prometheus.Gauge
}

func newConnMetrics(reg prometheus.Registerer) connMetrics {
	r := promauto.With(reg)

	return connMetrics{
		connCreateTotal: r.NewCounter(prometheus.CounterOpts{
			Namespace: "weaviate",
			Name:      "weaviate_grpc_conn_create_total",
			Help:      "Total gRPC connections created",
		}),
		connReuseTotal: r.NewCounter(prometheus.CounterOpts{
			Namespace: "weaviate",
			Name:      "weaviate_grpc_conn_reuse_total",
			Help:      "Total reused connections",
		}),
		connCloseTotal: r.NewCounter(prometheus.CounterOpts{
			Namespace: "weaviate",
			Name:      "weaviate_grpc_conn_close_total",
			Help:      "Total connections closed",
		}),
		connOpenGauge: r.NewGauge(prometheus.GaugeOpts{
			Namespace: "weaviate",
			Name:      "weaviate_grpc_conn_open",
			Help:      "Open connections",
		}),
	}
}

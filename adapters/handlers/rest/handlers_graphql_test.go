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

package rest

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/usecases/monitoring"
)

func TestGraphqlNamespacesBlockedCounter(t *testing.T) {
	t.Run("nil metrics yields no counter and a no-op log", func(t *testing.T) {
		m := newGraphqlRequestsTotal(nil, logrus.New())
		require.Nil(t, m.namespacesBlocked)
		require.NotPanics(t, m.logNamespacesBlocked)
	})

	t.Run("increments once per blocked request", func(t *testing.T) {
		reg := prometheus.NewPedanticRegistry()
		m := newGraphqlRequestsTotal(&monitoring.PrometheusMetrics{Registerer: reg}, logrus.New())
		require.NotNil(t, m.namespacesBlocked)

		require.Equal(t, float64(0), testutil.ToFloat64(m.namespacesBlocked))
		m.logNamespacesBlocked()
		m.logNamespacesBlocked()
		require.Equal(t, float64(2), testutil.ToFloat64(m.namespacesBlocked))
	})

	t.Run("re-registration reuses the existing counter", func(t *testing.T) {
		reg := prometheus.NewPedanticRegistry()
		metrics := &monitoring.PrometheusMetrics{Registerer: reg}
		first := newGraphqlRequestsTotal(metrics, logrus.New())
		second := newGraphqlRequestsTotal(metrics, logrus.New())
		require.NotNil(t, first.namespacesBlocked)
		require.NotNil(t, second.namespacesBlocked)

		first.logNamespacesBlocked()
		second.logNamespacesBlocked()
		require.Equal(t, float64(2), testutil.ToFloat64(second.namespacesBlocked))
	})
}

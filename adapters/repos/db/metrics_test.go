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

package db

import (
	"math"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/usecases/monitoring"
)

// The READY transition is observed with the whole shard load, which on a
// large shard takes minutes. With the client's default buckets (max 10s)
// every such load lands in +Inf and the histogram cannot answer how long
// loads take.
func TestShardStatusUpdateDurationBucketsCoverShardLoads(t *testing.T) {
	logger, _ := test.NewNullLogger()
	prom := *monitoring.GetMetrics()
	reg := prometheus.NewPedanticRegistry()
	prom.Registerer = reg
	m, err := NewMetrics(logger, &prom, "SomeClass", "n/a")
	require.NoError(t, err)

	m.ObserveUpdateShardStatus("READY", 2*time.Minute)

	families, err := reg.Gather()
	require.NoError(t, err)
	var maxFiniteBound float64
	for _, family := range families {
		if family.GetName() != "weaviate_index_shard_status_update_duration_seconds" {
			continue
		}
		for _, metric := range family.GetMetric() {
			for _, bucket := range metric.GetHistogram().GetBucket() {
				if bound := bucket.GetUpperBound(); !math.IsInf(bound, 1) && bound > maxFiniteBound {
					maxFiniteBound = bound
				}
			}
		}
	}

	require.GreaterOrEqual(t, maxFiniteBound, float64(120),
		"a two-minute shard load must land in a finite bucket, not in +Inf")
}

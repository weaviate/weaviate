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

package selfrecovery

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func TestNewRegistersMetricsWithConfigRegisterer(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	o := New(Config{Registerer: reg})
	o.metrics.GiveupTotal.Inc()

	count, err := testutil.GatherAndCount(reg, "weaviate_self_recovery_giveup_total")
	require.NoError(t, err)
	require.Equal(t, 1, count)
}

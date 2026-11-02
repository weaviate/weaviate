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

package monitoring

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// The two counters answer different operator questions: one says a record
// needs a decision no later load can make, the other that a build could not
// read a record at all. Swapped, both still move, and each shard load moves
// them again, so nothing downstream would show the mix-up.
func TestMigrationRecordCountersEachCountTheirOwnRecords(t *testing.T) {
	m := &PrometheusMetrics{
		MigrationRecordsWedged: prometheus.NewCounter(
			prometheus.CounterOpts{Name: "migration_records_wedged_total"}),
		MigrationRecordsNotUnderstood: prometheus.NewCounter(
			prometheus.CounterOpts{Name: "migration_records_not_understood_total"}),
	}

	m.AddMigrationRecordsWedged(2, 3)
	require.Equal(t, float64(2), testutil.ToFloat64(m.MigrationRecordsWedged),
		"the records this load left standing are the first argument")
	require.Equal(t, float64(3), testutil.ToFloat64(m.MigrationRecordsNotUnderstood),
		"and the records it could not read are the second")

	m.AddMigrationRecordsWedged(1, 0)
	require.Equal(t, float64(3), testutil.ToFloat64(m.MigrationRecordsWedged),
		"every load adds what it found, so the total sums over loads")
	require.Equal(t, float64(3), testutil.ToFloat64(m.MigrationRecordsNotUnderstood),
		"and a load with nothing unreadable adds nothing to the other")

	require.NotPanics(t, func() { (*PrometheusMetrics)(nil).AddMigrationRecordsWedged(1, 1) },
		"a shard still loads on a build with metrics switched off")
}

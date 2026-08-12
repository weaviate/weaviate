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

package lsmkv

import (
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

func newReadOpVecs() (count, failure *prometheus.CounterVec, ongoing *prometheus.GaugeVec, duration *prometheus.HistogramVec) {
	labels := []string{"operation", "component"}
	count = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "read_count"}, labels)
	failure = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "read_failure"}, labels)
	ongoing = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "read_ongoing"}, labels)
	duration = prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "read_duration"}, labels)
	return
}

// TestBuildReadOpHandles verifies the pre-resolved handles cover every
// operation/component pair the read path emits, and that each handle method
// targets the matching (operation, component) series.
func TestBuildReadOpHandles(t *testing.T) {
	count, failure, ongoing, duration := newReadOpVecs()
	handles := buildReadOpHandles(count, ongoing, failure, duration)

	wantOps := []string{readOpGet, readOpGetBySecondary}
	wantComponents := []string{memtableNames[0], memtableNames[1], componentSegmentGroup}
	require.Len(t, handles, len(wantOps)*len(wantComponents))
	for _, op := range wantOps {
		for _, component := range wantComponents {
			require.Contains(t, handles, readOpKey{op, component})
		}
	}

	h := handles[readOpKey{readOpGet, componentSegmentGroup}]
	h.incCount()
	h.incOngoing()
	h.incOngoing()
	h.decOngoing()
	h.incFailure()
	h.observeDuration(0)

	require.Equal(t, 1.0, testutil.ToFloat64(count.WithLabelValues(readOpGet, componentSegmentGroup)))
	require.Equal(t, 1.0, testutil.ToFloat64(ongoing.WithLabelValues(readOpGet, componentSegmentGroup)))
	require.Equal(t, 1.0, testutil.ToFloat64(failure.WithLabelValues(readOpGet, componentSegmentGroup)))

	// a different pair must stay untouched
	require.Equal(t, 0.0, testutil.ToFloat64(count.WithLabelValues(readOpGetBySecondary, memtableNames[0])))
}

func TestMetricsReadOp(t *testing.T) {
	count, failure, ongoing, duration := newReadOpVecs()
	handles := buildReadOpHandles(count, ongoing, failure, duration)
	m := &Metrics{
		bucketReadOpCountByComponent:        count,
		bucketReadOpOngoingByComponent:      ongoing,
		bucketReadOpFailureCountByComponent: failure,
		bucketReadOpDurationByComponent:     duration,
		readOpHandles:                       handles,
	}

	t.Run("known pair returns the pre-resolved handle", func(t *testing.T) {
		got := m.readOp(readOpGet, memtableNames[0])
		require.Same(t, handles[readOpKey{readOpGet, memtableNames[0]}], got)
	})

	t.Run("unknown pair resolves on demand", func(t *testing.T) {
		got := m.readOp("compaction", "unknown")
		require.NotNil(t, got)
		got.incCount()
		require.Equal(t, 1.0, testutil.ToFloat64(count.WithLabelValues("compaction", "unknown")))
	})

	t.Run("nil metrics yields a nil handle whose methods are no-ops", func(t *testing.T) {
		var nilMetrics *Metrics
		h := nilMetrics.readOp(readOpGet, componentSegmentGroup)
		require.Nil(t, h)
		require.NotPanics(t, func() {
			h.incCount()
			h.incOngoing()
			h.decOngoing()
			h.incFailure()
			h.observeDuration(0)
		})
	})
}

// TestBucketGetReadOpMetrics drives a real Bucket.Get against an on-disk segment
// and asserts the read increments the (get, segment_group) series. This guards
// the operation/component constants at the getFromSegmentGroup call site, which
// the isolated handle tests above cannot see.
func TestBucketGetReadOpMetrics(t *testing.T) {
	ctx := context.Background()
	logger, _ := test.NewNullLogger()

	metrics, err := NewMetrics(monitoring.GetMetrics(), "TestClass", "TestShard")
	require.NoError(t, err)

	b, err := NewBucketCreator().NewBucket(ctx, t.TempDir(), "", logger, metrics,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyReplace))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, b.Shutdown(context.Background())) })

	key, value := []byte("key"), []byte("value")
	require.NoError(t, b.Put(key, value))
	// move the object to a disk segment so Get reaches the segment-group path
	require.NoError(t, b.FlushAndSwitch())

	counter := metrics.bucketReadOpCountByComponent.WithLabelValues(readOpGet, componentSegmentGroup)
	before := testutil.ToFloat64(counter)

	got, err := b.Get(key)
	require.NoError(t, err)
	require.Equal(t, value, got)

	require.Equal(t, before+1, testutil.ToFloat64(counter))
}

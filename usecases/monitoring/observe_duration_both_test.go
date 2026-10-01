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
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newSummaryVec(name string, labels ...string) *prometheus.SummaryVec {
	return prometheus.NewSummaryVec(prometheus.SummaryOpts{Name: name, Help: name}, labels)
}

func summaryFor(t *testing.T, vec *prometheus.SummaryVec, labels ...string) *dto.Summary {
	t.Helper()
	m, err := vec.GetMetricWithLabelValues(labels...)
	require.NoError(t, err)
	var out dto.Metric
	require.NoError(t, m.(prometheus.Metric).Write(&out))
	return out.GetSummary()
}

// TestObserveDurationBoth pins the shared dual-emission helper that all four
// _ms -> _seconds migrations route through.
//
// The _ms summaries were fed by prometheus.NewTimer, which observes SECONDS
// despite the name. Both vectors must therefore receive the same seconds value:
// rescaling the _ms one would shift every historical series by 1000x.
func TestObserveDurationBoth(t *testing.T) {
	deprecated := newSummaryVec("thing_ms", "class_name")
	seconds := newSummaryVec("thing_seconds", "class_name")

	stop := ObserveDurationBoth(deprecated, seconds, "Article")
	time.Sleep(2 * time.Millisecond)
	stop()

	ms := summaryFor(t, deprecated, "Article")
	sec := summaryFor(t, seconds, "Article")

	assert.Equal(t, uint64(1), ms.GetSampleCount(), "deprecated vector must observe exactly once")
	assert.Equal(t, uint64(1), sec.GetSampleCount(), "replacement vector must observe exactly once")
	assert.Equal(t, ms.GetSampleSum(), sec.GetSampleSum(), "both vectors must record the identical value")

	// Seconds, not milliseconds: a 2ms sleep is well under 1.
	assert.Less(t, sec.GetSampleSum(), 1.0, "value must be in seconds")
	assert.Greater(t, sec.GetSampleSum(), 0.0)
}

// TestObserveDurationBothLabelMismatch pins that a cardinality error on one
// vector does not suppress the other, and never panics. The helper drops label
// errors on purpose: a metric must not fail the operation it measures.
func TestObserveDurationBothLabelMismatch(t *testing.T) {
	deprecated := newSummaryVec("mismatch_ms", "class_name")
	seconds := newSummaryVec("mismatch_seconds", "class_name", "backend_name")

	// Two labels: valid for `seconds`, wrong arity for `deprecated`.
	require.NotPanics(t, func() {
		ObserveDurationBoth(deprecated, seconds, "Article", "s3")()
	})

	assert.Equal(t, uint64(1), summaryFor(t, seconds, "Article", "s3").GetSampleCount(),
		"the vector with matching labels must still be observed")
}

// TestObserveDurationBothIndependentTimers pins that concurrent callers get
// independent start times rather than sharing one.
func TestObserveDurationBothIndependentTimers(t *testing.T) {
	deprecated := newSummaryVec("indep_ms", "class_name")
	seconds := newSummaryVec("indep_seconds", "class_name")

	first := ObserveDurationBoth(deprecated, seconds, "A")
	time.Sleep(5 * time.Millisecond)
	second := ObserveDurationBoth(deprecated, seconds, "B")
	second()
	first()

	a := summaryFor(t, seconds, "A").GetSampleSum()
	b := summaryFor(t, seconds, "B").GetSampleSum()
	assert.Greater(t, a, b, "the earlier-started timer must record the longer duration")
}

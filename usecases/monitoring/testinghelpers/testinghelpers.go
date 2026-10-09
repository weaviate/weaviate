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

// Package testinghelpers reads individual series out of a prometheus.Gatherer
// for tests. testutil.ToFloat64 cannot read summaries or histograms, and
// metrics on the default registry are only reachable through a gather.
//
// On the default registry compare deltas rather than absolute values: other
// tests in the same binary observe into the same series.
package testinghelpers

import (
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// SampleCount returns the _count of the summary or histogram series with the
// given name and exact label set.
func SampleCount(g prometheus.Gatherer, name string, labels prometheus.Labels) (uint64, error) {
	metric, err := gatherMetric(g, name, labels)
	if err != nil {
		return 0, err
	}
	switch {
	case metric.GetSummary() != nil:
		return metric.GetSummary().GetSampleCount(), nil
	case metric.GetHistogram() != nil:
		return metric.GetHistogram().GetSampleCount(), nil
	}
	return 0, fmt.Errorf("metric %q is neither a summary nor a histogram", name)
}

// SampleSum is the _sum counterpart of SampleCount.
func SampleSum(g prometheus.Gatherer, name string, labels prometheus.Labels) (float64, error) {
	metric, err := gatherMetric(g, name, labels)
	if err != nil {
		return 0, err
	}
	switch {
	case metric.GetSummary() != nil:
		return metric.GetSummary().GetSampleSum(), nil
	case metric.GetHistogram() != nil:
		return metric.GetHistogram().GetSampleSum(), nil
	}
	return 0, fmt.Errorf("metric %q is neither a summary nor a histogram", name)
}

// GaugeValue returns the value of the gauge series with the given name and
// exact label set (nil for a scalar gauge).
func GaugeValue(g prometheus.Gatherer, name string, labels prometheus.Labels) (float64, error) {
	metric, err := gatherMetric(g, name, labels)
	if err != nil {
		return 0, err
	}
	if metric.GetGauge() == nil {
		return 0, fmt.Errorf("metric %q is not a gauge", name)
	}
	return metric.GetGauge().GetValue(), nil
}

func gatherMetric(g prometheus.Gatherer, name string, labels prometheus.Labels) (*dto.Metric, error) {
	families, err := g.Gather()
	if err != nil {
		return nil, fmt.Errorf("gather metrics: %w", err)
	}

	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, metric := range family.GetMetric() {
			if labelsMatch(metric.GetLabel(), labels) {
				return metric, nil
			}
		}
		return nil, fmt.Errorf("metric %q has no series with labels %v", name, labels)
	}

	return nil, fmt.Errorf("metric %q is not registered", name)
}

func labelsMatch(pairs []*dto.LabelPair, want prometheus.Labels) bool {
	if len(pairs) != len(want) {
		return false
	}
	for _, pair := range pairs {
		if v, ok := want[pair.GetName()]; !ok || v != pair.GetValue() {
			return false
		}
	}
	return true
}

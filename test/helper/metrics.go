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

package helper

import (
	"bytes"
	"context"
	"fmt"
	"io"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

// ScrapeMetrics reads the Prometheus exposition of a Weaviate container,
// keyed by metric family name. The compose does not publish port 2112, so the
// scrape runs inside the container; the image ships wget, curl is the
// fallback. The node must run with PROMETHEUS_MONITORING_ENABLED=true.
func ScrapeMetrics(ctx context.Context, container testcontainers.Container) (map[string]*dto.MetricFamily, error) {
	code, reader, err := container.Exec(ctx, []string{
		"sh", "-c", "wget -qO- http://127.0.0.1:2112/metrics || curl -s http://127.0.0.1:2112/metrics",
	}, tcexec.Multiplexed())
	if err != nil {
		return nil, fmt.Errorf("exec metrics scrape: %w", err)
	}
	out, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("read metrics scrape: %w", err)
	}
	if code != 0 {
		return nil, fmt.Errorf("metrics scrape exited with %d: %s", code, out)
	}

	var parser expfmt.TextParser
	families, err := parser.TextToMetricFamilies(bytes.NewReader(out))
	if err != nil {
		return nil, fmt.Errorf("parse metrics: %w", err)
	}
	return families, nil
}

// FindMetric returns the series of the named family whose labels are exactly
// labels (nil for a series without labels), or false when the family or the
// series is absent.
func FindMetric(families map[string]*dto.MetricFamily, name string, labels map[string]string) (*dto.Metric, bool) {
	family, ok := families[name]
	if !ok {
		return nil, false
	}
	for _, metric := range family.GetMetric() {
		if len(metric.GetLabel()) != len(labels) {
			continue
		}
		match := true
		for _, pair := range metric.GetLabel() {
			if v, ok := labels[pair.GetName()]; !ok || v != pair.GetValue() {
				match = false
				break
			}
		}
		if match {
			return metric, true
		}
	}
	return nil, false
}

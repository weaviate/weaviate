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

package asyncrep_battle

import (
	"context"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	tcexec "github.com/testcontainers/testcontainers-go/exec"

	"github.com/weaviate/weaviate/test/docker"
)

// scrapeMetrics reads the unexposed :2112 endpoint from inside the container.
func scrapeMetrics(ctx context.Context, t *testing.T, compose *docker.DockerCompose, n int) string {
	t.Helper()
	node := compose.GetWeaviateNode(n)
	exitCode, reader, err := node.Container().Exec(ctx, []string{"sh", "-c", "wget -qO- localhost:2112/metrics"}, tcexec.Multiplexed())
	require.NoError(t, err, "scrape metrics on node %d", n)
	require.Equal(t, 0, exitCode, "metrics scrape exec failed on node %d", n)
	raw, err := io.ReadAll(reader)
	require.NoError(t, err)
	return string(raw)
}

// sumMetric sums all label variants of a metric; the exec stream may carry
// docker multiplex headers, so lines are extracted by tolerant regex.
func sumMetric(text, name string) float64 {
	re := regexp.MustCompile(fmt.Sprintf(`(?m)^%s(?:\{[^}]*\})? ([0-9eE.+-]+)$`, regexp.QuoteMeta(name)))
	sum := 0.0
	for _, m := range re.FindAllStringSubmatch(text, -1) {
		if v, err := strconv.ParseFloat(m[1], 64); err == nil {
			sum += v
		}
	}
	return sum
}

// metricWindow maps a node index to a captured metric value.
type metricWindow map[int]float64

func captureMetric(ctx context.Context, t *testing.T, compose *docker.DockerCompose, nodes []int, name string) metricWindow {
	t.Helper()
	w := metricWindow{}
	for _, n := range nodes {
		w[n] = sumMetric(scrapeMetrics(ctx, t, compose, n), name)
	}
	return w
}

// metricDelta returns current-minus-before per node for the same metric.
func metricDelta(ctx context.Context, t *testing.T, compose *docker.DockerCompose, before metricWindow, name string) map[int]float64 {
	t.Helper()
	out := map[int]float64{}
	for n, prev := range before {
		out[n] = sumMetric(scrapeMetrics(ctx, t, compose, n), name) - prev
	}
	return out
}

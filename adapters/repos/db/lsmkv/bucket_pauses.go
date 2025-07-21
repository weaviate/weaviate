//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package lsmkv

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

func (b *Bucket) doStartPauseTimer() {
	label := b.GetDir()
	if monitoring.GetMetrics().Group {
		label = "n/a"
	}
	if metric, err := monitoring.GetMetrics().BucketPauseDurations.GetMetricWithLabelValues(label); err == nil {
		b.pauseTimer = prometheus.NewTimer(metric)
	}
}

func (b *Bucket) doStopPauseTimer() {
	if b.pauseTimer != nil {
		b.pauseTimer.ObserveDuration()
	}
}

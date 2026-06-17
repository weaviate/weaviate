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
	"github.com/prometheus/client_golang/prometheus"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

func (b *Bucket) doStartPauseTimer() {
	label := b.GetDir()
	if monitoring.GetMetrics().Group {
		label = "n/a"
	}
	b.pauseTimerMu.Lock()
	defer b.pauseTimerMu.Unlock()
	b.pauseTimerCount++
	if b.pauseTimerCount > 1 {
		return
	}
	metric, err := monitoring.GetMetrics().BucketPauseDurations.GetMetricWithLabelValues(label)
	if err != nil {
		return
	}
	b.pauseTimer = prometheus.NewTimer(metric)
}

func (b *Bucket) doStopPauseTimer() {
	b.pauseTimerMu.Lock()
	defer b.pauseTimerMu.Unlock()
	if b.pauseTimerCount == 0 {
		return
	}
	b.pauseTimerCount--
	if b.pauseTimerCount > 0 {
		return
	}
	if b.pauseTimer != nil {
		b.pauseTimer.ObserveDuration()
		b.pauseTimer = nil
	}
}

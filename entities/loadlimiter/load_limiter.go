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

package loadlimiter

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"golang.org/x/sync/semaphore"
)

const (
	defaultLoadingLimit = 100
)

// LoadLimiter is a limiter to control loading concurrency.
// It throttles the loading instead of rejecting.
// The motivation of this limiter is the fact that loading requires multiple syscalls
// which can create a new OS threads and might result in hitting the internal thread count limit of Go runtime.
type LoadLimiter struct {
	sema *semaphore.Weighted

	shardsLoading          prometheus.Gauge
	waitingForPermitToLoad prometheus.Gauge
}

func NewLoadLimiter(reg prometheus.Registerer, name string, limit int) *LoadLimiter {
	r := promauto.With(reg)
	if limit == 0 {
		limit = defaultLoadingLimit
	}

	return &LoadLimiter{
		sema: semaphore.NewWeighted(int64(limit)),

		shardsLoading: r.NewGauge(prometheus.GaugeOpts{
			Name: name + "_loading",
		}),
		waitingForPermitToLoad: r.NewGauge(prometheus.GaugeOpts{
			Name: name + "_waiting_for_permit_to_load",
		}),
	}
}

func (l *LoadLimiter) Acquire(ctx context.Context) error {
	l.waitingForPermitToLoad.Inc()
	defer l.waitingForPermitToLoad.Dec()

	err := l.sema.Acquire(ctx, 1)
	if err != nil {
		return err
	}
	l.shardsLoading.Inc()
	return nil
}

func (l *LoadLimiter) Release() {
	l.sema.Release(1)
	l.shardsLoading.Dec()
}

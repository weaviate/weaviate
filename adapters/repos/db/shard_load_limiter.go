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

package db

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"golang.org/x/sync/semaphore"
)

const (
	defaultShardLoadingLimit = 500
)

// ShardLoadLimiter is a limiter to control how many shards are loaded concurrently.
// If too many shards are loaded in parallel, it throttles the loading instead of rejecting.
// The motivation of this limiter is the fact that loading a shard requires multiple syscalls
// which create a new OS thread is there is no unused ones. In case we try to load thousands of shards in parallel,
// we might hit internal thread count limit of Go runtime.
type ShardLoadLimiter struct {
	sema *semaphore.Weighted

	shardsLoading          prometheus.Gauge
	waitingForPermitToLoad prometheus.Gauge
}

func NewShardLoadLimiter(reg prometheus.Registerer, limit int) ShardLoadLimiter {
	r := promauto.With(reg)
	if limit == 0 {
		limit = defaultShardLoadingLimit
	}

	return ShardLoadLimiter{
		sema: semaphore.NewWeighted(int64(limit)),

		shardsLoading: r.NewGauge(prometheus.GaugeOpts{
			Name: "database_shards_loading",
		}),
		waitingForPermitToLoad: r.NewGauge(prometheus.GaugeOpts{
			Name: "database_shards_waiting_for_permit_to_load",
		}),
	}
}

func (l *ShardLoadLimiter) Acquire(ctx context.Context) error {
	l.waitingForPermitToLoad.Inc()
	defer l.waitingForPermitToLoad.Dec()

	err := l.sema.Acquire(ctx, 1)
	if err != nil {
		return err
	}
	l.shardsLoading.Inc()
	return nil
}

func (l *ShardLoadLimiter) Release() {
	l.sema.Release(1)
	l.shardsLoading.Dec()
}

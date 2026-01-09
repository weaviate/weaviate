package loadlimiter

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"golang.org/x/sync/semaphore"
)

const (
	defaultShardLoadingLimit = 500
)

// LoadLimiter is a limiter to control how many shards are loaded concurrently.
// If too many shards are loaded in parallel, it throttles the loading instead of rejecting.
// The motivation of this limiter is the fact that loading a shard requires multiple syscalls
// which create a new OS thread is there is no unused ones. In case we try to load thousands of shards in parallel,
// we might hit internal thread count limit of Go runtime.
type LoadLimiter struct {
	sema *semaphore.Weighted

	shardsLoading          prometheus.Gauge
	waitingForPermitToLoad prometheus.Gauge
}

func NewLoadLimiter(reg prometheus.Registerer, name string, limit int) *LoadLimiter {
	r := promauto.With(reg)
	if limit == 0 {
		limit = defaultShardLoadingLimit
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

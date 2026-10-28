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

package lazyshardunloading

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	apiclient "github.com/weaviate/weaviate/client"
	"github.com/weaviate/weaviate/entities/models"
)

func TestUnlicensedKeepsEveryShardLoaded(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	compose := start(ctx, t, newCompose(t, false, 1, fastKnobs(knobs{"max_loaded_shards": "5"})))
	node := compose.GetWeaviate()
	cl := newClient(node.URI())

	const class = "UnlicensedParity"
	tenants := tenantNames("t", 30)
	l := newLedger()
	createClass(ctx, t, cl, vectorClass(class, true, 1))
	createTenants(ctx, t, cl, class, tenants)
	seed(ctx, t, cl, l, class, tenants, 3, nil)

	loaded := func() int {
		byNode, err := nodeLoadedShards(ctx, cl, class)
		require.NoError(t, err)
		require.Len(t, byNode, 1)
		for _, shards := range byNode {
			return countTrue(shards)
		}
		return 0
	}
	require.Equal(t, len(tenants), loaded())
	require.Never(t, func() bool { return loaded() < len(tenants) }, 6*time.Second, 500*time.Millisecond,
		"an unlicensed node never unloads a shard")

	_, published, err := metric(ctx, node, metricLimit)
	require.NoError(t, err)
	require.False(t, published, "an unlicensed node builds no unloader")
	require.NoError(t, randomWorkload(ctx, cl, l, class, tenants, 300, 4))
	verifyAll(ctx, t, cl, l, class, tenants)
}

func TestManyTenantsStayWithinTheLimit(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	const limit = 20
	tenantCount := envInt(t, "LAZY_UNLOAD_TEST_TENANTS", 300)
	ops := envInt(t, "LAZY_UNLOAD_TEST_OPS", 2000)
	compose := start(ctx, t, newCompose(t, true, 1, fastKnobs(knobs{"max_loaded_shards": strconv.Itoa(limit)})))
	node := compose.GetWeaviate()
	cl := newClient(node.URI())
	waitLimit(ctx, t, compose, 1, limit)

	const class = "ManyTenants"
	tenants := tenantNames("t", tenantCount)
	l := newLedger()
	createClass(ctx, t, cl, vectorClass(class, true, 1))
	createTenants(ctx, t, cl, class, tenants)
	seed(ctx, t, cl, l, class, tenants, 3, nil)
	require.NoError(t, randomWorkload(ctx, cl, l, class, tenants, ops, 8), "every request succeeds while shards unload")

	atLimit := func(n int) bool { return n <= limit }
	eventuallyLoaded(ctx, t, compose, 1, 2*time.Minute, atLimit, "loaded shards drain to the limit")
	statuses, err := tenantStatuses(ctx, cl, class)
	require.NoError(t, err)
	require.Len(t, statuses, tenantCount)
	for name, status := range statuses {
		require.Equal(t, models.TenantActivityStatusHOT, status, "tenant %s", name)
	}
	verifyAll(ctx, t, cl, l, class, tenants)
	eventuallyLoaded(ctx, t, compose, 1, 2*time.Minute, atLimit, "loaded shards drain to the limit after a full read")
}

func TestBurstOfNewTenantsInsideMinResidency(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	const limit, warm, burst = 10, 10, 60
	compose := start(ctx, t, newCompose(t, true, 1, fastKnobs(knobs{
		"max_loaded_shards": strconv.Itoa(limit),
		"min_residency":     "1h",
	})))
	node := compose.GetWeaviate()
	cl := newClient(node.URI())
	waitLimit(ctx, t, compose, 1, limit)

	const class = "Burst"
	tenants := tenantNames("t", warm+burst)
	l := newLedger()
	createClass(ctx, t, cl, vectorClass(class, true, 1))
	createTenants(ctx, t, cl, class, tenants)
	seed(ctx, t, cl, l, class, tenants[:warm], 2, nil)
	eventuallyLoaded(ctx, t, compose, 1, time.Minute, func(n int) bool { return n == warm }, "the warm tenants are loaded")

	g, gctx := errgroup.WithContext(ctx)
	for i, tenant := range tenants[warm:] {
		g.Go(func() error {
			r := rand.New(rand.NewSource(int64(i)))
			if err := writeObjects(gctx, cl, l, class, tenant, 2, nil, r); err != nil {
				return err
			}
			id, _ := l.random(class, tenant, r)
			return readObject(gctx, cl, class, tenant, id, nil)
		})
	}
	require.NoError(t, g.Wait(), "a burst inside min_residency never fails a request")
	verifyAll(ctx, t, cl, l, class, tenants)
}

func TestSingleTenantCollectionsStayWithinTheLimit(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	const limit, collections = 8, 40
	compose := start(ctx, t, newCompose(t, true, 1, fastKnobs(knobs{"max_loaded_shards": strconv.Itoa(limit)})))
	node := compose.GetWeaviate()
	cl := newClient(node.URI())
	waitLimit(ctx, t, compose, 1, limit)

	l := newLedger()
	classes := make([]string, collections)
	for i := range classes {
		classes[i] = fmt.Sprintf("Single%02d", i)
		createClass(ctx, t, cl, vectorClass(classes[i], false, 1))
	}
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(8)
	for i, class := range classes {
		g.Go(func() error {
			r := rand.New(rand.NewSource(int64(i)))
			for range 3 {
				if err := writeObjects(gctx, cl, l, class, "", 2, nil, r); err != nil {
					return err
				}
				id, _ := l.random(class, "", r)
				if err := readObject(gctx, cl, class, "", id, nil); err != nil {
					return err
				}
			}
			return nil
		})
	}
	require.NoError(t, g.Wait())

	atLimit := func(n int) bool { return n <= limit }
	eventuallyLoaded(ctx, t, compose, 1, 2*time.Minute, atLimit, "loaded shards drain to the limit")
	for _, class := range classes {
		verifyAll(ctx, t, cl, l, class, []string{""})
	}
	eventuallyLoaded(ctx, t, compose, 1, 2*time.Minute, atLimit, "loaded shards drain to the limit after a full read")
}

func TestRestartLoadsAtMostTheLimit(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	const limit = 5
	compose := start(ctx, t, newCompose(t, true, 1, fastKnobs(knobs{"max_loaded_shards": strconv.Itoa(limit)})))
	cl := newClient(compose.GetWeaviate().URI())
	waitLimit(ctx, t, compose, 1, limit)

	const class = "Restart"
	tenants := tenantNames("t", 30)
	l := newLedger()
	createClass(ctx, t, cl, vectorClass(class, true, 1))
	createTenants(ctx, t, cl, class, tenants)
	seed(ctx, t, cl, l, class, tenants, 4, nil)
	atLimit := func(n int) bool { return n <= limit }
	eventuallyLoaded(ctx, t, compose, 1, time.Minute, atLimit, "loaded shards drain to the limit")

	require.NoError(t, compose.RestartAt(ctx, 0, nil))
	node := compose.GetWeaviate()
	cl = newClient(node.URI())
	waitLimit(ctx, t, compose, 1, limit)
	require.Never(t, func() bool {
		n, err := loadedShards(ctx, node)
		return err != nil || n > limit
	}, 10*time.Second, 500*time.Millisecond, "the restart loads at most the limit")
	verifyAll(ctx, t, cl, l, class, tenants)
	eventuallyLoaded(ctx, t, compose, 1, time.Minute, atLimit, "on-demand loads drain back to the limit")
}

func TestLiveKnobChanges(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	compose := start(ctx, t, newCompose(t, true, 1, fastKnobs(nil)))
	node := compose.GetWeaviate()
	cl := newClient(node.URI())
	waitLimit(ctx, t, compose, 1, 0)

	const class = "LiveKnobs"
	tenants := tenantNames("t", 30)
	all := len(tenants)
	l := newLedger()
	createClass(ctx, t, cl, vectorClass(class, true, 1))
	createTenants(ctx, t, cl, class, tenants)
	seed(ctx, t, cl, l, class, tenants, 2, nil)
	loadedIs := func(want int) func(int) bool { return func(n int) bool { return n == want } }
	atMost := func(limit int) func(int) bool { return func(n int) bool { return n <= limit } }
	touchAll := func(t *testing.T) { verifyAll(ctx, t, cl, l, class, tenants) }
	eventuallyLoaded(ctx, t, compose, 1, time.Minute, loadedIs(all), "with no limit every shard stays loaded")

	t.Run("tighten", func(t *testing.T) {
		setKnobs(ctx, t, compose, 1, fastKnobs(knobs{"max_loaded_shards": "10"}))
		waitLimit(ctx, t, compose, 1, 10)
		eventuallyLoaded(ctx, t, compose, 1, time.Minute, atMost(10), "a lowered limit drains")
	})

	t.Run("loosen", func(t *testing.T) {
		setKnobs(ctx, t, compose, 1, fastKnobs(knobs{"max_loaded_shards": "40"}))
		waitLimit(ctx, t, compose, 1, 40)
		touchAll(t)
		eventuallyLoaded(ctx, t, compose, 1, time.Minute, loadedIs(all), "a raised limit loads everything requested")
		require.Never(t, func() bool {
			n, err := loadedShards(ctx, node)
			return err != nil || n < all
		}, 5*time.Second, 500*time.Millisecond, "a limit above the loaded count unloads nothing")
	})

	t.Run("paused", func(t *testing.T) {
		setKnobs(ctx, t, compose, 1, fastKnobs(knobs{"max_loaded_shards": "5", "paused": "true"}))
		waitLimit(ctx, t, compose, 1, 5)
		require.Never(t, func() bool {
			n, err := loadedShards(ctx, node)
			return err != nil || n < all
		}, 5*time.Second, 500*time.Millisecond, "a paused unloader unloads nothing")
		touchAll(t)
		setKnobs(ctx, t, compose, 1, fastKnobs(knobs{"max_loaded_shards": "5"}))
		eventuallyLoaded(ctx, t, compose, 1, time.Minute, atMost(5), "unpausing drains to the limit")
	})

	t.Run("idle ttl without a limit", func(t *testing.T) {
		setKnobs(ctx, t, compose, 1, fastKnobs(knobs{"idle_ttl": "3s"}))
		waitLimit(ctx, t, compose, 1, 0)
		touchAll(t)
		hot := tenants[0]
		hotID := l.ids(class, hot)[0]
		stop := make(chan struct{})
		var queries atomic.Int64
		var wg sync.WaitGroup
		wg.Add(1)
		var queryErr error
		go func() {
			defer wg.Done()
			ticker := time.NewTicker(250 * time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case <-stop:
					return
				case <-ticker.C:
					if err := readObject(ctx, cl, class, hot, hotID, nil); err != nil {
						queryErr = err
						return
					}
					queries.Add(1)
				}
			}
		}()
		stopQueries := sync.OnceFunc(func() { close(stop); wg.Wait() })
		t.Cleanup(stopQueries)

		eventuallyLoaded(ctx, t, compose, 1, time.Minute, loadedIs(1), "idle shards unload, the queried one stays")
		hotLoaded, err := shardLoaded(ctx, cl, class, hot)
		require.NoError(t, err)
		require.True(t, hotLoaded, "the continuously queried tenant stays loaded")
		stopQueries()
		require.NoError(t, queryErr)
		require.Positive(t, queries.Load())
		eventuallyLoaded(ctx, t, compose, 1, time.Minute, loadedIs(0), "the last tenant unloads once idle")
		verifyAll(ctx, t, cl, l, class, tenants)
	})
}

func TestKillDuringUnloadingKeepsAcknowledgedWrites(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	const limit = 5
	compose := start(ctx, t, newCompose(t, true, 1, fastKnobs(knobs{"max_loaded_shards": strconv.Itoa(limit)})))
	node := compose.GetWeaviate()
	cl := newClient(node.URI())
	waitLimit(ctx, t, compose, 1, limit)

	const class = "Kill"
	tenants := tenantNames("t", 40)
	l := newLedger()
	createClass(ctx, t, cl, vectorClass(class, true, 1))
	createTenants(ctx, t, cl, class, tenants)
	seed(ctx, t, cl, l, class, tenants, 2, nil)
	baseline, _, err := metric(ctx, node, metricUnloads, `outcome="ok"`)
	require.NoError(t, err)

	wctx, stopWriters := context.WithCancel(ctx)
	defer stopWriters()
	var (
		wg         sync.WaitGroup
		killing    atomic.Bool
		preKillErr atomic.Pointer[error]
	)
	for w := range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := rand.New(rand.NewSource(int64(w)))
			for wctx.Err() == nil {
				if err := writeObjects(wctx, cl, l, class, tenants[r.Intn(len(tenants))], 1, nil, r); err != nil {
					if !killing.Load() {
						preKillErr.CompareAndSwap(nil, &err)
					}
					time.Sleep(50 * time.Millisecond)
				}
			}
		}()
	}
	require.EventuallyWithT(t, func(ct *assert.CollectT) {
		v, ok, err := metric(ctx, node, metricUnloads, `outcome="ok"`)
		require.NoError(ct, err)
		require.True(ct, ok)
		require.GreaterOrEqual(ct, v, baseline+20)
	}, 2*time.Minute, 500*time.Millisecond, "shards are actively unloading under the write load")

	zero := time.Duration(0)
	killing.Store(true)
	require.NoError(t, compose.RestartAt(ctx, 0, &zero))
	stopWriters()
	wg.Wait()
	if err := preKillErr.Load(); err != nil {
		require.NoError(t, *err, "a write failed before the kill")
	}

	cl = newClient(compose.GetWeaviate().URI())
	verifyAtLeast(ctx, t, cl, l, class, tenants)
}

// verifyAtLeast checks every acknowledged write survived; writes cut by a kill may also exist.
func verifyAtLeast(ctx context.Context, t *testing.T, cl *apiclient.Weaviate, l *ledger, class string, tenants []string) {
	t.Helper()
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(8)
	for _, tenant := range tenants {
		g.Go(func() error {
			ids := l.ids(class, tenant)
			n, err := countObjects(gctx, cl, class, tenant)
			if err != nil {
				return err
			}
			if n < len(ids) {
				return fmt.Errorf("%s/%s holds %d objects, %d acknowledged", class, tenant, n, len(ids))
			}
			for _, id := range ids {
				if err := readObject(gctx, cl, class, tenant, id, nil); err != nil {
					return err
				}
			}
			return nil
		})
	}
	require.NoError(t, g.Wait())
}

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

package idleshardcollector

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/weaviate/weaviate/entities/models"
)

func TestUnlicensedKeepsEveryShardLoaded(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	compose := start(ctx, t, newCompose(t, false, 1, fastKnobs(knobs{"idle_ttl": idleTTL})))
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
	require.Never(t, func() bool { return loaded() < len(tenants) }, 40*time.Second, 500*time.Millisecond,
		"an unlicensed node never unloads a shard")

	_, published, err := metric(ctx, node, metricLoaded)
	require.NoError(t, err)
	require.False(t, published, "an unlicensed node builds no unloader")
	require.NoError(t, randomWorkload(ctx, cl, l, class, tenants, 300, 4))
	verifyAll(ctx, t, cl, l, class, tenants)
}

func TestManyTenantsUnloadOnceIdle(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	tenantCount := envInt(t, "LAZY_UNLOAD_TEST_TENANTS", 300)
	ops := envInt(t, "LAZY_UNLOAD_TEST_OPS", 2000)
	compose := start(ctx, t, newCompose(t, true, 1, fastKnobs(knobs{"idle_ttl": idleTTL})))
	node := compose.GetWeaviate()
	cl := newClient(node.URI())
	waitUnloader(ctx, t, compose, 1)

	const class = "ManyTenants"
	tenants := tenantNames("t", tenantCount)
	l := newLedger()
	createClass(ctx, t, cl, vectorClass(class, true, 1))
	createTenants(ctx, t, cl, class, tenants)
	seed(ctx, t, cl, l, class, tenants, 3, nil)
	require.NoError(t, randomWorkload(ctx, cl, l, class, tenants, ops, 8), "every request succeeds while shards unload")

	eventuallyLoaded(ctx, t, compose, 1, 2*time.Minute, noneLoaded, "idle shards unload")
	statuses, err := tenantStatuses(ctx, cl, class)
	require.NoError(t, err)
	require.Len(t, statuses, tenantCount)
	for name, status := range statuses {
		require.Equal(t, models.TenantActivityStatusHOT, status, "tenant %s", name)
	}
	verifyAll(ctx, t, cl, l, class, tenants)
	eventuallyLoaded(ctx, t, compose, 1, 2*time.Minute, noneLoaded, "idle shards unload after a full read")
}

func TestSingleTenantCollectionsUnloadOnceIdle(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	const collections = 40
	compose := start(ctx, t, newCompose(t, true, 1, fastKnobs(knobs{"idle_ttl": idleTTL})))
	node := compose.GetWeaviate()
	cl := newClient(node.URI())
	waitUnloader(ctx, t, compose, 1)

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

	eventuallyLoaded(ctx, t, compose, 1, 2*time.Minute, noneLoaded, "idle shards unload")
	for _, class := range classes {
		verifyAll(ctx, t, cl, l, class, []string{""})
	}
	eventuallyLoaded(ctx, t, compose, 1, 2*time.Minute, noneLoaded, "idle shards unload after a full read")
}

func TestRestartUnloadsIdleShardsAgain(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	compose := start(ctx, t, newCompose(t, true, 1, fastKnobs(knobs{"idle_ttl": idleTTL})))
	cl := newClient(compose.GetWeaviate().URI())
	waitUnloader(ctx, t, compose, 1)

	const class = "Restart"
	tenants := tenantNames("t", 30)
	l := newLedger()
	createClass(ctx, t, cl, vectorClass(class, true, 1))
	createTenants(ctx, t, cl, class, tenants)
	seed(ctx, t, cl, l, class, tenants, 4, nil)
	eventuallyLoaded(ctx, t, compose, 1, 2*time.Minute, noneLoaded, "idle shards unload")

	require.NoError(t, compose.RestartAt(ctx, 0, nil))
	node := compose.GetWeaviate()
	cl = newClient(node.URI())
	waitUnloader(ctx, t, compose, 1)
	eventuallyLoaded(ctx, t, compose, 1, 2*time.Minute, noneLoaded, "the shards the restart loaded unload once idle")
	verifyAll(ctx, t, cl, l, class, tenants)
	eventuallyLoaded(ctx, t, compose, 1, 2*time.Minute, noneLoaded, "on-demand loads unload once idle")
}

func TestLiveKnobChanges(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	compose := start(ctx, t, newCompose(t, true, 1, fastKnobs(nil)))
	node := compose.GetWeaviate()
	cl := newClient(node.URI())
	waitUnloader(ctx, t, compose, 1)

	const class = "LiveKnobs"
	tenants := tenantNames("t", 30)
	all := len(tenants)
	l := newLedger()
	createClass(ctx, t, cl, vectorClass(class, true, 1))
	createTenants(ctx, t, cl, class, tenants)
	seed(ctx, t, cl, l, class, tenants, 2, nil)
	loadedIs := func(want int) func(int) bool { return func(n int) bool { return n == want } }
	touchAll := func(t *testing.T) { verifyAll(ctx, t, cl, l, class, tenants) }
	eventuallyLoaded(ctx, t, compose, 1, 2*time.Minute, loadedIs(all), "with idle_ttl off every shard stays loaded")

	t.Run("idle ttl on", func(t *testing.T) {
		setKnobs(ctx, t, compose, 1, fastKnobs(knobs{"idle_ttl": idleTTL}))
		eventuallyLoaded(ctx, t, compose, 1, 2*time.Minute, noneLoaded, "setting idle_ttl unloads idle shards")
	})

	t.Run("idle ttl off", func(t *testing.T) {
		setKnobs(ctx, t, compose, 1, fastKnobs(nil))
		time.Sleep(3 * time.Second)
		touchAll(t)
		eventuallyLoaded(ctx, t, compose, 1, 2*time.Minute, loadedIs(all), "every requested shard loads")
		require.Never(t, func() bool {
			n, err := loadedShards(ctx, node)
			return err != nil || n < all
		}, 40*time.Second, time.Second, "idle_ttl 0 unloads nothing")
	})

	t.Run("paused", func(t *testing.T) {
		setKnobs(ctx, t, compose, 1, fastKnobs(knobs{"idle_ttl": idleTTL, "paused": "true"}))
		require.Never(t, func() bool {
			n, err := loadedShards(ctx, node)
			return err != nil || n < all
		}, 40*time.Second, time.Second, "a paused unloader unloads nothing")
		touchAll(t)
		setKnobs(ctx, t, compose, 1, fastKnobs(knobs{"idle_ttl": idleTTL}))
		eventuallyLoaded(ctx, t, compose, 1, 2*time.Minute, noneLoaded, "unpausing unloads idle shards")
	})

	t.Run("idle ttl keeps a queried shard", func(t *testing.T) {
		setKnobs(ctx, t, compose, 1, fastKnobs(knobs{"idle_ttl": idleTTL}))
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

		eventuallyLoaded(ctx, t, compose, 1, 2*time.Minute, loadedIs(1), "idle shards unload, the queried one stays")
		hotLoaded, err := shardLoaded(ctx, cl, class, hot)
		require.NoError(t, err)
		require.True(t, hotLoaded, "the continuously queried tenant stays loaded")
		stopQueries()
		require.NoError(t, queryErr)
		require.Positive(t, queries.Load())
		eventuallyLoaded(ctx, t, compose, 1, 2*time.Minute, loadedIs(0), "the last tenant unloads once idle")
		verifyAll(ctx, t, cl, l, class, tenants)
	})
}

func TestKillDuringUnloadingKeepsAcknowledgedWrites(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	compose := start(ctx, t, newCompose(t, true, 1, fastKnobs(knobs{"idle_ttl": idleTTL})))
	node := compose.GetWeaviate()
	cl := newClient(node.URI())
	waitUnloader(ctx, t, compose, 1)

	const class = "Kill"
	tenants := tenantNames("t", 40)
	l := newLedger()
	createClass(ctx, t, cl, vectorClass(class, true, 1))
	createTenants(ctx, t, cl, class, tenants)
	seed(ctx, t, cl, l, class, tenants, 2, nil)
	baseline, _, err := metric(ctx, node, metricUnloads, `result="ok"`)
	require.NoError(t, err)

	wctx, stopWriters := context.WithCancel(ctx)
	defer stopWriters()
	var (
		wg         sync.WaitGroup
		killing    atomic.Bool
		preKillErr atomic.Pointer[error]
	)
	started := time.Now()
	for w := range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := rand.New(rand.NewSource(int64(w)))
			for wctx.Err() == nil {
				base := int(time.Since(started) / (3 * time.Second))
				tenant := tenants[(base+r.Intn(10))%len(tenants)]
				if err := writeObjects(wctx, cl, l, class, tenant, 1, nil, r); err != nil {
					if !killing.Load() {
						preKillErr.CompareAndSwap(nil, &err)
					}
				}
				time.Sleep(200 * time.Millisecond)
			}
		}()
	}
	require.EventuallyWithT(t, func(ct *assert.CollectT) {
		v, ok, err := metric(ctx, node, metricUnloads, `result="ok"`)
		require.NoError(ct, err)
		require.True(ct, ok)
		require.GreaterOrEqual(ct, v, baseline+20)
	}, 5*time.Minute, 500*time.Millisecond, "shards are actively unloading under the write load")

	zero := time.Duration(0)
	killing.Store(true)
	require.NoError(t, compose.RestartAt(ctx, 0, &zero))
	stopWriters()
	wg.Wait()
	if err := preKillErr.Load(); err != nil {
		require.NoError(t, *err, "a write failed before the kill")
	}

	cl = newClient(compose.GetWeaviate().URI())
	verifyAcknowledged(ctx, t, cl, l, class, tenants)
}

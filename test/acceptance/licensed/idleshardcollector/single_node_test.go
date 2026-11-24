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
	compose := start(ctx, t, newCompose(t, false, 1, fastKnobs(knobs{"idle_ttl": "1s"})))
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
	require.Never(t, func() bool { return loaded() < len(tenants) }, 10*time.Second, 500*time.Millisecond,
		"an unlicensed node never unloads a shard")

	_, published, err := metric(ctx, node, metricLoaded, "")
	require.NoError(t, err)
	require.False(t, published, "an unlicensed node builds no unloader")
	require.NoError(t, randomWorkload(ctx, cl, l, class, tenants, 300, 4))
	verifyAll(ctx, t, cl, l, class, tenants)
}

const (
	manyTenants             = 300
	manyTenantsOps          = 2000
	singleTenantCollections = 40
)

func TestManyTenantsUnloadOnceIdle(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	compose := start(ctx, t, newCompose(t, true, 1, fastKnobs(idleKnobs)))
	cl := newClient(compose.GetWeaviate().URI())
	waitUnloader(ctx, t, compose, 1)

	const class = "ManyTenants"
	tenants := tenantNames("t", manyTenants)
	l := newLedger()
	createClass(ctx, t, cl, vectorClass(class, true, 1))
	createTenants(ctx, t, cl, class, tenants)
	singles := make([]string, singleTenantCollections)
	for i := range singles {
		singles[i] = fmt.Sprintf("Single%02d", i)
		createClass(ctx, t, cl, vectorClass(singles[i], false, 1))
	}
	polls := pollNodesVerbose(ctx, t, compose, class)
	seed(ctx, t, cl, l, class, tenants, 3, nil)
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(8)
	for i, single := range singles {
		g.Go(func() error {
			r := rand.New(rand.NewSource(int64(i)))
			for range 3 {
				if err := writeObjects(gctx, cl, l, single, "", 2, nil, r); err != nil {
					return err
				}
				id, _ := l.random(single, "", r)
				if err := readObject(gctx, cl, single, "", id, nil); err != nil {
					return err
				}
			}
			return nil
		})
	}
	require.NoError(t, g.Wait())
	require.NoError(t, randomWorkload(ctx, cl, l, class, tenants, manyTenantsOps, 8), "every request succeeds while shards unload")

	polls.Store(0)
	eventuallyLoaded(ctx, t, compose, 1, 2*time.Minute, noneLoaded,
		"idle shards unload although the verbose nodes API reads them continuously")
	require.GreaterOrEqual(t, polls.Load(), minPollsAcrossIdleTTL, "the poll ran across the idle ttl")
	statuses, err := tenantStatuses(ctx, cl, class)
	require.NoError(t, err)
	require.Len(t, statuses, manyTenants)
	for name, status := range statuses {
		require.Equal(t, models.TenantActivityStatusHOT, status, "tenant %s", name)
	}
	verifyAll(ctx, t, cl, l, class, tenants)
	for _, single := range singles {
		verifyAll(ctx, t, cl, l, single, []string{""})
	}
	eventuallyLoaded(ctx, t, compose, 1, 2*time.Minute, noneLoaded, "idle shards unload after a full read")
}

func TestLiveKnobChanges(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	compose := start(ctx, t, newCompose(t, true, 1, fastKnobs(nil)))
	cl := newClient(compose.GetWeaviate().URI())
	waitUnloader(ctx, t, compose, 1)

	const class = "LiveKnobs"
	tenants := tenantNames("t", 30)
	l := newLedger()
	createClass(ctx, t, cl, vectorClass(class, true, 1))
	createTenants(ctx, t, cl, class, tenants)
	seed(ctx, t, cl, l, class, tenants, 2, nil)
	loadedIs := func(want int) func(int) bool { return func(n int) bool { return n == want } }
	eventuallyLoaded(ctx, t, compose, 1, 2*time.Minute, loadedIs(len(tenants)), "with idle_ttl off every shard stays loaded")

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

	setKnobs(ctx, t, compose, 1, fastKnobs(idleKnobs))
	eventuallyLoaded(ctx, t, compose, 1, 2*time.Minute, loadedIs(1), "setting idle_ttl unloads idle shards, the queried one stays")
	hotLoaded, err := shardLoaded(ctx, cl, class, hot)
	require.NoError(t, err)
	require.True(t, hotLoaded, "the continuously queried tenant stays loaded")
	stopQueries()
	require.NoError(t, queryErr)
	require.Positive(t, queries.Load())
	eventuallyLoaded(ctx, t, compose, 1, 2*time.Minute, noneLoaded, "the last tenant unloads once idle")
	verifyAll(ctx, t, cl, l, class, tenants)
}

func TestKillDuringUnloadingKeepsAcknowledgedWrites(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	compose := start(ctx, t, newCompose(t, true, 1, fastKnobs(idleKnobs)))
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
	waitUnloader(ctx, t, compose, 1)
	eventuallyLoaded(ctx, t, compose, 1, 20*time.Second, func(n int) bool { return n == len(tenants) }, "the read-back loads every shard")
	eventuallyLoaded(ctx, t, compose, 1, 2*time.Minute, noneLoaded, "the restarted collector unloads startup-loaded and on-demand-loaded shards")
}

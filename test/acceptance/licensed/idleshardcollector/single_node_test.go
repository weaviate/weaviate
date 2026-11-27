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
	"maps"
	"math/rand"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	apiclient "github.com/weaviate/weaviate/client"
)

func TestUnlicensedKeepsEveryShardLoaded(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	compose := start(ctx, t, newCompose(t, false, 1, "1s"))
	node := compose.GetWeaviate()
	cl := newClient(node.URI())

	const class = "UnlicensedParity"
	tenants := tenantNames(30)
	createClass(ctx, t, cl, vectorClass(class, true, 1))
	createTenants(ctx, t, cl, class, tenants)
	seed(ctx, t, cl, newLedger(), class, tenants, 3, nil)

	someUnloaded := func() bool {
		shards, err := loadedShards(ctx, cl, class, node.Name())
		return err != nil || len(shards) != len(tenants) || slices.Contains(slices.Collect(maps.Values(shards)), false)
	}
	require.False(t, someUnloaded(), "every shard is loaded")
	require.Never(t, someUnloaded, 10*time.Second, 500*time.Millisecond, "an unlicensed node never unloads a shard")
	_, published, err := metric(ctx, node, metricLoaded, "")
	require.NoError(t, err)
	require.False(t, published, "an unlicensed node builds no unloader")
}

const (
	manyTenants             = 300
	manyTenantsOps          = 2000
	singleTenantCollections = 40
	grpcReloadTenants       = 20
	killRounds              = 3
)

func TestManyTenantsUnloadOnceIdle(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	compose := start(ctx, t, newCompose(t, true, 1, idleTTL))
	node := compose.GetWeaviate()
	cl := newClient(node.URI())

	const class = "ManyTenants"
	tenants := tenantNames(manyTenants)
	l := newLedger()
	createClass(ctx, t, cl, vectorClass(class, true, 1))
	createTenants(ctx, t, cl, class, tenants)
	singles := make([]string, singleTenantCollections)
	for i := range singles {
		singles[i] = fmt.Sprintf("Single%02d", i)
		createClass(ctx, t, cl, vectorClass(singles[i], false, 1))
		seed(ctx, t, cl, l, singles[i], []string{""}, 6, nil)
	}
	var polls atomic.Int64
	every(ctx, t, 200*time.Millisecond, func(ctx context.Context) error {
		_, err := loadedShards(ctx, cl, class, node.Name())
		polls.Add(1)
		return err
	})
	seed(ctx, t, cl, l, class, tenants, 3, nil)
	require.NoError(t, randomWorkload(ctx, cl, l, class, tenants, manyTenantsOps, 8), "every request succeeds while shards unload")

	polls.Store(0)
	eventuallyLoaded(ctx, t, compose, 1, 0, "idle shards unload although the verbose nodes API reads them continuously")
	require.GreaterOrEqual(t, polls.Load(), int64(15), "the poll ran across the idle ttl")
	grpcTenants := tenants[:grpcReloadTenants]
	verifyGRPC(ctx, t, newGRPCClient(t, node.GrpcURI()), l, class, grpcTenants)
	shards, err := loadedShards(ctx, cl, class, node.Name())
	require.NoError(t, err)
	for _, tenant := range grpcTenants {
		require.True(t, shards[tenant], "the gRPC search reloaded %s", tenant)
	}
	requireAllHot(ctx, t, cl, class, manyTenants)
	verify(ctx, t, cl, l, class, tenants, true)
	for _, single := range singles {
		verify(ctx, t, cl, l, single, []string{""}, true)
	}
	eventuallyLoaded(ctx, t, compose, 1, 0, "idle shards unload after a full read")
}

func TestLiveKnobChanges(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	compose := start(ctx, t, newCompose(t, true, 1, ""))
	cl := newClient(compose.GetWeaviate().URI())

	const class = "LiveKnobs"
	tenants := tenantNames(30)
	l := newLedger()
	createClass(ctx, t, cl, vectorClass(class, true, 1))
	createTenants(ctx, t, cl, class, tenants)
	seed(ctx, t, cl, l, class, tenants, 2, nil)
	eventuallyLoaded(ctx, t, compose, 1, len(tenants), "with idle_ttl off every shard stays loaded")

	hot := tenants[0]
	hotID := l.ids(class, hot)[0]
	stopQueries := every(ctx, t, 250*time.Millisecond, func(ctx context.Context) error {
		return readObject(ctx, cl, class, hot, hotID, nil)
	})
	setIdleTTL(ctx, t, compose, 1, idleTTL)
	eventuallyLoaded(ctx, t, compose, 1, 1, "setting idle_ttl unloads idle shards, the queried one stays")
	shards, err := loadedShards(ctx, cl, class, compose.GetWeaviate().Name())
	require.NoError(t, err)
	require.True(t, shards[hot], "the continuously queried tenant stays loaded")
	require.NoError(t, stopQueries())
	eventuallyLoaded(ctx, t, compose, 1, 0, "the last tenant unloads once idle")
	verify(ctx, t, cl, l, class, tenants, true)
}

// TestKillsWhileUnloadingKeepAcknowledgedWrites SIGKILLs the node while unloads are completing; whether a
// teardown is mid-flight is timing, so the exact crash points are pinned by the db package's
// TestLazyLoadShard_CrashImageDuringUnloadKeepsAcknowledgedWrites.
func TestKillsWhileUnloadingKeepAcknowledgedWrites(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	compose := start(ctx, t, newCompose(t, true, 1, idleTTL))
	node := compose.GetWeaviate()
	var client atomic.Pointer[apiclient.Weaviate]
	client.Store(newClient(node.URI()))

	const class = "Kill"
	tenants := tenantNames(40)
	l := newLedger()
	createClass(ctx, t, client.Load(), vectorClass(class, true, 1))
	createTenants(ctx, t, client.Load(), class, tenants)
	seed(ctx, t, client.Load(), l, class, tenants, 2, nil)

	var killing atomic.Bool
	started := time.Now()
	stopWriters := make([]func() error, 4)
	for i := range stopWriters {
		stopWriters[i] = every(ctx, t, 200*time.Millisecond, func(ctx context.Context) error {
			cl := client.Load()
			tenant := tenants[(int(time.Since(started)/(3*time.Second))+rand.Intn(10))%len(tenants)]
			if err := writeObjects(ctx, cl, l, class, tenant, 1, nil); err != nil && !killing.Load() && cl == client.Load() {
				return err
			}
			return nil
		})
	}

	zero := time.Duration(0)
	for round := range killRounds {
		prev := -1.0
		require.EventuallyWithT(t, func(ct *assert.CollectT) {
			v, _, err := metric(ctx, node, metricUnloads, `result="ok"`)
			require.NoError(ct, err)
			advanced := prev >= 0 && v > prev
			prev = v
			require.True(ct, advanced, "no unload completed since the previous scrape")
		}, 5*time.Minute, 250*time.Millisecond, "round %d: shards are unloading under the write load", round)

		killing.Store(true)
		require.NoError(t, compose.RestartAt(ctx, 0, &zero))
		client.Store(newClient(compose.GetWeaviate().URI()))
		killing.Store(false)
	}
	for _, stop := range stopWriters {
		require.NoError(t, stop(), "a write failed outside a kill")
	}

	verify(ctx, t, client.Load(), l, class, tenants, false)
	eventuallyLoaded(ctx, t, compose, 1, len(tenants), "the read-back loads every shard")
	eventuallyLoaded(ctx, t, compose, 1, 0, "the restarted collector unloads startup-loaded and on-demand-loaded shards")
}

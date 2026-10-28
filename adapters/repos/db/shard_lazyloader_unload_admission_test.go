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

package db

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLazyLoadShard_RefusedLoadNeverMakesRoom(t *testing.T) {
	f := newUnloadFixture(t)
	f.lazy.memMonitor = failingAllocChecker{}

	_, _, err := f.lazy.loadIfCold(testCtx())
	require.ErrorIs(t, err, errInjectedMemoryPressure)
	require.Zero(t, f.unloader.beforeLoads.Load(), "a load memwatch refuses must not evict another shard")
}

func TestLazyLoadShard_TryUnloadSkipsAVictimAccessedSincePicked(t *testing.T) {
	f := newUnloadFixture(t)
	f.put(t)
	picked := f.lazy.LastAccess()
	require.NotZero(t, picked)

	f.lazy.lastAccess.Store(picked + 1)
	require.Equal(t, UnloadOutcomeReaccessed, f.lazy.TryUnload(testCtx(),
		UnloadRequest{Mode: UnloadModeRequest, PickedAt: picked, Reindex: noReindex}))
	require.NotNil(t, f.lazy.currentShard())

	require.Equal(t, UnloadOutcomeOK, f.lazy.TryUnload(testCtx(),
		UnloadRequest{Mode: UnloadModeRequest, PickedAt: picked + 1, Reindex: noReindex}))
}

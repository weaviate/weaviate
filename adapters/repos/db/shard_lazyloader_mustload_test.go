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

// A mustLoad-backed method cannot surface a memory refusal, so it must not
// panic on one, while the error-returning load paths keep refusing.
func TestLazyLoadShard_MustLoadUnderMemoryPressureDoesNotPanic(t *testing.T) {
	cases := []struct {
		name string
		call func(l *LazyLoadShard)
	}{
		{name: "Store", call: func(l *LazyLoadShard) { require.NotNil(t, l.Store()) }},
		{name: "Metrics", call: func(l *LazyLoadShard) { require.NotNil(t, l.Metrics()) }},
		{name: "Counter", call: func(l *LazyLoadShard) { require.NotNil(t, l.Counter()) }},
		{name: "Versioner", call: func(l *LazyLoadShard) { require.NotNil(t, l.Versioner()) }},
		{name: "GetPropertyLengthTracker", call: func(l *LazyLoadShard) { require.NotNil(t, l.GetPropertyLengthTracker()) }},
		{name: "hasGeoIndex", call: func(l *LazyLoadShard) { l.hasGeoIndex() }},
		{name: "SetStatusReadonly", call: func(l *LazyLoadShard) { require.NoError(t, l.SetStatusReadonly("test")) }},
		{name: "ForEachVectorIndex", call: func(l *LazyLoadShard) {
			require.NoError(t, l.ForEachVectorIndex(func(string, VectorIndex) error { return nil }))
		}},
		{name: "commitReplication", call: func(l *LazyLoadShard) { l.commitReplication(testCtx(), "missing-request") }},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newAddPropertyLazyFixture(t, "MustLoadPressure", singleShardState())
			for _, shard := range f.coldShards(t) {
				shard.memMonitor = failingAllocChecker{}

				_, _, err := shard.loadIfCold(testCtx())
				require.ErrorIs(t, err, errInjectedMemoryPressure)
				require.False(t, shard.isLoaded())

				require.NotPanics(t, func() { tc.call(shard) })
				require.True(t, shard.isLoaded())
			}
		})
	}
}

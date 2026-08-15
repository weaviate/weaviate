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
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestAnyLiveReindexForShard_RuntimeReindexDisabled pins the backup half
// of the kill switch: with the feature off the gate consults nothing, so
// a backup runs exactly as it would on a build without the gate.
//
// The lookup reports every shard as reindexing, so a gate that still ran
// would both refuse and bump the counter.
func TestAnyLiveReindexForShard_RuntimeReindexDisabled(t *testing.T) {
	tests := []struct {
		name       string
		disabled   bool
		wantBlock  bool
		wantLookup bool
	}{
		{name: "disabled skips the check", disabled: true},
		{name: "enabled keeps the check", wantBlock: true, wantLookup: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var lookups atomic.Int64
			db := &DB{config: Config{RuntimeReindexDisabled: tt.disabled}}
			db.SetShardReindexActivityLookup(func() (ShardReindexActivityLookup, bool) {
				lookups.Add(1)
				return func(string, string) bool { return true }, false
			})

			live, _ := db.AnyLiveReindexForShard("MyClass", "shard1")
			require.Equal(t, tt.wantBlock, live)
			require.Equal(t, tt.wantLookup, lookups.Load() > 0,
				"the backup path must make no reindex lookup while the feature is off")
		})
	}
}

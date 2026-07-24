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
	"os"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
)

// TestLazyLoadShardDropBlocksNewRequests pins that a dropped shard hands out no
// more references, whether or not it was loaded, and that one that was never
// loaded is not instantiated again — which would recreate the directory the
// drop just removed.
func TestLazyLoadShardDropBlocksNewRequests(t *testing.T) {
	const shardName = "lazyshard"

	tests := []struct {
		name string
		// load instantiates the shard before it is dropped, so drop delegates to
		// the loaded shard instead of taking the unloaded fast path
		load bool
	}{
		{name: "the shard was never loaded"},
		{name: "the shard was loaded", load: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, idx := testShard(t, t.Context(), "LazyDrop")
			class := &models.Class{Class: idx.Config.ClassName.String()}

			shardLike, err := idx.initShard(t.Context(), shardName, class, nil, false, true)
			require.NoError(t, err)
			lazy, ok := shardLike.(*LazyLoadShard)
			require.True(t, ok)

			require.NoError(t, os.MkdirAll(shardPath(idx.path(), shardName), 0o755))
			if tt.load {
				require.NoError(t, lazy.Load(t.Context()))
			}

			require.NoError(t, lazy.drop(false))

			_, err = lazy.preventShutdown()
			require.ErrorIs(t, err, errAlreadyShutdown)
			if !tt.load {
				require.ErrorIs(t, lazy.Load(t.Context()), errAlreadyShutdown)
			}
			require.NoDirExists(t, shardPath(idx.path(), shardName))
		})
	}
}

// TestShardDropWithConcurrentRequests pins that requests racing a drop are
// rejected rather than handed a reference to a shard that is being torn down.
// Run with -race.
func TestShardDropWithConcurrentRequests(t *testing.T) {
	const readers = 8

	shardLike, idx := testShard(t, t.Context(), "DropConcurrent")
	shard := underlyingShard(t, shardLike)

	stop := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < readers; i++ {
		wg.Add(1)
		enterrors.GoWrapper(func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				release, err := shard.preventShutdown()
				if err != nil {
					assert.ErrorIs(t, err, errAlreadyShutdown)
					return
				}
				release()
			}
		}, idx.logger)
	}

	require.NoError(t, shard.drop(false))
	close(stop)
	wg.Wait()

	_, err := shard.preventShutdown()
	require.ErrorIs(t, err, errAlreadyShutdown)
}

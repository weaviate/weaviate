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

package replica_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/weaviate/weaviate/usecases/replica"
	"github.com/weaviate/weaviate/usecases/replica/hashtree"
)

func TestPrefilterShardRoots(t *testing.T) {
	const (
		class = "C"
		local = "A"
		peer  = "B"
	)
	root := hashtree.Digest{1, 2}

	newFinder := func(t *testing.T) (*fakeFactory, *replica.Finder) {
		f := newFakeFactory(t, class, "s1", []string{local, peer}, true)
		f.AddShard("s2", []string{local, peer})
		return f, f.newFinder(local)
	}

	t.Run("all in sync returns empty", func(t *testing.T) {
		f, finder := newFinder(t)
		f.RClient.EXPECT().
			CompareHashTreeRoots(mock.Anything, peer, class, mock.Anything).
			Return([]string{}, nil)

		needFull, stats := finder.PrefilterShardRoots(context.Background(),
			map[string]hashtree.Digest{"s1": root, "s2": root})
		assert.Empty(t, needFull)
		assert.Equal(t, replica.PrefilterStats{OK: 1}, stats)
	})

	t.Run("diverging shard is flagged", func(t *testing.T) {
		f, finder := newFinder(t)
		f.RClient.EXPECT().
			CompareHashTreeRoots(mock.Anything, peer, class, mock.Anything).
			Return([]string{"s2"}, nil)

		needFull, stats := finder.PrefilterShardRoots(context.Background(),
			map[string]hashtree.Digest{"s1": root, "s2": root})
		assert.Equal(t, map[string]struct{}{"s2": {}}, needFull)
		assert.Equal(t, replica.PrefilterStats{OK: 1}, stats)
	})

	t.Run("unsupported peer falls back to full descent for its shards", func(t *testing.T) {
		f, finder := newFinder(t)
		f.RClient.EXPECT().
			CompareHashTreeRoots(mock.Anything, peer, class, mock.Anything).
			Return(nil, replica.ErrCompareHashTreeRootsUnsupported)

		needFull, stats := finder.PrefilterShardRoots(context.Background(),
			map[string]hashtree.Digest{"s1": root, "s2": root})
		assert.Equal(t, map[string]struct{}{"s1": {}, "s2": {}}, needFull)
		assert.Equal(t, replica.PrefilterStats{Unsupported: 1}, stats)
	})
}

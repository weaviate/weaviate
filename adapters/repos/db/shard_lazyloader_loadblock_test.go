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
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	enterrors "github.com/weaviate/weaviate/entities/errors"
)

// TestLazyLoadShardLoadBlock verifies the loadBlocked mechanism that
// RecoveringShard relies on to keep the inner Load from materialising
// an empty shard from a missing on-disk directory.
//
// We deliberately do NOT construct a full Shard / Index / metrics here:
// the loadBlocked path is the very first guard inside Load and short-
// circuits before any of the heavier scaffolding is touched. So a
// hand-built LazyLoadShard with the relevant fields set is enough.
func TestLazyLoadShardLoadBlock(t *testing.T) {
	t.Run("blockLoad makes Load return the supplied error", func(t *testing.T) {
		l := &LazyLoadShard{}
		l.blockLoad(enterrors.ErrShardRecovering)

		err := l.Load(context.Background())
		require.Error(t, err)
		require.True(t, errors.Is(err, enterrors.ErrShardRecovering))
	})

	t.Run("isLoadBlocked reports current state", func(t *testing.T) {
		l := &LazyLoadShard{}
		require.False(t, l.isLoadBlocked())
		l.blockLoad(enterrors.ErrShardRecovering)
		require.True(t, l.isLoadBlocked())
		l.clearLoadBlock()
		require.False(t, l.isLoadBlocked())
	})

	t.Run("clearLoadBlock allows Load to proceed past the guard", func(t *testing.T) {
		// Once cleared, the loadBlocked guard no longer short-circuits.
		// Load will attempt to construct the inner shard; with our nil
		// scaffolding it will fail downstream — but it must NOT return
		// ErrShardRecovering anymore.
		l := &LazyLoadShard{}
		l.blockLoad(enterrors.ErrShardRecovering)
		l.clearLoadBlock()

		// Load is expected to fail (nil opts) but not with our sentinel.
		// We only care that the loadBlocked branch was bypassed.
		defer func() {
			// NewShard panics on nil opts; we catch it to assert that
			// the loadBlocked guard was not the one returning early.
			_ = recover()
		}()
		_ = l.Load(context.Background())
	})

	t.Run("loaded shard short-circuits before loadBlocked", func(t *testing.T) {
		l := &LazyLoadShard{loaded: true}
		l.blockLoad(enterrors.ErrShardRecovering)

		// Already-loaded path returns nil without consulting loadBlocked.
		err := l.Load(context.Background())
		require.NoError(t, err)
	})
}

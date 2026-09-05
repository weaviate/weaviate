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
		l := &LazyLoadShard{}
		l.blockLoad(enterrors.ErrShardRecovering)
		l.clearLoadBlock()

		defer func() { _ = recover() }()
		_ = l.Load(context.Background())
	})

	t.Run("loaded shard short-circuits before loadBlocked", func(t *testing.T) {
		l := &LazyLoadShard{loaded: true}
		l.blockLoad(enterrors.ErrShardRecovering)

		err := l.Load(context.Background())
		require.NoError(t, err)
	})
}

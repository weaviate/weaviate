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

func TestAsLazyLoadShard(t *testing.T) {
	lazy := &LazyLoadShard{}
	got, ok := asLazyLoadShard(lazy)
	require.True(t, ok)
	require.Same(t, lazy, got)

	rec := &RecoveringShard{LazyLoadShard: &LazyLoadShard{}}
	got, ok = asLazyLoadShard(rec)
	require.True(t, ok)
	require.Same(t, rec.LazyLoadShard, got)

	_, ok = asLazyLoadShard(&Shard{})
	require.False(t, ok)
}

func TestRecoveringShardLoadKeepsBlock(t *testing.T) {
	r := &RecoveringShard{LazyLoadShard: &LazyLoadShard{}}
	r.blockLoad(enterrors.ErrShardRecovering)

	err := r.Load(context.Background())
	require.True(t, errors.Is(err, enterrors.ErrShardRecovering))
	require.True(t, r.isLoadBlocked(), "Load must not clear the recovery block")
}

func TestRecoveringShardPromoteClearsBlock(t *testing.T) {
	r := &RecoveringShard{LazyLoadShard: &LazyLoadShard{}}
	r.blockLoad(enterrors.ErrShardRecovering)

	defer func() {
		_ = recover()
		require.False(t, r.isLoadBlocked())
	}()
	_ = r.Promote(context.Background())
}

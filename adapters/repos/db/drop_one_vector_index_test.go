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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// sharedStateIndex stands in for dynamic: an index type whose plain Drop would
// reach beyond the one vector being dropped.
type sharedStateIndex struct {
	VectorIndex
	droppedWholeIndex bool
	droppedTargetOnly bool
}

func (i *sharedStateIndex) Drop(ctx context.Context, keepFiles bool) error {
	i.droppedWholeIndex = true
	return nil
}

func (i *sharedStateIndex) DropTargetVector(ctx context.Context) error {
	i.droppedTargetOnly = true
	return nil
}

// plainIndex owns nothing shard-wide, like hnsw or flat.
type plainIndex struct {
	VectorIndex
	dropped bool
}

func (i *plainIndex) Drop(ctx context.Context, keepFiles bool) error {
	i.dropped = true
	return nil
}

// TestDropOneVectorIndex_PrefersThePerVectorDrop pins the routing, which the
// dynamic package's own tests cannot: they call DropTargetVector directly, so
// a shard that stopped using it would leave them green while every per-vector
// drop went back to closing and deleting the shard-shared state DB.
func TestDropOneVectorIndex_PrefersThePerVectorDrop(t *testing.T) {
	ctx := context.Background()

	shared := &sharedStateIndex{}
	require.NoError(t, dropOneVectorIndex(ctx, shared))
	assert.True(t, shared.droppedTargetOnly,
		"an index owning shard-wide state must be dropped per target vector")
	assert.False(t, shared.droppedWholeIndex,
		"the whole-index Drop would take every sibling's state with it")
}

// TestDropOneVectorIndex_FallsBackForPlainIndexes pins the other half: types
// that own nothing shard-wide must still go through Drop, or dropping them
// would silently become a no-op.
func TestDropOneVectorIndex_FallsBackForPlainIndexes(t *testing.T) {
	plain := &plainIndex{}
	require.NoError(t, dropOneVectorIndex(context.Background(), plain))
	assert.True(t, plain.dropped, "an index with no shared state must still be dropped")
}

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

	"github.com/stretchr/testify/require"
)

// TestBitmapFactoryOnEmptyShard pins what the shard's BitmapFactory answers
// before the first object is written.
//
// The counter is the count of doc IDs allocated, so it is 0 on a fresh shard,
// and both consumers read it as one: the universe GetBitmap builds is the
// half-open range [0, count), and the fold's row-footprint hint is the ranges
// that span. Neither wants a highest-ID, which is why nothing reports one.
//
// Getting the universe wrong is not a read-only error. Searcher.docIDs inverts
// a deny-list filter against it, filteredAggregator reports the allow list's
// length as the meta count without resolving an object, and objectsByDocID
// prunes any ID it cannot resolve from the shared prefilled bitmap — which
// FillUp never restores. A phantom ID here costs the shard's first real object
// for the life of the process.
func TestBitmapFactoryOnEmptyShard(t *testing.T) {
	ctx := context.Background()
	shard, index := testShard(t, ctx, "TestClass")
	defer func() { require.NoError(t, index.drop()) }()

	s, ok := shard.(*Shard)
	require.True(t, ok, "the wiring under test is on the concrete shard's factory")

	require.Zero(t, s.counter.Get(), "a shard that has held no object counts none")
	require.Zero(t, s.bitmapFactory.DocIDCount(),
		"an empty shard has allocated no doc ID")

	universe, release := s.bitmapFactory.GetBitmap()
	defer release()
	require.Empty(t, universe.ToArray(),
		"an empty shard has no document for a deny-list filter to match")
}

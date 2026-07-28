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

//go:build integrationTest
// +build integrationTest

package lsmkv

import (
	"context"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

// The inverted reusable cursor reuses its key/value buffers across nodes. When
// CursorMap wraps it, a caller may hold the returned keys across iterations (the
// key is copied) but must read each node's values during its own iteration (they
// alias the reused buffer, valid only until the next call). This exercises both
// guarantees: without the returned-key copy the held keys collapse onto the last
// node, and without the deferred inner-cursor advance an early advance feeds the
// merger clobbered buffers.
func TestCursorMapInvertedReusable_HoldAcrossIterations(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	bucket, err := NewBucketCreator().NewBucket(ctx, dir, dir, nullLogger(), nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyInverted))
	require.NoError(t, err)
	defer bucket.Shutdown(ctx)
	bucket.SetMemtableThreshold(1e9) // never auto-flush mid-segment

	const (
		numTerms = 50
		docsPer  = 200 // > BLOCK_SIZE (128) -> multi-block postings per term
	)
	wantFirstDoc := map[string]uint64{}
	for term := 0; term < numTerms; term++ {
		key := fmt.Sprintf("term%03d", term)
		for d := 0; d < docsPer; d++ {
			docID := uint64(term*docsPer + d)
			require.NoError(t, bucket.MapSet([]byte(key), NewMapPairFromDocIdAndTf(docID, float32(d%10+1), 1, false)))
		}
		wantFirstDoc[key] = uint64(term * docsPer) // smallest docID under the term
	}
	require.NoError(t, bucket.FlushAndSwitch())

	c, err := bucket.MapCursor()
	require.NoError(t, err)
	defer c.Close()

	// Hold the returned keys WITHOUT copying (their copy is the cursor's job), but
	// read the value bytes during the iteration that produced them — that is the
	// contract, and it exercises the deferred advance (an early advance would feed
	// the merger buffers already overwritten by the next node).
	var keys [][]byte
	firstDocByIter := make([]uint64, 0, numTerms)
	for k, v := c.First(ctx); k != nil; k, v = c.Next(ctx) {
		keys = append(keys, k)
		require.NotEmpty(t, v)
		firstDocByIter = append(firstDocByIter, binary.BigEndian.Uint64(v[0].Key))
	}

	require.Len(t, keys, numTerms)
	for i := range keys {
		term := string(keys[i])
		require.Equal(t, fmt.Sprintf("term%03d", i), term, "keys must not collapse onto the last node")
		assert.Equal(t, wantFirstDoc[term], firstDocByIter[i], "correct values decoded for %s", term)
	}
}

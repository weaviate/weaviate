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
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/require"
)

func TestBatchDeleteReportsAnUnprocessedObject(t *testing.T) {
	// the integration job sets DISABLE_RECOVERY_ON_PANIC=true, under which the
	// malformed id takes the test binary down instead of failing one slot
	t.Setenv("DISABLE_RECOVERY_ON_PANIC", "false")

	ctx := context.Background()
	shard, _ := testShard(t, ctx, "TestBatchDeleteUnprocessed")

	cases := []struct {
		name   string
		uuids  []strfmt.UUID
		badIdx int
	}{
		// batchDeleteObject parses the id with uuid.MustParse, which panics
		// rather than erroring; the group recovers it and the slot is never written
		{name: "one malformed id", uuids: []strfmt.UUID{"not-a-uuid"}, badIdx: 0},
		{name: "malformed first", uuids: []strfmt.UUID{
			"not-a-uuid", "11111111-1111-1111-1111-111111111111",
		}, badIdx: 0},
		{name: "malformed among valid", uuids: []strfmt.UUID{
			"11111111-1111-1111-1111-111111111111", "not-a-uuid",
			"22222222-2222-2222-2222-222222222222",
		}, badIdx: 1},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			res := shard.DeleteObjectBatch(ctx, tt.uuids, time.Now(), false)
			require.Len(t, res, len(tt.uuids))

			require.Equal(t, tt.uuids[tt.badIdx], res[tt.badIdx].UUID,
				"the slot must name the object it stands for, not stay at its zero value")
			require.Error(t, res[tt.badIdx].Err,
				"a delete that panicked must not come back as a deleted object")

			for i := range res {
				if i == tt.badIdx {
					continue
				}
				require.NoError(t, res[i].Err,
					"slot %d: the seed must be overwritten by the delete that ran", i)
			}
		})
	}
}

// TestBatchDeleteNamesItsObjectsWhenTheContextIsDead guards the arm that fails
// every object at once: without the uuid, a caller has no way to match an
// error to the object it belongs to.
func TestBatchDeleteNamesItsObjectsWhenTheContextIsDead(t *testing.T) {
	t.Setenv("DISABLE_RECOVERY_ON_PANIC", "false")

	shard, _ := testShard(t, context.Background(), "TestBatchDeleteDeadContext")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	uuids := []strfmt.UUID{
		"11111111-1111-1111-1111-111111111111",
		"22222222-2222-2222-2222-222222222222",
	}
	res := shard.DeleteObjectBatch(ctx, uuids, time.Now(), false)
	require.Len(t, res, len(uuids))

	for i := range res {
		require.Equal(t, uuids[i], res[i].UUID,
			"slot %d must name the object it stands for", i)
		require.Error(t, res[i].Err, "slot %d: nothing was deleted", i)
	}
}

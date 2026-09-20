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

package replica

import (
	"context"
	"fmt"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storobj"
)

// Pins weaviate/0-weaviate-issues#385: don't report consistent while a replica still holds an unresolved tombstone.
func TestReadBatchPartIsConsistentThreeTier(t *testing.T) {
	const (
		shard = "S1"
		node  = "A"

		tContent   = int64(100)
		tTombstone = int64(200)
		tWinner    = int64(300)
	)

	id := strfmt.UUID("00000000-0000-0000-0000-0000000000aa")
	ids := []strfmt.UUID{id}

	for _, order := range [][]string{{"A", "B", "C"}, {"A", "C", "B"}} {
		t.Run(fmt.Sprintf("digest order %v", order), func(t *testing.T) {
			ctx := context.Background()
			c := newFakeReplicas(models.ReplicationConfigDeletionStrategyNoAutomatedResolution,
				[]string{"A", "B", "C"})
			c.put("A", id, tContent, false)
			c.put("B", id, tTombstone, true)
			c.put("C", id, tWinner, false)

			logger, _ := test.NewNullLogger()
			f := &finderStream{repairer: *c.newRepairer(t), log: logger}

			data := []*storobj.Object{{
				MarshallerVersion: 1,
				Object:            models.Object{ID: id, LastUpdateTimeUnix: tContent},
				BelongsToNode:     node,
				BelongsToShard:    shard,
			}}
			batch := ShardPart{Shard: shard, Node: node, Data: data, Index: []int{0}}

			votes := c.votes(order, ids)
			ch := make(chan Result[BatchReply], len(votes))
			for _, v := range votes {
				ch <- Result[BatchReply]{Value: v.BatchReply}
			}
			close(ch)

			require.NoError(t, <-f.readBatchPart(ctx, batch, ids, ch, len(votes)))

			t.Logf("isConsistent=%v; A=%+v B=%+v C=%+v",
				data[0].IsConsistent, c.get("A", id), c.get("B", id), c.get("C", id))

			assert.False(t, data[0].IsConsistent,
				"object reported consistent while a replica still holds an unresolved tombstone")
		})
	}
}

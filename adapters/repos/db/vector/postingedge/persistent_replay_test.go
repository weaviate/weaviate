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

package postingedge

import (
	"slices"
	"strings"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	ent "github.com/weaviate/weaviate/entities/vectorindex/postingedge"
)

// The queue of a shard deletes a chunk it handed over only once the index has
// flushed (queue.go, "the chunk is the only copy of its records until the
// index flushed"). A process that dies before that, or a flush that fails,
// has it hand the chunk over again: the index is given vectors it holds,
// under the same doc ids, maybe next to some it does not hold, since the
// workers apply the parts of a chunk at once. The index takes such a batch as
// a replay: it succeeds, what it held stays as it was, a vector deleted since
// is deleted once its delete is replayed too, and what it did not hold goes
// in. A doc id given again with
// another vector does not stop the batch either: the index keeps the vector
// it holds, takes the rest of the batch, and logs the conflict. So does a doc
// id given twice in one batch. Were a replay refused, the worker of the queue
// would take the refusal for a permanent error and discard the batch: the
// vectors of it the index did not hold would never be indexed
// (TestPostingEdge_QueueGoesOnAfterAReplay, through a shard).
func TestPersistentTakesAReplayedBatch(t *testing.T) {
	t.Parallel()
	const total = persistentBase + 200
	tests := []struct {
		name string
		// in is how many vectors the index takes before the replay, and
		// deleted how many of the last of them are deleted then.
		in, deleted int
		// replayFrom and replayTo are the doc ids given again.
		replayFrom, replayTo int
		// change gives another vector under the first doc id of the replay,
		// and twice gives the first doc id of the replay twice, the second
		// time with the vector of change.
		change, twice bool
	}{
		{name: "before the base, all kept aside", in: persistentBase / 2, replayFrom: 0, replayTo: persistentBase / 2},
		{name: "before the base, some deleted since", in: persistentBase / 2, deleted: 10, replayFrom: 0, replayTo: persistentBase / 2},
		{name: "after the base, all held", in: total, replayFrom: total - 50, replayTo: total},
		{name: "after the base, half held and half new", in: total - 25, replayFrom: total - 50, replayTo: total},
		{name: "after the base, some deleted since", in: total, deleted: 10, replayFrom: total - 50, replayTo: total},
		{
			name: "another vector under a held doc id, among held and new ones", in: total - 25, replayFrom: total - 50,
			replayTo: total, change: true,
		},
		{
			name: "a new doc id twice in a batch, the same vector", in: total - 25, replayFrom: total - 25, replayTo: total,
			twice: true,
		},
		{
			name: "a new doc id twice in a batch, another vector the second time", in: total - 25, replayFrom: total - 25,
			replayTo: total, twice: true, change: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			logger, hook := test.NewNullLogger()
			logger.SetLevel(logrus.DebugLevel)
			f := newPersistentFixtureMode(t, total, ent.CacheModeRouting, func(cfg *PersistentConfig) {
				cfg.noBackground = true
				cfg.Logger = logger
			})
			ctx := persistentCtx()
			if tt.in < persistentBase {
				require.NoError(t, f.index.AddBatch(ctx, sequentialIDs(tt.in, 0), f.vectors[:tt.in]))
			} else {
				f.add(t, 0, tt.in)
			}
			var gone []uint64
			for id := tt.in - tt.deleted; id < tt.in; id++ {
				gone = append(gone, uint64(id))
			}
			if len(gone) > 0 {
				require.NoError(t, f.index.Delete(gone...))
			}

			ids := sequentialIDs(tt.replayTo-tt.replayFrom, uint64(tt.replayFrom))
			replay := slices.Clone(f.vectors[tt.replayFrom:tt.replayTo])
			other := slices.Clone(f.vectors[tt.replayFrom+1])
			switch {
			case tt.twice && tt.change:
				ids, replay = append(ids, ids[0]), append(replay, other)
			case tt.twice:
				ids, replay = append(ids, ids[0]), append(replay, replay[0])
			case tt.change:
				replay[0] = other
			}
			require.NoError(t, f.index.AddBatch(ctx, ids, replay), "a replayed batch is taken")
			if len(gone) > 0 {
				// The deletes come after the inserts in the queue, and are
				// handed over again with them. A vector deleted while kept
				// aside left nothing behind, and comes back with the replay of
				// its insert until its delete is replayed too.
				require.NoError(t, f.index.Delete(gone...))
			}

			// The rest of the vectors go in after the replay, as the queue
			// goes on with the chunks after it.
			if tt.replayTo < total {
				f.add(t, tt.replayTo, total)
			}
			f.index.mu.RLock()
			g := f.index.g
			require.NotNil(t, g, "the index has its base")
			live := g.meta.Live
			held := 0
			for id := range uint64(total) {
				if g.holds(id) {
					held++
				}
			}
			resurrected := 0
			for _, id := range gone {
				if g.holds(id) {
					resurrected++
				}
			}
			first, err := f.index.vectorOf(uint64(tt.replayFrom))
			f.index.mu.RUnlock()
			require.NoError(t, err)
			assert.Equal(t, total-len(gone), held, "every vector held once")
			assert.LessOrEqual(t, live, total, "no doc id twice in the postings")
			assert.Zero(t, resurrected, "a vector deleted since its insert stays deleted when it is replayed")
			assert.Equal(t, distancer.Normalize(f.vectors[tt.replayFrom]), first,
				"the vector the index held first, not the one given again")

			conflicts := 0
			for _, entry := range hook.AllEntries() {
				if entry.Level == logrus.WarnLevel && strings.Contains(entry.Message, "another vector") {
					conflicts++
				}
			}
			if tt.change {
				assert.Equal(t, 1, conflicts, "the conflict is logged once")
			} else {
				assert.Zero(t, conflicts, "a replay with the same vectors is no conflict")
			}
			f.covered(t, "after the replay")
		})
	}
}

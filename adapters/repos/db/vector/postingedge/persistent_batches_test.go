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
	"context"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	enterrors "github.com/weaviate/weaviate/entities/errors"
	ent "github.com/weaviate/weaviate/entities/vectorindex/postingedge"
)

// batchTestPatience is how long the test of the batches waits for its callers,
// which return in well under a second, before it fails by itself.
const batchTestPatience = time.Minute

// A batch is applied once it has its plans, or once nobody is left searching,
// and never while another is: the rule the callers of AddBatch go by.
func TestInsertBatchIsDue(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name            string
		ready, planning int
		applying        bool
		want            bool
	}{
		{name: "no plan", ready: 0, planning: 0, want: false},
		{name: "no plan, callers searching", ready: 0, planning: 3, want: false},
		{name: "a few plans, callers searching", ready: 5, planning: 2, want: false},
		{name: "a few plans, nobody searching", ready: 5, planning: 0, want: true},
		{name: "one short of a batch, callers searching", ready: insertBatchSize - 1, planning: 1, want: false},
		{name: "a batch, callers searching", ready: insertBatchSize, planning: 2, want: true},
		{name: "more than a batch, callers searching", ready: insertBatchSize + 7, planning: 1, want: true},
		{name: "a batch, another being applied", ready: insertBatchSize, planning: 0, applying: true, want: false},
		{name: "a few plans, another being applied", ready: 5, planning: 0, applying: true, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var b insertBatches
			b.init()
			b.ready, b.planning, b.applying = make([]*batchEntry, tt.ready), tt.planning, tt.applying
			assert.Equal(t, tt.want, b.due())
		})
	}
}

// The callers that add at once are the searchers of one batch, and one of
// them its applier: the four callers here are held until all of them search,
// so their sixteen plans are one batch, every one of them searched over the
// index as it was before the batch, applied with the index held for writing
// once. A caller returns once its vectors are in the index. Two callers that
// give the same doc id in a batch both succeed, and the index holds it once.
// A batch that has its plans while other callers still search is not applied
// under them: it waits for their searches, and takes their plans.
func TestPersistentBatchesTheInsertsOfItsCallers(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		// same gives the second caller the first doc id of the first.
		same bool
		// size is how many plans the batch gathers before it is applied.
		size int
	}{
		{name: "four callers, sixteen doc ids", size: 1000},
		{name: "two callers give the same doc id", size: 1000},
		// The first caller to end its search finds the batch with its plans:
		// it waits for the searches that run, whose plans join its batch.
		{name: "a batch that has its plans while callers search waits for them", size: insertWindow},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			const callers, each = 4, insertWindow
			const total = persistentBase + callers*each
			f := newPersistentFixtureMode(t, total, ent.CacheModeRouting, func(cfg *PersistentConfig) {
				cfg.noBackground = true
			})
			f.add(t, 0, persistentBase)

			var joined sync.WaitGroup
			joined.Add(callers)
			var mu sync.Mutex
			var batches [][]uint64
			var seqs [][]uint64
			b := &f.index.batches
			b.mu.Lock()
			b.size = tt.size
			b.joined = func() {
				joined.Done()
				joined.Wait()
			}
			b.onApply = func(batch []*batchEntry) {
				mu.Lock()
				defer mu.Unlock()
				ids, seq := make([]uint64, len(batch)), make([]uint64, len(batch))
				for i, e := range batch {
					ids[i], seq[i] = e.id, e.plan.seq
				}
				batches, seqs = append(batches, ids), append(seqs, seq)
			}
			b.mu.Unlock()

			ctx := context.Background()
			eg := enterrors.NewErrorGroupWrapper(testLogger())
			for c := range callers {
				from := persistentBase + c*each
				ids := sequentialIDs(each, uint64(from))
				vectors := f.vectors[from : from+each]
				if tt.same && c == 1 {
					ids[0] = uint64(persistentBase)
				}
				eg.Go(func() error {
					if err := f.index.AddBatch(ctx, ids, vectors); err != nil {
						return err
					}
					// What the caller gave is in once it returns.
					for _, id := range ids {
						if !f.index.ContainsDoc(id) {
							return assert.AnError
						}
					}
					return nil
				})
			}
			// A caller whose plans nobody applies waits for ever: the test
			// says so itself, and does not leave it to the timeout of the suite.
			returned := make(chan error, 1)
			enterrors.GoWrapper(func() { returned <- eg.Wait() }, testLogger())
			select {
			case err := <-returned:
				require.NoError(t, err, "every caller succeeds, and finds its vectors in once it returns")
			case <-time.After(batchTestPatience):
				b.mu.Lock()
				ready, planning, applying := len(b.ready), b.planning, b.applying
				b.mu.Unlock()
				require.FailNowf(t, "the callers did not return",
					"after %s the callers of AddBatch still wait for their vectors to be applied: %d plans are left "+
						"in a batch nobody applies (%d callers searching, a batch being applied: %v)",
					batchTestPatience, ready, planning, applying)
			}

			mu.Lock()
			applied, plansSeqs := slices.Clone(batches), slices.Clone(seqs)
			mu.Unlock()
			require.Len(t, applied, 1, "the callers searched at once: one batch")
			assert.Len(t, applied[0], callers*each, "every plan of every caller in it")
			for _, seq := range plansSeqs[0] {
				assert.Equal(t, plansSeqs[0][0], seq, "every plan of the batch searched the index as it was before it")
			}
			b.mu.Lock()
			count, largest := b.applied, b.largest
			b.mu.Unlock()
			assert.Equal(t, 1, count)
			assert.Equal(t, callers*each, largest)

			f.index.mu.RLock()
			live := f.index.g.meta.Live
			f.index.mu.RUnlock()
			want := total
			if tt.same {
				// The doc id the second caller gave again went in once, and
				// the doc id it did not give is not in.
				want--
				assert.False(t, f.index.ContainsDoc(uint64(persistentBase+each)))
			}
			assert.Equal(t, want, live, "every doc id in once")
			f.covered(t, "after the batch")
		})
	}
}

// A caller alone is its own batch: its window, searched and applied, as
// insert does it, and so for every window of its vectors.
func TestPersistentACallerAloneIsItsOwnBatch(t *testing.T) {
	t.Parallel()
	const total = persistentBase + 40
	f := newPersistentFixtureMode(t, total, ent.CacheModeRouting, func(cfg *PersistentConfig) {
		cfg.noBackground = true
	})
	f.add(t, 0, total)
	b := &f.index.batches
	b.mu.Lock()
	applied, largest := b.applied, b.largest
	b.mu.Unlock()
	assert.Equal(t, (total-persistentBase)/insertWindow, applied, "a batch for every window")
	assert.Equal(t, insertWindow, largest)
	f.covered(t, "grown alone")
}

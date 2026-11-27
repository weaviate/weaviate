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
	"sync"

	"github.com/pkg/errors"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/entities/concurrency"
)

// The inserts of the callers that add at once go in batches, as v5.2 had them
// (bench-report.md of feat/posting-edge-index, "inserts con búsquedas en
// paralelo"): the callers are the searchers of a batch, and one of them its
// applier.
//
// Every caller searches for a window of its vectors and leaves the plans in
// the batch that is gathering. A batch is applied only when nobody is
// searching, and nobody starts a search while one is applied: every plan of a
// batch searched the index as the batch before it left it. Once the batch has
// insertBatchSize plans, or nobody is left searching, whoever left the last
// plan waits for the searches that are running, takes the plans of all of
// them, and applies them in the order they came, with the index held for
// writing once for the whole batch. A caller returns once its vectors are
// applied: a queue counts what AddBatch returned from as indexed.
//
// The callers of a shard are the shared workers of its node, which bring the
// parts of a chunk of its queue at once, and the goroutines of an import.

// insertBatchSize is how many plans a batch gathers before it is applied:
// the 32 of v5.2, whose loads at 0.95 and 0.99 stayed within 5 % of the
// inserts one at a time.
const insertBatchSize = 32

// insertBatches gathers the plans of the callers into batches.
type insertBatches struct {
	mu   sync.Mutex
	cond *sync.Cond
	// planning counts the callers searching now.
	planning int
	// applying tells that a batch is being applied, or waits for the
	// searches that run to end before it is.
	applying bool
	// ready has the plans searched and not applied yet, in the order they
	// came.
	ready []*batchEntry
	// applied counts the batches applied, and largest is the most plans one
	// had.
	applied, largest int
	// size is how many plans a batch gathers before it is applied:
	// insertBatchSize but for the tests. joined, if set, runs once a caller
	// counts as searching, and onApply with every batch before its plans are
	// applied, for the tests.
	size    int
	joined  func()
	onApply func(batch []*batchEntry)
}

// batchEntry is an insert in a batch: its plan, and once it is applied, its
// error.
type batchEntry struct {
	id   uint64
	plan *growPlan
	err  error
	done bool
}

func (b *insertBatches) init() {
	b.cond = sync.NewCond(&b.mu)
	b.size = insertBatchSize
}

// addInBatches inserts the vectors of a caller once the index has its base:
// window by window, their searches into the batch that gathers, and returns
// once every one of them is applied, with the first error of its own.
func (p *Persistent) addInBatches(ctx context.Context, ids []uint64, vectors [][]float32) error {
	entries := make([]*batchEntry, 0, len(ids))
	var err error
	for from := 0; from < len(ids) && err == nil; from += insertWindow {
		if err = ctx.Err(); err != nil {
			break
		}
		to := min(from+insertWindow, len(ids))
		var window []*batchEntry
		window, err = p.searchWindow(ctx, ids[from:to], vectors[from:to])
		entries = append(entries, window...)
	}
	// What this caller left in a batch is applied whatever happens to the
	// rest of its vectors: it waits for it, and gives its plans back after.
	b := &p.batches
	b.mu.Lock()
	for _, e := range entries {
		for !e.done {
			b.cond.Wait()
		}
	}
	b.mu.Unlock()
	for _, e := range entries {
		p.plans.Put(e.plan)
		if err == nil && e.err != nil {
			err = errors.Wrapf(e.err, "add vector of id %d", e.id)
		}
	}
	return err
}

// searchWindow searches for a window of a caller's vectors over the index as
// the last batch applied left it, leaves their plans in the batch that
// gathers, and applies the batch if it is due.
func (p *Persistent) searchWindow(ctx context.Context, ids []uint64, vectors [][]float32) ([]*batchEntry, error) {
	b := &p.batches
	b.mu.Lock()
	for b.applying {
		b.cond.Wait()
	}
	b.planning++
	b.mu.Unlock()
	if b.joined != nil {
		b.joined()
	}

	// The search of an insert reads its rows and its vectors one at a time,
	// in the goroutine that calls: the inserts run next to each other as
	// their callers do, and goroutines of their own for every round of a
	// search cost more than the reads they wait for (bench-report.md of
	// feat/posting-edge-index, "Dónde se va el tiempo del insert").
	planCtx := concurrency.CtxWithBudget(ctx, 1)
	entries := make([]*batchEntry, 0, len(ids))
	var err error
	p.mu.RLock()
	if err = p.usable(); err == nil {
		g := p.g
		for i, id := range ids {
			plan := p.plans.Get().(*growPlan)
			plan.work = nil
			if err = g.plan(planCtx, plan, id, distancer.Normalize(vectors[i])); err != nil {
				p.plans.Put(plan)
				err = errors.Wrapf(err, "add vector of id %d", id)
				break
			}
			entries = append(entries, &batchEntry{id: id, plan: plan})
		}
	}
	p.mu.RUnlock()

	b.mu.Lock()
	b.planning--
	b.ready = append(b.ready, entries...)
	p.applyIfDue(ctx)
	b.mu.Unlock()
	return entries, err
}

// due tells whether the batch that gathers is applied now: it has its plans,
// or nobody is left searching, and no other batch is being applied. The
// caller holds mu.
func (b *insertBatches) due() bool {
	return !b.applying && len(b.ready) > 0 && (len(b.ready) >= b.size || b.planning == 0)
}

// applyIfDue applies the batch that gathers if it has its plans, or if
// nobody is left searching, and wakes whoever waits on the batches. Plans
// left once a batch is applied, which nobody searching could apply, are
// applied too. The caller holds batches.mu, which it is given back held.
func (p *Persistent) applyIfDue(ctx context.Context) {
	b := &p.batches
	defer b.cond.Broadcast()
	for b.due() {
		b.applying = true
		// The searches that run end on the index as it is, and their plans
		// join this batch: nobody starts one until it is applied.
		for b.planning > 0 {
			b.cond.Wait()
		}
		batch := b.ready
		b.ready = nil
		if b.onApply != nil {
			b.onApply(batch)
		}
		b.mu.Unlock()
		p.applyBatch(ctx, batch)
		b.mu.Lock()
		for _, e := range batch {
			e.done = true
		}
		b.applied++
		b.largest = max(b.largest, len(batch))
		b.applying = false
		b.cond.Broadcast()
	}
}

// applyBatch applies the plans of a batch, in their order, with the index
// held for writing once for all of them. The error of a plan is its own; the
// index stops at one that leaves it halfway, as it does for a single insert.
//
// An insert pays its share of the passes that are on right after its writes,
// as one alone does: while one is on or due, the index is let go after each
// insert of the batch for it to pay (payInLine), and held again. Nothing of
// that runs by default but with deletes, or with vectors that came while the
// base was built.
func (p *Persistent) applyBatch(ctx context.Context, batch []*batchEntry) {
	planCtx := concurrency.CtxWithBudget(context.WithoutCancel(ctx), 1)
	p.writeMu.Lock()
	defer p.writeMu.Unlock()
	p.mu.Lock()
	g := p.g
	for _, e := range batch {
		e.err = p.write(planCtx, g, e.plan)
		if e.err != nil || !p.owesInLine() {
			continue
		}
		p.mu.Unlock()
		e.err = p.payInLine(planCtx, e.id)
		p.mu.Lock()
	}
	p.mu.Unlock()
	p.wakeIfDue()
}

// owesInLine tells whether an insert has a share to pay in line
// (payInLine): of the refresh of the edges or of the pass over the deleted
// vectors, on or due, or of the vectors still aside. The caller holds writeMu
// and mu.
func (p *Persistent) owesInLine() bool {
	if p.cfg.noMaintenance || p.g == nil {
		return false
	}
	return p.refreshing != nil || p.refreshDue() || p.cleaning != nil || p.g.cleanDue() || len(p.aside) > 0
}

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

package inverted

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/entities/concurrency"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/filters"
	entsInverted "github.com/weaviate/weaviate/entities/inverted"
)

// docBitmapContainsBatch folds one batched Contains filter into a docBitmap,
// returning the plan for the slow query log. ContainsNone is NOT(ContainsAny):
// the same union marked a deny list, inverted once in Searcher.docIDs.
func (s *Searcher) docBitmapContainsBatch(ctx context.Context, source containsBatchReaderSource,
	pv *propValuePair,
) (docBitmap, containsFoldPlan, error) {
	keys := pv.containsKeys
	if keys.Len() == 0 {
		// defensive: the folds adopt their first row as the accumulator, so zero
		// keys yields a nil bitmap rather than an empty one
		return docBitmap{}, containsFoldPlan{}, fmt.Errorf(
			"%w: contains fold on prop %q carries no keys, after the leaf was built",
			entsInverted.ErrInternal, pv.prop)
	}

	planner := containsFoldPlanner{docIDCount: s.bitmapFactory.DocIDCount()}
	// the smaller of the query's share and the machine, so the planner compares
	// one number against the batch rather than reaching for a second
	workerBudget := min(concurrency.BudgetFromCtx(ctx, concurrency.GOMAXPROCS),
		concurrency.GOMAXPROCS)
	plan, err := planner.plan(workerBudget, pv.operator, keys.Len())
	if err != nil {
		return docBitmap{}, containsFoldPlan{}, fmt.Errorf("%w: %w", entsInverted.ErrInternal, err)
	}

	isDenyList := pv.operator == filters.ContainsNone
	fold := containsFoldRunner{
		source:    source,
		keys:      keys,
		plan:      plan,
		logger:    s.logger,
		pool:      s.bitmapFactory.BufPool(),
		mergeConc: concurrency.BudgetFromCtxCapped(ctx, concurrency.SROAR_MERGE),
	}

	acc, accRelease, err := fold.run(ctx)
	if err != nil {
		return docBitmap{}, plan, err
	}
	return docBitmap{docIDs: acc, release: accRelease, isDenyList: isDenyList}, plan, nil
}

// containsBatchReaderSource opens a reader over part of a batch. A parallel
// fold calls it once per worker: a reader is forward-only over the keys it was
// given, so a worker cannot have its own position without its own reader.
type containsBatchReaderSource interface {
	newContainsBatchReader(keys entsInverted.SortedKeys) (containsBatchReader, error)
}

// roaringSetBatchReaderSource opens readers on one held view and keeps them.
// The workers write the counters stats() reads, so stats() is safe only once
// runWorkers has returned.
type roaringSetBatchReaderSource struct {
	view    lsmkv.NarrowedConsistentView
	readers []*lsmkv.RoaringSetBatchReader
}

func (s *roaringSetBatchReaderSource) newContainsBatchReader(
	keys entsInverted.SortedKeys,
) (containsBatchReader, error) {
	reader, err := lsmkv.NewRoaringSetBatchReader(s.view, keys)
	if err != nil {
		return nil, err
	}
	s.readers = append(s.readers, reader)
	return reader, nil
}

// stats sums what the readers did, reporting false when none ran: zeroed
// counters would otherwise read as "did no work" rather than "never got that
// far".
func (s *roaringSetBatchReaderSource) stats() (lsmkv.RoaringSetBatchReaderStats, bool) {
	var out lsmkv.RoaringSetBatchReaderStats
	for _, r := range s.readers {
		out.Add(r.Stats())
	}
	return out, len(s.readers) > 0
}

// containsBatchReader reads roaringset rows for one batched Contains fold under
// a single held view. One reader serves one goroutine.
//
// Next returns a row and its release, or an error and neither. Len equals the
// key count the reader was opened with.
type containsBatchReader interface {
	Len() int
	Next(mergeConc int) (*sroar.Bitmap, func(), error)
}

// How the batched Contains fold is sized.
const (
	// roaringContainerRange is the doc-ID span one roaring container covers.
	roaringContainerRange = 65536

	// roaringContainerMaxBytes is the largest a finalized roaring container can be:
	// a 4096-uint16 payload plus a 4-uint16 header, which array containers stay under.
	roaringContainerMaxBytes = 8200

	// containsFoldMemoryBudget bounds what fetching concurrently adds to ONE
	// fold: one filter leaf on one shard. Nothing sums it across a request's
	// filter children, its shards, or the requests in flight.
	containsFoldMemoryBudget int64 = 64 << 20

	// maxContainsFoldWorkers is the most workers the budget affords: a worker
	// costs at least its reader's window whatever the row size.
	maxContainsFoldWorkers = int(containsFoldMemoryBudget / lsmkv.BatchReaderWindowBytes)
)

// containsMinKeysPerWorker is the smallest share worth giving a worker. A worker
// costs a reader and one whole-partial merge, neither of which shrinks with the
// share, and a smaller share cannot repay them.
const containsMinKeysPerWorker = 32

// containsAccumulatorMinKeysPerWorker gates the union folds: below this many
// keys the incremental Or fold runs instead. Per WORKER, since each stages its
// own Accumulator. A var so BenchmarkDocIDs_ContainsAnyAccumulatorGate can
// sweep it.
var containsAccumulatorMinKeysPerWorker = 256

// containsWorkersOverrideForTests, when non-zero, pins the fetch-worker count,
// capped only by the key count. It outranks the query budget and the memory
// clamp, which is why nothing but a test or benchmark may set it.
var containsWorkersOverrideForTests = 0

func errUnsupportedContainsOperator(op filters.Operator) error {
	return fmt.Errorf("unsupported operator %q for batched contains", op.Name())
}

// errIntersectionSettled cancels the fold's own fetch and never reaches a
// caller: the fold answers with an empty bitmap.
var errIntersectionSettled = errors.New("contains fold: intersection settled")

// containsFoldStrategy is how a planned fold combines the rows it reads.
type containsFoldStrategy uint8

const (
	// ContainsAll: incremental merges with the empty-intersection early exit.
	// No accumulator variant — on disjoint-ish data the exit reads a handful of
	// keys, which no bulk-union structure beats.
	foldStrategyIntersection containsFoldStrategy = iota
	// a small ContainsAny/ContainsNone, merging rows one at a time
	foldStrategyUnionIncremental
	// a large one, depositing rows into an Accumulator
	foldStrategyUnionAccumulator
)

func (s containsFoldStrategy) String() string {
	switch s {
	case foldStrategyIntersection:
		return "intersection"
	case foldStrategyUnionIncremental:
		return "union-incremental"
	case foldStrategyUnionAccumulator:
		return "union-accumulator"
	default:
		// a value outside the enum says so rather than borrowing a real
		// strategy's name in the slow query log
		return fmt.Sprintf("unknown(%d)", uint8(s))
	}
}

// containsFoldPlanner decides how one batch will be folded.
type containsFoldPlanner struct {
	// docIDCount sizes the largest row a worker can hold: one past the highest
	// doc ID ever allocated.
	docIDCount uint64
}

// plan decides how a batch will be folded: the strategy from the operator and
// key count, the reader count from what workerBudget and the memory budget
// afford. workerBudget is the most workers the caller allows.
func (p containsFoldPlanner) plan(workerBudget int, op filters.Operator,
	numKeys int,
) (containsFoldPlan, error) {
	// numKeys needs no term of its own: numKeys/containsMinKeysPerWorker is
	// never larger, so "no more workers than keys" is subsumed, and the floor
	// gives a small batch exactly one worker.
	planned := p.clampWorkers(max(1, min(workerBudget,
		numKeys/containsMinKeysPerWorker)))

	switch op {
	case filters.ContainsAll:
		return containsFoldPlan{
			strategy: foldStrategyIntersection,
			workers:  planned,
		}.applyWorkersOverride(numKeys), nil

	case filters.ContainsAny, filters.ContainsNone:
		return p.planUnion(numKeys, planned).applyWorkersOverride(numKeys), nil

	default:
		// defensive: a non-Contains operator must never pick a fold — a silent
		// union here would return plausible but wrong results
		return containsFoldPlan{}, errUnsupportedContainsOperator(op)
	}
}

// planUnion picks between the union strategies. The Accumulator is skipped with
// too few keys to repay its materialization pass, and when one worker's worst
// case already exceeds the budget.
func (p containsFoldPlanner) planUnion(numKeys, planned int) containsFoldPlan {
	switch {
	// planned is at least 1, and integer division gives the smallest share the
	// split produces — the conservative one to gate on
	case numKeys/planned < containsAccumulatorMinKeysPerWorker,
		p.perWorkerFootprintBytes() > containsFoldMemoryBudget:
		return containsFoldPlan{strategy: foldStrategyUnionIncremental, workers: planned}
	default:
		return containsFoldPlan{strategy: foldStrategyUnionAccumulator, workers: planned}
	}
}

// clampWorkers bounds the planned reader count by how many worst-case workers
// the budget affords, flooring at one: the clamp bounds what concurrency adds,
// not the one reader's worth a fold needs regardless.
func (p containsFoldPlanner) clampWorkers(planned int) int {
	maxWorkers := int(containsFoldMemoryBudget / p.perWorkerFootprintBytes())
	return max(1, min(planned, maxWorkers))
}

// perWorkerFootprintBytes is what one worker holds at once: two row-sized
// structures plus its reader's window. Two because the running partial and the
// row being merged in are both live across Or.
func (p containsFoldPlanner) perWorkerFootprintBytes() int64 {
	return perWorkerFootprintFor(p.rowFootprintBytes())
}

// perWorkerFootprintFor is the arithmetic, taking the row size rather than
// deriving it. rowFootprintBytes saturates at its own ceiling before any doc-ID
// count reaches the boundary below, so this is the only way to walk the guard.
func perWorkerFootprintFor(row int64) int64 {
	maxContainsFoldLiveRows := int64(2)  // the running partial and the row merged into it
	maxContainsFoldMemtables := int64(2) // Active and Flushing, per [lsmkv.BucketConsistentView]

	// A footprint that wrapped negative would floor the clamp at one worker.
	if row > math.MaxInt64/(maxContainsFoldLiveRows+maxContainsFoldMemtables) {
		return math.MaxInt64
	}
	// The budget caps neither memtable: each takes its first row whatever it costs.
	window := max(int64(lsmkv.BatchReaderWindowBytes), maxContainsFoldMemtables*row)
	return maxContainsFoldLiveRows*row + window
}

// rowFootprintBytes is the worst case for one roaring-encoded structure over
// the shard's doc IDs: every range it could touch, each a full container. A
// row, a partial and an Accumulator's staging are all bounded by it. Costing
// the ceiling is what lets the fold size itself without reading data first.
func (p containsFoldPlanner) rowFootprintBytes() int64 {
	// Rounding up wraps once the count is within a range of the ceiling, which
	// would cost a row nothing and buy the fold every worker.
	if p.docIDCount > math.MaxUint64-(roaringContainerRange-1) {
		return math.MaxInt64
	}
	// A shard holding nothing spans no range, which falls out of the division
	// rather than needing a case of its own.
	ranges := (p.docIDCount + roaringContainerRange - 1) / roaringContainerRange
	return int64(ranges) * roaringContainerMaxBytes
}

// containsFoldPlan is the fold's decision: what to run, over how many readers
// (1 meaning sequential). Returned to the caller for the slow query log.
type containsFoldPlan struct {
	strategy containsFoldStrategy
	workers  int
}

// keyRangeFor returns worker w's share of numKeys keys, as a half-open range over
// the batch.
//
// The first numKeys%workers workers take one extra key, so no two shares
// differ by more than one: 4097 keys over 5 workers is 820/820/819/819/819.
func (p containsFoldPlan) keyRangeFor(numKeys, w int) (from, to int) {
	base, extra := numKeys/p.workers, numKeys%p.workers
	from = w*base + min(w, extra)
	to = from + base
	if w < extra {
		to++
	}
	return from, to
}

// applyWorkersOverride lets a test pin the reader count, capped by the key count. It
// does not re-run the strategy gate, so a sweep can vary the worker count with
// the strategy fixed — which is also the only way to reach an accumulator on a
// share below its own threshold.
func (p containsFoldPlan) applyWorkersOverride(numKeys int) containsFoldPlan {
	if containsWorkersOverrideForTests > 0 {
		p.workers = min(containsWorkersOverrideForTests, numKeys)
	}
	return p
}

// containsFoldRunner runs what containsFoldPlanner decided, over the readers
// the plan asked for. It lives for one docBitmapContainsBatch call and must not
// outlive the view its source opens readers on.
type containsFoldRunner struct {
	source containsBatchReaderSource
	keys   entsInverted.SortedKeys
	plan   containsFoldPlan
	logger logrus.FieldLogger
	// pool is where the accumulator strategy materializes its result; only that
	// strategy reads it.
	pool roaringset.BitmapBufPool
	// mergeConc is a sequential walk's merge budget, which is all of it since
	// nothing else runs. The parallel folds do not read it — runWorkers derives
	// a per-worker figure, their workers merging at the same time.
	mergeConc int
}

// run executes the strategy the plan chose, over one reader or several.
//
// Every fold checks the caller's context as it reads and none after, so a batch
// that finished before the cancellation landed keeps its result.
func (f containsFoldRunner) run(ctx context.Context) (*sroar.Bitmap, func(), error) {
	parallel := f.plan.workers > 1

	switch f.plan.strategy {
	case foldStrategyIntersection:
		if parallel {
			return f.incrementalParallel(ctx, filters.ContainsAll)
		}
		return f.incremental(ctx, filters.ContainsAll)

	case foldStrategyUnionIncremental:
		if parallel {
			return f.incrementalParallel(ctx, filters.ContainsAny)
		}
		return f.incremental(ctx, filters.ContainsAny)

	case foldStrategyUnionAccumulator:
		if parallel {
			return f.accumulatorParallel(ctx)
		}
		return f.accumulator(ctx)
	}

	// No default arm, so exhaustive reports a strategy added without one:
	// .golangci.yml sets default-signifies-exhaustive, which would exempt this
	// switch the moment it grew one. Unreachable today.
	return nil, nil, fmt.Errorf("%w: unsupported fold strategy %s",
		entsInverted.ErrInternal, f.plan.strategy)
}

// readRowErr names where in the batch a read failed. Row and range are 0-based
// indices into the whole batch, and from/to must be the caller's own share —
// that share is what identifies the worker that failed.
func readRowErr(i, from, to int, err error) error {
	return fmt.Errorf("read row %d of [%d,%d): %w", from+i, from, to, err)
}

// incremental merges rows one key at a time under op: union for ContainsAny,
// intersection for ContainsAll, which additionally stops as soon as the
// intersection is empty — no remaining key can change an empty result, so the
// exit skips only reads that cannot matter.
func (f containsFoldRunner) incremental(ctx context.Context, op filters.Operator,
) (*sroar.Bitmap, func(), error) {
	reader, err := f.source.newContainsBatchReader(f.keys)
	if err != nil {
		return nil, nil, err
	}

	var acc *sroar.Bitmap
	accRelease := noopRelease
	// The running partial is a pooled buffer in a local, and a panic unwinds
	// this frame with it. Cleared on the one path that hands it to the caller,
	// and nil after a failed merge, which released both operands itself.
	defer func() {
		if accRelease != nil {
			accRelease()
		}
	}()

	for i := range reader.Len() {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}

		bm, release, err := reader.Next(f.mergeConc)
		if err != nil {
			return nil, nil, readRowErr(i, 0, reader.Len(), err)
		}

		acc, accRelease, err = f.merge(op, f.mergeConc, acc, accRelease, bm, release)
		if err != nil {
			return nil, nil, err
		}

		if op == filters.ContainsAll && acc.IsEmpty() {
			break
		}
	}
	// non-nil, since the reader walks the batch it was opened with and that
	// holds at least one key, so the first iteration adopts its fetched bitmap
	out, outRelease := acc, accRelease
	accRelease = nil // the caller owns it from here
	return out, outRelease, nil
}

// accumulator unions the rows of all keys through a sroar.Accumulator: each
// fetched row is deposited into its dense per-64K-range staging and released
// immediately, and the final bitmap is assembled once, exactly sized.
func (f containsFoldRunner) accumulator(ctx context.Context) (*sroar.Bitmap, func(), error) {
	reader, err := f.source.newContainsBatchReader(f.keys)
	if err != nil {
		return nil, nil, err
	}

	// One budget for the read and the deposit: they never run at once, since a
	// row is read, then deposited, then released.
	acc := sroar.NewAccumulator().WithConc(f.mergeConc)
	for i := range reader.Len() {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		bm, release, err := reader.Next(f.mergeConc)
		if err != nil {
			return nil, nil, readRowErr(i, 0, reader.Len(), err)
		}
		// Or never retains bm, so the row's buffer goes straight back.
		acc.Or(bm)
		release()
	}

	result, release := f.pool.AccumulatorToBuf(acc)
	return result, release, nil
}

// incrementalParallel is the incremental fold with the batch split across
// workers, each merging its share into the shared result as soon as it is done.
//
// For ContainsAll the early exit is coarser than the sequential fold's — a
// worker can only prove its own share empty — so emptiness found there is
// genuine but may come later than a single walk would have found it.
func (f containsFoldRunner) incrementalParallel(ctx context.Context, op filters.Operator,
) (*sroar.Bitmap, func(), error) {
	// A cause, because gctx.Err() reads the same whether this fold stopped its
	// own fetch, the caller cancelled, or a sibling failed — and only the first
	// lets a worker drop what it has read.
	fetchCtx, cancelFetch := context.WithCancelCause(ctx)
	defer cancelFetch(nil)

	// merged is the shared result, added to under mu. Once runWorkers returns
	// they are all finished, so everything below reads it unlocked.
	var (
		mu            sync.Mutex
		merged        *sroar.Bitmap
		mergedRelease = noopRelease
	)
	// Released unless the fold hands it back: only the success return defuses
	// this, by setting mergedRelease to noopRelease first.
	defer func() { mergedRelease() }()

	err := f.runWorkers(fetchCtx,
		func(gctx context.Context, reader containsBatchReader, mergeConc, from, to int) error {
			var acc *sroar.Bitmap
			accRelease := noopRelease
			// A panic unwinds this frame with the pooled partial: the error
			// group recovers the panic into its own error, so nothing else
			// would put the buffer back. Cleared below by the one path that
			// hands the partial on, and nil after a failed merge, which
			// released both operands itself.
			defer func() {
				if accRelease != nil {
					accRelease()
				}
			}()

			for i := range reader.Len() {
				if gctx.Err() != nil {
					if errors.Is(context.Cause(gctx), errIntersectionSettled) {
						return nil
					}
					return gctx.Err()
				}
				bm, release, err := reader.Next(mergeConc)
				if err != nil {
					return readRowErr(i, from, to, err)
				}

				acc, accRelease, err = f.merge(op, mergeConc, acc, accRelease, bm, release)
				if err != nil {
					// merge released both operands
					return err
				}

				if op == filters.ContainsAll && acc.IsEmpty() {
					cancelFetch(errIntersectionSettled)
					break
				}
			}

			if acc == nil {
				return nil
			}

			mu.Lock()
			defer mu.Unlock()

			// the same fold over shares rather than over rows, and merge takes
			// the share whatever happens next, so nothing here frees it
			var err error
			merged, mergedRelease, err = f.merge(op, mergeConc, merged, mergedRelease, acc, accRelease)
			// merge owns the partial either way now, so the guard above must not
			// release it a second time.
			accRelease = nil
			if err != nil {
				// merge released both operands, so the shared slot holds nothing
				merged, mergedRelease = nil, noopRelease
				return err
			}

			if op == filters.ContainsAll && merged.IsEmpty() {
				// Shares still to arrive can only shrink this, so an empty
				// running result settles the batch.
				cancelFetch(errIntersectionSettled)
			}
			return nil
		})
	if err != nil {
		return nil, nil, err
	}

	// fetchCtx, not gctx: the error group cancels its own context once Wait
	// returns, success included, so gctx carries no usable cause by here.
	if errors.Is(context.Cause(fetchCtx), errIntersectionSettled) {
		return sroar.NewBitmap(), noopRelease, nil
	}

	result, release := merged, mergedRelease
	mergedRelease = noopRelease
	return result, release, nil
}

// accumulatorParallel is the accumulator fold with the batch split across
// workers, each depositing its share into its own Accumulator and merging that
// into the shared one when done.
//
// sroar's Accumulator is not safe for concurrent use: a worker owns its own
// until it hands it over, and the shared one is only touched under the mutex.
func (f containsFoldRunner) accumulatorParallel(ctx context.Context) (*sroar.Bitmap, func(), error) {
	var (
		mu     sync.Mutex
		merged *sroar.Accumulator
	)

	err := f.runWorkers(ctx,
		func(gctx context.Context, reader containsBatchReader, mergeConc, from, to int) error {
			// a fraction of the merge budget, not the whole of it — the
			// workers all deposit at once
			acc := sroar.NewAccumulator().WithConc(mergeConc)
			for i := range reader.Len() {
				if err := gctx.Err(); err != nil {
					return err
				}
				bm, release, err := reader.Next(mergeConc)
				if err != nil {
					return readRowErr(i, from, to, err)
				}
				acc.Or(bm)
				release()
			}

			mu.Lock()
			defer mu.Unlock()
			if merged == nil {
				// the first share to arrive becomes the destination rather than
				// being copied into an empty one
				merged = acc
				return nil
			}
			// still a share and not the whole budget: the workers that have not
			// finished are reading while this runs
			merged.OrAcc(acc)
			return nil
		})
	if err != nil {
		// An accumulator is plain memory rather than a pooled buffer, so
		// whatever was merged before the failure needs no release — unlike the
		// incremental fold's partials, it is simply dropped.
		return nil, nil, err
	}
	result, release := f.pool.AccumulatorToBuf(merged)
	return result, release, nil
}

// runWorkers gives each worker its own reader over its own share and runs them
// together; a worker merges what it produces itself.
//
// The empty-share skip below is a backstop no plan reaches: workers is capped
// at the key count, so every worker gets at least one key.
func (f containsFoldRunner) runWorkers(ctx context.Context,
	work func(gctx context.Context, reader containsBatchReader, mergeConc, from, to int) error,
) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	eg, gctx := enterrors.NewErrorGroupWithContextWrapper(f.logger, ctx)

	// Divided by the worker count, so the workers' merges together stay inside
	// the query's budget instead of each taking it whole. Under
	// DISABLE_SROAR_MERGE_BUDGET each worker takes the fixed constant instead.
	mergeConc := concurrency.BudgetFromCtxCapped(
		concurrency.ContextWithFractionalBudget(gctx, f.plan.workers, concurrency.GOMAXPROCS),
		concurrency.SROAR_MERGE)

	for w := 0; w < f.plan.workers; w++ {
		from, to := f.plan.keyRangeFor(f.keys.Len(), w)
		if from == to {
			continue
		}
		reader, err := f.source.newContainsBatchReader(f.keys.Sub(from, to))
		if err != nil {
			// Only w == 0 reaches this in production — the reader's one failure
			// is the view's strategy check, the same for every reader. Past it,
			// running workers may have failed on their own, so both are kept.
			cancel()
			if werr := eg.Wait(); werr != nil && !errors.Is(werr, context.Canceled) {
				return errors.Join(err, werr)
			}
			return err
		}

		// the reader is this goroutine's alone
		eg.Go(func() error {
			return work(gctx, reader, mergeConc, from, to)
		})
	}

	return eg.Wait()
}

// merge folds one bitmap into a running result, adopting it when there is
// nothing to merge into rather than copying.
//
// A panic between the read and this call drops the row's pooled buffer.
// Deliberately unguarded: nothing in the tree panics there — roaringset's own
// fires inside the read — and a guard costs a set-and-clear per row on every arm.
//
// On failure both operands have already been released, so a caller must not
// release again — unlike the read and cancellation paths, where its accumulator
// is still its own.
func (f containsFoldRunner) merge(op filters.Operator, mergeConc int,
	acc *sroar.Bitmap, accRelease func(), bm *sroar.Bitmap, release func(),
) (*sroar.Bitmap, func(), error) {
	if acc == nil {
		return bm, release, nil
	}
	return mergeAllowlistBitmaps(op, mergeConc, acc, accRelease, bm, release)
}

// mergeAllowlistBitmaps folds b into a under op (ContainsAny -> union,
// ContainsAll -> intersection), returning the result and its release and
// releasing the other operand.
//
// Both must be allowlists: a row is that key's additions minus its deletions,
// never a deny list, so the deny-list algebra mergeBitmapsAndOrWithDenyList
// carries is not needed here.
//
// Direction as mergeBitmapsAndOrWithDenyList picks it.
func mergeAllowlistBitmaps(op filters.Operator, maxConc int,
	a *sroar.Bitmap, aRelease func(), b *sroar.Bitmap, bRelease func(),
) (*sroar.Bitmap, func(), error) {
	switch op {
	case filters.ContainsAny:
		if a.NumContainers() < b.NumContainers() {
			a, aRelease, b, bRelease = b, bRelease, a, aRelease
		}
		a.OrConc(b, maxConc)
	case filters.ContainsAll:
		if a.NumContainers() > b.NumContainers() {
			a, aRelease, b, bRelease = b, bRelease, a, aRelease
		}
		a.AndConc(b, maxConc)
	default:
		aRelease()
		bRelease()
		return nil, nil, errUnsupportedContainsOperator(op)
	}
	bRelease()
	return a, aRelease, nil
}

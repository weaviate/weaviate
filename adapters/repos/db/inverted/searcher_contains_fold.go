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
	"fmt"

	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/entities/concurrency"
	"github.com/weaviate/weaviate/entities/filters"
	entsInverted "github.com/weaviate/weaviate/entities/inverted"
)

// docBitmapContainsBatch folds one batched Contains filter into a docBitmap.
// ContainsNone is NOT(ContainsAny): the same union marked a deny list, inverted
// once in Searcher.docIDs.
//
// Every per-key fetch is an OperatorEqual read on a roaringset bucket, so every
// row is an allowlist and the folds skip mergeBitmapsAndOrWithDenyList's
// deny-list algebra entirely.
func (s *Searcher) docBitmapContainsBatch(ctx context.Context, source containsBatchReaderSource,
	pv *propValuePair,
) (docBitmap, error) {
	keys := pv.containsKeys
	if keys.Len() == 0 {
		// defensive: the folds adopt their first row as the accumulator, so zero
		// keys yields a nil bitmap rather than an empty one
		return docBitmap{}, fmt.Errorf("%w: contains fold on prop %q carries no keys",
			entsInverted.ErrInternal, pv.prop)
	}

	strategy, err := containsFoldStrategyFor(pv.operator, keys.Len())
	if err != nil {
		return docBitmap{}, fmt.Errorf("%w: %w", entsInverted.ErrInternal, err)
	}

	isDenyList := pv.operator == filters.ContainsNone
	fold := containsFoldRunner{
		source:    source,
		keys:      keys,
		strategy:  strategy,
		pool:      s.bitmapFactory.BufPool(),
		mergeConc: concurrency.BudgetFromCtxCapped(ctx, concurrency.SROAR_MERGE),
	}

	acc, accRelease, err := fold.run(ctx)
	if err != nil {
		return docBitmap{}, err
	}
	return docBitmap{docIDs: acc, release: accRelease, isDenyList: isDenyList}, nil
}

// containsBatchReaderSource opens a reader over part of a batch. A reader is
// forward-only over the keys it was given, so a caller that wants a position of
// its own needs a reader of its own.
type containsBatchReaderSource interface {
	newContainsBatchReader(keys entsInverted.SortedKeys) (containsBatchReader, error)
}

// roaringSetBatchReaderSource opens readers on one held view and keeps them, so
// what they did can be reported once the fold has finished with them.
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

// containsAnyAccumulatorMinKeys gates the union folds: below this many keys the
// incremental Or fold runs instead, since an Accumulator's staging blocks and
// finalize scan are not worth setting up to union a handful of rows. A var so
// BenchmarkDocIDs_ContainsAnyAccumulatorGate can sweep it.
//
// The crossover is shape-dependent — clustered result doc IDs favour the
// Accumulator at far fewer keys than doc IDs spread across the ID space — so
// this is deliberately conservative; the large-N folds it exists for sit far
// above it either way.
var containsAnyAccumulatorMinKeys = 256

// containsFoldStrategy is how a fold combines the rows it reads.
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

// containsFoldStrategyFor picks the strategy from the operator and the key count.
func containsFoldStrategyFor(op filters.Operator, numKeys int) (containsFoldStrategy, error) {
	switch op {
	case filters.ContainsAll:
		return foldStrategyIntersection, nil
	case filters.ContainsAny, filters.ContainsNone:
		if numKeys < containsAnyAccumulatorMinKeys {
			return foldStrategyUnionIncremental, nil
		}
		return foldStrategyUnionAccumulator, nil
	default:
		// defensive: a non-Contains operator must never pick a fold — a silent
		// union here would return plausible but wrong results
		return 0, fmt.Errorf("unsupported operator %q for batched contains", op.Name())
	}
}

// containsFoldRunner runs the chosen strategy over the reader its source opens.
// It lives for one docBitmapContainsBatch call and must not outlive the view its
// source opens readers on.
type containsFoldRunner struct {
	source   containsBatchReaderSource
	keys     entsInverted.SortedKeys
	strategy containsFoldStrategy
	// pool is where the accumulator strategy materializes its result; only that
	// strategy reads it.
	pool roaringset.BitmapBufPool
	// mergeConc is the walk's merge budget, which is all of it since nothing else
	// runs.
	mergeConc int
}

// run executes the chosen strategy.
//
// Every fold checks the caller's context as it reads and none after, so a batch
// that finished before the cancellation landed keeps its result.
func (f containsFoldRunner) run(ctx context.Context) (*sroar.Bitmap, func(), error) {
	switch f.strategy {
	case foldStrategyIntersection:
		return f.incremental(ctx, filters.ContainsAll)
	case foldStrategyUnionIncremental:
		return f.incremental(ctx, filters.ContainsAny)
	case foldStrategyUnionAccumulator:
		return f.accumulator(ctx)
	}

	// No default arm, so exhaustive reports a strategy added without one:
	// .golangci.yml sets default-signifies-exhaustive, which would exempt this
	// switch the moment it grew one. Unreachable today.
	return nil, nil, fmt.Errorf("%w: unsupported fold strategy %s",
		entsInverted.ErrInternal, f.strategy)
}

// readRowErr names the position in the batch, counting from one, since a batch
// this size gives an operator nothing else to go on.
func readRowErr(i, n int, err error) error {
	return fmt.Errorf("read row %d of %d: %w", i+1, n, err)
}

// incremental merges rows one key at a time under op: union for ContainsAny,
// intersection for ContainsAll, which additionally stops as soon as the
// intersection is empty — no remaining key can change an empty result, so the
// exit skips only reads that cannot matter. It skips their cost too, from the
// next window on: the window holding the exit was filled whole, so the saving is
// a window's granularity, not a key's.
func (f containsFoldRunner) incremental(ctx context.Context, op filters.Operator,
) (*sroar.Bitmap, func(), error) {
	reader, err := f.source.newContainsBatchReader(f.keys)
	if err != nil {
		return nil, nil, err
	}

	var acc *sroar.Bitmap
	accRelease := noopRelease
	for i := range reader.Len() {
		if err := ctx.Err(); err != nil {
			accRelease()
			return nil, nil, err
		}

		bm, release, err := reader.Next(f.mergeConc)
		if err != nil {
			accRelease()
			return nil, nil, readRowErr(i, reader.Len(), err)
		}

		acc, accRelease, err = f.merge(op, f.mergeConc, acc, accRelease, bm, release)
		if err != nil {
			return nil, nil, err
		}

		if op == filters.ContainsAll && acc.IsEmpty() {
			break
		}
	}
	// non-nil, since the reader walks the batch it was opened with and that holds
	// at least one key, so the first iteration adopts its fetched bitmap
	return acc, accRelease, nil
}

// accumulator unions the rows of all keys through a sroar.Accumulator: each
// fetched row is deposited into its dense per-64K-range staging and released
// immediately, and the final bitmap is assembled once, exactly sized.
//
// This replaces one structural Or per key — an O(container) memmove even for a
// single-doc row — with O(1) bit deposits, bounding memory at the staging blocks
// plus one row in flight, plus the one memtable window its reader caches.
func (f containsFoldRunner) accumulator(ctx context.Context) (*sroar.Bitmap, func(), error) {
	reader, err := f.source.newContainsBatchReader(f.keys)
	if err != nil {
		return nil, nil, err
	}

	// TODO aliszka:gh12242 wire mergeConc into the accumulator's Or once sroar
	// supports concurrent deposits; today it bounds only the per-row disk merge
	// and the deposits themselves are single-threaded.
	acc := sroar.NewAccumulator()
	for i := range reader.Len() {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		bm, release, err := reader.Next(f.mergeConc)
		if err != nil {
			return nil, nil, readRowErr(i, reader.Len(), err)
		}
		// Or never retains bm, so the row's buffer goes straight back.
		acc.Or(bm)
		release()
	}

	result, release := f.pool.AccumulatorToBuf(acc)
	return result, release, nil
}

// merge folds one bitmap into a running result, adopting it when there is
// nothing to merge into rather than copying.
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
// releasing the other operand. Both must be allowlists.
//
// Direction as mergeBitmapsAndOrWithDenyList picks it: union the smaller into
// the larger, intersect the larger into the smaller, to minimize container
// operations. NumContainers is an O(1) header read.
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
		return nil, nil, fmt.Errorf("unsupported operator %q for batched contains", op.Name())
	}
	bRelease()
	return a, aRelease, nil
}

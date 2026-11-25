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

package sorter

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"slices"
	"time"

	"github.com/google/uuid"
	"github.com/weaviate/sroar"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/storobj"
)

type invertedSorter struct {
	store           *lsmkv.Store
	dataTypesHelper *dataTypesHelper

	// objects is pinned by the first tie that needs UUIDs, until the sort returns
	objects *pinnedObjects
}

type pinnedObjects struct {
	bucket        *lsmkv.Bucket
	count         int
	lookup        func(ctx context.Context, pos int, seckey, buffer []byte) ([]byte, []byte, error)
	releaseView   func()
	releaseBucket func()
}

// NewInvertedSorter constructs the specialised sorter that walks an
// inverted-index bucket whose *value* layout is a roaring bitmap of docIDs.
//
// Compared with the generic objects-bucket sorter this variant can avoid
// deserialising whole objects and instead streams the docIDs that already
// appear in the required byte order.  It wins whenever:
//
//   - The *first* ORDER-BY key is filterable (hence indexed) **and** uses a
//     byte-order-preserving encoding (date, int, number).
//
//   - Additional sort keys are optional.  Whenever two objects tie on the
//     current key, the sorter **recursively** invokes itself on the tied set
//     with the remaining keys, guaranteeing full lexicographic ordering without
//     resorting to an in-memory N log N sort.
//
// No locks are taken; the sorter assumes the underlying lsmkv.Store provides
// its usual read-concurrency guarantees.
func NewInvertedSorter(store *lsmkv.Store, dataTypesHelper *dataTypesHelper) *invertedSorter {
	return &invertedSorter{
		store:           store,
		dataTypesHelper: dataTypesHelper,
	}
}

// SortDocIDs returns at most *limit* document IDs ordered lexicographically by
// the sequence of sort clauses in *sort*. All candidate IDs come from *ids*,
// a pre-filtered allow-list.
//
// ## Fast path vs. Slow path
//
//   - **Fast path (ASC)** – a forward cursor walk on the roaring-set bucket of
//     the first sort key, stopping once *limit* matches are found.
//
//   - **Slow path (DESC)** – instead of a full reverse scan we probe the key
//     space at coarse quantiles (see `quantileKeysForDescSort`) and walk only the
//     tail window that can contain the last *limit* rows.
//
// IDs that tie on every clause are ordered by UUID, never by docID: docIDs
// differ between replicas, so pages would differ by the replica read.
//
// ## Handling multi-column ORDER BY
//
// When several rows tie on the current key **and** further sort clauses
// remain, the sorter
//
// 1. Builds an `AllowList` for just the tied IDs.
// 2. Recursively calls `sortDocIDsWithNesting` with the remaining clauses.
// 3. Splices the recursively ordered IDs back into the output slice.
//
// This yields full lexicographic ordering without an in-memory *N log N* sort
// and keeps memory usage ≤ *limit*.
//
// ## Early exit & complexity
//
//   - Each recursion level stops scanning as soon as *limit* IDs are finalised.
//   - **Best case** (few ties, small limit): *O(rowsHit)*.
//   - **Worst case** (deep recursion with many ties): *O(rowsHit × depth)*,
//     where *depth* ≤ `len(sort)`.
//
// A concise trace (rows scanned, seeks, recursion depth, elapsed time) is
// appended to the slow-query log stored in `ctx`, allowing operators to review
// plan quality under production load.
func (is *invertedSorter) SortDocIDs(
	ctx context.Context,
	limit int,
	sort []filters.Sort,
	ids helpers.AllowList,
) ([]uint64, error) {
	defer is.releaseObjects()
	return is.sortDocIDsWithNesting(ctx, limit, sort, ids, 0)
}

func (is *invertedSorter) pinObjects() (*pinnedObjects, error) {
	if is.objects != nil {
		return is.objects, nil
	}
	bucket, release := is.store.AcquireBucketForRead(helpers.ObjectsBucketLSM)
	if bucket == nil {
		return nil, fmt.Errorf("bucket %q: %w", helpers.ObjectsBucketLSM, lsmkv.ErrBucketNotFound)
	}
	count, err := bucket.CountApproximate()
	if err != nil {
		release()
		return nil, fmt.Errorf("count objects: %w", err)
	}
	is.objects = &pinnedObjects{bucket: bucket, count: count, releaseBucket: release}
	return is.objects, nil
}

func (is *invertedSorter) releaseObjects() {
	if is.objects == nil {
		return
	}
	if is.objects.releaseView != nil {
		is.objects.releaseView()
	}
	is.objects.releaseBucket()
	is.objects = nil
}

// get reads an object by docID; all lookups of a sort share one view.
func (p *pinnedObjects) get(ctx context.Context, docID []byte, buf []byte) ([]byte, []byte, error) {
	if p.lookup == nil {
		p.lookup, p.releaseView = p.bucket.SecondaryViewLookup()
	}
	return p.lookup(ctx, 0, docID, buf)
}

// sortDocIDsWithNesting is the entrypoint into a (potentially recursive) sort,
// the nesting indicator is purely used for the query slow log annotation
//
// on each entry, we make a new decision on whether to go down the ASC fast
// path or the DESC windowed walk
func (is *invertedSorter) sortDocIDsWithNesting(
	ctx context.Context,
	limit int,
	sort []filters.Sort,
	ids helpers.AllowList,
	nesting int,
) ([]uint64, error) {
	if len(sort) < 1 {
		// this should never happen, the query planner should already have chosen
		// another strategy
		return nil, fmt.Errorf("no sort clause provided, expected at least one sort clause")
	}

	propNames, orders, err := extractPropNamesAndOrders(sort)
	if err != nil {
		return nil, err
	}

	// pinned for the whole sort: the bucket backs every cursor opened below,
	// and an unpinned pointer can be shut down between the lookup and the scan
	bucketName := helpers.BucketFromPropNameLSM(propNames[0])
	bucket, release := is.store.AcquireBucketForRead(bucketName)
	if bucket == nil {
		return nil, fmt.Errorf("bucket %q: %w", bucketName, lsmkv.ErrBucketNotFound)
	}
	defer release()
	if bucket.Strategy() != lsmkv.StrategyRoaringSet {
		// this should never happen, the query planner should already have chosen
		// another strategy
		return nil, fmt.Errorf("expected roaring set bucket for property %s, got %s",
			propNames[0], bucket.Strategy())
	}

	switch orders[0] {
	case "asc":
		return is.sortRoaringSetASC(ctx, bucket, limit, sort, ids, nesting)
	case "desc":
		return is.sortRoaringSetDESC(ctx, bucket, limit, sort, ids, nesting)
	default:
		return nil, fmt.Errorf("unsupported sort order %s", orders[0])
	}
}

// sortRoaringSetASC walks the roaring set bucket in ascending order, i.e. the
// fast path. It stops after the row that reaches the limit; that row is read
// in full, because its ties are cut by UUID.
func (is *invertedSorter) sortRoaringSetASC(ctx context.Context, bucket *lsmkv.Bucket,
	limit int, sort []filters.Sort, ids helpers.AllowList, nesting int,
) ([]uint64, error) {
	startTime := time.Now()
	cursor := bucket.CursorRoaringSetCtx(ctx)
	defer cursor.Close()

	foundIDs := make([]uint64, 0, limit)
	rowsEvaluated := 0
	defer is.annotateASC(ctx, nesting, &rowsEvaluated, &foundIDs, startTime)

	for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
		rowsEvaluated++
		remaining := limit - len(foundIDs)
		var row []uint64
		var err error
		if ids == nil && len(sort) == 1 && v.GetCardinality() > remaining {
			// the row the limit cuts: pick from its bitmap without copying it
			row, err = is.firstByUUID(ctx, bitmapTieGroup(v), remaining)
		} else {
			row, err = is.orderRow(ctx, is.extractDocIDsFromBitmap(ids, v), sort, remaining, nesting)
		}
		if err != nil {
			return nil, err
		}
		foundIDs = append(foundIDs, row...)
		if len(foundIDs) >= limit {
			foundIDs = foundIDs[:limit]
			break
		}
	}

	return foundIDs, nil
}

// sortRoaringSetDESC uses quantile-keys to estimate a window where the
// matching IDs are contained. It will start with the last/highest window as
// the index is in ASC order, but we are interested in DESC values. If it
// cannot find enough IDs, it will move to the previous window. In the worst
// case (perfect negative correlation) this will lead to a full scan of the
// inverted index bucket.
//
// A DESC search can never exit a window early, it always needs to read the
// full window because the best matches will always be at the end of the
// window. The minimum read is always an entire window. However, it uses
// quantile keys to estimate good windows and should not use more than 1 or 2
// windows in most cases.
//
// Rows are ordered (nested clauses, then UUID) only once they are known to
// reach the result, so rows of a window beyond the limit cost no reads.
func (is *invertedSorter) sortRoaringSetDESC(
	ctx context.Context,
	bucket *lsmkv.Bucket,
	limit int,
	sort []filters.Sort,
	ids helpers.AllowList,
	nesting int,
) ([]uint64, error) {
	startTime := time.Now()
	qks := is.quantileKeysForDescSort(ctx, limit, ids, bucket, nesting)

	var rows [][]uint64 // highest value first
	seeksRequired := 0
	idCountBeforeCutoff := 0
	rowsEvaluated := 0
	whenComplete := is.annotateDESC(ctx, nesting, len(qks), startTime, &rowsEvaluated, &idCountBeforeCutoff, &seeksRequired)
	defer whenComplete()

	for qkIndex := len(qks) - 1; qkIndex >= 0; qkIndex-- {
		seeksRequired++
		startKey, endKey := cursorKeysForDESCWindow(qks, qkIndex)

		windowRows, rowsInWindow := is.processDESCWindow(ctx, bucket, startKey, endKey, ids)
		rowsEvaluated += rowsInWindow
		// the index is in ASC order, so the window's best rows are at its end
		for j := len(windowRows) - 1; j >= 0; j-- {
			rows = append(rows, windowRows[j])
			idCountBeforeCutoff += len(windowRows[j])
		}
		if idCountBeforeCutoff >= limit {
			// we have enough ids, no need to continue
			break
		}
	}

	foundIDs := make([]uint64, 0, min(limit, idCountBeforeCutoff))
	for _, row := range rows {
		if len(foundIDs) >= limit {
			break
		}
		row, err := is.orderRow(ctx, row, sort, limit-len(foundIDs), nesting)
		if err != nil {
			return nil, err
		}
		foundIDs = append(foundIDs, row[:min(len(row), limit-len(foundIDs))]...)
	}
	return foundIDs, nil
}

func cursorKeysForDESCWindow(qks [][]byte, qkIndex int) ([]byte, []byte) {
	startKey := qks[qkIndex]

	var endKey []byte
	if qkIndex < len(qks)-1 {
		endKey = qks[qkIndex+1]
	}

	return startKey, endKey
}

// processDESCWindow returns the matching rows of one window in ASC order.
// Unlike ASC it can never exit early: the best matches are at the window's end.
func (is *invertedSorter) processDESCWindow(
	ctx context.Context,
	bucket *lsmkv.Bucket,
	startKey []byte,
	endKey []byte,
	ids helpers.AllowList,
) ([][]uint64, int) {
	rowsEvaluated := 0
	var rows [][]uint64
	cursor := bucket.CursorRoaringSetCtx(ctx)
	defer cursor.Close()

	for k, v := cursor.Seek(startKey); k != nil; k, v = cursor.Next() {
		if endKey != nil && bytes.Compare(k, endKey) >= 0 {
			break
		}

		rowsEvaluated++
		if row := is.extractDocIDsFromBitmap(ids, v); len(row) > 0 {
			rows = append(rows, row)
		}
	}

	return rows, rowsEvaluated
}

// extractDocIDsFromBitmap returns the row's docIDs, in ascending order, that
// match the (pre-filtered) allow-list. Without an allow-list every docID
// matches.
func (is *invertedSorter) extractDocIDsFromBitmap(ids helpers.AllowList, bm *sroar.Bitmap) []uint64 {
	found := make([]uint64, 0, bm.GetCardinality())
	it := bm.NewIterator()
	for i := 0; i < bm.GetCardinality(); i++ {
		if id := it.Next(); ids == nil || ids.Contains(id) {
			found = append(found, id)
		}
	}
	return found
}

// orderRow orders the docIDs that share one value of the current clause: by
// the remaining clauses, then by UUID. Only the first keep are needed.
func (is *invertedSorter) orderRow(ctx context.Context, row []uint64, sort []filters.Sort,
	keep, nesting int,
) ([]uint64, error) {
	if len(row) < 2 {
		return row, nil
	}
	if len(sort) > 1 {
		return is.startNestedSort(ctx, row, sort[1:], nesting)
	}
	return is.firstByUUID(ctx, sliceTieGroup(row), keep)
}

// tieGroup is the docIDs that tie on every clause.
type tieGroup struct {
	n        int
	contains func(docID uint64) bool
	docIDs   func() []uint64
}

func bitmapTieGroup(bm *sroar.Bitmap) tieGroup {
	return tieGroup{n: bm.GetCardinality(), contains: bm.Contains, docIDs: bm.ToArray}
}

func sliceTieGroup(docIDs []uint64) tieGroup {
	var members *sroar.Bitmap
	return tieGroup{
		n: len(docIDs),
		contains: func(docID uint64) bool {
			if members == nil {
				members = roaringset.NewBitmap(docIDs...)
			}
			return members.Contains(docID)
		},
		docIDs: func() []uint64 { return docIDs },
	}
}

// uuidScanStepsPerLookup is how many objects a scan in UUID order passes in
// the time one object lookup by docID takes.
const uuidScanStepsPerLookup = 8

// firstByUUID returns the keep docIDs of the group with the smallest UUIDs, in
// UUID order. A docID whose object is gone is left out.
//
// Looking up every docID costs g.n lookups. Scanning the objects bucket, which
// is keyed by UUID, passes about keep*count/g.n objects when the group's UUIDs
// are spread evenly, so a large group scans. UUIDs that follow the sort value
// (time-ordered ids) can push the group to the end of the bucket, so the scan
// stops at what the lookups cost and falls back to them.
func (is *invertedSorter) firstByUUID(ctx context.Context, g tieGroup, keep int) ([]uint64, error) {
	objects, err := is.pinObjects()
	if err != nil {
		return nil, err
	}
	keep = min(keep, g.n)
	if g.n*g.n*uuidScanStepsPerLookup > keep*objects.count {
		out, complete, err := firstByUUIDScan(ctx, objects.bucket, g.contains, keep, g.n*uuidScanStepsPerLookup)
		if err != nil || complete {
			return out, err
		}
	}
	return firstByUUIDLookups(ctx, objects, g.docIDs(), keep)
}

func firstByUUIDLookups(ctx context.Context, objects *pinnedObjects, docIDs []uint64, keep int) ([]uint64, error) {
	type tied struct {
		id    uuid.UUID
		docID uint64
	}
	found := make([]tied, 0, len(docIDs))
	key := make([]byte, 8)
	var buf []byte
	for _, docID := range docIDs {
		binary.LittleEndian.PutUint64(key, docID)
		data, newBuf, err := objects.get(ctx, key, buf)
		if err != nil {
			return nil, fmt.Errorf("read doc id %d: %w", docID, err)
		}
		buf = newBuf
		if data == nil {
			continue
		}
		id, err := storobj.UUIDFromBinary(data)
		if err != nil {
			return nil, fmt.Errorf("read uuid of doc id %d: %w", docID, err)
		}
		found = append(found, tied{id: id, docID: docID})
	}
	slices.SortFunc(found, func(a, b tied) int { return bytes.Compare(a.id[:], b.id[:]) })

	out := docIDs[:0]
	for _, f := range found[:min(keep, len(found))] {
		out = append(out, f.docID)
	}
	return out, nil
}

// uuidScanWindow is how many memtable keys a scan in UUID order copies at a
// time, so a short scan never copies a whole memtable.
const uuidScanWindow = 1024

// firstByUUIDScan walks the objects bucket in UUID order and collects the
// first keep members. It gives up after budget objects; complete reports
// whether it finished.
func firstByUUIDScan(ctx context.Context, objects *lsmkv.Bucket, contains func(uint64) bool,
	keep, budget int,
) (out []uint64, complete bool, err error) {
	s := &uuidScan{contains: contains, keep: keep, budget: budget, out: make([]uint64, 0, keep)}
	if keep == 0 {
		return s.out, true, nil
	}
	var from []byte
	for {
		if err := ctx.Err(); err != nil {
			return nil, false, err
		}
		cursor, until := objects.CursorReplaceDigestReusableFirst(9, from, uuidScanWindow) // version and docID
		done, err := s.window(ctx, cursor, from)
		if err != nil || done || until == nil {
			return s.result(err)
		}
		from = append(bytes.Clone(until), 0) // the smallest key after until
	}
}

type uuidScan struct {
	contains            func(uint64) bool
	keep, budget, steps int
	out                 []uint64
	stopped             bool // out of budget
}

// window scans the cursor's keys from from and closes it. It reports whether
// the scan is done before the cursor ends.
func (s *uuidScan) window(ctx context.Context, cursor *lsmkv.CursorReplace, from []byte) (bool, error) {
	defer cursor.Close()

	var k, v []byte
	if from == nil {
		k, v = cursor.First()
	} else {
		k, v = cursor.Seek(from)
	}
	for ; k != nil; k, v = cursor.Next() {
		if s.steps == s.budget {
			s.stopped = true
			return true, nil
		}
		if s.steps++; s.steps%4096 == 0 {
			if err := ctx.Err(); err != nil {
				return true, err
			}
		}
		docID, err := storobj.DocIDFromBinary(v)
		if err != nil {
			return true, fmt.Errorf("read doc id of %x: %w", k, err)
		}
		if s.contains(docID) {
			if s.out = append(s.out, docID); len(s.out) == s.keep {
				return true, nil
			}
		}
	}
	return false, nil
}

func (s *uuidScan) result(err error) ([]uint64, bool, error) {
	if err != nil || s.stopped {
		return nil, false, err
	}
	return s.out, true, nil
}

func (is *invertedSorter) startNestedSort(
	ctx context.Context,
	ids []uint64,
	remainingSort []filters.Sort,
	nesting int,
) ([]uint64, error) {
	// we have identical ids and we have more than one sort clause, so we
	// need to start a sub-query to sort them
	helpers.AnnotateSlowQueryLogAppend(ctx, "sort_query_planner",
		helpers.SprintfWithNesting(nesting, "start sub-query for %d ids with identical value", len(ids)))

	sortedIDs, err := is.sortDocIDsWithNesting(ctx, len(ids), remainingSort, helpers.NewAllowList(ids...), nesting+1)
	if err != nil {
		return nil, fmt.Errorf("failed to sort ids with identical value: %w", err)
	}

	return sortedIDs, nil
}

func (is *invertedSorter) quantileKeysForDescSort(ctx context.Context, limit int,
	ids helpers.AllowList, invertedBucket *lsmkv.Bucket, nesting int,
) [][]byte {
	zeroByte := [][]byte{{0x00}}

	// a torn-down store leaves no bucket to count; fall through to the full
	// index scan below rather than dereferencing nil
	totalCount := 0
	if ob, release := is.store.AcquireBucketForRead(helpers.ObjectsBucketLSM); ob != nil {
		totalCount = ob.CountAsync()
		release()
	}
	if totalCount == 0 {
		// no objects, likely no disk segments yet, force a full index scan
		return zeroByte
	}
	var matchRate float64
	if ids == nil {
		// no allow-list, so we assume all IDs match
		matchRate = 1.0
	} else {
		matchRate = float64(ids.Len()) / float64(totalCount)
	}

	estimatedRowsHit := max(1, int(float64(limit)/matchRate*2)) // safety factor of 20
	if estimatedRowsHit > totalCount {
		helpers.AnnotateSlowQueryLogAppend(ctx, "sort_query_planner",
			helpers.SprintfWithNesting(nesting, "estimated rows hit (%d) is greater than total count (%d), "+
				"force a full index scan", estimatedRowsHit, totalCount))
		// full scan, just return zero byte (effectively same as cursor.First())
		return zeroByte
	}

	neededQuantiles := totalCount / estimatedRowsHit
	quantiles := invertedBucket.QuantileKeys(neededQuantiles)
	if len(quantiles) == 0 {
		// no quantiles found, this can happen if there are no disk segments, but
		// there could still be memtables, force a full scan
		helpers.AnnotateSlowQueryLogAppend(ctx, "sort_query_planner",
			"no quantiles found, force a full index scan")
		return zeroByte
	}

	return append(zeroByte, quantiles...)
}

func (is *invertedSorter) annotateASC(
	ctx context.Context,
	nesting int,
	rowsEvaluated *int,
	foundIDs *[]uint64,
	startTime time.Time,
) {
	helpers.AnnotateSlowQueryLogAppend(ctx, "sort_query_planner",
		helpers.SprintfWithNesting(nesting, "evaluated %d rows, found %d ids in %s",
			*rowsEvaluated, len(*foundIDs), time.Since(startTime)))
}

func (is *invertedSorter) annotateDESC(
	ctx context.Context,
	nesting int,
	qksLen int,
	startTime time.Time,
	rowsEvaluated, idCountBeforeCutoff, seeksRequired *int,
) func() {
	helpers.AnnotateSlowQueryLogAppend(ctx, "sort_query_planner",
		helpers.SprintfWithNesting(nesting, "identified %d quantile keys for descending sort", qksLen))

	return func() {
		helpers.AnnotateSlowQueryLogAppend(ctx, "sort_query_planner",
			helpers.SprintfWithNesting(nesting, "evaluated %d rows, found %d ids, "+
				"actual match ratio is %.2f, seeks required: %d (took %s)",
				*rowsEvaluated, *idCountBeforeCutoff, float64(*idCountBeforeCutoff)/float64(*rowsEvaluated),
				*seeksRequired, time.Since(startTime)))
	}
}

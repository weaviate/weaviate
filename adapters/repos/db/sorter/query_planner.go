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
	"fmt"
	"slices"
	"time"

	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/config/runtime"
)

const (

	// Will involve some random disk access
	FixedCostIndexSeek = 200

	FixedCostRowRead = 100

	FixedCostJSONUnmarshal = 500

	// FixedCostObjectsBucketRow is roughly made up of the cost of identifying
	// the correct key in the segment index (which is multiple IOPS), then
	// reading the whole row, which contains the full object (including the
	// vector). We are applying a 10x read penalty compared to inverted index
	// rows which tend to be fairly tiny. Last but not least, we need to do some
	// JSON marshalling to extract the desired prop value
	FixedCostObjectsBucketRow = FixedCostIndexSeek + 10*FixedCostRowRead + FixedCostJSONUnmarshal

	// correlationSampleSlices is how many quantile slices of the first sort
	// property's index emptyWalkPrefix samples, correlationSampleIDs how many IDs
	// it checks, in whole rows, at the start of each.
	correlationSampleSlices = 32
	correlationSampleIDs    = 32

	// minExpectedSampleHits is the match count emptyWalkPrefix's empty samples
	// must have been expected to hold. Seeing none of 5 expected happens by
	// chance under 1% of the time.
	minExpectedSampleHits = 5

	// FullScanCandidateShare is the share of a shard's objects past which
	// looking up each candidate costs more than reading every object with a
	// cursor, which skips the index seek.
	FullScanCandidateShare = float64(10*FixedCostRowRead+FixedCostJSONUnmarshal) / FixedCostObjectsBucketRow

	// FixedCostInvertedBucketRow is the cost of reading the inverted index,
	// since it is already sorted in ascending order, we can skip the index and
	// start a cursor which has no cost other than reading the payload in each
	// iteration.
	FixedCostInvertedBucketRow = FixedCostRowRead
)

type queryPlanner struct {
	store            *lsmkv.Store
	dataTypesHelper  *dataTypesHelper
	invertedDisabled *runtime.DynamicValue[bool]
}

// NewQueryPlanner wires a lightweight cost-based planner around an lsmkv.Store.
// The planner’s only job is to decide—per user query—whether it is cheaper to
// satisfy an ORDER-BY clause by
//
//  1. streaming rows directly from the objects bucket and sorting in memory, or
//  2. exploiting the fact that an inverted-index bucket is already ordered
//     lexicographically on the property being sorted.
//
// Both the store and the DataTypesHelper are passed by reference and may be
// shared across many concurrent planners; the function does not assume
// ownership or perform any locking.
func NewQueryPlanner(store *lsmkv.Store, dataTypesHelper *dataTypesHelper,
	invertedDisabled *runtime.DynamicValue[bool],
) *queryPlanner {
	return &queryPlanner{
		store:            store,
		dataTypesHelper:  dataTypesHelper,
		invertedDisabled: invertedDisabled,
	}
}

// EstimateCosts returns two independent cost estimates—(objectsCost,
// invertedCost)—for producing the first *limit* rows of a sorted result:
//
//   - objectsCost   – walk the objects bucket, deserialize the full object,
//     extract the sort value, keep the top-N in memory.
//
//   - invertedCost  – open a cursor on the inverted-index bucket that is
//     already sorted by the required key and stream until N
//     matches have been found (with extra work if DESC order
//     forces a reverse scan). A multi-property sort adds reading
//     the first limit candidates from the objects bucket.
//
// Costs are *dimensionless units* calibrated to roughly match the relative
// latency of random I/O, sequential I/O and JSON unmarshalling on a modern
// NVMe or similar system.  The estimate deliberately:
//
//   - Discounts the first *limit* object fetches because the query will need
//     those pages anyway, hence they are almost always in cache.
//
//   - Inflates random seeks (≃ FixedCostIndexSeek) so that HDD-backed
//     deployments or other IOPS-constrained systems (e.g. EBS volumes)
//     still pick the right plan.
//
//   - Ignores filter/sort correlation: the result is slightly pessimistic when
//     the predicate is negatively correlated with the sort key, optimistic when
//     positively correlated.  That simplification keeps cost evaluation O(1) and
//     good-enough in practice.
//
// The method appends a human-readable trace of its arithmetic to the slow-query
// log embedded in *ctx* so that operators can review the decision later.
func (s *queryPlanner) EstimateCosts(ctx context.Context, ids helpers.AllowList, limit int,
	sort []filters.Sort,
) (float64, float64) {
	totalObjects := countObjects(s.store)
	var matches int
	if ids == nil {
		matches = totalObjects
	} else {
		matches = ids.Len()
	}
	var filterMatchRatio float64
	if totalObjects == 0 {
		// this can happen when there are no disk segments yet, just assume a 100% match ratio
		filterMatchRatio = 1.0
	} else {
		filterMatchRatio = float64(matches) / float64(totalObjects)
	}

	helpers.AnnotateSlowQueryLogAppend(ctx, "sort_query_planner",
		fmt.Sprintf("matches=%d, limit=%d, filterMatchRatio=%.2f",
			matches, limit, filterMatchRatio))

	// The rationale for the discount is that we will eventually read n=limit
	// objects from the object store anyway, so pages are extremely likely to be
	// cached. Therefore we can discount the disk interaction subustantially.
	//
	// This effectively means for queries where matches<=limit, the objects
	// bucket strategy is a lot more attractive
	costObjectsDiscounted := discountedObjectReads(limit, matches)
	costObjectsBucketUndiscounted := float64(FixedCostObjectsBucketRow * max(matches-limit, 0))
	costObjectsBucket := costObjectsDiscounted + costObjectsBucketUndiscounted
	helpers.AnnotateSlowQueryLogAppend(ctx, "sort_query_planner",
		fmt.Sprintf("estimated costs for objects bucket strategy: %.2f (discounted: %.2f, undiscounted: %.2f)",
			costObjectsBucket, costObjectsDiscounted, costObjectsBucketUndiscounted))

	walkedIDs := float64(limit) / filterMatchRatio
	costInvertedBucket := invertedBucketCost(walkedIDs, costObjectsDiscounted, sort)
	if sort[0].Order == "desc" {
		helpers.AnnotateSlowQueryLogAppend(ctx, "sort_query_planner",
			fmt.Sprintf("sort order is 'desc', inverted bucket walk cost adjusted from %.2f to %.2f",
				FixedCostInvertedBucketRow*walkedIDs, invertedBucketCost(walkedIDs, 0, sort[:1])))
	}
	if len(sort) > 1 {
		helpers.AnnotateSlowQueryLogAppend(ctx, "sort_query_planner",
			fmt.Sprintf("multiple (%d) sort criteria, inverted bucket cost includes "+
				"reading candidates from objects bucket: %.2f", len(sort), costObjectsDiscounted))
	}

	helpers.AnnotateSlowQueryLogAppend(ctx, "sort_query_planner",
		fmt.Sprintf("estimated costs for inverted bucket strategy: %.2f", costInvertedBucket))
	return costObjectsBucket, costInvertedBucket
}

// countObjects returns the objects bucket's flushed count, without the memtable,
// or 0 for a torn-down store. The pin keeps the count from racing a teardown
// that starts right after the lookup.
func countObjects(store *lsmkv.Store) int {
	bucket, release := store.AcquireBucketForRead(helpers.ObjectsBucketLSM)
	if bucket == nil {
		return 0
	}
	defer release()
	return bucket.CountAsync()
}

// discountedObjectReads is the cost of reading the first limit matches from
// the objects bucket, see EstimateCosts for the discount.
func discountedObjectReads(limit, matches int) float64 {
	return float64(min(limit, matches) * (0.3*(FixedCostRowRead+FixedCostIndexSeek) + FixedCostJSONUnmarshal))
}

// invertedBucketCost is the inverted strategy's cost for a walk past walkedIDs
// index entries. A descending walk pays a safety margin and a seek for its
// guessed entrypoint, a multi-property sort candidateReads for its candidates.
func invertedBucketCost(walkedIDs, candidateReads float64, sort []filters.Sort) float64 {
	cost := FixedCostInvertedBucketRow * walkedIDs
	if sort[0].Order == "desc" {
		cost = 2*cost + FixedCostIndexSeek
	}
	if len(sort) > 1 {
		cost += candidateReads
	}
	return cost
}

// Do reports whether the inverted index of the first sort property serves this
// sort more cheaply than the objects bucket. Each reason it says no is
// recorded in the slow-query trace.
func (s *queryPlanner) Do(ctx context.Context, ids helpers.AllowList, limit int, sort []filters.Sort) (useInverted bool, err error) {
	startTime := time.Now()
	helpers.AnnotateSlowQueryLogAppend(ctx, "sort_query_planner", "START PLANNING")
	defer func() {
		helpers.AnnotateSlowQueryLogAppend(ctx, "sort_query_planner",
			fmt.Sprintf("COMPLETED PLANNING in %s", time.Since(startTime)))
	}()

	useInverted = false
	propNames, _, err := extractPropNamesAndOrders(sort)
	if err != nil {
		return useInverted, err
	}

	dt := s.dataTypesHelper.getType(propNames[0])
	switch dt {
	case schema.DataTypeDate, schema.DataTypeInt, schema.DataTypeNumber:
		// supported data type

	default:
		helpers.AnnotateSlowQueryLogAppend(ctx, "sort_query_planner",
			fmt.Sprintf("data type '%s' of property '%s' is currently not supported by "+
				"inverted strategy, using objects bucket strategy", dt, propNames[0]))
		return useInverted, err

	}

	if limit == 0 && len(sort) > 1 {
		helpers.AnnotateSlowQueryLogAppend(ctx, "sort_query_planner",
			"limit 0 sorts every match, which the candidate walk would read anyway, "+
				"using objects bucket strategy")
		return useInverted, err
	}

	costObjectsBucket, costInvertedBucket := s.EstimateCosts(ctx, ids, limit, sort)
	if costInvertedBucket > costObjectsBucket {
		// inverted strategy is more expensive, no need to plan further, just use
		// objects strategy
		return useInverted, err
	}

	if ids == nil && len(sort) > 1 {
		if maxCandidates := int(FullScanCandidateShare * float64(countObjects(s.store))); limit > maxCandidates {
			helpers.AnnotateSlowQueryLogAppend(ctx, "sort_query_planner",
				fmt.Sprintf("limit %d exceeds %d candidates, the share of objects worth a lookup each, "+
					"using objects bucket strategy", limit, maxCandidates))
			return useInverted, err
		}
	}

	costSavings := float64(costObjectsBucket) / float64(costInvertedBucket)

	if s.store.Bucket(helpers.BucketFromPropNameLSM(propNames[0])) == nil {
		// no bucket found could mean that property is not indexed or some
		// unexpected error
		if !s.dataTypesHelper.hasFilterableIndex(propNames[0]) {
			helpers.AnnotateSlowQueryLogAppend(ctx, "sort_query_planner",
				fmt.Sprintf("property '%s' is not indexed (filterable), the query planner "+
					"predicts an estimated cost savings of %.2fx, consider indexing this property, "+
					"falling back to objects bucket strategy", propNames[0], costSavings))
			return useInverted, err
		}

		// the prop is indexed, but no bucket found, this is unexpected, but we can
		// still fall back to the slower strategy
		helpers.AnnotateSlowQueryLogAppend(ctx, "sort_query_planner",
			fmt.Sprintf("unexpected: property '%s' is indexed (filterable), but no bucket found, "+
				"falling back to objects bucket strategy", propNames[0]))
		return useInverted, err
	}

	if s.dataTypesHelper.filterableDisabled(propNames[0]) {
		helpers.AnnotateSlowQueryLogAppend(ctx, "sort_query_planner",
			fmt.Sprintf("property '%s' is not filterable yet, its index may still be building, "+
				"falling back to objects bucket strategy", propNames[0]))
		return useInverted, err
	}

	if s.invertedDisabled.Get() {
		helpers.AnnotateSlowQueryLogAppend(ctx, "sort_query_planner",
			fmt.Sprintf("the query planner predicts an estimated cost savings of %.2fx, "+
				"however the inverted sorter is globally disabled using a feature flag",
				costSavings))
		return useInverted, err
	}

	if ids != nil {
		if sampledCost, ok := s.sampledInvertedCost(ctx, ids, limit, sort, costObjectsBucket); ok {
			helpers.AnnotateSlowQueryLogAppend(ctx, "sort_query_planner",
				fmt.Sprintf("estimated costs for inverted bucket strategy from sampled match positions: %.2f",
					sampledCost))
			if sampledCost > costObjectsBucket {
				helpers.AnnotateSlowQueryLogAppend(ctx, "sort_query_planner",
					"matches sit far into the inverted index walk, falling back to objects bucket strategy")
				return useInverted, err
			}
			costSavings = costObjectsBucket / sampledCost
		}
	}

	helpers.AnnotateSlowQueryLogAppend(ctx, "sort_query_planner",
		fmt.Sprintf("predicted cost savings of %.2fx for inverted sorter on property '%s'",
			costSavings, propNames[0]))
	useInverted = true
	return useInverted, err
}

// sampledInvertedCost is EstimateCosts' inverted cost with the walk lengthened
// by emptyWalkPrefix. ok is false when emptyWalkPrefix returns 0, or sampling is
// skipped because even a walk over the whole index costs less than costObjects.
func (s *queryPlanner) sampledInvertedCost(ctx context.Context, ids helpers.AllowList, limit int,
	sort []filters.Sort, costObjects float64,
) (cost float64, ok bool) {
	totalObjects := countObjects(s.store)
	matches := ids.Len()
	if totalObjects == 0 || matches == 0 {
		return 0, false
	}
	candidateReads := discountedObjectReads(limit, matches)
	if invertedBucketCost(float64(totalObjects), candidateReads, sort) <= costObjects {
		helpers.AnnotateSlowQueryLogAppend(ctx, "sort_query_planner",
			"walking the whole inverted index costs less than objects bucket strategy, skipping sampling")
		return 0, false
	}
	bucket, release := s.store.AcquireBucketForRead(helpers.BucketFromPropNameLSM(sort[0].Path[0]))
	if bucket == nil {
		return 0, false
	}
	defer release()

	matchRatio := float64(matches) / float64(totalObjects)
	prefix := emptyWalkPrefix(ctx, bucket, ids, matchRatio, totalObjects, sort[0].Order == "desc")
	if prefix == 0 {
		return 0, false
	}
	walkedIDs := prefix + float64(limit)/matchRatio
	return invertedBucketCost(walkedIDs, candidateReads, sort), true
}

// emptyWalkPrefix estimates how many index entries the inverted walk passes
// before its first match, sampling the start of each quantile slice in walk order.
// It returns 0 when no slice matched or too few IDs were sampled to tell.
func emptyWalkPrefix(ctx context.Context, bucket *lsmkv.Bucket, ids helpers.AllowList,
	matchRatio float64, totalObjects int, desc bool,
) float64 {
	qks := bucket.QuantileKeys(correlationSampleSlices)
	if len(qks) == 0 {
		return 0
	}

	// slice i starts at starts[i] and ends before starts[i+1], and a nil start
	// is the index's first key
	starts := append([][]byte{nil}, qks...)
	order := make([]int, len(starts))
	for i := range order {
		order[i] = i
	}
	if desc {
		slices.Reverse(order)
	}

	cursor := bucket.CursorRoaringSetCtx(ctx)
	defer cursor.Close()
	sampled, emptySlices := 0, 0
	for _, i := range order {
		var end []byte
		if i+1 < len(starts) {
			end = starts[i+1]
		}
		checked, hit := sampleSlice(cursor, starts[i], end, ids)
		if hit {
			break
		}
		sampled += checked
		emptySlices++
	}

	// no slice hit, so the matches sit in stretches the samples skipped
	if emptySlices == len(starts) || float64(sampled)*matchRatio < minExpectedSampleHits {
		return 0
	}
	return float64(emptySlices) * float64(totalObjects) / float64(len(starts))
}

// sampleSlice checks whole rows at or after start, and before end, against ids
// until correlationSampleIDs IDs are checked or a row matches.
func sampleSlice(cursor lsmkv.CursorRoaringSet, start, end []byte, ids helpers.AllowList) (checked int, hit bool) {
	var k []byte
	var bm *sroar.Bitmap
	if start == nil {
		k, bm = cursor.First()
	} else {
		k, bm = cursor.Seek(start)
	}

	for ; k != nil && checked < correlationSampleIDs; k, bm = cursor.Next() {
		if end != nil && bytes.Compare(k, end) >= 0 {
			break
		}
		card := bm.GetCardinality()
		if rowIntersects(ids, bm, card) {
			return checked, true
		}
		checked += card
	}
	return checked, false
}

func rowIntersects(ids helpers.AllowList, row *sroar.Bitmap, card int) bool {
	if bl, ok := ids.(*helpers.BitmapAllowList); ok {
		return bl.Bm.Intersects(row)
	}
	it := row.NewIterator()
	for n := card; n > 0; n-- {
		if ids.Contains(it.Next()) {
			return true
		}
	}
	return false
}

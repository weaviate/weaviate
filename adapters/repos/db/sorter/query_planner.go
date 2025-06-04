//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package sorter

import (
	"context"
	"fmt"
	"time"

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
//     forces a reverse scan).
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
	totalObjects := s.store.Bucket(helpers.ObjectsBucketLSM).CountAsync()
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
	costObjectsDiscounted := float64(min(limit, matches) * (0.3*(FixedCostRowRead+FixedCostIndexSeek) + FixedCostJSONUnmarshal))
	costObjectsBucketUndiscounted := float64(FixedCostObjectsBucketRow * max(matches-limit, 0))
	costObjectsBucket := costObjectsDiscounted + costObjectsBucketUndiscounted
	helpers.AnnotateSlowQueryLogAppend(ctx, "sort_query_planner",
		fmt.Sprintf("estimated costs for objects bucket strategy: %.2f (discounted: %.2f, undiscounted: %.2f)",
			costObjectsBucket, costObjectsDiscounted, costObjectsBucketUndiscounted))

	costInvertedBucket := float64(FixedCostInvertedBucketRow*limit) / filterMatchRatio

	if sort[0].Order == "desc" {
		// If the sort order is descending, we need to guess an entrypoint and then
		// read from there. This requires a safety margin and a seek
		//
		// Note: We are not estimating filter correlation at this point, so in the
		// worst case (perfect negative correlation) the cost could be much higher
		initial := costInvertedBucket
		costInvertedBucket *= 2
		costInvertedBucket += FixedCostIndexSeek
		helpers.AnnotateSlowQueryLogAppend(ctx, "sort_query_planner",
			fmt.Sprintf("sort order is 'desc', inverted bucket cost adjusted from %.2f to %.2f",
				initial, costInvertedBucket))

	}

	helpers.AnnotateSlowQueryLogAppend(ctx, "sort_query_planner",
		fmt.Sprintf("estimated costs for inverted bucket strategy: %.2f", costInvertedBucket))
	return costObjectsBucket, costInvertedBucket
}

// Do is the public entry point that turns a query description into a binary
// plan choice.  It returns *useInverted == true* when **all** of the following
// hold:
//
//   - The inverted-index strategy is cheaper according to EstimateCosts.
//
//   - The key’s logical type (date, int, number) preserves the same ordering
//     at the byte level that the inverted index uses.
//
//   - The LSM bucket for that property is present; if not, the method
//     opportunistically reminds the caller that marking the field as
//     filterable would unlock the faster plan.
//
// When any pre-condition fails, Do silently falls back to the objects-bucket
// scan and records the reason in the slow-query trace.
func (s *queryPlanner) Do(ctx context.Context, ids helpers.AllowList, limit int, sort []filters.Sort) (useInverted bool, err error) {
	startTime := time.Now()
	helpers.AnnotateSlowQueryLogAppend(ctx, "sort_query_planner", "START PLANNING")
	defer func() {
		helpers.AnnotateSlowQueryLogAppend(ctx, "sort_query_planner",
			fmt.Sprintf("COMPLETED PLANNING in %s", time.Since(startTime)))
	}()

	costObjectsBucket, costInvertedBucket := s.EstimateCosts(ctx, ids, limit, sort)

	useInverted = false
	if costInvertedBucket > costObjectsBucket {
		// inverted strategy is more expensive, no need to plan further, just use
		// objects strategy
		return
	}

	if len(sort) > 1 {
		helpers.AnnotateSlowQueryLogAppend(ctx, "sort_query_planner",
			fmt.Sprintf("inverted strategy has lower cost, but query has multiple (%d) "+
				"sort criteria which is currently not supported, "+
				"fallback to objects bucket strategy", len(sort)))
		return
	}

	propNames, _, err := extractPropNamesAndOrders(sort)
	if err != nil {
		return
	}

	dt := s.dataTypesHelper.getType(propNames[0])
	switch dt {
	case schema.DataTypeDate, schema.DataTypeInt, schema.DataTypeNumber:
		// supported data type

	default:
		helpers.AnnotateSlowQueryLogAppend(ctx, "sort_query_planner",
			fmt.Sprintf("inverted strategy has lower cost, but data type '%s' of property '%s' "+
				"is currently not supported, falling back to objects bucket strategy", dt, propNames[0]))
		return

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
			return
		}

		// the prop is indexed, but no bucket found, this is unexpected, but we can
		// still fall back to the slower strategy
		helpers.AnnotateSlowQueryLogAppend(ctx, "sort_query_planner",
			fmt.Sprintf("unexpected: property '%s' is indexed (filterable), but no bucket found, "+
				"falling back to objects bucket strategy", propNames[0]))
		return
	}

	if s.invertedDisabled.Get() {
		helpers.AnnotateSlowQueryLogAppend(ctx, "sort_query_planner",
			fmt.Sprintf("the query planner predicts an estimated cost savings of %.2fx, "+
				"however the inverted sorter is globally disabled using a feature flag",
				costSavings))
		return
	}

	helpers.AnnotateSlowQueryLogAppend(ctx, "sort_query_planner",
		fmt.Sprintf("predicted cost savings of %.2fx for inverted sorter on property '%s'",
			costSavings, propNames[0]))
	useInverted = true
	return
}

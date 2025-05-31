package sorter

import (
	"context"
	"fmt"
	"time"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/schema"
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

func (s *lsmSorter) estimateCosts(ctx context.Context, ids helpers.AllowList, limit int,
	sort []filters.Sort,
) (float64, float64) {
	matches := ids.Len()
	totalObjects := s.bucket.CountAsync()
	filterMatchRatio := float64(matches) / float64(totalObjects)

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

func (s *lsmSorter) queryPlan(ctx context.Context, ids helpers.AllowList, limit int, sort []filters.Sort) (useInverted bool, err error) {
	startTime := time.Now()
	helpers.AnnotateSlowQueryLogAppend(ctx, "sort_query_planner", "START PLANNING")
	defer func() {
		helpers.AnnotateSlowQueryLogAppend(ctx, "sort_query_planner",
			fmt.Sprintf("COMPLETED PLANNING in %s", time.Since(startTime)))
	}()

	costObjectsBucket, costInvertedBucket := s.estimateCosts(ctx, ids, limit, sort)

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

	if s.store.Bucket(helpers.BucketFromPropNameLSM(propNames[0])) == nil {
		// no bucket found could mean that property is not indexed or some
		// unexpected error
		if !s.dataTypesHelper.hasFilterableIndex(propNames[0]) {
			costSavings := float64(costObjectsBucket) / float64(costInvertedBucket)
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

	useInverted = true
	return
}

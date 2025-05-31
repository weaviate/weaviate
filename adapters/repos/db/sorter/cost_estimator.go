package sorter

import (
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/entities/filters"
)

const (

	// Will involve some random disk access
	FixedCostIndexSeek = 200

	FixedCostRowRead = 100

	FixedCostJSONUnmarshal = 100

	// FixedCostObjectsBucketRow is roughly made up of the cost of identifying
	// the correct key in the segment index (which is multiple IOPS), then
	// reading the whole row, which contains the full object (including the
	// vector). Last but not least, we need to do some JSON marshalling to
	// extract the desired prop value
	FixedCostObjectsBucketRow = FixedCostIndexSeek + 3*FixedCostRowRead + FixedCostJSONUnmarshal

	// FixedCostInvertedBucketRow is the cost of reading the inverted index,
	// since it is already sorted in ascending order, we can skip the index and
	// start a cursor which has no cost other than reading the payload in each
	// iteration.
	FixedCostInvertedBucketRow = FixedCostRowRead
)

func (s *lsmSorter) estimateCosts(ids helpers.AllowList, limit int,
	sort []filters.Sort,
) (float64, float64) {
	matches := ids.Len()
	totalObjects := s.bucket.CountAsync()

	filterMatchRatio := float64(matches) / float64(totalObjects)

	costObjectsBucket := float64(FixedCostObjectsBucketRow * matches)
	costInvertedBucket := float64(FixedCostInvertedBucketRow*limit) / filterMatchRatio

	if sort[0].Order == "desc" {
		// If the sort order is descending, we need to guess an entrypoint and then
		// read from there. This requires a safety margin and a seek
		//
		// Note: We are not estimating filter correlation at this point, so in the worst case (negative correlation) the cost could be much higher
		costInvertedBucket *= 2
		costInvertedBucket += FixedCostIndexSeek
	}
	return costObjectsBucket, costInvertedBucket
}

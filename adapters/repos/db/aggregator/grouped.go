package aggregator

import (
	"context"
	"fmt"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/entities/aggregation"
)

// groupedAggregator performs aggregation in groups. This is a two step
// processes. First a whole-db scan is performed to identify the groups, then
// the top-n groups are selected (the rest is discarded). Only for those top
// groups an actual aggregation is performed
type groupedAggregator struct {
	*Aggregator
}

func newGroupedAggregator(agg *Aggregator) *groupedAggregator {
	return &groupedAggregator{Aggregator: agg}
}

func (ga *groupedAggregator) Do(ctx context.Context) (*aggregation.Result, error) {
	out := aggregation.Result{}

	if ga.params.Filters != nil {
		// s := fa.getSchema.GetSchemaSkipAuth()
		// ids, err := inverted.NewSearcher(fa.db, s, fa.invertedRowCache).
		// 	DocIDs(ctx, fa.params.Filters, false, fa.params.ClassName)
		// if err != nil {
		// 	return nil, errors.Wrap(err, "retrieve doc IDs from searcher")
		// }
		return nil, fmt.Errorf("combining groupBy and filter not supported yet")
	}

	groups, err := ga.identifyGroups(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "identify groups")
	}

	out.Groups = make([]aggregation.Group, len(groups))
	for i, g := range groups {
		res, err := ga.aggregateGroup(ctx, g.res, g.docIDs)
		if err != nil {
			return nil, errors.Wrapf(err, "aggregate group %d (%v)", i,
				g.res.GroupedBy.Value)
		}
		out.Groups[i] = res
	}

	return &out, nil
}

// group is a helper construct that contains the final aggregation.Group which
// will evenetually be served to the user. But it also contains the list of
// docIDs in that group, so we can use those to perform the actual aggregation
// (for each group) in a second step
type group struct {
	res    aggregation.Group
	docIDs []uint32
}

func (ga *groupedAggregator) identifyGroups(ctx context.Context) ([]group, error) {
	limit := 100 // reasonable default in case we get none
	if ga.params.Limit != nil {
		limit = *ga.params.Limit
	}
	return newGrouper(ga.Aggregator, limit).Do(ctx)
}

func (ga *groupedAggregator) aggregateGroup(ctx context.Context,
	in aggregation.Group, ids []uint32) (aggregation.Group, error) {
	out := in
	fa := newFilteredAggregator(ga.Aggregator)
	props, err := fa.properties(ctx, ids)
	if err != nil {
		return out, errors.Wrap(err, "aggregate properties")
	}

	out.Properties = props
	return out, nil
}

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

package aggregator

import (
	"context"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/entities/aggregation"
)

// groupedAggregator performs aggregation in groups. This is a two-step
// process. First a whole-db scan is performed to identify the groups, then
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
// will eventually be served to the user. But it also contains the list of
// docIDs in that group, so we can use those to perform the actual aggregation
// (for each group) in a second step
type group struct {
	res    aggregation.Group
	docIDs []uint64
}

func (ga *groupedAggregator) identifyGroups(ctx context.Context) ([]group, error) {
	limit := 100 // reasonable default in case we get none
	if ga.params.Limit != nil {
		limit = *ga.params.Limit
	}
	return newGrouper(ga.Aggregator, limit).Do(ctx)
}

func (ga *groupedAggregator) aggregateGroup(ctx context.Context,
	in aggregation.Group, ids []uint64,
) (aggregation.Group, error) {
	out := in
	fa := newFilteredAggregator(ga.Aggregator)
	props, err := fa.properties(ctx, ids)
	if err != nil {
		return out, errors.Wrap(err, "aggregate properties")
	}

	out.Properties = props
	return out, nil
}

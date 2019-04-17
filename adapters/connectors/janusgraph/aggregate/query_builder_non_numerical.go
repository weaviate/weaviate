/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@creativesoftwarefdn.org
 */
package aggregate

import (
	"fmt"

	"github.com/creativesoftwarefdn/weaviate/adapters/handlers/graphql/local/aggregate"
	"github.com/creativesoftwarefdn/weaviate/gremlin"
)

func (b *Query) nonNumericalProp(prop aggregate.Property) (*propertyAggregation, error) {
	aggregators := []*aggregation{}
	for _, aggregator := range prop.Aggregators {

		newAggregator, err := b.nonNumericalPropAggregators(aggregator)
		if err != nil {
			return nil, fmt.Errorf("cannot build query for aggregator prop '%s': %s", aggregator, err)
		}

		if newAggregator == nil {
			continue
		}

		aggregators = append(aggregators, newAggregator)
	}

	return b.mergeAggregators(aggregators, prop)
}

func (b *Query) nonNumericalPropAggregators(aggregator aggregate.Aggregator) (*aggregation, error) {
	switch aggregator {
	case aggregate.Count:
		return &aggregation{label: string(aggregator), aggregation: gremlin.New().Count()}, nil
	default:
		return nil, fmt.Errorf("analysis '%s' not supported for non-numerical prop", aggregator)
	}
}

/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2018 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * AUTHOR: Bob van Luijt (bob@kub.design)
 * See www.creativesoftwarefdn.org for details
 * Contact: @CreativeSofwFdn / bob@kub.design
 */
package aggregate

import (
	"fmt"
	"strings"

	"github.com/creativesoftwarefdn/weaviate/graphqlapi/local/aggregate"
	"github.com/creativesoftwarefdn/weaviate/gremlin"
)

type numericalAggregation struct {
	label       string
	aggregation *gremlin.Query
}

func (b *Query) numericalProp(prop aggregate.Property) (*propertyAggregation, error) {
	aggregators := []*numericalAggregation{}
	for _, aggregator := range prop.Aggregators {

		newAggregator, err := b.numericalPropAggregators(aggregator)
		if err != nil {
			return nil, fmt.Errorf("cannot build query for aggregator prop '%s': %s", aggregator, err)
		}

		if newAggregator == nil {
			continue
		}

		aggregators = append(aggregators, newAggregator)
	}

	return b.numericalPropMergeAggregators(aggregators, prop)
}

func (b *Query) numericalPropAggregators(aggregator aggregate.Aggregator) (*numericalAggregation, error) {
	switch aggregator {
	case aggregate.Count:
		return &numericalAggregation{label: string(aggregator), aggregation: gremlin.New().Count()}, nil
	case aggregate.Mean:
		return &numericalAggregation{label: string(aggregator), aggregation: gremlin.New().Mean()}, nil
	case aggregate.Mode:
		return &numericalAggregation{
			label:       string(aggregator),
			aggregation: gremlin.New().GroupCount().OrderLocalByValuesSelectKeysLimit("decr", 1),
		}, nil
	case aggregate.Sum:
		return &numericalAggregation{label: string(aggregator), aggregation: gremlin.New().Sum()}, nil
	case aggregate.Maximum:
		return &numericalAggregation{label: string(aggregator), aggregation: gremlin.New().Max()}, nil
	case aggregate.Minimum:
		return &numericalAggregation{label: string(aggregator), aggregation: gremlin.New().Min()}, nil
	default:
		return nil, fmt.Errorf("analysis '%s' not supported for int prop", aggregator)
	}
}

func (b *Query) numericalPropMergeAggregators(aggregationQueries []*numericalAggregation,
	prop aggregate.Property) (*propertyAggregation, error) {
	selections := []string{}
	matchFragments := []string{}

	for _, a := range aggregationQueries {
		mappedPropName := b.mappedPropertyName(b.params.ClassName, prop.Name)
		selection := fmt.Sprintf("%s__%s", mappedPropName, a.label)
		q := gremlin.New().
			As("a").
			Unfold().
			Values([]string{mappedPropName}).
			Raw("." + a.aggregation.String()).
			As(selection)

		matchFragments = append(matchFragments, q.String())
		selections = append(selections, selection)
	}

	return &propertyAggregation{
		matchQueryFragment: gremlin.New().Raw(strings.Join(matchFragments, ",")),
		selections:         selections,
	}, nil
}

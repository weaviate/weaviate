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
	"strings"

	"github.com/creativesoftwarefdn/weaviate/adapters/connectors/janusgraph/gremlin"
	"github.com/creativesoftwarefdn/weaviate/usecases/kinds"
)

func (b *Query) crefProp(prop kinds.AggregateProperty) (*propertyAggregation, error) {
	aggregators := []*aggregation{}
	for _, aggregator := range prop.Aggregators {

		newAggregator, err := b.crefPropAggregators(aggregator)
		if err != nil {
			return nil, fmt.Errorf("cannot build query for aggregator prop '%s': %s", aggregator, err)
		}

		if newAggregator == nil {
			continue
		}

		aggregators = append(aggregators, newAggregator)
	}

	return b.mergeCrefAggregators(aggregators, prop)
}

func (b *Query) crefPropAggregators(aggregator kinds.Aggregator) (*aggregation, error) {
	switch aggregator {
	case kinds.CountAggregator:
		return &aggregation{label: string(aggregator), aggregation: gremlin.New().Count()}, nil
	default:
		return nil, fmt.Errorf("analysis '%s' not supported for non-numerical prop", aggregator)
	}
}

func (b *Query) mergeCrefAggregators(aggregationQueries []*aggregation,
	prop kinds.AggregateProperty) (*propertyAggregation, error) {
	selections := []string{}
	matchFragments := []string{}

	var possibleProjectStep string

	for _, a := range aggregationQueries {
		mappedPropName := b.mappedPropertyName(b.params.ClassName, untitle(prop.Name))
		selection := fmt.Sprintf("%s__%s", mappedPropName, a.label)
		if len(aggregationQueries) == 1 {
			possibleProjectStep = "." + gremlin.New().Project(selection).String()
		}

		q := gremlin.New().
			As("a").
			Unfold().
			OutWithLabel(mappedPropName).
			Raw("." + a.aggregation.String()).
			Raw(possibleProjectStep).
			As(selection)

		matchFragments = append(matchFragments, q.String())
		selections = append(selections, selection)
	}

	return &propertyAggregation{
		matchQueryFragment: gremlin.New().Raw(strings.Join(matchFragments, ",")),
		selections:         selections,
	}, nil
}

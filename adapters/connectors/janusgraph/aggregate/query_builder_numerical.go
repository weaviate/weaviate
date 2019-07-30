//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
// 
//  Copyright © 2016 - 2019 Weaviate. All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

package aggregate

import (
	"fmt"
	"strings"

	"github.com/semi-technologies/weaviate/adapters/connectors/janusgraph/gremlin"
	"github.com/semi-technologies/weaviate/usecases/traverser"
)

type aggregation struct {
	label       string
	aggregation *gremlin.Query
}

func (b *Query) numericalProp(prop traverser.AggregateProperty) (*propertyAggregation, error) {
	aggregators := []*aggregation{}
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

	return b.mergeAggregators(aggregators, prop)
}

func (b *Query) numericalPropAggregators(aggregator traverser.Aggregator) (*aggregation, error) {
	switch aggregator {
	case traverser.CountAggregator:
		return &aggregation{label: string(aggregator), aggregation: gremlin.New().Count()}, nil
	case traverser.MeanAggregator:
		return &aggregation{label: string(aggregator), aggregation: gremlin.New().Mean()}, nil
	case traverser.ModeAggregator:
		return &aggregation{
			label:       string(aggregator),
			aggregation: gremlin.New().GroupCount().OrderLocalByValuesSelectKeysLimit("decr", 1),
		}, nil
	case traverser.SumAggregator:
		return &aggregation{label: string(aggregator), aggregation: gremlin.New().Sum()}, nil
	case traverser.MaximumAggregator:
		return &aggregation{label: string(aggregator), aggregation: gremlin.New().Max()}, nil
	case traverser.MinimumAggregator:
		return &aggregation{label: string(aggregator), aggregation: gremlin.New().Min()}, nil
	default:
		return nil, fmt.Errorf("analysis '%s' not supported for int prop", aggregator)
	}
}

func (b *Query) mergeAggregators(aggregationQueries []*aggregation,
	prop traverser.AggregateProperty) (*propertyAggregation, error) {
	selections := []string{}
	matchFragments := []string{}

	var possibleProjectStep string

	for _, a := range aggregationQueries {
		mappedPropName := b.mappedPropertyName(b.params.ClassName, prop.Name)
		selection := fmt.Sprintf("%s__%s", mappedPropName, a.label)
		if len(aggregationQueries) == 1 {
			possibleProjectStep = "." + gremlin.New().Project(selection).String()
		}

		q := gremlin.New().
			As("a").
			Unfold().
			Values([]string{mappedPropName}).
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

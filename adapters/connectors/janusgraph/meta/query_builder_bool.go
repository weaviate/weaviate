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
package meta

import (
	"fmt"

	"github.com/creativesoftwarefdn/weaviate/adapters/connectors/janusgraph/gremlin"
	"github.com/creativesoftwarefdn/weaviate/usecases/kinds"
)

func (b *Query) booleanProp(prop kinds.MetaProperty) (*gremlin.Query, error) {
	q := gremlin.New()

	// retrieving the total true and total false values is a single operation in
	// Gremlin, however the user can use the graphQL API to retrieve any of the
	// for total/percentage analysis props. So if the number of those props is
	// greater than one we want to process these props only once.
	processedTotalsPropsYet := false

	analysisQueries := []*gremlin.Query{}
	for _, analysis := range prop.StatisticalAnalyses {
		if isBooleanTotalsProp(analysis) && processedTotalsPropsYet {
			continue
		}

		analysisQuery, err := b.booleanPropAnalysis(prop, analysis)
		if err != nil {
			return nil, err
		}

		if isBooleanTotalsProp(analysis) {
			processedTotalsPropsYet = true
		}

		if analysisQuery == nil {
			continue
		}

		analysisQueries = append(analysisQueries, analysisQuery)

	}

	q = q.Values([]string{b.mappedPropertyName(b.params.ClassName, prop.Name)}).Union(analysisQueries...)

	return q, nil
}

func isBooleanTotalsProp(analysis kinds.StatisticalAnalysis) bool {
	switch analysis {
	case kinds.TotalTrue, kinds.TotalFalse, kinds.PercentageTrue, kinds.PercentageFalse:
		return true
	default:
		return false
	}
}

func (b *Query) booleanPropAnalysis(prop kinds.MetaProperty,
	analysis kinds.StatisticalAnalysis) (*gremlin.Query, error) {
	switch analysis {
	case kinds.Count:
		return b.booleanPropCount(prop)
	case kinds.TotalTrue, kinds.TotalFalse, kinds.PercentageTrue, kinds.PercentageFalse:
		return b.booleanPropTotals(prop)
	case kinds.Type:
		// type is handled by the type inspector, not coming from the db
		return nil, nil
	default:
		return nil, fmt.Errorf("unrecognized statistical analysis prop '%#v'", analysis)
	}
}

func (b *Query) booleanPropCount(prop kinds.MetaProperty) (*gremlin.Query, error) {
	q := gremlin.New()

	q = q.Count().Project("count").Project(string(prop.Name))

	return q, nil
}

func (b *Query) booleanPropTotals(prop kinds.MetaProperty) (*gremlin.Query, error) {
	q := gremlin.New()

	q = q.GroupCount().Unfold().Project(string(prop.Name))

	return q, nil
}

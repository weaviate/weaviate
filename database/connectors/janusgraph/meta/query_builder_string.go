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
	"strings"

	"github.com/creativesoftwarefdn/weaviate/graphqlapi/local/getmeta"
	"github.com/creativesoftwarefdn/weaviate/gremlin"
)

const (
	// StringTopOccurrences is an intermediary structure that contains counts for
	// string values. However, they need to be post-processed, thus they are
	// referenced by a constant name
	StringTopOccurrences = "topOccurrences"
)

func (b *Query) stringProp(prop getmeta.MetaProperty) (*gremlin.Query, error) {
	q := gremlin.New()

	// retrieving the total true and total false values is a single operation in
	// Gremlin, however the user can use the graphQL API to retrieve any of the
	// for total/percentage analysis props. So if the number of those props is
	// greater than one we want to process these props only once.
	processedCountPropsYet := false

	analysisQueries := []*gremlin.Query{}
	for _, analysis := range prop.StatisticalAnalyses {
		if isStringCountProp(analysis) && processedCountPropsYet {
			continue
		}

		analysisQuery, err := b.stringPropAnalysis(prop, analysis)
		if err != nil {
			return nil, err
		}

		if isStringCountProp(analysis) {
			processedCountPropsYet = true
		}

		if analysisQuery == nil {
			continue
		}

		analysisQueries = append(analysisQueries, analysisQuery)

	}

	q = concatGremlin(analysisQueries...)

	return q, nil
}

func isStringCountProp(analysis getmeta.StatisticalAnalysis) bool {
	switch analysis {
	case getmeta.TopOccurrencesValue, getmeta.TopOccurrencesOccurs:
		return true
	default:
		return false
	}
}

func (b *Query) stringPropAnalysis(prop getmeta.MetaProperty,
	analysis getmeta.StatisticalAnalysis) (*gremlin.Query, error) {
	switch analysis {
	case getmeta.Count:
		return b.stringPropCount(prop)
	case getmeta.TopOccurrencesValue, getmeta.TopOccurrencesOccurs:
		return b.stringPropTopOccurrences(prop)
	case getmeta.Type:
		// skip because type is handled by the type inspector
		return nil, nil
	default:
		return nil, fmt.Errorf("unrecognized statistical analysis prop '%#v'", analysis)
	}
}

func (b *Query) stringPropCount(prop getmeta.MetaProperty) (*gremlin.Query, error) {
	q := gremlin.New()

	q = q.HasProperty(b.mappedPropertyName(b.params.ClassName, prop.Name)).
		Count().
		Project("count").
		Project(string(prop.Name))

	return q, nil
}

func (b *Query) stringPropTopOccurrences(prop getmeta.MetaProperty) (*gremlin.Query, error) {
	return gremlin.New().HasProperty(b.mappedPropertyName(b.params.ClassName, prop.Name)).
		GroupCount().By(b.mappedPropertyName(b.params.ClassName, prop.Name)).
		OrderLocalByValuesLimit("decr", 3).
		Project(StringTopOccurrences).
		Project(string(prop.Name)), nil
}

func concatGremlin(queries ...*gremlin.Query) *gremlin.Query {
	queryStrings := make([]string, len(queries), len(queries))
	for i, q := range queries {
		queryStrings[i] = q.String()
	}

	return gremlin.New().Raw(strings.Join(queryStrings, ", "))
}

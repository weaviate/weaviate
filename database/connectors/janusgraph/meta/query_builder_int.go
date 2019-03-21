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

	"github.com/creativesoftwarefdn/weaviate/graphqlapi/local/getmeta"
	"github.com/creativesoftwarefdn/weaviate/gremlin"
)

type intAnalysis struct {
	label       string
	aggregation *gremlin.Query
}

func (b *Query) intProp(prop getmeta.MetaProperty) (*gremlin.Query, error) {
	analyses := []*intAnalysis{}
	for _, analysis := range prop.StatisticalAnalyses {

		newAnalysis, err := b.intPropAnalysis(analysis)
		if err != nil {
			return nil, fmt.Errorf("cannot build query for analysis prop '%s': %s", analysis, err)
		}

		if newAnalysis == nil {
			continue
		}

		analyses = append(analyses, newAnalysis)
	}

	return b.intPropMergeAnalyses(analyses, prop)
}

func (b *Query) intPropAnalysis(analysis getmeta.StatisticalAnalysis) (*intAnalysis, error) {
	switch analysis {
	case getmeta.Count:
		return &intAnalysis{label: string(analysis), aggregation: gremlin.New().Count()}, nil
	case getmeta.Mean:
		return &intAnalysis{label: string(analysis), aggregation: gremlin.New().Mean()}, nil
	case getmeta.Sum:
		return &intAnalysis{label: string(analysis), aggregation: gremlin.New().Sum()}, nil
	case getmeta.Maximum:
		return &intAnalysis{label: string(analysis), aggregation: gremlin.New().Max()}, nil
	case getmeta.Minimum:
		return &intAnalysis{label: string(analysis), aggregation: gremlin.New().Min()}, nil
	case getmeta.Type:
		// skip type as it's handled by the type inspector
		return nil, nil
	default:
		return nil, fmt.Errorf("analysis '%s' not supported for int prop", analysis)
	}
}

func (b *Query) intPropMergeAnalyses(analyses []*intAnalysis,
	prop getmeta.MetaProperty) (*gremlin.Query, error) {

	aggregations := []*gremlin.Query{}
	for _, a := range analyses {
		aggregations = append(aggregations, a.aggregation.Project(a.label).Project(string(prop.Name)))
	}

	return gremlin.New().
		Values([]string{b.mappedPropertyName(b.params.ClassName, prop.Name)}).
		Union(aggregations...), nil
}

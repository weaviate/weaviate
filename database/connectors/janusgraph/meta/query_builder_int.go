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

		analyses = append(analyses, newAnalysis)
	}

	return b.intPropMergeAnalyses(analyses, prop)
}

func (b *Query) intPropAnalysis(analysis getmeta.StatisticalAnalysis) (*intAnalysis, error) {
	switch analysis {
	case getmeta.Count:
		return &intAnalysis{label: string(analysis), aggregation: gremlin.New().CountLocal()}, nil
	default:
		return nil, fmt.Errorf("analysis '%s' not supported", analysis)
	}
}

func (b *Query) intPropMergeAnalyses(analyses []*intAnalysis,
	prop getmeta.MetaProperty) (*gremlin.Query, error) {
	q := gremlin.New().
		Aggregate("aggregation").
		By(b.mappedPropertyName(b.params.ClassName, prop.Name)).
		Cap("aggregation").Limit(1)

	labels := []string{}
	aggregations := []*gremlin.Query{}

	for _, a := range analyses {
		labels = append(labels, a.label)
		aggregations = append(aggregations, a.aggregation)
	}

	q = q.As(labels...).Select(labels)

	for _, a := range aggregations {
		q = q.ByQuery(a)
	}

	if len(analyses) == 1 {
		// just one analysis prop is a special case, because in multiple cases we
		// are using select(<1>,<2>, ...<n>), this means we will receive a map that
		// has the selections as keys. However, if we only ask for a single prop,
		// Gremlin doesn't see a need to return a map and simply returns the
		// primitive value. This will of course either break our post-processing or
		// will not have the format the graphql API expects. We thus need to add an
		// additional as().project().by() step to wrap the primtive prop in a map -
		// but only if it's only a single analysis prop.
		//
		// Additionally we need to be careful that we don't reuse any labels in our
		// as().project().by() step that already have specific aggregation meaning.
		// Therefore we are renaming the as step to <analysisProp>_combined.
		q = q.AsProjectBy(fmt.Sprintf("%s_combined", labels[0]), labels[0])
	}

	return q.AsProjectBy(string(prop.Name)), nil

}

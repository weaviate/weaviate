package meta

import (
	"fmt"

	"github.com/creativesoftwarefdn/weaviate/database/connectors/janusgraph/state"
	"github.com/creativesoftwarefdn/weaviate/database/schema"
	"github.com/creativesoftwarefdn/weaviate/graphqlapi/local/getmeta"
	"github.com/creativesoftwarefdn/weaviate/gremlin"
)

const (
	// BoolGroupCount is an intermediary structure that contains counts for
	// boolean values. However, they need to be post-processed, thus they are
	// referenced by a constant name
	BoolGroupCount = "boolGroupCount"
)

type Query struct {
	params     *getmeta.Params
	nameSource nameSource
}

func NewQuery(params *getmeta.Params, nameSource nameSource) *Query {
	return &Query{params: params, nameSource: nameSource}
}

type nameSource interface {
	GetMappedPropertyName(className schema.ClassName, propName schema.PropertyName) state.MappedPropertyName
}

func (b *Query) String() (string, error) {
	q := gremlin.New()

	// get only one prop for now
	prop := b.params.Properties[0]
	propQuery, err := b.booleanProp(prop)
	if err != nil {
		return "", err
	}

	q = q.Union(propQuery)

	return fmt.Sprintf(".%s", q.String()), nil

}

func (b *Query) booleanProp(prop getmeta.MetaProperty) (*gremlin.Query, error) {
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

		analysisQueries = append(analysisQueries, analysisQuery)

	}

	q = q.Union(analysisQueries...).AsProjectBy(string(prop.Name))

	return q, nil
}

func isBooleanTotalsProp(analysis getmeta.StatisticalAnalysis) bool {
	switch analysis {
	case getmeta.TotalTrue, getmeta.TotalFalse, getmeta.PercentageTrue, getmeta.PercentageFalse:
		return true
	default:
		return false
	}
}

func (b *Query) booleanPropAnalysis(prop getmeta.MetaProperty,
	analysis getmeta.StatisticalAnalysis) (*gremlin.Query, error) {
	switch analysis {
	case getmeta.Count:
		return b.booleanPropCount(prop)
	case getmeta.TotalTrue, getmeta.TotalFalse, getmeta.PercentageTrue, getmeta.PercentageFalse:
		return b.booleanPropTotals(prop)
	default:
		return nil, fmt.Errorf("unrecognized statistical analysis prop '%#v'", analysis)
	}
}

func (b *Query) booleanPropCount(prop getmeta.MetaProperty) (*gremlin.Query, error) {
	q := gremlin.New()

	q = q.HasProperty(b.mappedPropertyName(b.params.ClassName, prop.Name)).
		Count().
		AsProjectBy("count")

	return q, nil
}

func (b *Query) booleanPropTotals(prop getmeta.MetaProperty) (*gremlin.Query, error) {
	q := gremlin.New()

	q = q.GroupCount().By(b.mappedPropertyName(b.params.ClassName, prop.Name)).
		AsProjectBy(BoolGroupCount)

	return q, nil
}

func (b *Query) mappedPropertyName(className schema.ClassName,
	propName schema.PropertyName) string {
	if b.nameSource == nil {
		return string(propName)
	}

	return string(b.nameSource.GetMappedPropertyName(className, propName))
}

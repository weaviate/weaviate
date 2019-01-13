package meta

import (
	"fmt"

	"github.com/creativesoftwarefdn/weaviate/database/connectors/janusgraph/state"
	"github.com/creativesoftwarefdn/weaviate/database/schema"
	"github.com/creativesoftwarefdn/weaviate/graphqlapi/local/getmeta"
	"github.com/creativesoftwarefdn/weaviate/gremlin"
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

	// get only one analysis for now
	// analysis := prop.StatisticalAnalyses[0]
	countQuery, err := b.booleanPropCount(prop)
	if err != nil {
		return nil, err
	}

	q = q.Union(countQuery).AsProjectBy(string(prop.Name))

	return q, nil
}

func (b *Query) booleanPropCount(prop getmeta.MetaProperty) (*gremlin.Query, error) {
	q := gremlin.New()

	q = q.HasProperty(b.mappedPropertyName(b.params.ClassName, prop.Name)).
		Count().
		AsProjectBy("count")

	return q, nil
}

func (b *Query) mappedPropertyName(className schema.ClassName,
	propName schema.PropertyName) string {
	if b.nameSource == nil {
		return string(propName)
	}

	return string(b.nameSource.GetMappedPropertyName(className, propName))
}

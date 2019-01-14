package meta

import (
	"fmt"

	"github.com/creativesoftwarefdn/weaviate/database/connectors/janusgraph/state"
	"github.com/creativesoftwarefdn/weaviate/database/schema"
	"github.com/creativesoftwarefdn/weaviate/database/schema/kind"
	"github.com/creativesoftwarefdn/weaviate/graphqlapi/local/getmeta"
	"github.com/creativesoftwarefdn/weaviate/gremlin"
	"github.com/creativesoftwarefdn/weaviate/models"
)

type Query struct {
	params     *getmeta.Params
	nameSource nameSource
	typeSource typeSource
}

func NewQuery(params *getmeta.Params, nameSource nameSource, typeSource typeSource) *Query {
	return &Query{params: params, nameSource: nameSource, typeSource: typeSource}
}

type nameSource interface {
	GetMappedPropertyName(className schema.ClassName, propName schema.PropertyName) state.MappedPropertyName
}

type typeSource interface {
	GetProperty(kind kind.Kind, className schema.ClassName,
		propName schema.PropertyName) (error, *models.SemanticSchemaClassProperty)
	FindPropertyDataType(dataType []string) (schema.PropertyDataType, error)
}

func (b *Query) String() (string, error) {
	q := gremlin.New()

	props := b.params.Properties
	propQueries := make([]*gremlin.Query, len(props), len(props))

	for i, prop := range props {
		propQuery, err := b.prop(prop)
		if err != nil {
			return "", fmt.Errorf("could not build get meta query for prop '%s': %s", prop.Name, err)
		}

		propQueries[i] = propQuery
	}

	q = q.Union(propQueries...)
	return fmt.Sprintf(".%s", q.String()), nil
}

func (b *Query) prop(prop getmeta.MetaProperty) (*gremlin.Query, error) {
	err, parsed := b.typeSource.GetProperty(b.params.Kind, b.params.ClassName, prop.Name)
	if err != nil {
		return nil, fmt.Errorf("could not find property '%s' in schema: %s", prop.Name, err)
	}

	dataType, err := b.typeSource.FindPropertyDataType(parsed.AtDataType)
	if err != nil {
		return nil, fmt.Errorf("could not find data type of prop '%s': %s", prop.Name, err)
	}

	if !dataType.IsPrimitive() {
		return nil, fmt.Errorf("GetMeta is not supported with non-primitive types in Janusgraph yet")
	}

	switch dataType.AsPrimitive() {
	case schema.DataTypeBoolean:
		return b.booleanProp(prop)
	case schema.DataTypeInt:
		return b.intProp(prop)
	default:
		return nil, fmt.Errorf("unsupported primitive property data type: %s", dataType.AsPrimitive())
	}
}

func (b *Query) mappedPropertyName(className schema.ClassName,
	propName schema.PropertyName) string {
	if b.nameSource == nil {
		return string(propName)
	}

	return string(b.nameSource.GetMappedPropertyName(className, propName))
}

package traverser

import (
	"fmt"

	"github.com/semi-technologies/weaviate/entities/aggregation"
	"github.com/semi-technologies/weaviate/entities/schema"
	schemaUC "github.com/semi-technologies/weaviate/usecases/schema"
)

type typeInspector struct {
	schema schema.Schema
}

func newTypeInspector(schemaGetter schemaUC.SchemaGetter) *typeInspector {
	schema := schemaGetter.GetSchemaSkipAuth()
	return &typeInspector{
		schema: schema,
	}
}

func (i *typeInspector) WithTypes(res *aggregation.Result, params AggregateParams) (*aggregation.Result, error) {
	if res == nil {
		return nil, nil
	}

	for _, prop := range params.Properties {

		if !i.hasTypeAggregator(prop.Aggregators) {
			// nothing to do for us
			continue
		}

		// TODO: gh-949 fix order
		err, schemaProp := i.schema.GetProperty(params.Kind, params.ClassName, prop.Name)
		if err != nil {
			return nil, fmt.Errorf("with types: prop %s: %v", prop.Name, err)
		}

		i.extendResWithType(res, prop.Name.String(), schemaProp.DataType)
	}

	return res, nil
}

func (i *typeInspector) hasTypeAggregator(aggs []Aggregator) bool {
	for _, agg := range aggs {
		if agg == TypeAggregator {
			return true
		}
	}

	return false
}

func (i *typeInspector) extendResWithType(res *aggregation.Result, propName string, dataType []string) {
	for groupIndex, group := range res.Groups {
		prop, ok := group.Properties[propName]
		if !ok {
			prop = aggregation.Property{}
		}

		prop.SchemaType = dataType
		res.Groups[groupIndex].Properties[propName] = prop
	}
}

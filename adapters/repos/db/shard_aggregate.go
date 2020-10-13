package db

import (
	"context"
	"fmt"

	"github.com/boltdb/bolt"
	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/adapters/repos/db/helpers"
	"github.com/semi-technologies/weaviate/entities/aggregation"
	"github.com/semi-technologies/weaviate/entities/schema"
	schemaUC "github.com/semi-technologies/weaviate/usecases/schema"
	"github.com/semi-technologies/weaviate/usecases/traverser"
)

func (s *Shard) aggregate(ctx context.Context,
	params traverser.AggregateParams) (*aggregation.Result, error) {
	return NewAggregator(s, params, s.index.getSchema).Do(ctx)
}

type Aggregator struct {
	shard     *Shard
	params    traverser.AggregateParams
	getSchema schemaUC.SchemaGetter
}

func NewAggregator(shard *Shard, params traverser.AggregateParams,
	getSchema schemaUC.SchemaGetter) *Aggregator {
	return &Aggregator{
		shard:     shard,
		params:    params,
		getSchema: getSchema,
	}
}

func (a *Aggregator) Do(ctx context.Context) (*aggregation.Result, error) {
	out := aggregation.Result{}

	if a.params.GroupBy != nil {
		return nil, fmt.Errorf("grouping not supported yet")
	}

	// without grouping there is always exactly one group
	out.Groups = make([]aggregation.Group, 1)

	if a.params.IncludeMetaCount {
		if err := a.addMetaCount(ctx, &out); err != nil {
			return nil, errors.Wrap(err, "add meta count")
		}
	}

	props, err := a.properties(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "aggregate properties")
	}

	out.Groups[0].Properties = props

	return &out, nil
}

func (a *Aggregator) addMetaCount(ctx context.Context,
	out *aggregation.Result) error {
	if err := a.shard.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(helpers.ObjectsBucket)
		out.Groups[0].Count = b.Stats().KeyN

		return nil
	}); err != nil {
		return err
	}

	return nil
}

func (a *Aggregator) properties(
	ctx context.Context) (map[string]aggregation.Property, error) {
	if len(a.params.Properties) == 0 {
		return nil, nil
	}

	out := map[string]aggregation.Property{}

	for _, prop := range a.params.Properties {
		if err := ctx.Err(); err != nil {
			return nil, errors.Wrapf(err, "start property %s", prop.Name)
		}

		analyzed, err := a.property(ctx, prop)
		if err != nil {
			return nil, errors.Wrapf(err, "property %s", prop.Name)
		}

		out[prop.Name.String()] = analyzed
	}

	return out, nil
}

func (a *Aggregator) property(ctx context.Context,
	prop traverser.AggregateProperty) (aggregation.Property, error) {
	out := aggregation.Property{}

	aggType, dt, err := a.aggTypeOfProperty(prop.Name)
	if err != nil {
		return out, err
	}

	switch aggType {
	case aggregation.PropertyTypeNumerical:
		if dt == schema.DataTypeNumber {
			return a.floatProperty(ctx, prop)
		} else {
			return a.intProperty(ctx, prop)
		}
	default:
		return out, fmt.Errorf("aggreation type %s not supported yet", aggType)
	}
}

func (a *Aggregator) aggTypeOfProperty(
	name schema.PropertyName) (aggregation.PropertyType, schema.DataType, error) {
	s := a.getSchema.GetSchemaSkipAuth()
	schemaProp, err := s.GetProperty(a.params.Kind, a.params.ClassName, name)
	if err != nil {
		return "", "", errors.Wrapf(err, "property %s", name)
	}

	if schema.IsRefDataType(schemaProp.DataType) {
		return aggregation.PropertyTypeReference, "", nil
	}

	dt := schema.DataType(schemaProp.DataType[0])
	switch dt {
	case schema.DataTypeInt, schema.DataTypeNumber:
		return aggregation.PropertyTypeNumerical, dt, nil
	case schema.DataTypeBoolean:
		return aggregation.PropertyTypeBoolean, dt, nil
	case schema.DataTypeText, schema.DataTypeString:
		return aggregation.PropertyTypeText, dt, nil
	case schema.DataTypeGeoCoordinates:
		return "", "", fmt.Errorf("dataType geoCoordinates can't be aggregated")
	case schema.DataTypePhoneNumber:
		return "", "", fmt.Errorf("dataType phoneNumber can't be aggregated")
	default:
		return "", "", fmt.Errorf("unrecoginzed dataType %v", schemaProp.DataType[0])
	}
}

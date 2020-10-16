package aggregator

import (
	"context"
	"fmt"

	"github.com/boltdb/bolt"
	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/adapters/repos/db/helpers"
	"github.com/semi-technologies/weaviate/entities/aggregation"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/usecases/traverser"
)

// unfilteredAggregator allows for relatively efficient whole-dataset
// aggregations, because it uses the invert index which is already grouped and
// ordered. Numerical aggregations can therefore be relatively efficient.
//
// As opposed to reading n objects, the unfiltered aggregator read x rows per
// props. X can be different for each prop.
//
// However, this aggregator does not work with subselections of the dataset,
// such as when grouping or a filter is set.
type unfilteredAggregator struct {
	*Aggregator
}

func newUnfilteredAggregator(agg *Aggregator) *unfilteredAggregator {
	return &unfilteredAggregator{Aggregator: agg}
}

func (ua *unfilteredAggregator) Do(ctx context.Context) (*aggregation.Result, error) {
	out := aggregation.Result{}

	// without grouping there is always exactly one group
	out.Groups = make([]aggregation.Group, 1)

	if ua.params.IncludeMetaCount {
		if err := ua.addMetaCount(ctx, &out); err != nil {
			return nil, errors.Wrap(err, "add meta count")
		}
	}

	props, err := ua.properties(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "aggregate properties")
	}

	out.Groups[0].Properties = props

	return &out, nil
}

func (ua *unfilteredAggregator) addMetaCount(ctx context.Context,
	out *aggregation.Result) error {
	if err := ua.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(helpers.ObjectsBucket)
		out.Groups[0].Count = b.Stats().KeyN

		return nil
	}); err != nil {
		return err
	}

	return nil
}

func (ua unfilteredAggregator) properties(
	ctx context.Context) (map[string]aggregation.Property, error) {
	if len(ua.params.Properties) == 0 {
		return nil, nil
	}

	out := map[string]aggregation.Property{}

	for _, prop := range ua.params.Properties {
		if err := ctx.Err(); err != nil {
			return nil, errors.Wrapf(err, "start property %s", prop.Name)
		}

		analyzed, err := ua.property(ctx, prop)
		if err != nil {
			return nil, errors.Wrapf(err, "property %s", prop.Name)
		}

		if analyzed == nil {
			continue
		}

		out[prop.Name.String()] = *analyzed
	}

	return out, nil
}

func (ua unfilteredAggregator) property(ctx context.Context,
	prop traverser.AggregateProperty) (*aggregation.Property, error) {
	aggType, dt, err := ua.aggTypeOfProperty(prop.Name)
	if err != nil {
		return nil, err
	}

	switch aggType {
	case aggregation.PropertyTypeNumerical:
		if dt == schema.DataTypeNumber {
			return ua.floatProperty(ctx, prop)
		} else {
			return ua.intProperty(ctx, prop)
		}
	case aggregation.PropertyTypeBoolean:
		return ua.boolProperty(ctx, prop)
	case aggregation.PropertyTypeText:
		return ua.textProperty(ctx, prop)
	case aggregation.PropertyTypeReference:
		// ignore, as this is handled outside the repo in the uc
		return nil, nil
	default:
		return nil, fmt.Errorf("aggreation type %s not supported yet", aggType)
	}
}

func (ua unfilteredAggregator) aggTypeOfProperty(
	name schema.PropertyName) (aggregation.PropertyType, schema.DataType, error) {
	s := ua.getSchema.GetSchemaSkipAuth()
	schemaProp, err := s.GetProperty(ua.params.Kind, ua.params.ClassName, name)
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

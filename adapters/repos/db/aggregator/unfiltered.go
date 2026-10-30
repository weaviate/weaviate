//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package aggregator

import (
	"context"
	"fmt"

	"github.com/pkg/errors"
	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/entities/aggregation"
	"github.com/weaviate/weaviate/entities/schema"
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
	out *aggregation.Result,
) error {
	b := ua.store.Bucket(helpers.ObjectsBucketLSM)
	if b == nil {
		return errors.Errorf("objects bucket is nil")
	}

	count, err := b.Count(ctx)
	if err != nil {
		return fmt.Errorf("count objects: %w", err)
	}
	out.Groups[0].Count = count

	return nil
}

func (ua unfilteredAggregator) properties(
	ctx context.Context,
) (map[string]aggregation.Property, error) {
	if len(ua.params.Properties) == 0 {
		return nil, nil
	}

	out := map[string]aggregation.Property{}
	var scans []*objectScan

	for _, prop := range ua.params.Properties {
		if err := ctx.Err(); err != nil {
			return nil, errors.Wrapf(err, "start property %s", prop.Name)
		}

		analyzed, scan, err := ua.property(ctx, prop)
		if err != nil {
			return nil, errors.Wrapf(err, "property %s", prop.Name)
		}

		if scan != nil {
			scans = append(scans, scan)
			continue
		}

		if analyzed == nil {
			continue
		}

		out[prop.Name.String()] = *analyzed
	}

	if err := ua.scanObjects(ctx, scans); err != nil {
		return nil, err
	}
	for _, scan := range scans {
		out[scan.name.String()] = scan.result()
	}

	return out, nil
}

// property aggregates a property that has an index to read, or returns the
// scan to run over the objects bucket for one that has none.
func (ua unfilteredAggregator) property(ctx context.Context,
	prop aggregation.ParamProperty,
) (*aggregation.Property, *objectScan, error) {
	aggType, dt, err := ua.aggTypeOfProperty(prop.Name)
	if err != nil {
		return nil, nil, err
	}

	var analyzed *aggregation.Property
	switch aggType {
	case aggregation.PropertyTypeNumerical:
		switch dt {
		case schema.DataTypeNumber:
			analyzed, err = ua.floatProperty(ctx, prop)
		case schema.DataTypeNumberArray, schema.DataTypeIntArray:
			return nil, ua.numberArrayScan(prop), nil
		default:
			analyzed, err = ua.intProperty(ctx, prop)
		}
	case aggregation.PropertyTypeBoolean:
		switch dt {
		case schema.DataTypeBooleanArray:
			return nil, ua.boolArrayScan(prop), nil
		default:
			analyzed, err = ua.boolProperty(ctx, prop)
		}
	case aggregation.PropertyTypeText:
		return nil, ua.textScan(prop), nil
	case aggregation.PropertyTypeDate:
		switch dt {
		case schema.DataTypeDateArray:
			return nil, ua.dateArrayScan(prop), nil
		default:
			analyzed, err = ua.dateProperty(ctx, prop)
		}
	case aggregation.PropertyTypeReference:
		// ignore, as this is handled outside the repo in the uc
		return nil, nil, nil
	default:
		return nil, nil, fmt.Errorf("aggreation type %s not supported yet", aggType)
	}
	return analyzed, nil, err
}

// objectScan aggregates one property that is read from the stored objects.
// Its aggregator is safe for concurrent use, as the scan feeds it from
// several goroutines.
type objectScan struct {
	name   schema.PropertyName
	add    func(object []byte) error
	result func() aggregation.Property
}

// scanObjects reads the objects bucket once and feeds every object to all
// scans, so the cost of a query does not grow with the number of properties
// that have to be read from the objects.
func (ua unfilteredAggregator) scanObjects(ctx context.Context, scans []*objectScan) error {
	if len(scans) == 0 {
		return nil
	}

	b := ua.store.Bucket(helpers.ObjectsBucketLSM)
	if b == nil {
		return errors.Errorf("could not find bucket %s", helpers.ObjectsBucketLSM)
	}

	extract := func(_ []byte, v []byte, _ [][]byte, _ *sroar.Bitmap) error {
		for _, scan := range scans {
			if err := scan.add(v); err != nil {
				return errors.Wrapf(err, "property %s", scan.name)
			}
		}
		return nil
	}

	return iteratorConcurrently(ctx, b, func() Cursor { return ReplaceCursor{b.CursorReplaceReusable()} }, extract, ua.logger)
}

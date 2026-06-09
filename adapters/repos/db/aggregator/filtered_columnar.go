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
	"math"

	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/aggregation"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

// columnarPointLookupThreshold decides between per-docID point lookups and a
// block scan with an allow bitmap. Below one block's worth of docIDs, the
// O(K log N) lookups beat touching every block.
const columnarPointLookupThreshold = 2048

// aggregateColumnarProps feeds all columnar-backed properties from their
// column buckets and returns the names of the properties that still need the
// object-scan path. This avoids the per-docID object fetch + unmarshal that
// dominates filtered aggregations; when every requested property is
// columnar-backed, the object bucket is not touched at all.
func (fa *filteredAggregator) aggregateColumnarProps(ctx context.Context,
	propAggs propAggs, ids []uint64,
) ([]string, error) {
	remaining := make([]string, 0, len(propAggs))

	class := fa.getSchema.ReadOnlyClass(fa.params.ClassName.String())

	for name, agg := range propAggs {
		bucket, dt := fa.columnarBucketFor(class, name, agg)
		if bucket == nil {
			remaining = append(remaining, name)
			continue
		}
		if err := fa.aggregateFromColumnar(ctx, bucket, dt, agg, ids); err != nil {
			return nil, fmt.Errorf("columnar aggregation for prop %q: %w", name, err)
		}
	}
	return remaining, nil
}

// columnarBucketFor returns the columnar bucket for the property, or nil if
// the property is not columnar-backed (no schema flag, no bucket, or an
// aggregation/data type the column store does not hold).
func (fa *filteredAggregator) columnarBucketFor(class *models.Class,
	propName string, agg propAgg,
) (*lsmkv.Bucket, schema.DataType) {
	switch agg.aggType {
	case aggregation.PropertyTypeNumerical:
		if agg.dataType != schema.DataTypeInt && agg.dataType != schema.DataTypeNumber {
			return nil, ""
		}
	case aggregation.PropertyTypeDate:
		if agg.dataType != schema.DataTypeDate {
			return nil, ""
		}
	default:
		return nil, ""
	}

	if class == nil {
		return nil, ""
	}
	prop, err := schema.GetPropertyByName(class, propName)
	if err != nil || !inverted.HasColumnarIndex(prop) {
		return nil, ""
	}

	// During an enable-columnar migration the bucket exists (created by
	// the strategy's PreReindexHook) but is incomplete until this shard's
	// swap lands. Serving from it would silently return partial
	// aggregations — fall back to the object-scan path instead. Nil
	// callback means "no readiness info wired" (tests); the
	// bucket-existence check below remains the only gate then.
	if fa.isColumnarLocallyReady != nil && !fa.isColumnarLocallyReady(propName) {
		return nil, ""
	}

	bucket := fa.store.Bucket(helpers.BucketColumnarFromPropNameLSM(propName))
	if bucket == nil || bucket.Strategy() != lsmkv.StrategyColumnar {
		return nil, ""
	}
	return bucket, agg.dataType
}

func (fa *filteredAggregator) aggregateFromColumnar(ctx context.Context,
	bucket *lsmkv.Bucket, dt schema.DataType, agg propAgg, ids []uint64,
) error {
	add := func(bits uint64) error {
		switch dt {
		case schema.DataTypeNumber:
			return agg.numericalAgg.AddFloat64(math.Float64frombits(bits))
		case schema.DataTypeInt:
			return agg.numericalAgg.AddFloat64(float64(int64(bits)))
		case schema.DataTypeDate:
			return agg.dateAgg.AddTimestampNano(int64(bits))
		default:
			return fmt.Errorf("unexpected columnar data type %s", dt)
		}
	}

	if len(ids) < columnarPointLookupThreshold {
		for _, docID := range ids {
			if err := ctx.Err(); err != nil {
				return err
			}
			bits, ok := bucket.ColumnarLookupBits(docID, 0)
			if !ok {
				continue // object has no value for this property
			}
			if err := add(bits); err != nil {
				return err
			}
		}
		return nil
	}

	allow := sroar.NewBitmap()
	allow.SetMany(ids)

	var addErr error
	rows := 0
	if err := bucket.ColumnarScan(0, allow, func(_ uint64, bits uint64) bool {
		rows++
		if rows%4096 == 0 {
			if err := ctx.Err(); err != nil {
				addErr = err
				return false
			}
		}
		addErr = add(bits)
		return addErr == nil
	}); err != nil {
		return err
	}
	return addErr
}

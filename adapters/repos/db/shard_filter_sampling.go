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

package db

import (
	"context"
	"fmt"
	"time"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/filtersampling"
	entinverted "github.com/weaviate/weaviate/entities/inverted"
	"github.com/weaviate/weaviate/entities/schema"
)

func (s *Shard) FilterSampling(ctx context.Context, params filtersampling.Params) (*filtersampling.Result, error) {
	bucketName := helpers.BucketFromPropNameLSM(params.PropertyName)
	bucket := s.store.Bucket(bucketName)

	if bucket == nil {
		return nil, fmt.Errorf("property %q has no filterable index", params.PropertyName)
	}

	if bucket.Strategy() != lsmkv.StrategyRoaringSet {
		return nil, fmt.Errorf("property %q does not have a filterable index (wrong bucket strategy)", params.PropertyName)
	}

	// Get the property data type from schema
	class := s.index.getSchema.ReadOnlyClass(s.index.Config.ClassName.String())
	if class == nil {
		return nil, fmt.Errorf("class %s not found in schema", s.index.Config.ClassName)
	}

	var dataType schema.DataType
	for _, prop := range class.Properties {
		if prop.Name == params.PropertyName {
			if len(prop.DataType) > 0 {
				dataType = schema.DataType(prop.DataType[0])
			}
			break
		}
	}

	// Get quantile keys for sampling
	quantileKeys := bucket.QuantileKeys(params.SampleCount)

	samples := make([]filtersampling.Sample, 0, params.SampleCount)

	if len(quantileKeys) == 0 {
		// Empty bucket OR data only in memtable (not yet flushed to disk)
		// Fall back to cursor iteration to capture memtable data
		cursor := bucket.CursorRoaringSet()
		defer cursor.Close()

		count := 0
		for k, bm := cursor.First(); k != nil && count < params.SampleCount; k, bm = cursor.Next() {
			value := decodePropertyValue(k, dataType)
			samples = append(samples, filtersampling.Sample{
				Value:       value,
				Cardinality: uint64(bm.GetCardinality()),
			})
			count++
		}
	} else {
		// Use quantile keys - these are either:
		// a) All unique values (if fewer than requested) - PRD satisfied
		// b) Evenly distributed sample (if more than requested) - PRD satisfied
		for _, qk := range quantileKeys {
			bm, release, err := bucket.RoaringSetGet(qk)
			if err != nil {
				continue // Key may be tombstoned, skip
			}

			value := decodePropertyValue(qk, dataType)
			samples = append(samples, filtersampling.Sample{
				Value:       value,
				Cardinality: uint64(bm.GetCardinality()),
			})
			release()
		}
	}

	// Calculate coverage
	totalObjects := uint64(s.counter.Get())
	var totalCovered uint64
	for _, sample := range samples {
		totalCovered += sample.Cardinality
	}

	percentCovered := 0.0
	if totalObjects > 0 {
		percentCovered = (float64(totalCovered) / float64(totalObjects)) * 100
	}

	return &filtersampling.Result{
		Samples:                 samples,
		TotalObjects:            totalObjects,
		EstimatedPercentCovered: percentCovered,
	}, nil
}

// decodePropertyValue decodes a byte slice key from the inverted index into its typed value
func decodePropertyValue(key []byte, dataType schema.DataType) any {
	switch dataType {
	case schema.DataTypeText, schema.DataTypeTextArray:
		return string(key)

	case schema.DataTypeInt, schema.DataTypeIntArray:
		if val, err := entinverted.ParseLexicographicallySortableInt64(key); err == nil {
			return val
		}
		return string(key)

	case schema.DataTypeNumber, schema.DataTypeNumberArray:
		if val, err := entinverted.ParseLexicographicallySortableFloat64(key); err == nil {
			return val
		}
		return string(key)

	case schema.DataTypeBoolean, schema.DataTypeBooleanArray:
		if len(key) == 1 {
			return key[0] == 1
		}
		return string(key)

	case schema.DataTypeDate, schema.DataTypeDateArray:
		// Dates are stored as int64 (nanoseconds since epoch)
		if val, err := entinverted.ParseLexicographicallySortableInt64(key); err == nil {
			t := time.Unix(0, val)
			return t.Format(time.RFC3339Nano)
		}
		return string(key)

	case schema.DataTypeUUID, schema.DataTypeUUIDArray:
		// UUIDs are stored as strings
		return string(key)

	default:
		// For any other type, return as string
		return string(key)
	}
}

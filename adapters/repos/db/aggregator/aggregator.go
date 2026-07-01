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
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted/stopwords"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/entities/aggregation"
	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/modules"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
)

type vectorIndex interface {
	SearchByVectorDistance(ctx context.Context, vector []float32, targetDistance float32, maxLimit int64,
		allowList helpers.AllowList) ([]uint64, []float32, error)
	SearchByVector(ctx context.Context, vector []float32, k int, allowList helpers.AllowList) ([]uint64, []float32, error)
}

type vectorIndexMulti interface {
	SearchByMultiVectorDistance(ctx context.Context, vector [][]float32, targetDistance float32,
		maxLimit int64, allowList helpers.AllowList) ([]uint64, []float32, error)
	SearchByMultiVector(ctx context.Context, vector [][]float32, k int, allowList helpers.AllowList) ([]uint64, []float32, error)
}

type Aggregator struct {
	logger                 logrus.FieldLogger
	store                  *lsmkv.Store
	params                 aggregation.Params
	getSchema              schemaUC.SchemaGetter
	classSearcher          inverted.ClassSearcher // to support ref-filters
	vectorIndex            vectorIndex
	stopwordProvider       *stopwords.Provider
	shardVersion           uint16
	propLenTracker         *inverted.JsonShardMetaData
	isFallbackToSearchable inverted.IsFallbackToSearchable
	tenant                 string
	nestedCrossRefLimit    int64
	bitmapFactory          *roaringset.BitmapFactory
	modules                *modules.Provider
	defaultLimit           int64
}

func New(store *lsmkv.Store, params aggregation.Params,
	getSchema schemaUC.SchemaGetter, classSearcher inverted.ClassSearcher,
	stopwordProvider *stopwords.Provider, shardVersion uint16,
	vectorIndex vectorIndex, logger logrus.FieldLogger,
	propLenTracker *inverted.JsonShardMetaData,
	isFallbackToSearchable inverted.IsFallbackToSearchable,
	tenant string, nestedCrossRefLimit int64,
	bitmapFactory *roaringset.BitmapFactory,
	modules *modules.Provider, defaultLimit int64,
) *Aggregator {
	return &Aggregator{
		logger:                 logger,
		store:                  store,
		params:                 params,
		getSchema:              getSchema,
		classSearcher:          classSearcher,
		stopwordProvider:       stopwordProvider,
		shardVersion:           shardVersion,
		vectorIndex:            vectorIndex,
		propLenTracker:         propLenTracker,
		isFallbackToSearchable: isFallbackToSearchable,
		tenant:                 tenant,
		nestedCrossRefLimit:    nestedCrossRefLimit,
		bitmapFactory:          bitmapFactory,
		modules:                modules,
		defaultLimit:           defaultLimit,
	}
}

func (a *Aggregator) GetPropertyLengthTracker() *inverted.JsonShardMetaData {
	return a.propLenTracker
}

func (a *Aggregator) Do(ctx context.Context) (*aggregation.Result, error) {
	wantsCardinality := false
	for _, p := range a.params.Properties {
		if p.ApproximateCardinality {
			wantsCardinality = true
			break
		}
	}

	if !wantsCardinality {
		return a.dispatch(ctx, a.params.Properties)
	}

	// Approximate cardinality is a whole-bucket bloom estimate that does not
	// need the per-type object scan, so run the normal aggregation only for
	// properties that also request other aggregators. This keeps a
	// cardinality-only property cheap.
	normal := make([]aggregation.ParamProperty, 0, len(a.params.Properties))
	for _, p := range a.params.Properties {
		if len(p.Aggregators) > 0 {
			normal = append(normal, p)
		}
	}

	res, err := a.dispatch(ctx, normal)
	if err != nil {
		return nil, err
	}
	a.addApproximateCardinalities(res)
	return res, nil
}

func (a *Aggregator) dispatch(ctx context.Context, props []aggregation.ParamProperty) (*aggregation.Result, error) {
	agg := a
	if len(props) != len(a.params.Properties) {
		cp := *a
		cp.params.Properties = props
		agg = &cp
	}

	if agg.params.GroupBy != nil {
		return newGroupedAggregator(agg).Do(ctx)
	}

	isVectorEmpty, err := dto.IsVectorEmpty(agg.params.SearchVector)
	if err != nil {
		return nil, fmt.Errorf("aggregator: %w", err)
	}

	if agg.params.Filters != nil || !isVectorEmpty || agg.params.Hybrid != nil {
		return newFilteredAggregator(agg).Do(ctx)
	}

	return newUnfilteredAggregator(agg).Do(ctx)
}

// addApproximateCardinalities attaches a bloom-filter distinct-value estimate
// to every property that requested it. It is best-effort: a property whose
// bucket is missing or whose bloom filters can't be merged (geometry mismatch
// on an uncompacted multi-segment bucket) is left without an estimate rather
// than failing the whole aggregation.
func (a *Aggregator) addApproximateCardinalities(res *aggregation.Result) {
	for _, p := range a.params.Properties {
		if !p.ApproximateCardinality {
			continue
		}
		est, err := a.approximateCardinality(p.Name)
		if err != nil {
			a.logger.WithField("action", "aggregate_approximate_cardinality").
				WithField("property", p.Name.String()).Error(err)
			continue
		}
		if est == nil {
			continue
		}
		name := p.Name.String()
		for gi := range res.Groups {
			if res.Groups[gi].Properties == nil {
				res.Groups[gi].Properties = map[string]aggregation.Property{}
			}
			prop := res.Groups[gi].Properties[name]
			v := *est
			prop.ApproximateCardinality = &v
			res.Groups[gi].Properties[name] = prop
		}
	}
}

// approximateCardinality returns the bloom-filter distinct-key estimate for a
// property, taking the highest estimate across its inverted-index buckets
// (filterable and searchable), or nil if the property has neither on this shard.
// A bucket that errors is skipped; an error is only returned if every existing
// bucket errored.
func (a *Aggregator) approximateCardinality(name schema.PropertyName) (*uint32, error) {
	bucketNames := []string{
		helpers.BucketFromPropNameLSM(name.String()),
		helpers.BucketSearchableFromPropNameLSM(name.String()),
	}

	var (
		best    uint32
		found   bool
		lastErr error
	)
	for _, bn := range bucketNames {
		b := a.store.Bucket(bn)
		if b == nil {
			continue
		}
		est, err := b.GetKeysCount()
		if err != nil {
			lastErr = err
			continue
		}
		if !found || est > best {
			best = est
			found = true
		}
	}
	if !found {
		return nil, lastErr
	}
	return &best, nil
}

func (a *Aggregator) aggTypeOfProperty(
	name schema.PropertyName,
) (aggregation.PropertyType, schema.DataType, error) {
	class := a.getSchema.ReadOnlyClass(a.params.ClassName.String())
	if class == nil {
		return "", "", fmt.Errorf("could not find class %s in schema", a.params.ClassName)
	}
	schemaProp, err := schema.GetPropertyByName(class, name.String())
	if err != nil {
		return "", "", errors.Wrapf(err, "property %s", name)
	}

	if schema.IsRefDataType(schemaProp.DataType) {
		return aggregation.PropertyTypeReference, schema.DataTypeCRef, nil
	}

	dt := schema.DataType(schemaProp.DataType[0])
	switch dt {
	case schema.DataTypeInt, schema.DataTypeNumber, schema.DataTypeIntArray,
		schema.DataTypeNumberArray:
		return aggregation.PropertyTypeNumerical, dt, nil
	case schema.DataTypeBoolean, schema.DataTypeBooleanArray:
		return aggregation.PropertyTypeBoolean, dt, nil
	case schema.DataTypeText, schema.DataTypeTextArray:
		return aggregation.PropertyTypeText, dt, nil
	case schema.DataTypeDate, schema.DataTypeDateArray:
		return aggregation.PropertyTypeDate, dt, nil
	case schema.DataTypeGeoCoordinates, schema.DataTypePhoneNumber:
		return "", "", fmt.Errorf("dataType %s can't be aggregated", dt)
	default:
		return "", "", fmt.Errorf("unrecoginzed dataType %v", dt)
	}
}

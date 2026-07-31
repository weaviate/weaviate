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
	"github.com/weaviate/weaviate/entities/models"
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
	logger                  logrus.FieldLogger
	store                   *lsmkv.Store
	params                  aggregation.Params
	getSchema               schemaUC.SchemaGetter
	classSearcher           inverted.ClassSearcher // to support ref-filters
	vectorIndex             vectorIndex
	stopwordProvider        *stopwords.Provider
	shardVersion            uint16
	propLenTracker          *inverted.JsonShardMetaData
	isFallbackToSearchable  inverted.IsFallbackToSearchable
	isRangeableLocallyReady inverted.IsRangeableLocallyReady
	tenant                  string
	nestedCrossRefLimit     int64
	bitmapFactory           *roaringset.BitmapFactory
	modules                 *modules.Provider
	defaultLimit            int64
	// tokResolver, when non-nil, is propagated to inverted.Searcher /
	// inverted.BM25Searcher built by this aggregator so query input
	// gets analyzed under the per-shard tokenization overlay during
	// the FINALIZING window of a change-tokenization migration. Nil
	// means "no overlay configured" — query input is tokenized against
	// prop.Tokenization directly (tests and callers with no in-flight
	// migration).
	tokResolver inverted.TokenizationResolver
	// bucketPinResolver, when non-nil, is propagated to every BM25Searcher
	// built by this aggregator. See [inverted.SearchableBucketPinningResolver].
	bucketPinResolver inverted.SearchableBucketPinningResolver
}

// WithSearchableBucketPinningResolver: nil (the default) keeps non-pinning behavior.
func (a *Aggregator) WithSearchableBucketPinningResolver(
	r inverted.SearchableBucketPinningResolver,
) *Aggregator {
	a.bucketPinResolver = r
	return a
}

func New(store *lsmkv.Store, params aggregation.Params,
	getSchema schemaUC.SchemaGetter, classSearcher inverted.ClassSearcher,
	stopwordProvider *stopwords.Provider, shardVersion uint16,
	vectorIndex vectorIndex, logger logrus.FieldLogger,
	propLenTracker *inverted.JsonShardMetaData,
	isFallbackToSearchable inverted.IsFallbackToSearchable,
	isRangeableLocallyReady inverted.IsRangeableLocallyReady,
	tenant string, nestedCrossRefLimit int64,
	bitmapFactory *roaringset.BitmapFactory,
	modules *modules.Provider, defaultLimit int64,
	tokResolver inverted.TokenizationResolver,
) *Aggregator {
	return &Aggregator{
		logger:                  logger,
		store:                   store,
		params:                  params,
		getSchema:               getSchema,
		classSearcher:           classSearcher,
		stopwordProvider:        stopwordProvider,
		shardVersion:            shardVersion,
		vectorIndex:             vectorIndex,
		propLenTracker:          propLenTracker,
		isFallbackToSearchable:  isFallbackToSearchable,
		isRangeableLocallyReady: isRangeableLocallyReady,
		tenant:                  tenant,
		nestedCrossRefLimit:     nestedCrossRefLimit,
		bitmapFactory:           bitmapFactory,
		modules:                 modules,
		defaultLimit:            defaultLimit,
		tokResolver:             tokResolver,
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

	// the estimate is whole-bucket, so it would repeat identically on every
	// group: under group_by the flag is ignored and the request runs exactly as
	// it would without it
	if !wantsCardinality || a.params.GroupBy != nil {
		return a.dispatch(ctx, a.params.Properties)
	}

	if err := a.validateCardinalityProperties(); err != nil {
		return nil, err
	}

	normal := dispatchableProperties(a.params.Properties)
	if len(normal) == 0 && !a.params.IncludeMetaCount {
		// a dispatch here would search, fetch objects and aggregate nothing, to
		// produce the single empty group it always starts from
		res := &aggregation.Result{Groups: make([]aggregation.Group, 1)}
		a.addApproximateCardinalities(res)
		return res, nil
	}

	res, err := a.dispatch(ctx, normal)
	if err != nil {
		return nil, err
	}
	a.addApproximateCardinalities(res)
	return res, nil
}

// dispatchableProperties drops cardinality-only properties: the bloom
// estimate needs no object scan. A property that requested no cardinality
// stays in even without aggregators, as it would without the flag on its
// siblings.
func dispatchableProperties(props []aggregation.ParamProperty) []aggregation.ParamProperty {
	normal := make([]aggregation.ParamProperty, 0, len(props))
	for _, p := range props {
		if len(p.Aggregators) > 0 || !p.ApproximateCardinality {
			normal = append(normal, p)
		}
	}
	return normal
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

// validateCardinalityProperties rejects every property requesting an estimate
// that cannot be produced: an unknown name, or one whose values never land in a
// bucket the estimate can read. Both would otherwise pass silently — the
// dispatch path only looks a property up when it has an aggregator to run.
func (a *Aggregator) validateCardinalityProperties() error {
	names := make([]schema.PropertyName, 0, len(a.params.Properties))
	for _, p := range a.params.Properties {
		if p.ApproximateCardinality {
			names = append(names, p.Name)
		}
	}
	if len(names) == 0 {
		return nil
	}

	class := a.getSchema.ReadOnlyClass(a.params.ClassName.String())
	if class == nil {
		return fmt.Errorf("could not find class %s in schema", a.params.ClassName)
	}
	for _, name := range names {
		prop, err := schema.GetPropertyByName(class, name.String())
		if err != nil {
			return errors.Wrapf(err, "property %s", name)
		}
		if !hasCardinalityIndex(prop) {
			return fmt.Errorf("property %s: approximate cardinality requires a "+
				"filterable or searchable inverted index", name)
		}
	}

	return nil
}

// hasCardinalityIndex reports whether the property's values reach a filterable
// or searchable LSM bucket, the only place the estimate reads keys from. Geo
// coordinates live in a dedicated geo index and nested objects in their own
// bucket namespace; blobs and phone numbers are never analyzed at all.
func hasCardinalityIndex(prop *models.Property) bool {
	if !inverted.HasFilterableIndex(prop) && !inverted.HasSearchableIndex(prop) {
		return false
	}
	if schema.IsRefDataType(prop.DataType) {
		return true
	}
	switch dt, _ := schema.AsPrimitive(prop.DataType); dt {
	case schema.DataTypeText, schema.DataTypeTextArray,
		schema.DataTypeInt, schema.DataTypeIntArray,
		schema.DataTypeNumber, schema.DataTypeNumberArray,
		schema.DataTypeBoolean, schema.DataTypeBooleanArray,
		schema.DataTypeDate, schema.DataTypeDateArray,
		schema.DataTypeUUID, schema.DataTypeUUIDArray:
		return true
	default:
		return false
	}
}

// addApproximateCardinalities attaches the estimate to every property that
// requested it. Best-effort: a property whose bucket is missing or errors is
// left without an estimate rather than failing the whole aggregation.
func (a *Aggregator) addApproximateCardinalities(res *aggregation.Result) {
	for _, p := range a.params.Properties {
		if !p.ApproximateCardinality {
			continue
		}
		est, err := a.approximateCardinality(p.Name)
		if err != nil {
			a.logger.WithField("action", "aggregate_approximate_cardinality").
				WithField("property", p.Name.String()).
				Errorf("could not estimate approximate cardinality: %v", err)
			continue
		}
		if est == nil {
			a.logger.WithField("action", "aggregate_approximate_cardinality").
				WithField("property", p.Name.String()).
				Debug("no filterable or searchable bucket on this shard")
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

// approximateCardinality returns the highest distinct-key estimate across the
// property's filterable and searchable buckets, or nil if it has neither on
// this shard. An error is only returned if every existing bucket errored.
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
		// GetKeysCount panics when a lazily loaded segment fails to load, and
		// the recovered request must not leave the bucket pinned: Bucket.Shutdown
		// drains pins without a timeout, so a leaked one hangs every later
		// shutdown or bucket replacement on that bucket.
		est, exists, err := func() (uint32, bool, error) {
			b, release := a.store.AcquireBucketForRead(bn)
			defer release()
			if b == nil {
				return 0, false, nil
			}
			est, err := b.GetKeysCount()
			return est, true, err
		}()
		if !exists {
			continue
		}
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

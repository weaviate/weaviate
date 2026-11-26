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

	"github.com/go-openapi/strfmt"

	"github.com/weaviate/weaviate/adapters/repos/db/aggregator"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/aggregation"
	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/modules"
	"github.com/weaviate/weaviate/usecases/traverser"
	"github.com/weaviate/weaviate/usecases/traverser/hybrid"
)

// needsGlobalObjectSelection reports whether an aggregation ranks objects and
// keeps only the best ones: a vector search with objectLimit, or a hybrid
// search, which always runs with a limit (objectLimit or the hybrid default).
//
// Each shard applies that limit to its own data only, so on a collection with
// several shards the shard results would cover limit objects per shard, and
// not the limit best objects of the collection. Such an aggregation must pick
// its objects across all shards first.
func needsGlobalObjectSelection(params aggregation.Params) (bool, error) {
	if params.Hybrid != nil {
		return true, nil
	}
	if params.ObjectLimit == nil {
		return false, nil
	}
	isVectorEmpty, err := dto.IsVectorEmpty(params.SearchVector)
	if err != nil {
		return false, err
	}
	return !isVectorEmpty, nil
}

// withGloballySelectedObjects resolves which objects a ranked aggregation
// covers across all shards of the index, the way a single shard holding all
// the data would, and returns params that aggregate exactly those objects: the
// ranking is replaced by a filter on their ids (combined with the caller's
// filter), which every shard, local or remote, evaluates on its own part.
func (i *Index) withGloballySelectedObjects(ctx context.Context,
	replProps *additional.ReplicationProperties, params aggregation.Params,
	modules *modules.Provider,
) (aggregation.Params, error) {
	var (
		found []strfmt.UUID
		err   error
	)
	if params.Hybrid != nil {
		found, err = i.hybridAggregationIDs(ctx, replProps, params, modules)
	} else {
		found, err = i.vectorAggregationIDs(ctx, replProps, params, *params.ObjectLimit)
	}
	if err != nil {
		return params, fmt.Errorf("select aggregation objects across shards: %w", err)
	}

	ids := make([]string, 0, len(found))
	seen := make(map[strfmt.UUID]struct{}, len(found))
	for _, id := range found {
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		ids = append(ids, id.String())
	}

	idFilter := idsFilter(params.ClassName, ids)
	root := idFilter
	if params.Filters != nil && params.Filters.Root != nil {
		root = &filters.Clause{
			Operator: filters.OperatorAnd,
			Operands: []filters.Clause{*params.Filters.Root, *idFilter},
		}
	}

	selected := params
	selected.Filters = &filters.LocalFilter{Root: root}
	selected.SearchVector = nil
	selected.TargetVector = ""
	selected.Certainty = 0
	selected.ObjectLimit = nil
	selected.NearVector = nil
	selected.NearObject = nil
	selected.Hybrid = nil
	return selected, nil
}

// The selection searches below need only ids and ranking scores, never the
// properties (NoProps): the second pass reads what the aggregation needs.

// vectorAggregationIDs returns the ids of the limit objects of the whole index
// that are closest to the search vector, trimmed by certainty like a shard
// trims its own (see aggregator.searchByVector).
func (i *Index) vectorAggregationIDs(ctx context.Context,
	replProps *additional.ReplicationProperties, params aggregation.Params, limit int,
) ([]strfmt.UUID, error) {
	objs, dists, err := i.objectVectorSearch(ctx, []models.Vector{params.SearchVector},
		[]string{params.TargetVector}, 0, limit, params.Filters, nil, nil,
		additional.Properties{NoProps: true}, replProps, params.Tenant, nil, nil)
	if err != nil {
		return nil, fmt.Errorf("vector search: %w", err)
	}
	objs, _ = trimByCertainty(objs, dists, params.Certainty)
	ids := make([]strfmt.UUID, len(objs))
	for j, obj := range objs {
		ids[j] = obj.ID()
	}
	return ids, nil
}

// hybridAggregationIDs runs the hybrid search of a hybrid aggregation over
// the whole index: the same BM25 and vector legs and the same fusion as a
// shard (see aggregator.filteredAggregator.hybrid), with each leg searching all
// shards.
func (i *Index) hybridAggregationIDs(ctx context.Context,
	replProps *additional.ReplicationProperties, params aggregation.Params,
	modules *modules.Provider,
) ([]strfmt.UUID, error) {
	limit := int(i.Config.QueryHybridMaximumResults)
	if params.ObjectLimit != nil {
		limit = *params.ObjectLimit
	}

	class := i.getSchema.ReadOnlyClass(params.ClassName.String())
	if class == nil {
		return nil, fmt.Errorf("could not find class %s in schema", params.ClassName)
	}

	sparseSearch := func() ([]*storobj.Object, []float32, error) {
		kw := aggregator.HybridKeywordRanking(params.Hybrid, class)
		kw.ChooseSearchableProperties(class)
		objs, scores, err := i.objectSearch(ctx, limit, params.Filters, kw, nil, nil,
			additional.Properties{NoProps: true}, replProps, params.Tenant, 0, nil)
		if err != nil {
			return nil, nil, fmt.Errorf("aggregate sparse search: %w", err)
		}
		return objs, scores, nil
	}

	denseSearch := func(vec models.Vector) ([]*storobj.Object, []float32, error) {
		objs, dists, err := i.objectVectorSearch(ctx, []models.Vector{vec},
			[]string{params.TargetVector}, 0, limit, params.Filters, nil, nil,
			additional.Properties{NoProps: true}, replProps, params.Tenant, nil, nil)
		if err != nil {
			return nil, nil, fmt.Errorf("aggregate dense search: %w", err)
		}
		objs, dists = trimByCertainty(objs, dists, params.Certainty)
		return objs, dists, nil
	}

	res, err := hybrid.Search(ctx, &hybrid.Params{
		HybridSearch: params.Hybrid,
		Class:        params.ClassName.String(),
	}, i.logger, sparseSearch, denseSearch, nil, modules, i.getSchema, traverser.NewTargetParamHelper())
	if err != nil {
		return nil, err
	}
	res = aggregator.CapFusedResults(res, limit)

	ids := make([]strfmt.UUID, len(res))
	for j, r := range res {
		ids[j] = r.ID
	}
	return ids, nil
}

// trimByCertainty keeps the leading results within the distance that
// certainty allows. Results are sorted by distance, as they come from a vector
// search.
func trimByCertainty(objs []*storobj.Object, dists []float32, certainty float64,
) ([]*storobj.Object, []float32) {
	if certainty <= 0 {
		return objs, dists
	}
	targetDist := float32(1-certainty) * 2
	n := 0
	for n < len(dists) && n < len(objs) && dists[n] <= targetDist {
		n++
	}
	return objs[:n], dists[:n]
}

// matchNothingID is used to build a filter that matches no object: an id
// cannot be equal and not equal to the same value.
const matchNothingID = "00000000-0000-0000-0000-000000000000"

// idsFilter matches exactly the objects with the given ids. An empty list
// matches nothing; ContainsAny would reject it as a search without terms.
func idsFilter(className schema.ClassName, ids []string) *filters.Clause {
	idPath := func() *filters.Path {
		return &filters.Path{Class: className, Property: schema.PropertyName(filters.InternalPropID)}
	}
	if len(ids) == 0 {
		return &filters.Clause{
			Operator: filters.OperatorAnd,
			Operands: []filters.Clause{
				{Operator: filters.OperatorEqual, On: idPath(), Value: &filters.Value{Value: matchNothingID, Type: schema.DataTypeText}},
				{Operator: filters.OperatorNotEqual, On: idPath(), Value: &filters.Value{Value: matchNothingID, Type: schema.DataTypeText}},
			},
		}
	}
	return &filters.Clause{
		Operator: filters.ContainsAny,
		On:       idPath(),
		Value:    &filters.Value{Value: ids, Type: schema.DataTypeText},
	}
}

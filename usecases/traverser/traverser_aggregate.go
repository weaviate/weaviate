//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package traverser

import (
	"context"
	"fmt"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/entities/aggregation"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
)

// Aggregate resolves meta queries
func (t *Traverser) Aggregate(ctx context.Context, principal *models.Principal,
	params *aggregation.Params,
) (interface{}, error) {
	t.metrics.QueriesAggregateInc(params.ClassName.String())
	defer t.metrics.QueriesAggregateDec(params.ClassName.String())

	err := t.authorizer.Authorize(principal, "get", "traversal/*")
	if err != nil {
		return nil, err
	}

	unlock, err := t.locks.LockConnector()
	if err != nil {
		return nil, enterrors.NewErrLockConnector(err)
	}
	defer unlock()

	inspector := newTypeInspector(t.schemaGetter)

	if params.NearVector != nil || params.NearObject != nil || len(params.ModuleParams) > 0 {
		className := params.ClassName.String()
		err = t.nearParamsVector.validateNearParams(params.NearVector,
			params.NearObject, params.ModuleParams, className)
		if err != nil {
			return nil, err
		}
		searchVector, targetVector, err := t.nearParamsVector.vectorFromParams(ctx,
			params.NearVector, params.NearObject, params.ModuleParams, className, params.Tenant)
		if err != nil {
			return nil, err
		}

		targetVector, err = t.targetVectorParamHelper.GetTargetVectorOrDefault(t.schemaGetter.GetSchemaSkipAuth(),
			className, targetVector)
		if err != nil {
			return nil, err
		}
		params.TargetVector = targetVector
		params.SearchVector = searchVector

		certainty := t.nearParamsVector.extractCertaintyFromParams(params.NearVector,
			params.NearObject, params.ModuleParams)

		if certainty == 0 && params.ObjectLimit == nil {
			return nil, fmt.Errorf("must provide certainty or objectLimit with vector search")
		}
		params.Certainty = certainty
	}

	if params.Hybrid != nil && params.Hybrid.Vector == nil && params.Hybrid.Query != "" {
		targetVector := ""
		if len(params.Hybrid.TargetVectors) == 1 {
			targetVector = params.Hybrid.TargetVectors[0]
		}
		targetVector, err = t.targetVectorParamHelper.GetTargetVectorOrDefault(t.schemaGetter.GetSchemaSkipAuth(),
			params.ClassName.String(), targetVector)
		if err != nil {
			return nil, err
		}
		vec, err := t.nearParamsVector.modulesProvider.
			VectorFromInput(ctx, params.ClassName.String(), params.Hybrid.Query, targetVector)
		if err != nil {
			return nil, err
		}
		params.Hybrid.Vector = vec
	}

	if params.Filters != nil {
		if err := filters.ValidateFilters(t.schemaGetter.GetSchemaSkipAuth(), params.Filters); err != nil {
			return nil, errors.Wrap(err, "invalid 'where' filter")
		}
	}

	res, err := t.vectorSearcher.Aggregate(ctx, *params)
	if err != nil || res == nil {
		return nil, err
	}

	return inspector.WithTypes(res, *params)
}

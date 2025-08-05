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
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/modules"
)

// Aggregate resolves meta queries
func (t *Traverser) Aggregate(ctx context.Context, principal *models.Principal,
	params *aggregation.Params,
) (interface{}, error) {
	t.metrics.QueriesAggregateInc(params.ClassName.String())
	defer t.metrics.QueriesAggregateDec(params.ClassName.String())

	inspector := newTypeInspector(t.schemaGetter.ReadOnlyClass)

	// validate here, because filters can contain references that need to be authorized
	if err := t.validateFilters(ctx, principal, params.Filters); err != nil {
		return nil, errors.Wrap(err, "invalid 'where' filter")
	}

	if params.NearVector != nil || params.NearObject != nil || len(params.ModuleParams) > 0 {
		className := params.ClassName.String()
		err := t.nearParamsVector.validateNearParams(params.NearVector,
			params.NearObject, params.ModuleParams, className)
		if err != nil {
			return nil, err
		}
		targetVectors, err := t.nearParamsVector.targetFromParams(ctx,
			params.NearVector, params.NearObject, params.ModuleParams, className, params.Tenant)
		if err != nil {
			return nil, err
		}

		targetVectors, err = t.targetVectorParamHelper.GetTargetVectorOrDefault(t.schemaGetter.GetSchemaSkipAuth(),
			className, targetVectors)
		if err != nil {
			return nil, err
		}

		searchVector, err := t.nearParamsVector.vectorFromParams(ctx,
			params.NearVector, params.NearObject, params.ModuleParams, className, params.Tenant, targetVectors[0], 0)
		if err != nil {
			return nil, err
		}

		params.TargetVector = targetVectors[0]
		params.SearchVector = searchVector

		certainty := t.nearParamsVector.extractCertaintyFromParams(params.NearVector,
			params.NearObject, params.ModuleParams, nil)

		if certainty == 0 && params.ObjectLimit == nil {
			return nil, fmt.Errorf("must provide certainty or objectLimit with vector search")
		}
		params.Certainty = certainty
	}

	if params.Hybrid != nil && params.Hybrid.Vector == nil {
		var targetVectors []string
		if len(params.Hybrid.TargetVectors) == 1 {
			targetVectors = params.Hybrid.TargetVectors[:1]
		}
		targetVectors, err := t.targetVectorParamHelper.GetTargetVectorOrDefault(t.schemaGetter.GetSchemaSkipAuth(), params.ClassName.String(), targetVectors)
		if err != nil {
			return nil, err
		}
		if len(targetVectors) == 0 {
			params.TargetVector = ""
		} else {
			params.TargetVector = targetVectors[0]
		}

		certainty := t.nearParamsVector.extractCertaintyFromParams(params.NearVector,
			params.NearObject, params.ModuleParams, params.Hybrid)

		if certainty == 0 && params.ObjectLimit == nil {
			return nil, fmt.Errorf("must provide certainty or objectLimit with vector search")
		}

		params.Certainty = certainty
	}

	var mp *modules.Provider

	if t.nearParamsVector.modulesProvider != nil {
		mp = t.nearParamsVector.modulesProvider.(*modules.Provider)
	}

	res, err := t.vectorSearcher.Aggregate(ctx, *params, mp)
	if err != nil || res == nil {
		return nil, err
	}

	return inspector.WithTypes(res, *params)
}

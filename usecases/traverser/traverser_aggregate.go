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

package traverser

import (
	"context"
	"fmt"
	"slices"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/entities/aggregation"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/modules"
)

// Aggregate resolves meta queries
func (t *Traverser) Aggregate(ctx context.Context, principal *models.Principal, params *aggregation.Params) (any, error) {
	// TODO(dyma): if the collection is referenced by its alias, the metric may underestimate the true number of queries.
	// Do we want to resolve the alias before recording this query?
	t.metrics.QueriesAggregateInc(params.ClassName.String())
	defer t.metrics.QueriesAggregateDec(params.ClassName.String())

	if cls := t.schemaGetter.ResolveAlias(params.ClassName.String()); cls != "" {
		params.ClassName = schema.ClassName(cls)
	}

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

	if params.Hybrid != nil {
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

		if params.Hybrid.Vector == nil {
			certainty := t.nearParamsVector.extractCertaintyFromParams(params.NearVector,
				params.NearObject, params.ModuleParams, params.Hybrid)

			if certainty == 0 && params.ObjectLimit == nil {
				return nil, fmt.Errorf("must provide certainty or objectLimit with vector search")
			}

			params.Certainty = certainty
		}
	}

	var mp *modules.Provider

	if t.nearParamsVector.modulesProvider != nil {
		mp = t.nearParamsVector.modulesProvider.(*modules.Provider)
	}

	res, err := t.vectorSearcher.Aggregate(ctx, *params, mp)
	if err != nil || res == nil {
		return nil, err
	}

	if err := addTypeInformation(res, params, t.schemaGetter.ReadOnlyClass); err != nil {
		return nil, err
	}
	return res, nil
}

// addTypeInformation traverses the list of requested aggregations and supplies property type information
// for properties which request that via [aggregation.TypeAggregator].
func addTypeInformation(r *aggregation.Result, params *aggregation.Params, getClass func(string) *models.Class) error {
	if r == nil {
		return nil
	}

	c := getClass(params.ClassName.String())
	if c == nil {
		return fmt.Errorf("could not find class %s in schema", params.ClassName)
	}

	for _, requested := range params.Properties {
		if !slices.Contains(requested.Aggregators, aggregation.TypeAggregator) {
			continue
		}

		property, err := schema.GetPropertyByName(c, requested.Name.String())
		if err != nil {
			return err
		}

		pdt, err := schema.FindPropertyDataTypeWithRefs(getClass, property.DataType, false, "")
		if err != nil {
			return err
		}

		var dt schema.DataType
		switch {
		case pdt.IsPrimitive():
			dt = pdt.AsPrimitive()
		case pdt.IsNested():
			dt = pdt.AsNested() // TODO: check if sufficient just schematype
		case pdt.IsReference():
			dt = schema.DataTypeCRef
		}

		for i := range r.Groups {
			out, ok := r.Groups[i].Properties[property.Name]
			if !ok {
				out = aggregation.Property{}
			}

			out.SchemaType = string(dt)
			if pdt.IsReference() {
				out.Type = aggregation.PropertyTypeReference
				out.ReferenceAggregation.PointingTo = property.DataType
			}
			r.Groups[i].Properties[property.Name] = out
		}
	}

	return nil
}

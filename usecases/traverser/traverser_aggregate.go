//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package traverser

import (
	"context"
	"fmt"

	"github.com/semi-technologies/weaviate/entities/aggregation"
	"github.com/semi-technologies/weaviate/entities/models"
)

// Aggregate resolves meta queries
func (t *Traverser) Aggregate(ctx context.Context, principal *models.Principal,
	params *aggregation.Params) (interface{}, error) {
	err := t.authorizer.Authorize(principal, "get", "traversal/*")
	if err != nil {
		return nil, err
	}

	unlock, err := t.locks.LockConnector()
	if err != nil {
		return nil, fmt.Errorf("could not acquire lock: %v", err)
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
		searchVector, err := t.nearParamsVector.vectorFromParams(ctx,
			params.NearVector, params.NearObject, params.ModuleParams, className)
		if err != nil {
			return nil, err
		}
		params.SearchVector = searchVector
		params.Certainty = t.nearParamsVector.extractCertaintyFromParams(params.NearVector,
			params.NearObject, params.ModuleParams)
	}

	res, err := t.vectorSearcher.Aggregate(ctx, *params)
	if err != nil {
		return nil, err
	}

	return inspector.WithTypes(res, *params)
}

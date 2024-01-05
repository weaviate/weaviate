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
	"time"

	"github.com/weaviate/weaviate/entities/dto"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
)

func (t *Traverser) GetClass(ctx context.Context, principal *models.Principal,
	params dto.GetParams,
) ([]interface{}, error) {
	before := time.Now()

	ok := t.ratelimiter.TryInc()
	if !ok {
		// we currently have no concept of error status code or typed errors in
		// GraphQL, so there is no other way then to send a message containing what
		// we want to convey
		return nil, enterrors.NewErrRateLimit()
	}

	defer t.ratelimiter.Dec()

	t.metrics.QueriesGetInc(params.ClassName)
	defer t.metrics.QueriesGetDec(params.ClassName)
	defer t.metrics.QueriesObserveDuration(params.ClassName, before.UnixMilli())

	err := t.authorizer.Authorize(principal, "get", "traversal/*")
	if err != nil {
		return nil, err
	}

	unlock, err := t.locks.LockConnector()
	if err != nil {
		return nil, enterrors.NewErrLockConnector(err)
	}
	defer unlock()

	certainty := ExtractCertaintyFromParams(params)
	if certainty != 0 || params.AdditionalProperties.Certainty {
		// if certainty is provided as input, we must ensure
		// that the vector index is configured to use cosine
		// distance
		if err := t.validateGetDistanceParams(params); err != nil {
			return nil, err
		}
	}

	return t.explorer.GetClass(ctx, params)
}

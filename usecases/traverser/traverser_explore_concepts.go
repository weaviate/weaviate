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

	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/search"
	"github.com/semi-technologies/weaviate/entities/searchparams"
)

// Explore through unstructured search terms
func (t *Traverser) Explore(ctx context.Context,
	principal *models.Principal, params ExploreParams,
) ([]search.Result, error) {
	if params.Limit == 0 {
		params.Limit = 20
	}

	err := t.authorizer.Authorize(principal, "get", "traversal/*")
	if err != nil {
		return nil, err
	}

	// to conduct a cross-class vector search, all classes must
	// be configured with the same vector index distance type.
	// additionally, certainty cannot be passed to Explore when
	// the classes are configured to use a distance type other
	// than cosine.
	if err := t.validateExploreDistance(params); err != nil {
		return nil, err
	}

	return t.explorer.CrossClassVectorSearch(ctx, params)
}

// ExploreParams are the parameters used by the GraphQL `Explore { }` API
type ExploreParams struct {
	NearVector   *searchparams.NearVector
	NearObject   *searchparams.NearObject
	Offset       int
	Limit        int
	ModuleParams map[string]interface{}

	// TODO: legacy check for backwards compatibility
	// once certainty is fully deprecated, this can
	// be removed
	WithCertaintyProp bool
}

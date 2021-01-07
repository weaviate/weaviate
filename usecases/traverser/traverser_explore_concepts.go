//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package traverser

import (
	"context"

	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/search"
)

// Explore through unstructured search terms
func (t *Traverser) Explore(ctx context.Context,
	principal *models.Principal, params NearTextParams) ([]search.Result, error) {
	if params.Limit == 0 {
		params.Limit = 20
	}

	err := t.authorizer.Authorize(principal, "get", "traversal/*")
	if err != nil {
		return nil, err
	}

	return t.explorer.Concepts(ctx, params)
}

// TODO: This belongs to the text2vec-contextionary module
// NearTextParams to do a vector based explore search
type NearTextParams struct {
	Values       []string
	Limit        int
	MoveTo       ExploreMove
	MoveAwayFrom ExploreMove
	Certainty    float64
	Network      bool
}

// ExploreMove moves an existing Search Vector closer (or further away from) a specific other search term
type ExploreMove struct {
	Values []string
	Force  float32
}

type NearVectorParams struct {
	Vector    []float32
	Certainty float64
}

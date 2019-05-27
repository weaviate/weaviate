/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/semi-technologies/weaviate/blob/develop/LICENSE.md
 * DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@semi.technology
 */package kinds

import (
	"context"
	"fmt"

	"github.com/semi-technologies/weaviate/entities/models"
)

// LocalFetchFuzzy with Search
func (t *Traverser) LocalFetchFuzzy(ctx context.Context, principal *models.Principal,
	params FetchFuzzySearch) (interface{}, error) {

	err := t.authorizer.Authorize(principal, "get", "traversal/*")
	if err != nil {
		return nil, err
	}

	words := t.contextionaryProvider.GetSchemaContextionary().
		SafeGetSimilarWordsWithCertainty(params.Value, params.Certainty)

	res, err := t.repo.LocalFetchFuzzy(ctx, words)
	if err != nil {
		return nil, fmt.Errorf("could not perform fuzzy search in connector: %v", err)
	}

	return res, nil
}

// FetchFuzzySearch fro LocalFetchFuzzy
type FetchFuzzySearch struct {
	Value     string
	Certainty float32
}

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

	"github.com/davecgh/go-spew/spew"
	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
)

// LocalFetchFuzzy with Search
func (t *Traverser) LocalFetchFuzzy(ctx context.Context, principal *models.Principal,
	params FetchFuzzySearch) (interface{}, error) {

	err := t.authorizer.Authorize(principal, "get", "traversal/*")
	if err != nil {
		return nil, err
	}

	words, err := t.c11y.SafeGetSimilarWordsWithCertainty(ctx, params.Value, params.Certainty)
	if err != nil {
		return nil, fmt.Errorf("could not retrieve context: %v", err)
	}

	res, err := t.repo.LocalFetchFuzzy(ctx, words)
	if err != nil {
		return nil, fmt.Errorf("could not perform fuzzy search in connector: %v", err)
	}

	return res, nil
}

func (t *Traverser) ExploreConcepts(ctx context.Context,
	principal *models.Principal, params ExploreConceptsParams) ([]VectorSearchResult, error) {

	err := t.authorizer.Authorize(principal, "get", "traversal/*")
	if err != nil {
		return nil, err
	}

	vector, err := t.vectorizer.Corpi(ctx, params.Values)
	if err != nil {
		return nil, fmt.Errorf("vectorize explore concepts search terms: %v", err)
	}

	res, err := t.vectorSearcher.VectorSearch(ctx, "concepts", vector)
	if err != nil {
		return nil, fmt.Errorf("vector search: %v", err)
	}

	spew.Dump(res)

	return res, nil
}

// ExploreConceptsParams to do a vector based explore search
type ExploreConceptsParams struct {
	Values []string
}

// FetchFuzzySearch fro LocalFetchFuzzy
type FetchFuzzySearch struct {
	Value     string
	Certainty float32
}

// VectorSearchResult contains some info of a concept (kind), but not all. For
// additional info the ID can be used to retrieve the full concept from the
// connector storage
type VectorSearchResult struct {
	ID        strfmt.UUID
	Kind      kind.Kind
	ClassName string
	Score     float32
	Vector    []float32
	Beacon    string
}

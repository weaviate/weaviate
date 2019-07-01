/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
 * LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
 * CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@semi.technology
 */package kinds

import (
	"context"
	"fmt"

	"github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
)

// SearchResult is a single search result. See wrapping Search Results for the Type
type SearchResult struct {
	Name      string
	Kind      kind.Kind
	Certainty float32
}

// SearchResults is grouping of SearchResults for a SchemaSearch
type SearchResults struct {
	Type    SearchType
	Results []SearchResult
}

// Len of the result set
func (r SearchResults) Len() int {
	return len(r.Results)
}

// LocalFetchKindClass searches for a specific class based on FetchSearch params
func (t *Traverser) LocalFetchKindClass(ctx context.Context, principal *models.Principal,
	params *FetchSearch) (interface{}, error) {

	err := t.authorizer.Authorize(principal, "get", "traversal/*")
	if err != nil {
		return nil, err
	}

	unlock, err := t.locks.LockConnector()
	if err != nil {
		return nil, fmt.Errorf("could not acquire lock: %v", err)
	}
	defer unlock()

	possibleClasses, err := t.c11y.SchemaSearch(ctx, params.Class)
	if err != nil {
		return nil, err
	}

	properties, err := t.addPossibleNamesToProperties(ctx, params.Properties)
	if err != nil {
		return nil, err
	}

	connectorParams := &FetchParams{
		Kind:               params.Class.Kind,
		PossibleClassNames: possibleClasses,
		Properties:         properties,
	}

	if len(possibleClasses.Results) == 0 {
		return nil, fmt.Errorf("the contextionary contains no close matches to " +
			"the provided class name. Try using different search terms or lowering the " +
			"desired certainty")
	}

	if len(properties) == 0 {
		return nil, fmt.Errorf("the contextionary contains no close matches to " +
			"the provided property name. Try using different search terms or lowering " +
			"the desired certainty")
	}

	return t.repo.LocalFetchKindClass(ctx, connectorParams)
}

func (t *Traverser) addPossibleNamesToProperties(ctx context.Context,
	props []FetchSearchProperty) ([]FetchProperty, error) {
	properties := make([]FetchProperty, len(props), len(props))
	for i, prop := range props {
		possibleNames, err := t.c11y.SchemaSearch(ctx, prop.Search)
		if err != nil {
			return nil, err
		}
		properties[i] = FetchProperty{
			PossibleNames: possibleNames,
			Match:         prop.Match,
		}
	}

	return properties, nil
}

// FetchSearch describes the search params for a specific class, as well as a
// list of properties which should match
type FetchSearch struct {
	Class      SearchParams
	Properties []FetchSearchProperty
}

// FetchSearchProperty describes both the search params, as well as the match
// criteria for a single property as part of a search
type FetchSearchProperty struct {
	Search SearchParams
	Match  FetchPropertyMatch
}

// TODO: don't depend on a gql package, move filters to entities

// FetchPropertyMatch defines how in the db connector this property should be used
// as a filter
type FetchPropertyMatch struct {
	Operator filters.Operator
	Value    *filters.Value
}

// FetchParams to describe the Local->GetMeta->Kind->Class query. Will be passed to
// the individual connector methods responsible for resolving the GetMeta
// query.
type FetchParams struct {
	Kind               kind.Kind
	PossibleClassNames SearchResults
	Properties         []FetchProperty
}

// FetchProperty is a combination of possible names to use for the property as well
// as a match object to perform filtering actions in the db connector based on
// this property
type FetchProperty struct {
	PossibleNames SearchResults
	Match         FetchPropertyMatch
}

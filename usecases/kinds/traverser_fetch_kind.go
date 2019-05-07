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

	contextionary "github.com/semi-technologies/weaviate/contextionary/schema"
	"github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
)

// LocalFetchKindClass searches for a specific class based on FetchSearch params
func (t *Traverser) LocalFetchKindClass(ctx context.Context, params *FetchSearch) (interface{}, error) {
	unlock, err := t.locks.LockConnector()
	if err != nil {
		return nil, fmt.Errorf("could not acquire lock: %v", err)
	}
	defer unlock()

	contextionary := t.contextionaryProvider.GetSchemaContextionary()
	possibleClasses, err := contextionary.SchemaSearch(params.Class)
	if err != nil {
		return nil, err
	}

	properties, err := t.addPossibleNamesToProperties(params.Properties, contextionary)
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

func (t *Traverser) addPossibleNamesToProperties(props []FetchSearchProperty, c11y c11y) ([]FetchProperty, error) {
	properties := make([]FetchProperty, len(props), len(props))
	for i, prop := range props {
		possibleNames, err := c11y.SchemaSearch(prop.Search)
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
	Class      contextionary.SearchParams
	Properties []FetchSearchProperty
}

// FetchSearchProperty describes both the search params, as well as the match
// criteria for a single property as part of a search
type FetchSearchProperty struct {
	Search contextionary.SearchParams
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
	PossibleClassNames contextionary.SearchResults
	Properties         []FetchProperty
}

// FetchProperty is a combination of possible names to use for the property as well
// as a match object to perform filtering actions in the db connector based on
// this property
type FetchProperty struct {
	PossibleNames contextionary.SearchResults
	Match         FetchPropertyMatch
}

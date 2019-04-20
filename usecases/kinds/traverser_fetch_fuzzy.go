/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@creativesoftwarefdn.org
 */package kinds

import "fmt"

// TODO: move contextionary and schema contextionary into uc, so we don't depend on db

func (t *Traverser) LocalFetchFuzzy(params FetchFuzzySearch) (interface{}, error) {
	words := t.contextionaryProvider.GetSchemaContextionary().
		SafeGetSimilarWordsWithCertainty(params.Value, params.Certainty)
	res, err := t.repo.LocalFetchFuzzy(words)
	if err != nil {
		return nil, fmt.Errorf("could not perform fuzzy search in connector: %v", err)
	}

	return res, nil
}

type FetchFuzzySearch struct {
	Value     string
	Certainty float32
}

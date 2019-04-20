package kinds

import "fmt"

// TODO: move contextionary and schema contextionary into uc, so we don't depend on db

func (t *Traverser) LocalFetchFuzzy(params FetchFuzzySearch) (interface{}, error) {
	words := t.contextionary.SafeGetSimilarWordsWithCertainty(params.Value, params.Certainty)
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

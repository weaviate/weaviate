package contextionary

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/creativesoftwarefdn/weaviate/database/schema/kind"
	"github.com/creativesoftwarefdn/weaviate/models"
)

// SearchType to search for either class names or property names
type SearchType string

const (
	// SearchTypeClass to search the contextionary for class names
	SearchTypeClass SearchType = "class"
	// SearchTypeProperty to search the contextionary for property names
	SearchTypeProperty SearchType = "property"
)

// SearchParams to be used for a SchemaSearch. See individual properties for
// additional documentation on what they do
type SearchParams struct {
	// SearchType can be SearchTypeClass or SearchTypeProperty
	SearchType SearchType

	// Name is the string-representation of the class or property name
	Name string

	// Keywords (optional). If no keywords are specified, only the class name
	// will be used as a search query.
	Keywords models.SemanticSchemaKeywords

	// Kind as in Thing or Class, not required if SearchType == SearchTypeProperty
	Kind kind.Kind

	// Certaintiy must be a value between 0 and 1. The higher it is the narrower
	// is the search, the lower it is, the wider the search is
	Certainty float32
}

// Validate the feasibility of the specified arguments
func (p SearchParams) Validate() error {
	return nil
}

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

// SchemaSearch can be used to search for related classes and properties, see
// documentation of SearchParams for more details on how to use it and
// documentation on SearchResults for more details on how to use the return
// value
func (mi *MemoryIndex) SchemaSearch(p SearchParams) (SearchResults, error) {
	result := SearchResults{}
	// TODO: Validation
	if err := p.Validate(); err != nil {
		return result, fmt.Errorf("invalid search params: %s", err)
	}

	rawResults, err := mi.knnSearch(p.Name)
	if err != nil {
		return result, fmt.Errorf("could not perform knn search: %s", err)
	}

	if p.SearchType == SearchTypeClass {
		return mi.handleClassSearch(p, rawResults)
	}

	return result, nil
}

func (mi *MemoryIndex) handleClassSearch(p SearchParams, search rawResults) (SearchResults, error) {
	return SearchResults{
		Type:    p.SearchType,
		Results: search.extractClassNames(p.Kind),
	}, nil
}

func (mi *MemoryIndex) knnSearch(name string) (rawResults, error) {
	name = strings.ToLower(name)
	itemIndex := mi.WordToItemIndex(name)
	if ok := itemIndex.IsPresent(); !ok {
		return nil, fmt.Errorf(
			"the word '%s' is not present in the contextionary and therefore not a valid search term", name)
	}

	list, distances, err := mi.GetNnsByItem(itemIndex, 1000000, 3)
	if err != nil {
		return nil, fmt.Errorf("could not get nearest neighbors for '%s': %s", name, err)
	}

	results := make(rawResults, len(list), len(list))
	for i := range list {
		word, err := mi.ItemIndexToWord(list[i])
		if err != nil {
			return results, fmt.Errorf("got a result from kNN search, but don't have a word for this index: %s", err)
		}

		results[i] = rawResult{
			name:     word,
			distance: distances[i],
		}
	}

	return results, nil
}

// rawResult is a helper struct to contain the results of the kNN-search. It
// does not yet contain the desired output. This means the names can be both
// classes/properties and arbitrary words. Furthermore the certainty has not
// yet been normalized , so it is merely the raw kNN distance
type rawResult struct {
	name     string
	distance float32
}

type rawResults []rawResult

func (r rawResults) extractClassNames(k kind.Kind) []SearchResult {
	var results []SearchResult
	regex := regexp.MustCompile(fmt.Sprintf("^\\$%s\\[([A-Za-z]+)\\]$", k.AllCapsName()))

	for _, rawRes := range r {
		if regex.MatchString(rawRes.name) {
			results = append(results, SearchResult{
				Name:      regex.FindStringSubmatch(rawRes.name)[1], //safe because we ran .MatchString before
				Certainty: distanceToCertainty(rawRes.distance),
				Kind:      k,
			})
		}
	}

	return results
}

func distanceToCertainty(d float32) float32 {
	return 1 - d/12
}

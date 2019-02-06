package contextionary

import (
	"testing"

	"github.com/creativesoftwarefdn/weaviate/database/schema/kind"
	"github.com/stretchr/testify/assert"
)

type SearchType string

type schemaSearchTest struct {
	name           string
	words          map[string][]float32
	searchParams   SearchParams
	expectedResult SearchResults
	expectedError  error
}

type schemaSearchTests []schemaSearchTest

func (s schemaSearchTests) Assert(t *testing.T) {
	for _, test := range s {
		t.Run(test.name, func(t *testing.T) {
			// build c11y
			builder := InMemoryBuilder(3)
			for word, vectors := range test.words {
				builder.AddWord(word, NewVector(vectors))
			}

			c11y := builder.Build(3)

			// perform search
			res, err := c11y.SchemaSearch(test.searchParams)

			// assert error
			assert.Equal(t, test.expectedError, err, "should match the expected error")

			// assert result
			assert.Equal(t, test.expectedResult, res, "should match the expected result")
		})
	}

}

func Test__SchemaSearch(t *testing.T) {
	tests := schemaSearchTests{{
		name: "className exactly in the contextionary, no keywords, no close other results",
		words: map[string][]float32{
			"$THING[Car]": {5, 5, 5},
			"car":         {4.7, 5.2, 5},
		},
		searchParams: SearchParams{
			SearchType: SearchTypeClass,
			Name:       "Car",
			Kind:       kind.THING_KIND,
			Certainty:  0.9,
		},
		expectedError: nil,
		expectedResult: SearchResults{
			Type: SearchTypeClass,
			Results: []SearchResult{
				SearchResult{
					Name:      "Car",
					Kind:      kind.THING_KIND,
					Certainty: 0.9699532,
				},
			},
		},
	}}

	tests.Assert(t)
}

package inverted

import (
	"sort"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/storobj"
	"github.com/stretchr/testify/assert"
)

func TestBM25Searcher_RankedResults(t *testing.T) {
	type testcase struct {
		testName      string
		givenObjects  []*storobj.Object
		givenScores   []float32
		expectedOrder []string
	}

	tests := []testcase{
		{
			testName: "with multiple results",
			givenObjects: []*storobj.Object{
				{Object: models.Object{ID: strfmt.UUID("40d3be3e-2ecc-49c8-b37c-d8983164848b")}},
				{Object: models.Object{ID: strfmt.UUID("31bdf9ef-d1c0-4b43-8331-1a89a48c1d2b")}},
				{Object: models.Object{ID: strfmt.UUID("4432797a-ef18-429f-83dc-d971dd9e4dd0")}},
				{Object: models.Object{ID: strfmt.UUID("8ef8c6fd-93b5-4452-b3c3-cef1cd0a18ed")}},
				{Object: models.Object{ID: strfmt.UUID("d79f0d2d-ebc5-4dad-b3df-323bc1e6f183")}},
			},
			givenScores: []float32{12, 34, 100, 43, 2},
			expectedOrder: []string{
				"4432797a-ef18-429f-83dc-d971dd9e4dd0",
				"8ef8c6fd-93b5-4452-b3c3-cef1cd0a18ed",
				"31bdf9ef-d1c0-4b43-8331-1a89a48c1d2b",
				"40d3be3e-2ecc-49c8-b37c-d8983164848b",
				"d79f0d2d-ebc5-4dad-b3df-323bc1e6f183",
			},
		},
		{
			testName: "with a single result",
			givenObjects: []*storobj.Object{
				{Object: models.Object{ID: strfmt.UUID("4a483f11-7b2f-452b-be49-f7844dbc5693")}},
			},
			givenScores: []float32{1},
			expectedOrder: []string{
				"4a483f11-7b2f-452b-be49-f7844dbc5693",
			},
		},
		{
			testName:      "with no results",
			givenObjects:  []*storobj.Object{},
			givenScores:   []float32{},
			expectedOrder: []string{},
		},
	}

	for _, test := range tests {
		t.Run(test.testName, func(t *testing.T) {
			res := &RankedResults{
				Objects: test.givenObjects,
				Scores:  test.givenScores,
			}

			sort.Sort(res)

			for i := range res.Objects {
				assert.Equal(t, test.expectedOrder[i], res.Objects[i].ID().String())
			}
		})
	}
}

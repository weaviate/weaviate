package meta

import (
	"testing"

	"github.com/creativesoftwarefdn/weaviate/database/connectors/janusgraph/state"
	"github.com/creativesoftwarefdn/weaviate/database/schema"
	gm "github.com/creativesoftwarefdn/weaviate/graphqlapi/local/getmeta"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testCase struct {
	name          string
	inputProps    []gm.MetaProperty
	expectedQuery string
}

type testCases []testCase

func (tests testCases) AssertQuery(t *testing.T, nameSource nameSource) {
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			params := &gm.Params{
				Properties: test.inputProps,
			}
			query, err := NewQuery(params, nameSource).String()
			require.Nil(t, err, "should not error")
			assert.Equal(t, test.expectedQuery, query, "should match the query")

		})

	}

}

func Test_QueryBuilder(t *testing.T) {

	tests := testCases{
		testCase{
			name: "with only a boolean, with only count",
			inputProps: []gm.MetaProperty{
				gm.MetaProperty{
					Name:                "isCapital",
					StatisticalAnalyses: []gm.StatisticalAnalysis{gm.Count},
				},
			},
			expectedQuery: `.union(` +
				`union(has("isCapital").count().as("count").project("count").by(select("count"))).as("isCapital").project("isCapital").by(select("isCapital"))` +
				`)`,
		},
	}

	tests.AssertQuery(t, nil)

}

func Test_QueryBuilderWithNamesource(t *testing.T) {

	tests := testCases{
		testCase{
			name: "with only a boolean, with only count",
			inputProps: []gm.MetaProperty{
				gm.MetaProperty{
					Name:                "isCapital",
					StatisticalAnalyses: []gm.StatisticalAnalysis{gm.Count},
				},
			},
			expectedQuery: `.union(` +
				`union(has("prop_20").count().as("count").project("count").by(select("count"))).as("isCapital").project("isCapital").by(select("isCapital"))` +
				`)`,
		},
	}

	tests.AssertQuery(t, &fakeNameSource{})

}

type fakeNameSource struct{}

func (f *fakeNameSource) GetMappedPropertyName(className schema.ClassName,
	propName schema.PropertyName) state.MappedPropertyName {
	switch propName {
	case schema.PropertyName("inCountry"):
		return "prop_15"
	}
	return state.MappedPropertyName("prop_20")
}

func (f *fakeNameSource) GetMappedClassName(className schema.ClassName) state.MappedClassName {
	return state.MappedClassName("class_18")
}

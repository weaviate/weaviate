package meta

import (
	"testing"

	gm "github.com/creativesoftwarefdn/weaviate/graphqlapi/local/getmeta"
)

func Test_QueryBuilder_RefProps(t *testing.T) {
	tests := testCases{
		testCase{
			name: "with ref prop and only count",
			inputProps: []gm.MetaProperty{
				gm.MetaProperty{
					Name:                "InCountry",
					StatisticalAnalyses: []gm.StatisticalAnalysis{gm.Count},
				},
			},
			expectedQuery: `
				.union(
					union(
						outE("inCountry").count().as("refcount").project("count").by(select("refcount"))
					)
					.as("InCountry").project("InCountry").by(select("InCountry"))
				)
			`,
		},

		testCase{
			name: "with ref prop and: pointingTo, count",
			inputProps: []gm.MetaProperty{
				gm.MetaProperty{
					Name:                "InCountry",
					StatisticalAnalyses: []gm.StatisticalAnalysis{gm.PointingTo, gm.Count},
				},
			},
			expectedQuery: `
				.union(
					union(
						outE("inCountry").count().as("refcount").project("count").by(select("refcount"))
					)
					.as("InCountry").project("InCountry").by(select("InCountry"))
				)
			`,
		},

		testCase{
			name: "with only pointingTo",
			inputProps: []gm.MetaProperty{
				gm.MetaProperty{
					Name:                "InCountry",
					StatisticalAnalyses: []gm.StatisticalAnalysis{gm.PointingTo},
				},
			},
			expectedQuery: `
				.union()
			`,
		},
	}

	tests.AssertQuery(t, nil)
}

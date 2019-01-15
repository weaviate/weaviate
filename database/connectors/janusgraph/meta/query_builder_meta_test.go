package meta

import (
	"testing"

	gm "github.com/creativesoftwarefdn/weaviate/graphqlapi/local/getmeta"
)

func Test_QueryBuilder_MetaProps(t *testing.T) {
	tests := testCases{
		testCase{
			name: "with meta.count",
			inputProps: []gm.MetaProperty{
				gm.MetaProperty{
					Name:                "meta",
					StatisticalAnalyses: []gm.StatisticalAnalysis{gm.Count},
				},
			},
			expectedQuery: `
				.union(
					union(
						count().as("count").project("count").by(select("count"))
					)
					.as("meta").project("meta").by(select("meta"))
				)
			`,
		},
	}

	tests.AssertQuery(t, nil)
}

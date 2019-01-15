package meta

import (
	"testing"

	gm "github.com/creativesoftwarefdn/weaviate/graphqlapi/local/getmeta"
)

func Test_QueryBuilder_NumberProps(t *testing.T) {
	tests := testCases{
		testCase{
			name: "with only an int, with only count",
			inputProps: []gm.MetaProperty{
				gm.MetaProperty{
					Name:                "area",
					StatisticalAnalyses: []gm.StatisticalAnalysis{gm.Count},
				},
			},
			expectedQuery: `
				.union(
					aggregate("aggregation").by("area").cap("aggregation").limit(1)
						.as("count")
						.select("count")
						.by(count(local))
						.as("count_combined").project("count").by(select("count_combined"))
						.as("area").project("area").by(select("area"))
				)
			`,
		},

		testCase{
			name: "with only an int, with all props",
			inputProps: []gm.MetaProperty{
				gm.MetaProperty{
					Name: "area",
					StatisticalAnalyses: []gm.StatisticalAnalysis{
						gm.Average, gm.Sum, gm.Highest, gm.Lowest, gm.Count, gm.Type,
					},
				},
			},
			expectedQuery: `
				.union(
					aggregate("aggregation").by("area").cap("aggregation").limit(1)
						.as("average", "sum", "highest", "lowest", "count")
						.select("average", "sum", "highest", "lowest", "count")
						.by(mean(local)).by(sum(local)).by(max(local)).by(min(local)).by(count(local))
						.as("area").project("area").by(select("area"))
				)
			`,
		},
	}

	tests.AssertQuery(t, nil)

}

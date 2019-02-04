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
 */
package meta

import (
	"testing"

	gm "github.com/creativesoftwarefdn/weaviate/graphqlapi/local/getmeta"
)

func Test_QueryBuilder_IntProps(t *testing.T) {
	tests := testCases{
		testCase{
			name: "with only an int, with only count",
			inputProps: []gm.MetaProperty{
				gm.MetaProperty{
					Name:                "population",
					StatisticalAnalyses: []gm.StatisticalAnalysis{gm.Count},
				},
			},
			expectedQuery: `
				.union(
					aggregate("aggregation").by("population").cap("aggregation").limit(1)
						.as("count")
						.select("count")
						.by(count(local))
						.as("count_combined").project("count").by(select("count_combined"))
						.as("population").project("population").by(select("population"))
				)
			`,
		},

		testCase{
			name: "with only an int, with all props",
			inputProps: []gm.MetaProperty{
				gm.MetaProperty{
					Name: "population",
					StatisticalAnalyses: []gm.StatisticalAnalysis{
						gm.Mean, gm.Type, gm.Sum, gm.Maximum, gm.Minimum, gm.Count,
					},
				},
			},
			expectedQuery: `
				.union(
					aggregate("aggregation").by("population").cap("aggregation").limit(1)
						.as("mean", "sum", "maximum", "minimum", "count")
						.select("mean", "sum", "maximum", "minimum", "count")
						.by(mean(local)).by(sum(local)).by(max(local)).by(min(local)).by(count(local))
						.as("population").project("population").by(select("population"))
				)
			`,
		},
	}

	tests.AssertQuery(t, nil)

}

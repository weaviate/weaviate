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

// This file contains only a single test to verify that combining multiple
// props together works as intended and helpers for other tests. Each
// individual property type however, is also extensively tested, please see the
// test files for individual props with the format query_builder_<type>_test.go

func Test_QueryBuilder_MultipleProps(t *testing.T) {
	tests := testCases{
		testCase{
			name: "with multiple props",
			inputProps: []gm.MetaProperty{
				gm.MetaProperty{
					Name: "isCapital",
					StatisticalAnalyses: []gm.StatisticalAnalysis{
						gm.Count, gm.TotalTrue, gm.TotalFalse, gm.PercentageTrue, gm.PercentageFalse,
					},
				},
				gm.MetaProperty{
					Name: "population",
					StatisticalAnalyses: []gm.StatisticalAnalysis{
						gm.Average, gm.Sum, gm.Highest, gm.Lowest, gm.Count,
					},
				},
			},
			expectedQuery: `
				.union(
					union(
						has("isCapital").count()
							.as("count").project("count").by(select("count")),
						groupCount().by("isCapital")
							.as("boolGroupCount").project("boolGroupCount").by(select("boolGroupCount"))
					)
						.as("isCapital").project("isCapital").by(select("isCapital")),
					aggregate("aggregation").by("population").cap("aggregation").limit(1)
						.as("average", "sum", "highest", "lowest", "count")
						.select("average", "sum", "highest", "lowest", "count")
						.by(mean(local)).by(sum(local)).by(max(local)).by(min(local)).by(count(local))
						.as("population").project("population").by(select("population"))
				)
			`,
		},
	}

	tests.AssertQuery(t, nil)
}

func Test_QueryBuilder_MultiplePropsWithFilter(t *testing.T) {
	tests := testCases{
		testCase{
			name: "with multiple props",
			inputProps: []gm.MetaProperty{
				gm.MetaProperty{
					Name: "isCapital",
					StatisticalAnalyses: []gm.StatisticalAnalysis{
						gm.Count, gm.TotalTrue, gm.TotalFalse, gm.PercentageTrue, gm.PercentageFalse,
					},
				},
				gm.MetaProperty{
					Name: "population",
					StatisticalAnalyses: []gm.StatisticalAnalysis{
						gm.Average, gm.Sum, gm.Highest, gm.Lowest, gm.Count,
					},
				},
			},
			expectedQuery: `
			  .has("foo", eq("bar"))
				.union(
					union(
						has("isCapital").count()
							.as("count").project("count").by(select("count")),
						groupCount().by("isCapital")
							.as("boolGroupCount").project("boolGroupCount").by(select("boolGroupCount"))
					)
						.as("isCapital").project("isCapital").by(select("isCapital")),
					aggregate("aggregation").by("population").cap("aggregation").limit(1)
						.as("average", "sum", "highest", "lowest", "count")
						.select("average", "sum", "highest", "lowest", "count")
						.by(mean(local)).by(sum(local)).by(max(local)).by(min(local)).by(count(local))
						.as("population").project("population").by(select("population"))
				)
			`,
		},
	}

	filter := &fakeFilterSource{
		queryToReturn: `.has("foo", eq("bar"))`,
	}

	tests.AssertQueryWithFilterSource(t, nil, filter)
}

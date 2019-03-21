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
						gm.Mean, gm.Sum, gm.Maximum, gm.Minimum, gm.Count,
					},
				},
			},
			expectedQuery: `
				.union(
					values("isCapital").union(
						count().project("count").project("isCapital"),
						groupCount().unfold().project("isCapital")
					),
				  values("population").union(
					  mean().project("mean").project("population"),
					  sum().project("sum").project("population"),
					  max().project("maximum").project("population"),
					  min().project("minimum").project("population"),
					  count().project("count").project("population")
					)
				)
				.group().by(select(keys).unfold()).by(
					select(values).unfold().group()
					.by( select(keys).unfold())
					.by( select(values).unfold())
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
						gm.Mean, gm.Sum, gm.Maximum, gm.Minimum, gm.Count,
					},
				},
			},
			expectedQuery: `
			  .has("foo", eq("bar"))
				.union(
					values("isCapital").union(
						count().project("count").project("isCapital"),
						groupCount().unfold().project("isCapital")
					),
				  values("population").union(
					  mean().project("mean").project("population"),
					  sum().project("sum").project("population"),
					  max().project("maximum").project("population"),
					  min().project("minimum").project("population"),
					  count().project("count").project("population")
					)
				)
				.group().by(select(keys).unfold()).by(
					select(values).unfold().group()
					.by( select(keys).unfold())
					.by( select(values).unfold())
				)
			`,
		},
	}

	filter := &fakeFilterSource{
		queryToReturn: `.has("foo", eq("bar"))`,
	}

	tests.AssertQueryWithFilterSource(t, nil, filter)
}

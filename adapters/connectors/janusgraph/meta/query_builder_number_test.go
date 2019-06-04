/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/semi-technologies/weaviate/blob/develop/LICENSE.md
 * DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@semi.technology
 */
package meta

import (
	"testing"

	"github.com/semi-technologies/weaviate/usecases/traverser"
)

func Test_QueryBuilder_NumberProps(t *testing.T) {
	tests := testCases{
		testCase{
			name: "with only an int, with only count",
			inputProps: []traverser.MetaProperty{
				traverser.MetaProperty{
					Name:                "area",
					StatisticalAnalyses: []traverser.StatisticalAnalysis{traverser.Count},
				},
			},
			expectedQuery: `
				.union(
				  values("area").union(
					  count().project("count").project("area")
					)
				)
				.group().by(select(keys).unfold()).by(
					select(values).unfold().group()
					.by( select(keys).unfold())
					.by( select(values).unfold())
				)
			`,
		},

		testCase{
			name: "with only an int, with all props",
			inputProps: []traverser.MetaProperty{
				traverser.MetaProperty{
					Name: "area",
					StatisticalAnalyses: []traverser.StatisticalAnalysis{
						traverser.Mean, traverser.Type, traverser.Sum, traverser.Maximum, traverser.Minimum, traverser.Count,
					},
				},
			},
			expectedQuery: `
				.union(
				  values("area").union(
					  mean().project("mean").project("area"),
					  sum().project("sum").project("area"),
					  max().project("maximum").project("area"),
					  min().project("minimum").project("area"),
					  count().project("count").project("area")
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

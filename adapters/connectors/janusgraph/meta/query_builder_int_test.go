//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2019 Weaviate. All rights reserved.
//  LICENSE: https://github.com/semi-technologies/weaviate/blob/develop/LICENSE.md
//  DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

package meta

import (
	"testing"

	"github.com/semi-technologies/weaviate/usecases/traverser"
)

func Test_QueryBuilder_IntProps(t *testing.T) {
	tests := testCases{
		testCase{
			name: "with only an int, with only count",
			inputProps: []traverser.MetaProperty{
				traverser.MetaProperty{
					Name:                "population",
					StatisticalAnalyses: []traverser.StatisticalAnalysis{traverser.Count},
				},
			},
			expectedQuery: `
				.union(
				  values("population").union(
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

		testCase{
			name: "with only an int, with all props",
			inputProps: []traverser.MetaProperty{
				traverser.MetaProperty{
					Name: "population",
					StatisticalAnalyses: []traverser.StatisticalAnalysis{
						traverser.Mean, traverser.Type, traverser.Sum, traverser.Maximum, traverser.Minimum, traverser.Count,
					},
				},
			},
			expectedQuery: `
				.union(
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

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

	"github.com/semi-technologies/weaviate/usecases/kinds"
)

func Test_QueryBuilder_RefProps(t *testing.T) {
	tests := testCases{
		testCase{
			name: "with ref prop and only count",
			inputProps: []kinds.MetaProperty{
				kinds.MetaProperty{
					Name:                "InCountry",
					StatisticalAnalyses: []kinds.StatisticalAnalysis{kinds.Count},
				},
			},
			expectedQuery: `
				.union(
						outE("inCountry").count().project("count").project("InCountry")
				)
				.group().by(select(keys).unfold()).by(
					select(values).unfold().group()
					.by( select(keys).unfold())
					.by( select(values).unfold())
				)
			`,
		},

		testCase{
			name: "with ref prop and: pointingTo, count",
			inputProps: []kinds.MetaProperty{
				kinds.MetaProperty{
					Name:                "InCountry",
					StatisticalAnalyses: []kinds.StatisticalAnalysis{kinds.PointingTo, kinds.Count},
				},
			},
			expectedQuery: `
				.union(
						outE("inCountry").count().project("count").project("InCountry")
				)
				.group().by(select(keys).unfold()).by(
					select(values).unfold().group()
					.by( select(keys).unfold())
					.by( select(values).unfold())
				)
			`,
		},

		testCase{
			name: "with only pointingTo",
			inputProps: []kinds.MetaProperty{
				kinds.MetaProperty{
					Name:                "InCountry",
					StatisticalAnalyses: []kinds.StatisticalAnalysis{kinds.PointingTo},
				},
			},
			expectedQuery: `
				.union().group().by(select(keys).unfold()).by(
					select(values).unfold().group()
					.by( select(keys).unfold())
					.by( select(values).unfold())
				)
			`,
		},
	}

	tests.AssertQuery(t, nil)
}

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

	"github.com/creativesoftwarefdn/weaviate/usecases/kinds"
)

func Test_QueryBuilder_StringProps(t *testing.T) {
	tests := testCases{
		testCase{
			name: "with only a string, with only topOccurrences.value",
			inputProps: []kinds.MetaProperty{
				kinds.MetaProperty{
					Name:                "name",
					StatisticalAnalyses: []kinds.StatisticalAnalysis{kinds.TopOccurrencesValue},
				},
			},
			expectedQuery: `
				.union(
						has("name").groupCount().by("name")
							.order(local).by(values, decr).limit(local, 3).project("topOccurrences").project("name")
				)
				.group().by(select(keys).unfold()).by(
					select(values).unfold().group()
					.by( select(keys).unfold())
					.by( select(values).unfold())
				)
			`,
		},

		testCase{
			name: "with only a string, with only both topOccurrences.value and .occurs",
			inputProps: []kinds.MetaProperty{
				kinds.MetaProperty{
					Name:                "name",
					StatisticalAnalyses: []kinds.StatisticalAnalysis{kinds.TopOccurrencesValue, kinds.TopOccurrencesOccurs},
				},
			},
			expectedQuery: `
				.union(
						has("name").groupCount().by("name")
							.order(local).by(values, decr).limit(local, 3).project("topOccurrences").project("name")
				)
				.group().by(select(keys).unfold()).by(
					select(values).unfold().group()
					.by( select(keys).unfold())
					.by( select(values).unfold())
				)
			`,
		},

		testCase{
			name: "with only a string, with all possible props",
			inputProps: []kinds.MetaProperty{
				kinds.MetaProperty{
					Name:                "name",
					StatisticalAnalyses: []kinds.StatisticalAnalysis{kinds.Type, kinds.Count, kinds.TopOccurrencesValue, kinds.TopOccurrencesOccurs},
				},
			},
			expectedQuery: `
				.union(
				    has("name").count().project("count").project("name"),
						has("name").groupCount().by("name")
							.order(local).by(values, decr).limit(local, 3).project("topOccurrences").project("name")
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

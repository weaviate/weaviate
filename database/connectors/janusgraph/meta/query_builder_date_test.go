/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2018 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * AUTHOR: Bob van Luijt (bob@kub.design)
 * See www.creativesoftwarefdn.org for details
 * Contact: @CreativeSofwFdn / bob@kub.design
 */
package meta

import (
	"testing"

	gm "github.com/creativesoftwarefdn/weaviate/graphqlapi/local/getmeta"
)

func Test_QueryBuilder_DateProps(t *testing.T) {
	tests := testCases{
		testCase{
			name: "with only a string, with only topOccurrences.value",
			inputProps: []gm.MetaProperty{
				gm.MetaProperty{
					Name:                "dateOfFirstApperance",
					StatisticalAnalyses: []gm.StatisticalAnalysis{gm.TopOccurrencesValue},
				},
			},
			expectedQuery: `
				.union(
					union(
						groupCount().by("dateOfFirstApperance")
							.order(local).by(values, decr).limit(local, 3)
							.as("topOccurrences").project("topOccurrences").by(select("topOccurrences"))
						)
          .as("dateOfFirstApperance").project("dateOfFirstApperance").by(select("dateOfFirstApperance"))
				)
			`,
		},

		testCase{
			name: "with only a string, with only both topOccurrences.value and .occurs",
			inputProps: []gm.MetaProperty{
				gm.MetaProperty{
					Name:                "dateOfFirstApperance",
					StatisticalAnalyses: []gm.StatisticalAnalysis{gm.TopOccurrencesValue, gm.TopOccurrencesOccurs},
				},
			},
			expectedQuery: `
				.union(
					union(
						groupCount().by("dateOfFirstApperance")
							.order(local).by(values, decr).limit(local, 3)
							.as("topOccurrences").project("topOccurrences").by(select("topOccurrences"))
						)
          .as("dateOfFirstApperance").project("dateOfFirstApperance").by(select("dateOfFirstApperance"))
				)
			`,
		},

		testCase{
			name: "with only a string, with all possible props",
			inputProps: []gm.MetaProperty{
				gm.MetaProperty{
					Name: "dateOfFirstApperance",
					StatisticalAnalyses: []gm.StatisticalAnalysis{
						gm.Count, gm.Type, gm.TopOccurrencesValue, gm.TopOccurrencesOccurs},
				},
			},
			expectedQuery: `
				.union(
					union(
						has("dateOfFirstApperance").count()
							.as("count").project("count").by(select("count")),
						groupCount().by("dateOfFirstApperance")
							.order(local).by(values, decr).limit(local, 3)
							.as("topOccurrences").project("topOccurrences").by(select("topOccurrences"))
						)
				.as("dateOfFirstApperance").project("dateOfFirstApperance").by(select("dateOfFirstApperance"))
					)
			`,
		},
	}

	tests.AssertQuery(t, nil)

}

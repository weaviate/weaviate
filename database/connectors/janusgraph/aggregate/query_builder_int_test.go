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
package aggregate

import (
	"testing"

	"github.com/creativesoftwarefdn/weaviate/database/schema"
	ag "github.com/creativesoftwarefdn/weaviate/graphqlapi/local/aggregate"
	cf "github.com/creativesoftwarefdn/weaviate/graphqlapi/local/common_filters"
)

func Test_QueryBuilder_IntProps(t *testing.T) {
	tests := testCases{
		testCase{
			name: "with only an int, with only count",
			inputProps: []ag.Property{
				ag.Property{
					Name:        "population",
					Aggregators: []ag.Aggregator{ag.Count},
				},
			},
			inputGroupBy: &cf.Path{
				Class:    schema.ClassName("City"),
				Property: schema.PropertyName("isCapital"),
			},
			expectedQuery: `
				.group().by("isCapital").by(
					fold()
						.match(
							__.as("a").unfold().values("population").count().as("population__count")
						)
						.select("population__count").by(project("population__count")).as("population")
						.select("population").by(project("population"))
					)
				`,
		},

		// testCase{
		// 	name: "with only an int, with all props",
		// 	inputProps: []ag.Property{
		// 		ag.Property{
		// 			Name: "population",
		// 			Aggregators: []ag.Aggregator{
		// 				ag.Average, ag.Type, ag.Sum, ag.Highest, ag.Lowest, ag.Count,
		// 			},
		// 		},
		// 	},
		// 	expectedQuery: `
		// 		.union(
		// 			aggregate("aggregation").by("population").cap("aggregation").limit(1)
		// 				.as("average", "sum", "highest", "lowest", "count")
		// 				.select("average", "sum", "highest", "lowest", "count")
		// 				.by(mean(local)).by(sum(local)).by(max(local)).by(min(local)).by(count(local))
		// 				.as("population").project("population").by(select("population"))
		// 		)
		// 	`,
		// },
	}

	tests.AssertQuery(t, nil)

}

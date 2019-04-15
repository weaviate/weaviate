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
package aggregate

import (
	"testing"

	ag "github.com/creativesoftwarefdn/weaviate/adapters/handlers/graphql/local/aggregate"
	cf "github.com/creativesoftwarefdn/weaviate/adapters/handlers/graphql/local/common_filters"
	"github.com/creativesoftwarefdn/weaviate/database/schema"
)

func Test_QueryBuilder_IntProps(t *testing.T) {
	tests := testCases{
		testCase{
			name: "with only an int, with only count, grouped by a primitive prop",
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
							__.as("a").unfold().values("population").count().project("population__count").as("population__count")
						)
						.select("population__count").as("population").project("population")
					)
				`,
		},

		testCase{
			name: "with only an int, with all possible int props (except median), grouped by a primitive prop",
			inputProps: []ag.Property{
				ag.Property{
					Name:        "population",
					Aggregators: []ag.Aggregator{ag.Count, ag.Mean, ag.Sum, ag.Maximum, ag.Minimum, ag.Mode},
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
						__.as("a").unfold().values("population").count().as("population__count"),
						__.as("a").unfold().values("population").mean().as("population__mean"),
						__.as("a").unfold().values("population").sum().as("population__sum"),
						__.as("a").unfold().values("population").max().as("population__maximum"),
						__.as("a").unfold().values("population").min().as("population__minimum"),
						__.as("a").unfold().values("population").groupCount()
								.order(local).by(values, decr).select(keys).limit(local, 1).as("population__mode")
					)
						.select(
							"population__count", "population__mean", "population__sum", "population__maximum", "population__minimum", "population__mode"
						)
						.as("population")
						.project("population")
					)
				`,
		},
	}

	tests.AssertQuery(t, nil)

}

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

	"github.com/creativesoftwarefdn/weaviate/database/schema"
	ag "github.com/creativesoftwarefdn/weaviate/adapters/handlers/graphql/local/aggregate"
	cf "github.com/creativesoftwarefdn/weaviate/adapters/handlers/graphql/local/common_filters"
)

func Test_QueryBuilder_NonNumericalProps(t *testing.T) {
	tests := testCases{
		testCase{
			name: "counting a stirng prop, grouped by a primitive prop",
			inputProps: []ag.Property{
				ag.Property{
					Name:        "name",
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
							__.as("a").unfold().values("name").count().project("name__count").as("name__count")
						)
						.select("name__count").as("name").project("name")
					)
				`,
		},

		testCase{
			name: "counting a date prop, grouped by a primitive prop",
			inputProps: []ag.Property{
				ag.Property{
					Name:        "dateOfFirstApperance",
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
							__.as("a").unfold().values("dateOfFirstApperance").count().project("dateOfFirstApperance__count").as("dateOfFirstApperance__count")
						)
						.select("dateOfFirstApperance__count").as("dateOfFirstApperance").project("dateOfFirstApperance")
					)
				`,
		},

		testCase{
			name: "counting a bool prop, grouped by a primitive prop",
			inputProps: []ag.Property{
				ag.Property{
					Name:        "isCapital",
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
							__.as("a").unfold().values("isCapital").count().project("isCapital__count").as("isCapital__count")
						)
						.select("isCapital__count").as("isCapital").project("isCapital")
					)
				`,
		},
	}

	tests.AssertQuery(t, nil)

}

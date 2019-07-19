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

package aggregate

import (
	"testing"

	"github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/usecases/traverser"
)

// These tests only assert that multiple props work together correctly. See the
// tests for the individual property types for more detailed tests.

func Test_QueryBuilder_MultipleProps(t *testing.T) {
	tests := testCases{
		testCase{
			name: "counting a stirng prop, grouped by a primitive prop",
			inputProps: []traverser.AggregateProperty{
				traverser.AggregateProperty{
					Name:        "name",
					Aggregators: []traverser.Aggregator{traverser.CountAggregator},
				},
				traverser.AggregateProperty{
					Name:        "population",
					Aggregators: []traverser.Aggregator{traverser.CountAggregator},
				},
			},
			inputGroupBy: &filters.Path{
				Class:    schema.ClassName("City"),
				Property: schema.PropertyName("isCapital"),
			},
			expectedQuery: `
				.group().by("isCapital").by(
					fold()
						.match(
							__.as("a").unfold().values("name").count().project("name__count").as("name__count"),
							__.as("a").unfold().values("population").count().project("population__count").as("population__count")
						)
						.select("name__count").as("name")
						.select("population__count").as("population")
						.select("name", "population")
					)
				`,
		},
	}

	tests.AssertQuery(t, nil)
}

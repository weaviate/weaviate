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

package aggregate

import (
	"testing"

	"github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/usecases/traverser"
)

func Test_QueryBuilder_VariousGroupingStrategies_WithNameSource(t *testing.T) {
	propList := func() []traverser.AggregateProperty {
		return []traverser.AggregateProperty{
			traverser.AggregateProperty{
				Name:        "name",
				Aggregators: []traverser.Aggregator{traverser.CountAggregator},
			},
			traverser.AggregateProperty{
				Name:        "population",
				Aggregators: []traverser.Aggregator{traverser.CountAggregator},
			},
		}
	}

	matchSelectQuery := func() string {
		return `.by(
					fold()
						.match(
							__.as("a").unfold().values("prop_1").count().project("prop_1__count").as("prop_1__count"),
							__.as("a").unfold().values("prop_2").count().project("prop_2__count").as("prop_2__count")
						)
						.select("prop_1__count").as("name")
						.select("prop_2__count").as("population")
						.select("name", "population")
					)`
	}

	tests := testCases{
		testCase{
			name:       "group by single primitive prop",
			inputProps: propList(),
			inputGroupBy: &filters.Path{
				Class:    schema.ClassName("City"),
				Property: schema.PropertyName("isCapital"),
			},
			expectedQuery: `.group().by("prop_4")` + matchSelectQuery(),
		},
		testCase{
			name:       "group by reference one level deep",
			inputProps: propList(),
			inputGroupBy: &filters.Path{
				Class:    schema.ClassName("City"),
				Property: schema.PropertyName("inCountry"),
				Child: &filters.Path{
					Class:    schema.ClassName("Country"),
					Property: schema.PropertyName("name"),
				},
			},
			expectedQuery: `.group().by(out("prop_3").has("classId", "class_18").values("prop_1"))` + matchSelectQuery(),
		},
		testCase{
			name:       "group by reference 2 levels deep",
			inputProps: propList(),
			inputGroupBy: &filters.Path{
				Class:    schema.ClassName("City"),
				Property: schema.PropertyName("inCountry"),
				Child: &filters.Path{
					Class:    schema.ClassName("Country"),
					Property: schema.PropertyName("inContinent"),
					Child: &filters.Path{
						Class:    schema.ClassName("Continent"),
						Property: schema.PropertyName("name"),
					},
				},
			},
			expectedQuery: `.group().by(
				out("prop_3").has("classId", "class_18")
				.out("prop_5").has("classId", "class_19")
				.values("prop_1")
			)` + matchSelectQuery(),
		},
	}

	tests.AssertQuery(t, &fakeNameSource{})
}

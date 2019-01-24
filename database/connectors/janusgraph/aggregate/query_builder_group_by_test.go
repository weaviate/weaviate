package aggregate

import (
	"testing"

	"github.com/creativesoftwarefdn/weaviate/database/schema"
	ag "github.com/creativesoftwarefdn/weaviate/graphqlapi/local/aggregate"
	cf "github.com/creativesoftwarefdn/weaviate/graphqlapi/local/common_filters"
)

func Test_QueryBuilder_VariousGroupingStrategies_WithNameSource(t *testing.T) {
	propList := func() []ag.Property {
		return []ag.Property{
			ag.Property{
				Name:        "name",
				Aggregators: []ag.Aggregator{ag.Count},
			},
			ag.Property{
				Name:        "population",
				Aggregators: []ag.Aggregator{ag.Count},
			},
		}
	}

	matchSelectQuery := func() string {
		return `.by(
					fold()
						.match(
							__.as("a").unfold().values("prop_1").count().as("prop_1__count"),
							__.as("a").unfold().values("prop_2").count().as("prop_2__count")
						)
						.select("prop_1__count").by(project("prop_1__count")).as("name")
						.select("prop_2__count").by(project("prop_2__count")).as("population")
						.select("name", "population")
					)`
	}

	tests := testCases{
		testCase{
			name:       "group by single primitive prop",
			inputProps: propList(),
			inputGroupBy: &cf.Path{
				Class:    schema.ClassName("City"),
				Property: schema.PropertyName("isCapital"),
			},
			expectedQuery: `.group().by("prop_4")` + matchSelectQuery(),
		},
		testCase{
			name:       "group by reference one level deep",
			inputProps: propList(),
			inputGroupBy: &cf.Path{
				Class:    schema.ClassName("City"),
				Property: schema.PropertyName("inCountry"),
				Child: &cf.Path{
					Class:    schema.ClassName("Country"),
					Property: schema.PropertyName("name"),
				},
			},
			expectedQuery: `.group().by(out("prop_3").has("classId", "class_18").values("prop_1"))` + matchSelectQuery(),
		},
		testCase{
			name:       "group by reference 2 levels deep",
			inputProps: propList(),
			inputGroupBy: &cf.Path{
				Class:    schema.ClassName("City"),
				Property: schema.PropertyName("inCountry"),
				Child: &cf.Path{
					Class:    schema.ClassName("Country"),
					Property: schema.PropertyName("inContinent"),
					Child: &cf.Path{
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

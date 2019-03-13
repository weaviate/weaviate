package aggregate

import (
	"testing"

	"github.com/creativesoftwarefdn/weaviate/config"
	"github.com/creativesoftwarefdn/weaviate/database/schema"
	"github.com/creativesoftwarefdn/weaviate/database/schema/kind"
	"github.com/creativesoftwarefdn/weaviate/graphqlapi/local/common_filters"
)

func Test_ExtractAnalyticsPropsFromAggregate(t *testing.T) {
	t.Parallel()

	query :=
		`{ Aggregate { Things { 
			Car(groupBy:["madeBy", "Manufacturer", "name"], useAnalyticsEngine: true, forceRecalculate: true) { 
				horsepower { mean } 
			} 
		} } }`

	analytics := common_filters.AnalyticsProps{
		UseAnaltyicsEngine: true,
		ForceRecalculate:   true,
	}
	cfg := config.Environment{
		AnalyticsEngine: config.AnalyticsEngine{
			Enabled: true,
		},
	}

	expectedParams := &Params{
		Kind:      kind.THING_KIND,
		ClassName: schema.ClassName("Car"),
		Properties: []Property{
			{
				Name:        "horsepower",
				Aggregators: []Aggregator{Mean},
			},
		},
		GroupBy:   groupCarByMadeByManufacturerName(),
		Analytics: analytics,
	}
	resolver := newMockResolver(cfg)
	resolver.On("LocalAggregate", expectedParams).
		Return([]interface{}{}, nil).Once()

	resolver.AssertResolve(t, query)
}

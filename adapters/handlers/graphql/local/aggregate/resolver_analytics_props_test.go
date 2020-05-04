//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Holding B.V. (registered @ Dutch Chamber of Commerce no 75221632). All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

package aggregate

import (
	"testing"

	"github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
	"github.com/semi-technologies/weaviate/usecases/config"
	"github.com/semi-technologies/weaviate/usecases/traverser"
)

func Test_ExtractAnalyticsPropsFromAggregate(t *testing.T) {
	t.Parallel()

	query :=
		`{ Aggregate { Things { 
			Car(groupBy:["madeBy", "Manufacturer", "name"], useAnalyticsEngine: true, forceRecalculate: true) { 
				horsepower { mean } 
			} 
		} } }`

	analytics := filters.AnalyticsProps{
		UseAnaltyicsEngine: true,
		ForceRecalculate:   true,
	}
	cfg := config.Config{
		AnalyticsEngine: config.AnalyticsEngine{
			Enabled: true,
		},
	}

	expectedParams := &traverser.AggregateParams{
		Kind:      kind.Thing,
		ClassName: schema.ClassName("Car"),
		Properties: []traverser.AggregateProperty{
			{
				Name:        "horsepower",
				Aggregators: []traverser.Aggregator{traverser.MeanAggregator},
			},
		},
		GroupBy:   groupCarByMadeByManufacturerName(),
		Analytics: analytics,
	}
	resolver := newMockResolver(cfg)
	resolver.On("Aggregate", expectedParams).
		Return([]interface{}{}, nil).Once()

	resolver.AssertResolve(t, query)
}

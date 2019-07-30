//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2019 Weaviate. All rights reserved.
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
	"github.com/semi-technologies/weaviate/usecases/traverser"
)

func Test_QueryBuilder_ReferenceProps(t *testing.T) {
	tests := testCases{
		testCase{
			name: "counting a ref prop, grouped by a primitive prop",
			inputProps: []traverser.AggregateProperty{
				traverser.AggregateProperty{
					Name:        "inCountry",
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
							__.as("a").unfold().out("inCountry").count().project("inCountry__count").as("inCountry__count")
						)
						.select("inCountry__count").as("inCountry").project("inCountry")
					)
				`,
		},
	}

	tests.AssertQuery(t, nil)

}

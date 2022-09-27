//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package test

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/semi-technologies/weaviate/test/helper"
	graphqlhelper "github.com/semi-technologies/weaviate/test/helper/graphql"
	"github.com/stretchr/testify/assert"
)

func getResult(result *graphqlhelper.GraphQLResult, className, resultData, property string) interface{} {
	meta := result.Get("Aggregate", className).AsSlice()[0].(map[string]interface{})[property]
	return meta.(map[string]interface{})[resultData]
}

// run by setup_test.go
func runningAggregateArrayClassSanityCheck(t *testing.T) {
	t.Run("running Aggregate against City", func(t *testing.T) {
		query := `
			{
			  Aggregate {
				City
				%s
				{
				  meta {
					count
				  }
				  cityArea {
					count
				  }
				  isCapital {
					count
				  }
				  population {
					count
				  }
				  cityRights {
					count
				  }
				  history {
					count
				  }
				}
			  }
			}
		`

		tests := []struct {
			name    string
			filters string
		}{
			{
				name: "without filters",
			},
			{
				name:    "with where filter",
				filters: `( where:{operator: Like path:["id"] valueString:"*"} )`,
			},
			{
				name:    "with nearObject filter",
				filters: `( nearObject:{id: "660db307-a163-41d2-8182-560782cd018f" certainty: 0.1} )`,
			},
			{
				name: "with where and nearObject filter",
				filters: `(
					where:{operator: Like path:["id"] valueString:"*"}
					nearObject:{id: "660db307-a163-41d2-8182-560782cd018f" certainty: 0.1}
				)`,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, fmt.Sprintf(query, tt.filters))
				// One city object (missing island) has no entries for most properties (except population, isCapital and
				// location) and another one (null island) has nil for every entry
				assert.Equal(t, json.Number("6"), getResult(result, "City", "count", "meta"))
				assert.Equal(t, json.Number("4"), getResult(result, "City", "count", "cityArea"))
				assert.Equal(t, json.Number("5"), getResult(result, "City", "count", "isCapital"))
				assert.Equal(t, json.Number("5"), getResult(result, "City", "count", "population"))
				assert.Equal(t, json.Number("4"), getResult(result, "City", "count", "cityRights"))
				assert.Equal(t, json.Number("4"), getResult(result, "City", "count", "history"))
			})
		}
	})
}

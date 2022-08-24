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

// run by setup_test.go
func runningAggregateArrayClassSanityCheck(t *testing.T) {
	t.Run("running Aggregate against ArrayClass", func(t *testing.T) {
		query := `
			{
				Aggregate {
					ArrayClass
					%s
					{
						booleans{
							count
						}
						meta{
							count
						}
						datesAsStrings{
							count
						}
						dates{
							count
						}
						numbers{
							count
						}
						strings{
							count
						}
						texts{
							count
						}
						ints{
							count
						}
					}
				}
			}
		`
		getCount := func(result *graphqlhelper.GraphQLResult, className, property string) interface{} {
			meta := result.Get("Aggregate", className).AsSlice()[0].(map[string]interface{})[property]
			return meta.(map[string]interface{})["count"]
		}
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
				filters: `( nearObject:{id: "cfa3b21e-ca5f-4db7-a412-5fc6a23c534a" certainty: 0.9} )`,
			},
			{
				name: "with where and nearObject filter",
				filters: `(
					where:{operator: Like path:["id"] valueString:"*"}
					nearObject:{id: "cfa3b21e-ca5f-4db7-a412-5fc6a23c534a" certainty: 0.9}
				)`,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, fmt.Sprintf(query, tt.filters))
				assert.Equal(t, json.Number("3"), getCount(result, "ArrayClass", "meta"))
				assert.Equal(t, json.Number("6"), getCount(result, "ArrayClass", "booleans"))
				assert.Equal(t, json.Number("6"), getCount(result, "ArrayClass", "datesAsStrings"))
				assert.Equal(t, json.Number("6"), getCount(result, "ArrayClass", "dates"))
				assert.Equal(t, json.Number("6"), getCount(result, "ArrayClass", "numbers"))
				assert.Equal(t, json.Number("6"), getCount(result, "ArrayClass", "strings"))
				assert.Equal(t, json.Number("6"), getCount(result, "ArrayClass", "texts"))
				assert.Equal(t, json.Number("6"), getCount(result, "ArrayClass", "ints"))
			})
		}
	})
}

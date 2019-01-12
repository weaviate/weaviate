/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2018 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * AUTHOR: Bob van Luijt (bob@kub.design)
 * See www.creativesoftwarefdn.org for details
 * Contact: @CreativeSofwFdn / bob@kub.design
 */
package test

import (
	"testing"

	"github.com/creativesoftwarefdn/weaviate/test/acceptance/helper"
	"github.com/stretchr/testify/assert"
)

func TestLocalGetWithComplexFilter(t *testing.T) {
	t.Run("without filters <- this is the control", func(t *testing.T) {
		query := `
		{
			Local{
				Get {
					Things {
						Airport {
							code
						}
					}
				}
			}
		}
		`
		result := AssertGraphQL(t, helper.RootAuth, query)
		airports := result.Get("Local", "Get", "Things", "Airport").AsSlice()

		expected := []interface{}{
			map[string]interface{}{"code": "10000"},
			map[string]interface{}{"code": "20000"},
			map[string]interface{}{"code": "30000"},
			map[string]interface{}{"code": "40000"},
		}

		assert.ElementsMatch(t, expected, airports)
	})

	t.Run("with filters applied", func(t *testing.T) {
		query := `
		{
			Local{
				Get {
					Things {
						Airport(where:{
							operator:And
							operands: [
								{
									operator: GreaterThan,
									valueInt: 600000,
									path:["inCity", "City", "population"]
								}
								{
									operator: Equal,
									valueString:"Germany"
									path:["inCity", "City", "inCountry", "Country", "name"]
								}
							]
						}){
							code
						}
					}
				}
			}
		}
		`
		result := AssertGraphQL(t, helper.RootAuth, query)
		airports := result.Get("Local", "Get", "Things", "Airport").AsSlice()

		expected := []interface{}{
			map[string]interface{}{"code": "40000"},
		}

		assert.ElementsMatch(t, expected, airports)
	})
}

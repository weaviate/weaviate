//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2019 SeMI Holding B.V. (registered @ Dutch Chamber of Commerce no 75221632). All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

package test

import (
	"strings"
	"testing"

	"github.com/semi-technologies/weaviate/test/acceptance/helper"
	"github.com/stretchr/testify/assert"
)

func gettingObjectsWithGrouping(t *testing.T) {
	t.Run("without grouping <- this is the control", func(t *testing.T) {
		query := `
		{
				Get {
					Things {
						Company {
							name
						}
					}
				}
		}
		`
		result := AssertGraphQL(t, helper.RootAuth, query)
		companies := result.Get("Get", "Things", "Company").AsSlice()

		expected := []interface{}{
			map[string]interface{}{"name": "Microsoft Inc."},
			map[string]interface{}{"name": "Microsoft Incorporated"},
			map[string]interface{}{"name": "Microsoft"},
			map[string]interface{}{"name": "Apple Inc."},
			map[string]interface{}{"name": "Apple Incorporated"},
			map[string]interface{}{"name": "Apple"},
			map[string]interface{}{"name": "Google Inc."},
			map[string]interface{}{"name": "Google Incorporated"},
			map[string]interface{}{"name": "Google"},
		}

		assert.ElementsMatch(t, expected, companies)
	})

	t.Run("grouping mode set to closest", func(t *testing.T) {
		query := `
		{
				Get {
					Things {
						Company(group: {type: closest, force:0.07}) {
							name
						}
					}
				}
		}
		`
		result := AssertGraphQL(t, helper.RootAuth, query)
		companies := result.Get("Get", "Things", "Company").AsSlice()

		assert.Len(t, companies, 3)
		mustContain := []string{"Apple", "Microsoft", "Google"}
	outer:
		for _, toContain := range mustContain {
			for _, current := range companies {
				if strings.Contains(current.(map[string]interface{})["name"].(string), toContain) {
					continue outer
				}
			}

			t.Errorf("%s not contained in %v", toContain, companies)
		}
	})

	t.Run("grouping mode set to merge", func(t *testing.T) {
		query := `
		{
				Get {
					Things {
						Company(group: {type: merge, force:0.07}) {
							name
							InCity {
							  ... on City {
								  name
								}
							}
						}
					}
				}
		}
		`
		result := AssertGraphQL(t, helper.RootAuth, query)
		companies := result.Get("Get", "Things", "Company").AsSlice()

		assert.Len(t, companies, 3)
		mustContain := [][]string{
			[]string{"Apple", "Apple Inc.", "Apple Incorporated"},
			[]string{"Microsoft", "Microsoft Inc.", "Microsoft Incorporated"},
			[]string{"Google", "Google Inc.", "Google Incorporated"},
		}

		allContained := func(current map[string]interface{}, toContains []string) bool {
			for _, toContain := range toContains {
				if !strings.Contains(current["name"].(string), toContain) {
					return false
				}
			}
			return true
		}

	outer:
		for _, toContain := range mustContain {
			for _, current := range companies {
				if allContained(current.(map[string]interface{}), toContain) {
					continue outer
				}
			}

			t.Errorf("%s not contained in %v", toContain, companies)
		}
	})
}

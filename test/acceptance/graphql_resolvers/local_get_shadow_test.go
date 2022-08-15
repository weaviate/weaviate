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
	"testing"

	"github.com/semi-technologies/weaviate/test/helper"
	graphqlhelper "github.com/semi-technologies/weaviate/test/helper/graphql"
	"github.com/stretchr/testify/assert"
)

// run by setup_test.go
func runningNearObjectWithShadowedObjects(t *testing.T) {
	t.Run("running near object against shadow class", func(t *testing.T) {
		query := `
			{
				Get {
					NearObjectSearch (
						nearObject: {
							id : "aa44bbee-ca5f-4db7-a412-5fc6a2300001"
							certainty: 0.98
						}
					) {
						name
					}
				}
			}
		`

		for i := 0; i < 50; i++ {
			result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
			objs := result.Get("Get", "NearObjectSearch").AsSlice()

			expected := []interface{}{
				map[string]interface{}{"name": "Mount Everest"},
			}

			assert.Len(t, objs, 1)
			assert.ElementsMatch(t, expected, objs)
		}
	})
}

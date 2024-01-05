//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package test

import (
	"fmt"
	"testing"

	graphqlhelper "github.com/weaviate/weaviate/test/helper/graphql"

	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/test/helper"
)

func gettingObjectsWithCustomVectors(t *testing.T) {
	t.Run("through Get {}", func(t *testing.T) {
		query := `
		{
			Get {
				CustomVectorClass(nearVector:{vector:[1,1,1]}) {
					_additional {
						id
					}
				}
			}
		}
		`
		result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
		results := result.Get("Get", "CustomVectorClass").AsSlice()

		expected := []interface{}{
			map[string]interface{}{"_additional": map[string]interface{}{"id": string(cvc1)}},
			map[string]interface{}{"_additional": map[string]interface{}{"id": string(cvc2)}},
			map[string]interface{}{"_additional": map[string]interface{}{"id": string(cvc3)}},
		}

		assert.Equal(t, expected, results)
	})
}

func exploreObjectsWithCustomVectors(t *testing.T) {
	t.Run("through Explore {}", func(t *testing.T) {
		query := `
		{
			Explore(nearVector: {vector:[1,1,1]}) {
				beacon
			}
		}
		`
		result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
		results := result.Get("Explore").AsSlice()

		expected := []interface{}{
			map[string]interface{}{"beacon": fmt.Sprintf("weaviate://localhost/CustomVectorClass/%s", cvc1)},
			map[string]interface{}{"beacon": fmt.Sprintf("weaviate://localhost/CustomVectorClass/%s", cvc2)},
			map[string]interface{}{"beacon": fmt.Sprintf("weaviate://localhost/CustomVectorClass/%s", cvc3)},
		}

		assert.Equal(t, expected, results)
	})
}

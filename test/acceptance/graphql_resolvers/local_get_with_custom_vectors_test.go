//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package test

import (
	"testing"

	"github.com/semi-technologies/weaviate/test/acceptance/helper"
	"github.com/stretchr/testify/assert"
)

func gettingObjectsWithCustomVectors(t *testing.T) {
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
	result := AssertGraphQL(t, helper.RootAuth, query)
	airports := result.Get("Get", "CustomVectorClass").AsSlice()

	expected := []interface{}{
		map[string]interface{}{"_additional": map[string]interface{}{"id": string(cvc1)}},
		map[string]interface{}{"_additional": map[string]interface{}{"id": string(cvc2)}},
		map[string]interface{}{"_additional": map[string]interface{}{"id": string(cvc3)}},
	}

	assert.Equal(t, expected, airports)
}

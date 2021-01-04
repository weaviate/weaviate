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

package get

import (
	"testing"

	"github.com/semi-technologies/weaviate/entities/schema/kind"
	"github.com/semi-technologies/weaviate/usecases/traverser"
	"github.com/stretchr/testify/assert"
)

func TestGetNoNetworkRequestIsMadeWhenUserDoesntWantNetworkRef(t *testing.T) {
	t.Parallel()

	resolver := newMockResolver()

	expectedParams := traverser.GetParams{
		Kind:      kind.Object,
		ClassName: "SomeThing",
		Properties: []traverser.SelectProperty{
			{
				Name:        "uuid",
				IsPrimitive: true,
			},
		},
	}

	resolverResponse := []interface{}{
		map[string]interface{}{
			"uuid": "some-uuid-for-the-local-class",
		},
	}

	resolver.On("GetClass", expectedParams).
		Return(resolverResponse, nil).Once()

	query := "{ Get { SomeThing { uuid } } }"
	result := resolver.AssertResolve(t, query).Result

	expectedResult := map[string]interface{}{
		"Get": map[string]interface{}{
			"SomeThing": []interface{}{
				map[string]interface{}{
					"uuid": "some-uuid-for-the-local-class",
				},
			},
		},
	}

	assert.Equal(t, expectedResult, result, "should resolve the network cross-ref correctly")
}

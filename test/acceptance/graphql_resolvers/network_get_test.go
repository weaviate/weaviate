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

func TestNetworkGetSimple(t *testing.T) {
	result := AssertGraphQL(t, helper.RootAuth, "{ Network { Get { RemoteWeaviateForAcceptanceTest { Things { City { name } } } } } }")
	cities := result.Get("Network", "Get", "RemoteWeaviateForAcceptanceTest", "Things", "City").AsSlice()

	expected := []interface{}{
		map[string]interface{}{"name": "Hamburg"},
		map[string]interface{}{"name": "New York"},
		map[string]interface{}{"name": "Neustadt an der Weinstraße"},
		map[string]interface{}{"name": "Tokyo"},
	}

	assert.ElementsMatch(t, expected, cities)
}

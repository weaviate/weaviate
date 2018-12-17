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

// Note: Things.Instruments is not something that is present in our local schema
// This is on purpose to verify that we have support for a completely different
// schema on a remote instance.
func TestNetworkGetSimple(t *testing.T) {
	result := AssertGraphQL(t, helper.RootAuth, "{ Network { Get { RemoteWeaviateForAcceptanceTest { Things { Instruments { name } } } } } }")
	cities := result.Get("Network", "Get", "RemoteWeaviateForAcceptanceTest", "Things", "Instruments").AsSlice()

	expected := []interface{}{
		map[string]interface{}{"name": "Piano"},
		map[string]interface{}{"name": "Guitar"},
		map[string]interface{}{"name": "Bass Guitar"},
		map[string]interface{}{"name": "Talkbox"},
	}

	assert.ElementsMatch(t, expected, cities)
}

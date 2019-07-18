//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2019 Weaviate. All rights reserved.
//  LICENSE: https://github.com/semi-technologies/weaviate/blob/develop/LICENSE.md
//  DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

package test

import (
	"testing"

	"github.com/semi-technologies/weaviate/test/acceptance/helper"
	"github.com/stretchr/testify/assert"
)

// Note: Things.Instruments is not something that is present in our local schema
// This is on purpose to verify that we have support for a completely different
// schema on a remote instance.
func TestNetworkGetSimple(t *testing.T) {
	result := AssertGraphQL(t, helper.RootAuth, "{ Network { Get { RemoteWeaviateForAcceptanceTest { Things { Instruments { name } } } } } }")
	instruments := result.Get("Network", "Get", "RemoteWeaviateForAcceptanceTest", "Things", "Instruments").AsSlice()

	expected := []interface{}{
		map[string]interface{}{"name": "Piano"},
		map[string]interface{}{"name": "Guitar"},
		map[string]interface{}{"name": "Bass Guitar"},
		map[string]interface{}{"name": "Talkbox"},
	}

	assert.ElementsMatch(t, expected, instruments)
}

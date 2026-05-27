//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package inverted

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestResolveTokenization_NilResolverPassesThrough(t *testing.T) {
	got := ResolveTokenization(nil, "name", "word")
	assert.Equal(t, "word", got)
}

func TestResolveTokenization_NonNilResolverConsulted(t *testing.T) {
	// Resolver overrides everything it sees.
	r := func(propName, live string) string {
		if propName == "name" {
			return "field"
		}
		return live
	}
	assert.Equal(t, "field", ResolveTokenization(r, "name", "word"))
	// Unrelated prop falls back to live.
	assert.Equal(t, "word", ResolveTokenization(r, "other", "word"))
}

func TestResolveTokenization_PassThroughOnEmptyOverlay(t *testing.T) {
	// A resolver that returns live unchanged behaves identically to nil.
	r := func(propName, live string) string { return live }
	assert.Equal(t, "word", ResolveTokenization(r, "name", "word"))
}

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

package clients

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAnthropicMetaInfo(t *testing.T) {
	t.Run("when getting meta info for Anthropic", func(t *testing.T) {
		a := &anthropic{}
		meta, err := a.MetaInfo()

		assert.Nil(t, err)
		assert.NotNil(t, meta)

		name, ok := meta["name"]
		assert.True(t, ok)
		assert.Equal(t, "Generative Search - Anthropic", name)

		documentationHref, ok := meta["documentationHref"]
		assert.True(t, ok)
		assert.Equal(t, "https://docs.anthropic.com/en/api/getting-started", documentationHref)
	})
}

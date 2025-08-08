//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package modtransformers

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/entities/schema"
)

func TestConfigDefaults(t *testing.T) {
	t.Run("for properties", func(t *testing.T) {
		def := New().ClassConfigDefaults()

		assert.Equal(t, false, def["vectorizeClassName"])
	})

	t.Run("for the class", func(t *testing.T) {
		dt := schema.DataTypeText
		def := New().PropertyConfigDefaults(&dt)
		assert.Equal(t, false, def["vectorizePropertyName"])
		assert.Equal(t, false, def["skip"])
	})
}

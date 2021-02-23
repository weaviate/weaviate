package modtransformers

import (
	"testing"

	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/stretchr/testify/assert"
)

func TestConfigDefaults(t *testing.T) {
	t.Run("for properties", func(t *testing.T) {
		def := New().ClassConfigDefaults()

		assert.Equal(t, true, def["vectorizeClassName"])
		assert.Equal(t, "masked_mean", def["poolingStrategy"])
	})

	t.Run("for the class", func(t *testing.T) {
		dt := schema.DataTypeText
		def := New().PropertyConfigDefaults(&dt)
		assert.Equal(t, false, def["vectorizePropertyName"])
		assert.Equal(t, false, def["skip"])
	})
}

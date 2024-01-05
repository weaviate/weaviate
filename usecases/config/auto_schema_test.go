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

package config

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/entities/schema"
)

func TestConfig_AutoSchema(t *testing.T) {
	t.Run("invalid DefaultNumber", func(t *testing.T) {
		auth := AutoSchema{
			DefaultNumber: "float",
			DefaultString: schema.DataTypeText.String(),
			DefaultDate:   "date",
		}
		expected := fmt.Errorf("autoSchema.defaultNumber must be either 'int' or 'number")
		err := auth.Validate()
		assert.Equal(t, expected, err)
	})

	t.Run("invalid DefaultString", func(t *testing.T) {
		auth := AutoSchema{
			DefaultNumber: "int",
			DefaultString: "body",
			DefaultDate:   "date",
		}
		expected := fmt.Errorf("autoSchema.defaultString must be either 'string' or 'text")
		err := auth.Validate()
		assert.Equal(t, expected, err)
	})

	t.Run("invalid DefaultDate", func(t *testing.T) {
		auth := AutoSchema{
			DefaultNumber: "int",
			DefaultString: schema.DataTypeText.String(),
			DefaultDate:   "int",
		}
		expected := fmt.Errorf("autoSchema.defaultDate must be either 'date' or 'string' or 'text")
		err := auth.Validate()
		assert.Equal(t, expected, err)
	})

	t.Run("all valid AutoSchema configurations", func(t *testing.T) {
		auth := AutoSchema{
			DefaultNumber: "int",
			DefaultString: schema.DataTypeText.String(),
			DefaultDate:   "date",
		}
		err := auth.Validate()
		assert.Nil(t, err, "should not error")
	})
}

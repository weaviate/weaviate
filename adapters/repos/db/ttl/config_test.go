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

package ttl

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

func TestValidateObjectTTConfig(t *testing.T) {
	vFalse := false
	vTrue := true
	errType := func(e error) string {
		return fmt.Sprintf("%T", e)
	}

	createCollection := func() *models.Class {
		return &models.Class{
			ObjectTTLConfig: &models.ObjectTTLConfig{
				Enabled:    true,
				DeleteOn:   filters.InternalPropCreationTimeUnix,
				DefaultTTL: 3600,
			},
			InvertedIndexConfig: &models.InvertedIndexConfig{
				IndexTimestamps: true,
			},
			Properties: []*models.Property{
				{
					Name:     "CustomPropertyDate1",
					DataType: schema.DataTypeDate.PropString(),
				},
				{
					Name:            "CustomPropertyDate2",
					DataType:        schema.DataTypeDate.PropString(),
					IndexFilterable: &vTrue,
				},
				{
					Name:            "CustomPropertyDateNotFilterable",
					DataType:        schema.DataTypeDate.PropString(),
					IndexFilterable: &vFalse,
				},
				{
					Name:            "CustomPropertyDates",
					DataType:        schema.DataTypeDateArray.PropString(),
					IndexFilterable: &vTrue,
				},
				{
					Name:            "CustomPropertyInteger",
					DataType:        schema.DataTypeInt.PropString(),
					IndexFilterable: &vTrue,
				},
			},
		}
	}

	testCasesInvalid := []struct {
		name   string
		mutate func(c *models.Class)
		expErr error
	}{
		{
			name:   "empty deleteOn",
			mutate: func(c *models.Class) { c.ObjectTTLConfig.DeleteOn = "" },
			expErr: &errorEmptyDeleteOn{},
		},
		{
			name:   "invalid ttl (1)",
			mutate: func(c *models.Class) { c.ObjectTTLConfig.DefaultTTL = 12 },
			expErr: &errorInvalidDefaultTtl{},
		},
	}

	for _, tc := range testCasesInvalid {
		t.Run(tc.name, func(t *testing.T) {
			collection := createCollection()
			tc.mutate(collection)

			conf, err := ValidateObjectTTLConfig(collection)
			assert.Nil(t, conf)
			require.NotNil(t, err)
			assert.Equal(t, errType(tc.expErr), errType(err))
		})
	}

	// t.Run("empty deleteOn", func(t *testing.T) {
	// 	collection := createCollection()
	// 	collection.ObjectTTLConfig.DeleteOn = ""

	// 	conf, err := ValidateObjectTTLConfig(collection)
	// 	assert.Nil(t, conf)
	// 	e := &errorEmptyDeleteOn{}
	// 	assert.ErrorAs(t, err, &e)
	// })

	// t.Run("invalid ttl (1)", func(t *testing.T) {
	// 	collection := createCollection()
	// 	collection.ObjectTTLConfig.DefaultTTL = 12

	// 	conf, err := ValidateObjectTTLConfig(collection)
	// 	assert.Nil(t, conf)
	// 	e := &errorInvalidDefaultTtl{}
	// 	assert.ErrorAs(t, err, &e)
	// })

	// t.Run("invalid ttl (2)", func(t *testing.T) {
	// 	collection := createCollection()
	// 	collection.ObjectTTLConfig.DefaultTTL = 12

	// 	conf, err := ValidateObjectTTLConfig(collection)
	// 	assert.Nil(t, conf)
	// 	e := &errorInvalidDefaultTtl{}
	// 	assert.ErrorAs(t, err, &e)
	// })
}

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
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

func TestValidateObjectTTConfig(t *testing.T) {
	vFalse := false
	vTrue := true

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
					Name:            "customPropertyDate",
					DataType:        schema.DataTypeDate.PropString(),
					IndexFilterable: &vTrue,
				},
				{
					Name:     "customPropertyDateNotFilterableImplicit",
					DataType: schema.DataTypeDate.PropString(),
				},
				{
					Name:            "customPropertyDateNotFilterableExplicit",
					DataType:        schema.DataTypeDate.PropString(),
					IndexFilterable: &vFalse,
				},
				{
					Name:            "customPropertyDates",
					DataType:        schema.DataTypeDateArray.PropString(),
					IndexFilterable: &vTrue,
				},
				{
					Name:            "customPropertyInteger",
					DataType:        schema.DataTypeInt.PropString(),
					IndexFilterable: &vTrue,
				},
			},
		}
	}

	t.Run("invalid config", func(t *testing.T) {
		testCasesInvalid := []struct {
			name        string
			reconfigure func(c *models.Class)
			expErr      error
		}{
			{
				name:        "empty deleteOn",
				reconfigure: func(c *models.Class) { c.ObjectTTLConfig.DeleteOn = "" },
				expErr:      &errorEmptyDeleteOn{},
			},
			{
				name:        "no inverted config",
				reconfigure: func(c *models.Class) { c.InvertedIndexConfig = nil },
				expErr:      &errorTimestampsNotIndexed{},
			},
			{
				name:        "timestamps not indexed",
				reconfigure: func(c *models.Class) { c.InvertedIndexConfig.IndexTimestamps = false },
				expErr:      &errorTimestampsNotIndexed{},
			},
			{
				name:        "invalid ttl (too small)",
				reconfigure: func(c *models.Class) { c.ObjectTTLConfig.DefaultTTL = 12 },
				expErr:      &errorInvalidDefaultTtl{},
			},
			{
				name:        "invalid ttl (negative)",
				reconfigure: func(c *models.Class) { c.ObjectTTLConfig.DefaultTTL = -10 },
				expErr:      &errorInvalidDefaultTtl{},
			},
			{
				name:        "deleteOn custom property does not exist",
				reconfigure: func(c *models.Class) { c.ObjectTTLConfig.DeleteOn = "customPropertyNonExistent" },
				expErr:      &errorMissingDeleteOnProp{},
			},
			{
				name:        "deleteOn custom property invalid datatype (integer)",
				reconfigure: func(c *models.Class) { c.ObjectTTLConfig.DeleteOn = "customPropertyInteger" },
				expErr:      &errorInvalidDeleteOnPropDatatype{},
			},
			{
				name:        "deleteOn custom property invalid datatype (dates)",
				reconfigure: func(c *models.Class) { c.ObjectTTLConfig.DeleteOn = "customPropertyDates" },
				expErr:      &errorInvalidDeleteOnPropDatatype{},
			},
			{
				name:        "deleteOn custom property not indexed (explicit)",
				reconfigure: func(c *models.Class) { c.ObjectTTLConfig.DeleteOn = "customPropertyDateNotFilterableExplicit" },
				expErr:      &errorMissingDeleteOnPropIndex{},
			},
			{
				name:        "deleteOn custom property not indexed (implicit)",
				reconfigure: func(c *models.Class) { c.ObjectTTLConfig.DeleteOn = "customPropertyDateNotFilterableImplicit" },
				expErr:      &errorMissingDeleteOnPropIndex{},
			},
		}

		for _, tc := range testCasesInvalid {
			t.Run(tc.name, func(t *testing.T) {
				collection := createCollection()
				tc.reconfigure(collection)

				conf, err := ValidateObjectTTLConfig(collection)
				assert.Nil(t, conf)
				assert.ErrorAs(t, err, tc.expErr)
			})
		}
	})

	t.Run("valid config", func(t *testing.T) {
		testCasesValid := []struct {
			name        string
			reconfigure func(c *models.Class)
		}{
			{
				name:        "deleteOn creation time",
				reconfigure: func(c *models.Class) { /* c.ObjectTTLConfig.DeleteOn = filters.InternalPropCreationTimeUnix */ },
			},
			{
				name:        "deleteOn update time",
				reconfigure: func(c *models.Class) { c.ObjectTTLConfig.DeleteOn = filters.InternalPropLastUpdateTimeUnix },
			},
			{
				name:        "deleteOn custom property",
				reconfigure: func(c *models.Class) { c.ObjectTTLConfig.DeleteOn = "customPropertyDate" },
			},
			{
				name: "deleteOn custom property 0 default ttl",
				reconfigure: func(c *models.Class) {
					c.ObjectTTLConfig.DeleteOn = "customPropertyDate"
					c.ObjectTTLConfig.DefaultTTL = 0
				},
			},
			{
				name: "deleteOn custom property negative default ttl",
				reconfigure: func(c *models.Class) {
					c.ObjectTTLConfig.DeleteOn = "customPropertyDate"
					c.ObjectTTLConfig.DefaultTTL = -3600
				},
			},
			{
				name:        "no object ttl config",
				reconfigure: func(c *models.Class) { c.ObjectTTLConfig = nil },
			},
			{
				name:        "object ttl disabled",
				reconfigure: func(c *models.Class) { c.ObjectTTLConfig.Enabled = false },
			},
		}

		for _, tc := range testCasesValid {
			t.Run(tc.name, func(t *testing.T) {
				collection := createCollection()
				tc.reconfigure(collection)

				conf, err := ValidateObjectTTLConfig(collection)
				assert.Equal(t, conf, collection.ObjectTTLConfig)
				assert.NoError(t, err)
			})
		}
	})

	t.Run("deleteOn trimmed white spaces", func(t *testing.T) {
		testCasesTrimmed := []struct {
			name     string
			deleteOn string
		}{
			{
				name:     "creation time",
				deleteOn: filters.InternalPropCreationTimeUnix,
			},
			{
				name:     "update time",
				deleteOn: filters.InternalPropLastUpdateTimeUnix,
			},
			{
				name:     "custom property",
				deleteOn: "customPropertyDate",
			},
		}

		for _, tc := range testCasesTrimmed {
			t.Run(tc.name, func(t *testing.T) {
				collection := createCollection()
				collection.ObjectTTLConfig.DeleteOn = fmt.Sprintf("\t%s  ", tc.deleteOn)

				conf, err := ValidateObjectTTLConfig(collection)
				assert.Equal(t, conf.DeleteOn, tc.deleteOn)
				assert.NoError(t, err)
			})
		}
	})
}

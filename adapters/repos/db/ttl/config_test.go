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

package ttl

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/config/runtime"
)

func TestValidateObjectTTConfig(t *testing.T) {
	vFalse := false
	vTrue := true

	dbConfig := config.Config{
		ObjectsTTLDeleteSchedule: runtime.NewDynamicValue("@every 1h"),
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
					Name:              "customPropertyDate",
					DataType:          schema.DataTypeDate.PropString(),
					IndexFilterable:   &vTrue,
					IndexRangeFilters: &vTrue,
				},
				{
					Name:            "customPropertyDateFilterable",
					DataType:        schema.DataTypeDate.PropString(),
					IndexFilterable: &vTrue,
				},
				{
					Name:              "customPropertyDateRangeable",
					DataType:          schema.DataTypeDate.PropString(),
					IndexRangeFilters: &vTrue,
				},
				{
					Name:     "customPropertyDateNoIndexesImplicit",
					DataType: schema.DataTypeDate.PropString(),
				},
				{
					Name:              "customPropertyDateNoIndexesExplicit",
					DataType:          schema.DataTypeDate.PropString(),
					IndexFilterable:   &vFalse,
					IndexRangeFilters: &vFalse,
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

	t.Run("invalid config update", func(t *testing.T) {
		testCasesInvalid := []struct {
			name        string
			reconfigure func(c *models.Class)
			dbConfig    config.Config
			expErr      error
		}{
			{
				name:        "empty deleteOn",
				reconfigure: func(c *models.Class) { c.ObjectTTLConfig.DeleteOn = "" },
				dbConfig:    dbConfig,
				expErr:      &errorEmptyDeleteOn{},
			},
			{
				name:        "no inverted config",
				reconfigure: func(c *models.Class) { c.InvertedIndexConfig = nil },
				dbConfig:    dbConfig,
				expErr:      &errorTimestampsNotIndexed{},
			},
			{
				name:        "timestamps not indexed",
				reconfigure: func(c *models.Class) { c.InvertedIndexConfig.IndexTimestamps = false },
				dbConfig:    dbConfig,
				expErr:      &errorTimestampsNotIndexed{},
			},
			{
				name:        "invalid ttl (too small)",
				reconfigure: func(c *models.Class) { c.ObjectTTLConfig.DefaultTTL = 12 },
				dbConfig:    dbConfig,
				expErr:      &errorInvalidDefaultTtl{},
			},
			{
				name:        "invalid ttl (negative)",
				reconfigure: func(c *models.Class) { c.ObjectTTLConfig.DefaultTTL = -10 },
				dbConfig:    dbConfig,
				expErr:      &errorInvalidDefaultTtl{},
			},
			{
				name:        "deleteOn custom property does not exist",
				reconfigure: func(c *models.Class) { c.ObjectTTLConfig.DeleteOn = "customPropertyNonExistent" },
				dbConfig:    dbConfig,
				expErr:      &errorMissingDeleteOnProp{},
			},
			{
				name:        "deleteOn custom property invalid datatype (integer)",
				reconfigure: func(c *models.Class) { c.ObjectTTLConfig.DeleteOn = "customPropertyInteger" },
				dbConfig:    dbConfig,
				expErr:      &errorInvalidDeleteOnPropDatatype{},
			},
			{
				name:        "deleteOn custom property invalid datatype (dates)",
				reconfigure: func(c *models.Class) { c.ObjectTTLConfig.DeleteOn = "customPropertyDates" },
				dbConfig:    dbConfig,
				expErr:      &errorInvalidDeleteOnPropDatatype{},
			},
			{
				name:        "deleteOn custom property not indexed (explicit)",
				reconfigure: func(c *models.Class) { c.ObjectTTLConfig.DeleteOn = "customPropertyDateNoIndexesExplicit" },
				dbConfig:    dbConfig,
				expErr:      &errorMissingDeleteOnPropIndex{},
			},
			{
				name:        "deleteOn custom property not indexed (implicit)",
				reconfigure: func(c *models.Class) { c.ObjectTTLConfig.DeleteOn = "customPropertyDateNoIndexesImplicit" },
				dbConfig:    dbConfig,
				expErr:      &errorMissingDeleteOnPropIndex{},
			},
			{
				name:        "deleteOn custom property",
				reconfigure: func(c *models.Class) { c.ObjectTTLConfig.DeleteOn = "customPropertyDate" },
				dbConfig:    config.Config{ObjectsTTLDeleteSchedule: runtime.NewDynamicValue("")},
				expErr:      &errorScheduleNotSet{},
			},
		}

		for _, tc := range testCasesInvalid {
			t.Run(tc.name, func(t *testing.T) {
				collection := createCollection()
				tc.reconfigure(collection)

				conf, _, err := ValidateObjectTTLConfig(collection, true, tc.dbConfig)
				assert.Nil(t, conf)
				assert.ErrorAs(t, err, tc.expErr)
			})
		}
	})

	t.Run("invalid config create", func(t *testing.T) {
		testCasesInvalid := []struct {
			name        string
			reconfigure func(c *models.Class)
			dbConfig    config.Config
			expErr      error
		}{
			{
				name:        "empty deleteOn",
				reconfigure: func(c *models.Class) { c.ObjectTTLConfig.DeleteOn = "" },
				dbConfig:    dbConfig,
				expErr:      &errorEmptyDeleteOn{},
			},
			{
				name:        "invalid ttl (too small)",
				reconfigure: func(c *models.Class) { c.ObjectTTLConfig.DefaultTTL = 12 },
				dbConfig:    dbConfig,
				expErr:      &errorInvalidDefaultTtl{},
			},
			{
				name:        "invalid ttl (negative)",
				reconfigure: func(c *models.Class) { c.ObjectTTLConfig.DefaultTTL = -10 },
				dbConfig:    dbConfig,
				expErr:      &errorInvalidDefaultTtl{},
			},
			{
				name:        "deleteOn custom property does not exist",
				reconfigure: func(c *models.Class) { c.ObjectTTLConfig.DeleteOn = "customPropertyNonExistent" },
				dbConfig:    dbConfig,
				expErr:      &errorMissingDeleteOnProp{},
			},
			{
				name:        "deleteOn custom property invalid datatype (integer)",
				reconfigure: func(c *models.Class) { c.ObjectTTLConfig.DeleteOn = "customPropertyInteger" },
				dbConfig:    dbConfig,
				expErr:      &errorInvalidDeleteOnPropDatatype{},
			},
			{
				name:        "deleteOn custom property invalid datatype (dates)",
				reconfigure: func(c *models.Class) { c.ObjectTTLConfig.DeleteOn = "customPropertyDates" },
				dbConfig:    dbConfig,
				expErr:      &errorInvalidDeleteOnPropDatatype{},
			},
			{
				name:        "deleteOn custom property not indexed (explicit)",
				reconfigure: func(c *models.Class) { c.ObjectTTLConfig.DeleteOn = "customPropertyDateNoIndexesExplicit" },
				dbConfig:    dbConfig,
				expErr:      &errorMissingDeleteOnPropIndex{},
			},
			{
				name:        "deleteOn custom property not indexed (implicit)",
				reconfigure: func(c *models.Class) { c.ObjectTTLConfig.DeleteOn = "customPropertyDateNoIndexesImplicit" },
				dbConfig:    dbConfig,
				expErr:      &errorMissingDeleteOnPropIndex{},
			},
			{
				name:        "deleteOn custom property",
				reconfigure: func(c *models.Class) { c.ObjectTTLConfig.DeleteOn = "customPropertyDate" },
				dbConfig:    config.Config{ObjectsTTLDeleteSchedule: runtime.NewDynamicValue("")},
				expErr:      &errorScheduleNotSet{},
			},
		}

		for _, tc := range testCasesInvalid {
			t.Run(tc.name, func(t *testing.T) {
				collection := createCollection()
				tc.reconfigure(collection)

				conf, _, err := ValidateObjectTTLConfig(collection, false, tc.dbConfig)
				assert.Nil(t, conf)
				assert.ErrorAs(t, err, tc.expErr)
			})
		}
	})

	t.Run("valid config create", func(t *testing.T) {
		testCasesValid := []struct {
			name        string
			reconfigure func(c *models.Class)
		}{
			{
				name:        "deleteOn creation time",
				reconfigure: func(c *models.Class) { c.ObjectTTLConfig.DeleteOn = filters.InternalPropCreationTimeUnix },
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
				name:        "deleteOn custom property filterable",
				reconfigure: func(c *models.Class) { c.ObjectTTLConfig.DeleteOn = "customPropertyDateFilterable" },
			},
			{
				name:        "deleteOn custom property rangeable",
				reconfigure: func(c *models.Class) { c.ObjectTTLConfig.DeleteOn = "customPropertyDateRangeable" },
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

				conf, needsInvertedIndex, err := ValidateObjectTTLConfig(collection, false, dbConfig)
				assert.Equal(t, conf, collection.ObjectTTLConfig)
				assert.NoError(t, err)
				assert.False(t, needsInvertedIndex)
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

				conf, _, err := ValidateObjectTTLConfig(collection, false, dbConfig)
				assert.Equal(t, conf.DeleteOn, tc.deleteOn)
				assert.NoError(t, err)
			})
		}
	})
}

func TestIsTtlConfigChanged(t *testing.T) {
	base := &models.ObjectTTLConfig{
		Enabled:              true,
		DeleteOn:             filters.InternalPropCreationTimeUnix,
		DefaultTTL:           3600,
		FilterExpiredObjects: false,
	}

	copyConfig := func() *models.ObjectTTLConfig {
		c := *base
		return &c
	}

	t.Run("no change", func(t *testing.T) {
		assert.False(t, IsTtlConfigChanged(base, copyConfig()))
	})

	t.Run("both nil", func(t *testing.T) {
		assert.False(t, IsTtlConfigChanged(nil, nil))
	})

	t.Run("initial nil updated non-nil", func(t *testing.T) {
		assert.True(t, IsTtlConfigChanged(nil, base))
	})

	t.Run("initial non-nil updated nil", func(t *testing.T) {
		assert.True(t, IsTtlConfigChanged(base, nil))
	})

	t.Run("enabled changed", func(t *testing.T) {
		updated := copyConfig()
		updated.Enabled = false
		assert.True(t, IsTtlConfigChanged(base, updated))
	})

	t.Run("deleteOn changed", func(t *testing.T) {
		updated := copyConfig()
		updated.DeleteOn = filters.InternalPropLastUpdateTimeUnix
		assert.True(t, IsTtlConfigChanged(base, updated))
	})

	t.Run("defaultTTL changed", func(t *testing.T) {
		updated := copyConfig()
		updated.DefaultTTL = 7200
		assert.True(t, IsTtlConfigChanged(base, updated))
	})

	t.Run("filterExpiredObjects changed", func(t *testing.T) {
		updated := copyConfig()
		updated.FilterExpiredObjects = true
		assert.True(t, IsTtlConfigChanged(base, updated))
	})
}

func TestValidateObjectTTConfigNoInvertedTimestamp(t *testing.T) {
	dbConfig := config.Config{
		ObjectsTTLDeleteSchedule: runtime.NewDynamicValue("@every 1h"),
	}

	collection := &models.Class{
		ObjectTTLConfig: &models.ObjectTTLConfig{
			Enabled:    true,
			DeleteOn:   filters.InternalPropCreationTimeUnix,
			DefaultTTL: 3600,
		},
	}

	_, needsInvertedIndex, err := ValidateObjectTTLConfig(collection, false, dbConfig)
	assert.NoError(t, err)
	assert.True(t, needsInvertedIndex)

	_, _, err = ValidateObjectTTLConfig(collection, true, dbConfig)
	assert.Error(t, err)
}

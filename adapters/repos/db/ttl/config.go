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
	"strings"
	"time"

	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

const minDefaultTtl = time.Minute

func ValidateObjectTTLConfig(collection *models.Class) (*models.ObjectTTLConfig, error) {
	ttlConfig := collection.ObjectTTLConfig

	if !IsTtlEnabled(ttlConfig) {
		return ttlConfig, nil
	}

	deleteOn := strings.TrimSpace(ttlConfig.DeleteOn)
	switch deleteOn {
	case "":
		return nil, newErrorEmptyDeleteOn()

	case filters.InternalPropCreationTimeUnix, filters.InternalPropLastUpdateTimeUnix:
		if collection.InvertedIndexConfig == nil || !collection.InvertedIndexConfig.IndexTimestamps {
			return nil, newErrorTimestampsNotIndexed(deleteOn)
		}
		if defaultTtl := time.Duration(ttlConfig.DefaultTTL) * time.Second; defaultTtl < minDefaultTtl {
			return nil, newErrorInvalidDefaultTtl(deleteOn, ttlConfig.DefaultTTL)
		}

	default:
		var deleteOnProp *models.Property
		for _, prop := range collection.Properties {
			if deleteOn == prop.Name {
				deleteOnProp = prop
				break
			}
		}
		if deleteOnProp == nil {
			return nil, newErrorMissingDeleteOnProp(deleteOn)
		}
		if dt, _ := schema.AsPrimitive(deleteOnProp.DataType); dt != schema.DataTypeDate {
			return nil, newErrorInvalidDeleteOnPropDatatype(deleteOn, dt)
		}
		// TODO aliszka:ttl allow rangeable?
		if deleteOnProp.IndexFilterable == nil || !*deleteOnProp.IndexFilterable {
			return nil, newErrorMissingDeleteOnPropIndex(deleteOn)
		}
	}
	ttlConfig.DeleteOn = deleteOn
	return ttlConfig, nil
}

func IsTtlEnabled(config *models.ObjectTTLConfig) bool {
	return config != nil && config.Enabled
}

type errorTtl struct{ error }
type errorEmptyDeleteOn struct{ errorTtl }
type errorTimestampsNotIndexed struct{ errorTtl }
type errorInvalidDefaultTtl struct{ errorTtl }
type errorMissingDeleteOnProp struct{ errorTtl }
type errorInvalidDeleteOnPropDatatype struct{ errorTtl }
type errorMissingDeleteOnPropIndex struct{ errorTtl }

func (e errorTtl) Error() string {
	return e.error.Error()
}

func newErrorEmptyDeleteOn() errorEmptyDeleteOn {
	return errorEmptyDeleteOn{errorTtl{fmt.Errorf("missing value for \"deleteOn\". Set %q, %q or custom property of \"date\" type",
		filters.InternalPropCreationTimeUnix, filters.InternalPropLastUpdateTimeUnix)}}
}
func (e errorEmptyDeleteOn) Unwrap() error {
	return e.errorTtl
}

func newErrorTimestampsNotIndexed(deleteOn string) errorTimestampsNotIndexed {
	return errorTimestampsNotIndexed{errorTtl{fmt.Errorf("\"deleteOn\"=%q requires indexed timestamps. Enable \"invertedIndexConfig.indexTimestamps\"",
		deleteOn)}}
}
func (e errorTimestampsNotIndexed) Unwrap() error {
	return e.errorTtl
}

func newErrorInvalidDefaultTtl(deleteOn string, defaultTtl int64) errorInvalidDefaultTtl {
	return errorInvalidDefaultTtl{errorTtl{fmt.Errorf("defaultTtl value too small for \"deleteOn\"=%q. Required minimum \"%d\" seconds, given \"%d\" seconds",
		deleteOn, int64(minDefaultTtl/time.Second), defaultTtl)}}
}
func (e errorInvalidDefaultTtl) Unwrap() error {
	return e.errorTtl
}

func newErrorMissingDeleteOnProp(deleteOn string) errorMissingDeleteOnProp {
	return errorMissingDeleteOnProp{errorTtl{fmt.Errorf("property %q set as \"deleteOn\" not found among collection properties",
		deleteOn)}}
}
func (e errorMissingDeleteOnProp) Unwrap() error {
	return e.errorTtl
}

func newErrorInvalidDeleteOnPropDatatype(deleteOn string, dt schema.DataType) errorInvalidDeleteOnPropDatatype {
	return errorInvalidDeleteOnPropDatatype{errorTtl{fmt.Errorf("property %q set as \"deleteOn\" should have %q data type, %q given",
		deleteOn, schema.DataTypeDate, dt)}}
}
func (e errorInvalidDeleteOnPropDatatype) Unwrap() error {
	return e.errorTtl
}

func newErrorMissingDeleteOnPropIndex(deleteOn string) errorMissingDeleteOnPropIndex {
	return errorMissingDeleteOnPropIndex{errorTtl{fmt.Errorf("property %q set as \"deleteOn\" should have filterable index enabled",
		deleteOn)}}
}
func (e errorMissingDeleteOnPropIndex) Unwrap() error {
	return e.errorTtl
}

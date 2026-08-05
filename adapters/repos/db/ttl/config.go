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
	"os"
	"strings"
	"time"

	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/config"
)

const minDefaultTtl = time.Minute

func ValidateObjectTTLConfig(collection *models.Class, isUpdate bool, dbConfig config.Config) (*models.ObjectTTLConfig, bool, error) {
	ttlConfig := collection.ObjectTTLConfig

	if !IsTtlEnabled(ttlConfig) {
		return ttlConfig, false, nil
	}

	if dbConfig.ObjectsTTLDeleteSchedule.Get() == "" {
		return nil, false, newErrorScheduleNotSet()
	}

	minimumTTL := minDefaultTtl
	if envMinTtl := os.Getenv("OBJECTS_TTL_MINIMUM_DEFAULT_TTL"); envMinTtl != "" {
		if parsedMinTtl, err := time.ParseDuration(envMinTtl); err == nil {
			minimumTTL = parsedMinTtl
		}
	}

	needsInvertedIndexTimeStamp := false
	deleteOn := strings.TrimSpace(ttlConfig.DeleteOn)
	switch deleteOn {
	case "":
		return nil, false, newErrorEmptyDeleteOn()
	case filters.InternalPropCreationTimeUnix, filters.InternalPropLastUpdateTimeUnix:
		if collection.InvertedIndexConfig == nil || !collection.InvertedIndexConfig.IndexTimestamps {
			if isUpdate {
				return nil, false, newErrorTimestampsNotIndexed(deleteOn)
			} else {
				needsInvertedIndexTimeStamp = true
			}
		}
		if defaultTtl := time.Duration(ttlConfig.DefaultTTL) * time.Second; defaultTtl < minimumTTL {
			return nil, false, newErrorInvalidDefaultTtl(deleteOn, ttlConfig.DefaultTTL, minimumTTL)
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
			return nil, false, newErrorMissingDeleteOnProp(deleteOn)
		}
		if dt, _ := schema.AsPrimitive(deleteOnProp.DataType); dt != schema.DataTypeDate {
			return nil, false, newErrorInvalidDeleteOnPropDatatype(deleteOn, dt)
		}
		hasFilterable := deleteOnProp.IndexFilterable != nil && *deleteOnProp.IndexFilterable
		hasRangeable := deleteOnProp.IndexRangeFilters != nil && *deleteOnProp.IndexRangeFilters
		if !hasFilterable && !hasRangeable {
			return nil, false, newErrorMissingDeleteOnPropIndex(deleteOn)
		}
	}
	ttlConfig.DeleteOn = deleteOn
	return ttlConfig, needsInvertedIndexTimeStamp, nil
}

func IsTtlEnabled(config *models.ObjectTTLConfig) bool {
	return config != nil && config.Enabled
}

// IsTtlConfigChanged reports whether the TTL configuration differs between
// the two classes. Any change to the TTL settings (enabled, deleteOn,
// defaultTtl, filterExpiredObjects) is considered a change.
func IsTtlConfigChanged(initial, updated *models.ObjectTTLConfig) bool {
	if initial == nil && updated == nil {
		return false
	}
	if initial == nil || updated == nil {
		return true
	}
	return initial.Enabled != updated.Enabled ||
		initial.DeleteOn != updated.DeleteOn ||
		initial.DefaultTTL != updated.DefaultTTL ||
		initial.FilterExpiredObjects != updated.FilterExpiredObjects
}

type (
	errorTtl                         struct{ error }
	errorEmptyDeleteOn               struct{ errorTtl }
	errorTimestampsNotIndexed        struct{ errorTtl }
	errorInvalidDefaultTtl           struct{ errorTtl }
	errorMissingDeleteOnProp         struct{ errorTtl }
	errorInvalidDeleteOnPropDatatype struct{ errorTtl }
	errorMissingDeleteOnPropIndex    struct{ errorTtl }
	errorScheduleNotSet              struct{ errorTtl }
)

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

func newErrorInvalidDefaultTtl(deleteOn string, defaultTtl int64, minimumTTL time.Duration) errorInvalidDefaultTtl {
	return errorInvalidDefaultTtl{errorTtl{fmt.Errorf("defaultTtl value too small for \"deleteOn\"=%q. Required minimum \"%d\" seconds, given \"%d\" seconds",
		deleteOn, int64(minimumTTL/time.Second), defaultTtl)}}
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
	return errorMissingDeleteOnPropIndex{errorTtl{fmt.Errorf("property %q set as \"deleteOn\" should have filterable or rangeable index enabled",
		deleteOn)}}
}

func (e errorMissingDeleteOnPropIndex) Unwrap() error {
	return e.errorTtl
}

func newErrorScheduleNotSet() errorScheduleNotSet {
	return errorScheduleNotSet{errorTtl{fmt.Errorf("enabling objectTTL requires a running background scheduler. Set OBJECTS_TTL_DELETE_SCHEDULE to activate it")}}
}

func (e errorScheduleNotSet) Unwrap() error {
	return e.errorTtl
}

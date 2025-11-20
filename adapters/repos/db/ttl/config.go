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
		return &models.ObjectTTLConfig{Enabled: false}, nil
	}

	deleteOn := strings.TrimSpace(ttlConfig.DeleteOn)
	switch deleteOn {
	case "":
		return nil, newErrorEmptyDeleteOn("missing value for \"deleteOn\". Set %q, %q or custom property of \"date\" type",
			filters.InternalPropCreationTimeUnix, filters.InternalPropLastUpdateTimeUnix)

	case filters.InternalPropCreationTimeUnix, filters.InternalPropLastUpdateTimeUnix:
		if collection.InvertedIndexConfig == nil || !collection.InvertedIndexConfig.IndexTimestamps {
			return nil, newErrorTimestampsNotIndexed("\"deleteOn\"=%q requires indexed timestamps. Enable \"invertedIndexConfig.indexTimestamps\"",
				deleteOn)
		}
		if defaultTtl := time.Duration(ttlConfig.DefaultTTL) * time.Second; defaultTtl < minDefaultTtl {
			return nil, newErrorInvalidDefaultTtl("defaultTtl value too small for \"deleteOn\"=%q. Required minimum \"%d\" seconds, given \"%d\" seconds",
				deleteOn, int64(minDefaultTtl/time.Second), ttlConfig.DefaultTTL)
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
			return nil, newErrorMissingDeleteOnProp("property %q set as \"deleteOn\" not found among collection properties",
				deleteOn)
		}
		if dt, _ := schema.AsPrimitive(deleteOnProp.DataType); dt != schema.DataTypeDate {
			return nil, newErrorInvalidDeleteOnPropDatatype("property %q set as \"deleteOn\" should have %q data type, %q given",
				deleteOn, schema.DataTypeDate, dt)
		}
		if deleteOnProp.IndexFilterable == nil || !*deleteOnProp.IndexFilterable {
			return nil, newErrorMissingDeleteOnPropIndex("property %q set as \"deleteOn\" should have filterable index enabled",
				deleteOn)
		}
	}

	ttlConfig.DeleteOn = deleteOn
	return ttlConfig, nil
}

func IsTtlEnabled(config *models.ObjectTTLConfig) bool {
	return config != nil && config.Enabled
}

type errorEmptyDeleteOn struct{ err error }
type errorTimestampsNotIndexed struct{ err error }
type errorInvalidDefaultTtl struct{ err error }
type errorMissingDeleteOnProp struct{ err error }
type errorInvalidDeleteOnPropDatatype struct{ err error }
type errorMissingDeleteOnPropIndex struct{ err error }

func newErrorEmptyDeleteOn(format string, args ...any) *errorEmptyDeleteOn {
	return &errorEmptyDeleteOn{fmt.Errorf(format, args...)}
}

func (e *errorEmptyDeleteOn) Error() string {
	return e.err.Error()
}

func newErrorTimestampsNotIndexed(format string, args ...any) *errorTimestampsNotIndexed {
	return &errorTimestampsNotIndexed{fmt.Errorf(format, args...)}
}

func (e *errorTimestampsNotIndexed) Error() string {
	return e.err.Error()
}

func newErrorInvalidDefaultTtl(format string, args ...any) *errorInvalidDefaultTtl {
	return &errorInvalidDefaultTtl{fmt.Errorf(format, args...)}
}

func (e *errorInvalidDefaultTtl) Error() string {
	return e.err.Error()
}

func newErrorMissingDeleteOnProp(format string, args ...any) *errorMissingDeleteOnProp {
	return &errorMissingDeleteOnProp{fmt.Errorf(format, args...)}
}

func (e *errorMissingDeleteOnProp) Error() string {
	return e.err.Error()
}

func newErrorInvalidDeleteOnPropDatatype(format string, args ...any) *errorInvalidDeleteOnPropDatatype {
	return &errorInvalidDeleteOnPropDatatype{fmt.Errorf(format, args...)}
}

func (e *errorInvalidDeleteOnPropDatatype) Error() string {
	return e.err.Error()
}

func newErrorMissingDeleteOnPropIndex(format string, args ...any) *errorMissingDeleteOnPropIndex {
	return &errorMissingDeleteOnPropIndex{fmt.Errorf(format, args...)}
}

func (e *errorMissingDeleteOnPropIndex) Error() string {
	return e.err.Error()
}

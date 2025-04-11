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

package settings

import (
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/weaviate/weaviate/entities/moduletools"
)

type PropertyValuesHelper interface {
	GetPropertyAsInt(cfg moduletools.ClassConfig, name string, defaultValue *int) *int
	GetPropertyAsIntWithNotExists(cfg moduletools.ClassConfig, name string, defaultValue, notExistsValue *int) *int
	GetPropertyAsInt64(cfg moduletools.ClassConfig, name string, defaultValue *int64) *int64
	GetPropertyAsInt64WithNotExists(cfg moduletools.ClassConfig, name string, defaultValue, notExistsValue *int64) *int64
	GetPropertyAsFloat64(cfg moduletools.ClassConfig, name string, defaultValue *float64) *float64
	GetPropertyAsFloat64WithNotExists(cfg moduletools.ClassConfig, name string, defaultValue, notExistsValue *float64) *float64
	GetPropertyAsString(cfg moduletools.ClassConfig, name, defaultValue string) string
	GetPropertyAsStringWithNotExists(cfg moduletools.ClassConfig, name, defaultValue, notExistsValue string) string
	GetPropertyAsBool(cfg moduletools.ClassConfig, name string, defaultValue bool) bool
	GetPropertyAsBoolWithNotExists(cfg moduletools.ClassConfig, name string, defaultValue, notExistsValue bool) bool
	GetNumber(in interface{}) (float32, error)
}

type classPropertyValuesHelper struct {
	moduleName string
	altNames   []string
}

func NewPropertyValuesHelper(moduleName string) PropertyValuesHelper {
	return &classPropertyValuesHelper{moduleName: moduleName}
}

func NewPropertyValuesHelperWithAltNames(moduleName string, altNames []string) PropertyValuesHelper {
	return &classPropertyValuesHelper{moduleName, altNames}
}

func (h *classPropertyValuesHelper) GetPropertyAsInt(cfg moduletools.ClassConfig,
	name string, defaultValue *int,
) *int {
	return h.GetPropertyAsIntWithNotExists(cfg, name, defaultValue, defaultValue)
}

func (h *classPropertyValuesHelper) GetPropertyAsIntWithNotExists(cfg moduletools.ClassConfig,
	name string, defaultValue, notExistsValue *int,
) *int {
	if cfg == nil {
		// we would receive a nil-config on cross-class requests, such as Explore{}
		return notExistsValue
	}
	return getNumberValue(h.GetSettings(cfg), name, defaultValue, notExistsValue)
}

func (h *classPropertyValuesHelper) GetPropertyAsInt64(cfg moduletools.ClassConfig,
	name string, defaultValue *int64,
) *int64 {
	return h.GetPropertyAsInt64WithNotExists(cfg, name, defaultValue, defaultValue)
}

func (h *classPropertyValuesHelper) GetPropertyAsInt64WithNotExists(cfg moduletools.ClassConfig,
	name string, defaultValue, notExistsValue *int64,
) *int64 {
	if cfg == nil {
		// we would receive a nil-config on cross-class requests, such as Explore{}
		return notExistsValue
	}
	return getNumberValue(h.GetSettings(cfg), name, defaultValue, notExistsValue)
}

func (h *classPropertyValuesHelper) GetPropertyAsFloat64(cfg moduletools.ClassConfig,
	name string, defaultValue *float64,
) *float64 {
	return h.GetPropertyAsFloat64WithNotExists(cfg, name, defaultValue, defaultValue)
}

func (h *classPropertyValuesHelper) GetPropertyAsFloat64WithNotExists(cfg moduletools.ClassConfig,
	name string, defaultValue, notExistsValue *float64,
) *float64 {
	if cfg == nil {
		// we would receive a nil-config on cross-class requests, such as Explore{}
		return notExistsValue
	}
	return getNumberValue(h.GetSettings(cfg), name, defaultValue, notExistsValue)
}

func (h *classPropertyValuesHelper) GetPropertyAsString(cfg moduletools.ClassConfig,
	name, defaultValue string,
) string {
	return h.GetPropertyAsStringWithNotExists(cfg, name, defaultValue, defaultValue)
}

func (h *classPropertyValuesHelper) GetPropertyAsStringWithNotExists(cfg moduletools.ClassConfig,
	name, defaultValue, notExistsValue string,
) string {
	if cfg == nil {
		// we would receive a nil-config on cross-class requests, such as Explore{}
		return notExistsValue
	}

	value := h.GetSettings(cfg)[name]
	switch v := value.(type) {
	case nil:
		return notExistsValue
	case string:
		return v
	default:
		return defaultValue
	}
}

func (h *classPropertyValuesHelper) GetPropertyAsBool(cfg moduletools.ClassConfig,
	name string, defaultValue bool,
) bool {
	return h.GetPropertyAsBoolWithNotExists(cfg, name, defaultValue, defaultValue)
}

func (h *classPropertyValuesHelper) GetPropertyAsBoolWithNotExists(cfg moduletools.ClassConfig,
	name string, defaultValue, notExistsValue bool,
) bool {
	if cfg == nil {
		// we would receive a nil-config on cross-class requests, such as Explore{}
		return notExistsValue
	}

	value := h.GetSettings(cfg)[name]
	switch v := value.(type) {
	case nil:
		return notExistsValue
	case bool:
		return v
	case string:
		asBool, err := strconv.ParseBool(v)
		if err == nil {
			return asBool
		}
		return defaultValue
	default:
		return defaultValue
	}
}

func (h *classPropertyValuesHelper) GetNumber(in interface{}) (float32, error) {
	switch i := in.(type) {
	case float64:
		return float32(i), nil
	case float32:
		return i, nil
	case int:
		return float32(i), nil
	case string:
		num, err := strconv.ParseFloat(i, 64)
		if err != nil {
			return 0, err
		}
		return float32(num), err
	case json.Number:
		num, err := i.Float64()
		if err != nil {
			return 0, err
		}
		return float32(num), err
	default:
		return 0.0, fmt.Errorf("unrecognized type: %T", in)
	}
}

func (h *classPropertyValuesHelper) GetSettings(cfg moduletools.ClassConfig) map[string]interface{} {
	if h.moduleName != "" {
		if settings := cfg.ClassByModuleName(h.moduleName); len(settings) > 0 {
			return settings
		}
		for _, altName := range h.altNames {
			if settings := cfg.ClassByModuleName(altName); len(settings) > 0 {
				return settings
			}
		}
	}
	return cfg.Class()
}

func getNumberValue[T int | int64 | float64](settings map[string]interface{},
	name string, defaultValue, notExistsValue *T,
) *T {
	value := settings[name]
	switch v := value.(type) {
	case nil:
		return notExistsValue
	case json.Number:
		if asInt64V, err := v.Int64(); err == nil {
			return asNumber[int64, T](asInt64V)
		}
		return defaultValue
	case float32:
		return asNumber[float32, T](v)
	case float64:
		return asNumber[float64, T](v)
	case int:
		return asNumber[int, T](v)
	case int16:
		return asNumber[int16, T](v)
	case int32:
		return asNumber[int32, T](v)
	case int64:
		return asNumber[int64, T](v)
	case string:
		if asInt, err := strconv.Atoi(v); err == nil {
			return asNumber[int, T](asInt)
		}
		return defaultValue
	default:
		return defaultValue
	}
}

func asNumber[T int | int16 | int32 | int64 | float32 | float64, R int | int64 | float64](v T) *R {
	number := R(v)
	return &number
}

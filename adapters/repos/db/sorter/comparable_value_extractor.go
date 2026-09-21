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

package sorter

import (
	"encoding/json"
	"strconv"
	"time"

	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storobj"
)

type comparableValueExtractor struct {
	dataTypesHelper *dataTypesHelper
}

func newComparableValueExtractor(dataTypesHelper *dataTypesHelper) *comparableValueExtractor {
	return &comparableValueExtractor{dataTypesHelper}
}

func (e *comparableValueExtractor) extractFromBytes(objData []byte, propName string) interface{} {
	value, success, _ := storobj.ParseAndExtractProperty(objData, propName)
	// in case the property does not exist for the object return nil
	if len(value) == 0 {
		return nil
	}
	if success {
		switch e.dataTypesHelper.getType(propName) {
		case schema.DataTypeBlob, schema.DataTypeBlobHash:
			return &value[0]
		case schema.DataTypeText:
			return &value[0]
		case schema.DataTypeTextArray:
			return &value
		case schema.DataTypeDate:
			d := e.mustExtractDates(value[:1])[0]
			return &d
		case schema.DataTypeDateArray:
			da := e.mustExtractDates(value)
			return &da
		case schema.DataTypeNumber, schema.DataTypeInt:
			n := e.mustExtractNumbers(value[:1])[0]
			return &n
		case schema.DataTypeNumberArray, schema.DataTypeIntArray:
			na := e.mustExtractNumbers(value)
			return &na
		case schema.DataTypeBoolean:
			b := e.mustExtractBools(value[:1])[0]
			return &b
		case schema.DataTypeBooleanArray:
			ba := e.mustExtractBools(value)
			return &ba
		case schema.DataTypePhoneNumber:
			fa := e.toFloatArrayFromPhoneNumber(e.mustExtractPhoneNumber(value))
			return &fa
		case schema.DataTypeGeoCoordinates:
			fa := e.toFloatArrayFromGeoCoordinates(e.mustExtractGeoCoordinates(value))
			return &fa
		default:
			return nil
		}
	}
	return nil
}

func (e *comparableValueExtractor) extractFromObject(object *storobj.Object, propName string) interface{} {
	if propName == filters.InternalPropID || propName == filters.InternalPropBackwardsCompatID {
		id := object.ID().String()
		return &id
	}
	if propName == filters.InternalPropCreationTimeUnix {
		ts := float64(object.CreationTimeUnix())
		return &ts
	}
	if propName == filters.InternalPropLastUpdateTimeUnix {
		ts := float64(object.LastUpdateTimeUnix())
		return &ts
	}

	propertiesMap, ok := object.Properties().(map[string]interface{})
	if !ok {
		return nil
	}
	value, ok := propertiesMap[propName]
	if !ok {
		return nil
	}

	// A live object's property arrives in whichever shape the object carries:
	// typed values from a parsed request, or the generic JSON shapes
	// ([]interface{}, map[string]interface{}) of an object unmarshaled from
	// storage. A value matching neither shape extracts as nil, the same as a
	// missing property — a sort must degrade to "no value", never panic the
	// query.
	switch e.dataTypesHelper.getType(propName) {
	case schema.DataTypeBlob, schema.DataTypeBlobHash, schema.DataTypeText:
		if s, ok := value.(string); ok {
			return &s
		}
	case schema.DataTypeTextArray:
		if sa, ok := asStringSlice(value); ok {
			return &sa
		}
	case schema.DataTypeDate:
		if d, ok := asDate(value); ok {
			return &d
		}
	case schema.DataTypeDateArray:
		if da, ok := asDateSlice(value); ok {
			return &da
		}
	case schema.DataTypeNumber, schema.DataTypeInt:
		if n, ok := value.(float64); ok {
			return &n
		}
	case schema.DataTypeNumberArray, schema.DataTypeIntArray:
		if na, ok := asFloat64Slice(value); ok {
			return &na
		}
	case schema.DataTypeBoolean:
		if b, ok := value.(bool); ok {
			return &b
		}
	case schema.DataTypeBooleanArray:
		if ba, ok := asBoolSlice(value); ok {
			return &ba
		}
	case schema.DataTypePhoneNumber:
		if pn, ok := asPhoneNumber(value); ok {
			fa := e.toFloatArrayFromPhoneNumber(pn)
			return &fa
		}
	case schema.DataTypeGeoCoordinates:
		if gc, ok := asGeoCoordinates(value); ok {
			fa := e.toFloatArrayFromGeoCoordinates(gc)
			return &fa
		}
	}
	return nil
}

// asStringSlice accepts the two shapes a stored string-array property takes
// on a live object: a typed []string, or the []interface{} of a JSON-decoded
// object.
func asStringSlice(value interface{}) ([]string, bool) {
	switch v := value.(type) {
	case []string:
		return v, true
	case []interface{}:
		out := make([]string, len(v))
		for i := range v {
			s, ok := v[i].(string)
			if !ok {
				return nil, false
			}
			out[i] = s
		}
		return out, true
	default:
		return nil, false
	}
}

func asFloat64Slice(value interface{}) ([]float64, bool) {
	switch v := value.(type) {
	case []float64:
		return v, true
	case []interface{}:
		out := make([]float64, len(v))
		for i := range v {
			n, ok := v[i].(float64)
			if !ok {
				return nil, false
			}
			out[i] = n
		}
		return out, true
	default:
		return nil, false
	}
}

func asBoolSlice(value interface{}) ([]bool, bool) {
	switch v := value.(type) {
	case []bool:
		return v, true
	case []interface{}:
		out := make([]bool, len(v))
		for i := range v {
			b, ok := v[i].(bool)
			if !ok {
				return nil, false
			}
			out[i] = b
		}
		return out, true
	default:
		return nil, false
	}
}

// asDate accepts the two shapes a date property takes on a live object: the
// request validator's typed time.Time, or the RFC3339 string of a
// JSON-decoded object. A malformed string is a missing value, not a panic.
func asDate(value interface{}) (time.Time, bool) {
	switch v := value.(type) {
	case time.Time:
		return v, true
	case string:
		d, err := time.Parse(time.RFC3339, v)
		if err != nil {
			return time.Time{}, false
		}
		return d, true
	default:
		return time.Time{}, false
	}
}

// asDateSlice mirrors asDate for date arrays: []time.Time from the validator,
// or string elements ([]string / []interface{}) parsed fallibly.
func asDateSlice(value interface{}) ([]time.Time, bool) {
	if da, ok := value.([]time.Time); ok {
		return da, true
	}
	sa, ok := asStringSlice(value)
	if !ok {
		return nil, false
	}
	out := make([]time.Time, len(sa))
	for i := range sa {
		d, err := time.Parse(time.RFC3339, sa[i])
		if err != nil {
			return nil, false
		}
		out[i] = d
	}
	return out, true
}

// asPhoneNumber accepts the typed *models.PhoneNumber of a parsed request or
// the map[string]interface{} of a JSON-decoded object. A map must carry the
// two numeric fields the comparator reads; a partial map (e.g. {}) is a
// missing value, not a zero phone number.
func asPhoneNumber(value interface{}) (*models.PhoneNumber, bool) {
	switch v := value.(type) {
	case *models.PhoneNumber:
		return v, v != nil
	case map[string]interface{}:
		countryCode, ok := v["countryCode"].(float64)
		if !ok {
			return nil, false
		}
		national, ok := v["national"].(float64)
		if !ok {
			return nil, false
		}
		return &models.PhoneNumber{
			CountryCode: uint64(countryCode),
			National:    uint64(national),
		}, true
	default:
		return nil, false
	}
}

// asGeoCoordinates mirrors asPhoneNumber for geo properties.
func asGeoCoordinates(value interface{}) (*models.GeoCoordinates, bool) {
	switch v := value.(type) {
	case *models.GeoCoordinates:
		return v, v != nil
	case map[string]interface{}:
		lon, ok := v["longitude"].(float64)
		if !ok {
			return nil, false
		}
		lat, ok := v["latitude"].(float64)
		if !ok {
			return nil, false
		}
		lonF, latF := float32(lon), float32(lat)
		return &models.GeoCoordinates{Longitude: &lonF, Latitude: &latF}, true
	default:
		return nil, false
	}
}

func (e *comparableValueExtractor) mustExtractNumbers(value []string) []float64 {
	numbers := make([]float64, len(value))
	for i := range value {
		number, err := strconv.ParseFloat(value[i], 64)
		if err != nil {
			panic("sorter: not a number")
		}
		numbers[i] = number
	}
	return numbers
}

func (e *comparableValueExtractor) mustExtractBools(value []string) []bool {
	bools := make([]bool, len(value))
	for i := range value {
		switch value[i] {
		case "true":
			bools[i] = true
		case "false":
			bools[i] = false
		default:
			panic("sorter: not a bool")
		}
	}
	return bools
}

func (e *comparableValueExtractor) mustExtractDates(value []string) []time.Time {
	dates := make([]time.Time, len(value))
	for i := range value {
		date, err := time.Parse(time.RFC3339, value[i])
		if err != nil {
			panic("sorter: not a date")
		}
		dates[i] = date
	}
	return dates
}

func (e *comparableValueExtractor) mustExtractPhoneNumber(value []string) *models.PhoneNumber {
	if len(value) == 1 {
		var phoneNumber *models.PhoneNumber
		if err := json.Unmarshal([]byte(value[0]), &phoneNumber); err == nil {
			return phoneNumber
		}
	}
	panic("sorter: not a phone number")
}

func (e *comparableValueExtractor) mustExtractGeoCoordinates(value []string) *models.GeoCoordinates {
	if len(value) == 1 {
		var geoCoordinates *models.GeoCoordinates
		if err := json.Unmarshal([]byte(value[0]), &geoCoordinates); err == nil {
			return geoCoordinates
		}
	}
	panic("sorter: not a geo coordinates")
}

func (e *comparableValueExtractor) toFloatArrayFromPhoneNumber(value *models.PhoneNumber) []float64 {
	return []float64{float64(value.CountryCode), float64(value.National)}
}

func (e *comparableValueExtractor) toFloatArrayFromGeoCoordinates(value *models.GeoCoordinates) []float64 {
	fa := make([]float64, 2)
	if value.Longitude != nil {
		fa[0] = float64(*value.Longitude)
	}
	if value.Latitude != nil {
		fa[1] = float64(*value.Latitude)
	}
	return fa
}

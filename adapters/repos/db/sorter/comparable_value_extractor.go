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
		case schema.DataTypeBlob:
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

	switch e.dataTypesHelper.getType(propName) {
	case schema.DataTypeBlob:
		s := value.(string)
		return &s
	case schema.DataTypeText:
		s := value.(string)
		return &s
	case schema.DataTypeTextArray:
		sa := value.([]string)
		return &sa
	case schema.DataTypeDate:
		d := e.mustExtractDates([]string{value.(string)})[0]
		return &d
	case schema.DataTypeDateArray:
		da := e.mustExtractDates(value.([]string))
		return &da
	case schema.DataTypeNumber, schema.DataTypeInt:
		n := value.(float64)
		return &n
	case schema.DataTypeNumberArray, schema.DataTypeIntArray:
		na := value.([]float64)
		return &na
	case schema.DataTypeBoolean:
		b := value.(bool)
		return &b
	case schema.DataTypeBooleanArray:
		ba := value.([]bool)
		return &ba
	case schema.DataTypePhoneNumber:
		fa := e.toFloatArrayFromPhoneNumber(value.(*models.PhoneNumber))
		return &fa
	case schema.DataTypeGeoCoordinates:
		fa := e.toFloatArrayFromGeoCoordinates(value.(*models.GeoCoordinates))
		return &fa
	default:
		return nil
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

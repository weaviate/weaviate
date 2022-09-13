//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package sorter

import (
	"encoding/json"
	"strconv"

	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/storobj"
)

type lsmPropertyExtractor struct {
	className   schema.ClassName
	classHelper *classHelper
	property    string
}

func newPropertyExtractor(className schema.ClassName,
	classHelper *classHelper, property string,
) *lsmPropertyExtractor {
	return &lsmPropertyExtractor{className, classHelper, property}
}

func (e *lsmPropertyExtractor) getProperty(v []byte) interface{} {
	prop, success, _ := storobj.ParseAndExtractProperty(v, e.property)
	// in case the property does not exist for the object return nil
	if len(prop) == 0 {
		return nil
	}
	if success {
		if e.property == "id" || e.property == "_id" {
			// handle special ID property
			return prop[0]
		}
		if e.property == "_creationTimeUnix" || e.property == "_lastUpdateTimeUnix" {
			// handle special _creationTimeUnix and _lastUpdateTimeUnix property
			return e.mustExtractNumber(prop)[0]
		}
		switch e.getDataType() {
		case schema.DataTypeString, schema.DataTypeText, schema.DataTypeBlob:
			return prop[0]
		case schema.DataTypeStringArray, schema.DataTypeTextArray:
			return prop
		case schema.DataTypeNumber, schema.DataTypeInt:
			return e.mustExtractNumber(prop)[0]
		case schema.DataTypeNumberArray, schema.DataTypeIntArray:
			return e.mustExtractNumber(prop)
		case schema.DataTypeDate:
			return prop[0]
		case schema.DataTypeDateArray:
			return prop
		case schema.DataTypeBoolean:
			return e.mustExtractBool(prop)[0]
		case schema.DataTypeBooleanArray:
			return e.mustExtractBool(prop)
		case schema.DataTypePhoneNumber:
			return e.mustExtractPhoneNumber(prop)
		case schema.DataTypeGeoCoordinates:
			return e.mustExtractGeoCoordinates(prop)
		default:
			return nil
		}
	}
	return nil
}

func (e *lsmPropertyExtractor) mustExtractNumber(value []string) []float64 {
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

func (e *lsmPropertyExtractor) mustExtractBool(value []string) []bool {
	bools := make([]bool, len(value))
	for i := range value {
		bools[i] = value[i] == "true"
	}
	return bools
}

func (e *lsmPropertyExtractor) mustExtractPhoneNumber(value []string) *models.PhoneNumber {
	var phoneNumber *models.PhoneNumber
	if len(value) == 1 {
		if err := json.Unmarshal([]byte(value[0]), &phoneNumber); err != nil {
			panic("sorter: not a phone number")
		}
	}
	return phoneNumber
}

func (e *lsmPropertyExtractor) mustExtractGeoCoordinates(value []string) *models.GeoCoordinates {
	var geoCoordinates *models.GeoCoordinates
	if len(value) == 1 {
		if err := json.Unmarshal([]byte(value[0]), &geoCoordinates); err != nil {
			panic("sorter: not a geo coordinates")
		}
	}
	return geoCoordinates
}

func (e *lsmPropertyExtractor) getDataType() schema.DataType {
	dataType := e.classHelper.getDataType(e.className.String(), e.property)
	if len(dataType) > 0 {
		return schema.DataType(dataType[0])
	}
	return ""
}

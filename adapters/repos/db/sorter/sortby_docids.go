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

type sortByDocIDs struct {
	docIDs   []uint64
	data     [][]byte
	property string
	dataType []string
	sortBy
}

func newSortByDocIDs(docIDs []uint64, data [][]byte, property, order string, dataType []string) sortByDocIDs {
	return sortByDocIDs{docIDs, data, property, dataType, sortBy{comparator{order}}}
}

func (s sortByDocIDs) Len() int {
	return len(s.docIDs)
}

func (s sortByDocIDs) Swap(i, j int) {
	s.docIDs[i], s.docIDs[j] = s.docIDs[j], s.docIDs[i]
	s.data[i], s.data[j] = s.data[j], s.data[i]
}

func (s sortByDocIDs) Less(i, j int) bool {
	return s.sortBy.lessBy(s.getProperty(i), s.getProperty(j), s.getDataType())
}

func (s sortByDocIDs) getDataType() schema.DataType {
	if len(s.dataType) > 0 {
		return schema.DataType(s.dataType[0])
	}
	return ""
}

func (s sortByDocIDs) getProperty(i int) interface{} {
	prop, success, _ := storobj.ParseAndExtractTextProp(s.data[i], s.property)
	if success {
		switch s.getDataType() {
		case schema.DataTypeString, schema.DataTypeText, schema.DataTypeBlob:
			return prop[0]
		case schema.DataTypeStringArray, schema.DataTypeTextArray:
			return prop
		case schema.DataTypeNumber, schema.DataTypeInt:
			return s.mustExtractNumber(prop)[0]
		case schema.DataTypeNumberArray, schema.DataTypeIntArray:
			return s.mustExtractNumber(prop)
		case schema.DataTypeDate:
			return prop[0]
		case schema.DataTypeDateArray:
			return prop
		case schema.DataTypeBoolean:
			return s.mustExtractBool(prop)[0]
		case schema.DataTypeBooleanArray:
			return s.mustExtractBool(prop)
		case schema.DataTypePhoneNumber:
			return s.mustExtractPhoneNumber(prop)
		case schema.DataTypeGeoCoordinates:
			return s.mustExtractGeoCoordinates(prop)
		default:
			return nil
		}
	}
	return nil
}

func (s sortByDocIDs) mustExtractNumber(value []string) []float64 {
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

func (s sortByDocIDs) mustExtractBool(value []string) []bool {
	bools := make([]bool, len(value))
	for i := range value {
		bools[i] = value[i] == "true"
	}
	return bools
}

func (s sortByDocIDs) mustExtractPhoneNumber(value []string) *models.PhoneNumber {
	var phoneNumber *models.PhoneNumber
	if len(value) == 1 {
		if err := json.Unmarshal([]byte(value[0]), &phoneNumber); err != nil {
			panic("sorter: not a phone number")
		}
	}
	return phoneNumber
}

func (s sortByDocIDs) mustExtractGeoCoordinates(value []string) *models.GeoCoordinates {
	var geoCoordinates *models.GeoCoordinates
	if len(value) == 1 {
		if err := json.Unmarshal([]byte(value[0]), &geoCoordinates); err != nil {
			panic("sorter: not a geo coordinates")
		}
	}
	return geoCoordinates
}

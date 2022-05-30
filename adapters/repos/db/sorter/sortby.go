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
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
)

type sortBy struct {
	comparator *comparator
}

func newSortBy(comparator *comparator) *sortBy {
	return &sortBy{comparator}
}

func (s *sortBy) compare(i, j interface{}, dataType schema.DataType) bool {
	switch dataType {
	case schema.DataTypeString, schema.DataTypeText, schema.DataTypeBlob:
		a, b := s.asString(i), s.asString(j)
		return s.comparator.compareString(a, b)
	case schema.DataTypeStringArray, schema.DataTypeTextArray:
		a, b := s.asStringArray(i), s.asStringArray(j)
		return s.comparator.compareString(a, b)
	case schema.DataTypeNumber, schema.DataTypeInt:
		a, b := s.asNumber(i), s.asNumber(j)
		return s.comparator.compareFloat64(a, b)
	case schema.DataTypeNumberArray, schema.DataTypeIntArray:
		a, b := s.asNumberArray(i), s.asNumberArray(j)
		return s.comparator.compareFloat64(a, b)
	case schema.DataTypeDate:
		a, b := s.asDate(i), s.asDate(j)
		return s.comparator.compareDate(a, b)
	case schema.DataTypeDateArray:
		a, b := s.asDateArray(i), s.asDateArray(j)
		return s.comparator.compareDate(a, b)
	case schema.DataTypeBoolean:
		a, b := s.asBool(i), s.asBool(j)
		return s.comparator.compareBool(a, b)
	case schema.DataTypeBooleanArray:
		a, b := s.asBoolArray(i), s.asBoolArray(j)
		return s.comparator.compareBool(a, b)
	case schema.DataTypePhoneNumber:
		a, b := s.asPhoneNumber(i), s.asPhoneNumber(j)
		return s.comparator.compareFloat64(a, b)
	case schema.DataTypeGeoCoordinates:
		a, b := s.asGeoCoordinate(i), s.asGeoCoordinate(j)
		return s.comparator.compareFloat64(a, b)
	default:
		// don't sort
		return false
	}
}

func (s *sortBy) asString(prop interface{}) *string {
	if prop != nil {
		if asString, ok := prop.(string); ok {
			return &asString
		}
	}
	return nil
}

func (s *sortBy) asStringArray(prop interface{}) *string {
	if prop != nil {
		if arr, ok := prop.([]string); ok && len(arr) > 0 {
			var sb strings.Builder
			for i := range arr {
				sb.WriteString(arr[i])
			}
			res := sb.String()
			return &res
		}
	}
	return nil
}

func (s *sortBy) asNumber(prop interface{}) *float64 {
	if prop != nil {
		if asNumber, ok := prop.(float64); ok {
			return &asNumber
		}
	}
	return nil
}

func (s *sortBy) asNumberArray(prop interface{}) *float64 {
	if prop != nil {
		if arr, ok := prop.([]float64); ok && len(arr) > 0 {
			var res float64
			for i := range arr {
				res += arr[i]
			}
			return &res
		}
	}
	return nil
}

func (s *sortBy) asDate(prop interface{}) *time.Time {
	if asString := s.asString(prop); asString != nil {
		if date, err := s.parseDate(*asString); err == nil {
			return &date
		}
	}
	return nil
}

func (s *sortBy) asDateArray(prop interface{}) *time.Time {
	if prop != nil {
		if arr, ok := prop.([]string); ok && len(arr) > 0 {
			var res int64
			for i := range arr {
				if date, err := s.parseDate(arr[i]); err == nil {
					res += date.Unix()
				}
			}
			sum := time.Unix(res, 0)
			return &sum
		}
	}
	return nil
}

func (s *sortBy) asBool(prop interface{}) *bool {
	if prop != nil {
		if asBool, ok := prop.(bool); ok {
			return &asBool
		}
	}
	return nil
}

func (s *sortBy) asBoolArray(prop interface{}) *bool {
	if prop != nil {
		if arr, ok := prop.([]bool); ok && len(arr) > 0 {
			res := false
			for i := range arr {
				res = res && arr[i]
			}
			return &res
		}
	}
	return nil
}

func (s *sortBy) asPhoneNumber(prop interface{}) *float64 {
	if prop != nil {
		if phoneNumber, ok := prop.(*models.PhoneNumber); ok {
			phoneStr := fmt.Sprintf("%v%v", phoneNumber.CountryCode, phoneNumber.National)
			if phone, err := strconv.ParseFloat(phoneStr, 64); err == nil {
				return &phone
			}
		}
	}
	return nil
}

func (s *sortBy) asGeoCoordinate(prop interface{}) *float64 {
	if prop != nil {
		if geoCoordinates, ok := prop.(*models.GeoCoordinates); ok {
			if longitude := geoCoordinates.Longitude; longitude != nil {
				long := float64(*longitude)
				return &long
			}
		}
	}
	return nil
}

func (s *sortBy) parseDate(in string) (time.Time, error) {
	return time.Parse(time.RFC3339, in)
}

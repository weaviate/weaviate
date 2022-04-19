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
	"github.com/semi-technologies/weaviate/entities/schema"
)

type classHelper struct {
	schema schema.Schema
}

func newClassHelper(schema schema.Schema) *classHelper {
	return &classHelper{schema}
}

func (s *classHelper) getDataType(className, property string) []string {
	class := s.schema.GetClass(schema.ClassName(className))
	for _, prop := range class.Properties {
		if prop.Name == property {
			return prop.DataType
		}
	}
	return nil
}

func (s *classHelper) propertyExists(className, property string) bool {
	dataType := s.getDataType(className, property)
	if dataType == nil {
		return false
	}
	return true
}

func (s *classHelper) isDataTypeSupported(dataType []string) bool {
	if len(dataType) == 1 {
		switch schema.DataType(dataType[0]) {
		case schema.DataTypeString, schema.DataTypeText, schema.DataTypeBlob,
			schema.DataTypeStringArray, schema.DataTypeTextArray,
			schema.DataTypeNumber, schema.DataTypeInt,
			schema.DataTypeNumberArray, schema.DataTypeIntArray,
			schema.DataTypeDate, schema.DataTypeDateArray,
			schema.DataTypeBoolean, schema.DataTypeBooleanArray,
			schema.DataTypePhoneNumber, schema.DataTypeGeoCoordinates:
			return true
		default:
			return false
		}
	}
	return false
}

func (s *classHelper) getOrder(order string) string {
	switch order {
	case "asc", "desc":
		return order
	default:
		return "asc"
	}
}

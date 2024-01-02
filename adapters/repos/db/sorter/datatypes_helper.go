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
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

type dataTypesHelper struct {
	class     *models.Class
	dataTypes map[string][]string
}

func newDataTypesHelper(class *models.Class) *dataTypesHelper {
	return &dataTypesHelper{class, make(map[string][]string)}
}

func (h *dataTypesHelper) getStrings(propName string) []string {
	if dataType, ok := h.dataTypes[propName]; ok {
		return dataType
	}

	h.dataTypes[propName] = h.find(propName)
	return h.dataTypes[propName]
}

func (h *dataTypesHelper) find(propName string) []string {
	if propName == filters.InternalPropID || propName == filters.InternalPropBackwardsCompatID {
		return schema.DataTypeText.PropString()
	}
	if propName == filters.InternalPropCreationTimeUnix || propName == filters.InternalPropLastUpdateTimeUnix {
		return []string{string(schema.DataTypeInt)}
	}
	for _, property := range h.class.Properties {
		if property.Name == propName {
			return property.DataType
		}
	}
	return nil
}

func (h *dataTypesHelper) getType(propName string) schema.DataType {
	strings := h.getStrings(propName)
	if len(strings) > 0 {
		return schema.DataType(strings[0])
	}
	return ""
}

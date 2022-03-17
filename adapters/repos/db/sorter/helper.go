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

type sorterClassHelper struct {
	schema schema.Schema
}

func (s *sorterClassHelper) getDataType(className, property string) []string {
	var dataType []string
	class := s.schema.GetClass(schema.ClassName(className))
	for _, prop := range class.Properties {
		if prop.Name == property {
			dataType = prop.DataType
		}
	}
	return dataType
}

func (s *sorterClassHelper) getOrder(order string) string {
	switch order {
	case "asc", "desc":
		return order
	default:
		return "asc"
	}
}

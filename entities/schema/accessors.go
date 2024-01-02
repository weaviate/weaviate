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

package schema

import (
	"github.com/weaviate/weaviate/entities/models"
)

func (s *Schema) GetClass(className ClassName) *models.Class {
	class, err := GetClassByName(s.Objects, string(className))
	if err != nil {
		return nil
	}

	return class
}

// FindClassByName will find either a Thing or Class by name.
func (s *Schema) FindClassByName(className ClassName) *models.Class {
	semSchemaClass, err := GetClassByName(s.Objects, string(className))
	if err == nil {
		return semSchemaClass
	}

	return nil
}

// func (s *Schema) GetKindOfClass(className ClassName) (kind.Kind, bool) {
// 	_, err := GetClassByName(s.Objects, string(className))
// 	if err == nil {
// 		return kind.Object, true
// 	}

// 	return "", false
// }

func (s *Schema) GetProperty(className ClassName, propName PropertyName) (*models.Property, error) {
	semSchemaClass, err := GetClassByName(s.Objects, string(className))
	if err != nil {
		return nil, err
	}

	semProp, err := GetPropertyByName(semSchemaClass, string(propName))
	if err != nil {
		return nil, err
	}

	return semProp, nil
}

func (s *Schema) GetPropsOfType(propType string) []ClassAndProperty {
	return extractAllOfPropType(s.Objects.Classes, propType)
}

func extractAllOfPropType(classes []*models.Class, propType string) []ClassAndProperty {
	var result []ClassAndProperty
	for _, class := range classes {
		for _, prop := range class.Properties {
			if prop.DataType[0] == propType {
				result = append(result, ClassAndProperty{
					ClassName:    ClassName(class.Class),
					PropertyName: PropertyName(prop.Name),
				})
			}
		}
	}

	return result
}

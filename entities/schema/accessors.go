//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package schema

import (
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
)

func (s *Schema) GetClass(k kind.Kind, className ClassName) *models.Class {
	schema := s.SemanticSchemaFor(k)
	class, err := GetClassByName(schema, string(className))

	if err != nil {
		return nil
	}

	return class
}

// FindClassByName will find either a Thing or Class by name.
func (s *Schema) FindClassByName(className ClassName) *models.Class {
	if s.Things != nil {
		semSchemaClass, err := GetClassByName(s.Things, string(className))
		if err == nil {
			return semSchemaClass
		}
	}

	if s.Actions != nil {
		semSchemaClass, err := GetClassByName(s.Actions, string(className))
		if err == nil {
			return semSchemaClass
		}
	}

	return nil
}

func (s *Schema) GetKindOfClass(className ClassName) (kind.Kind, bool) {
	if s.Things != nil {
		_, err := GetClassByName(s.Things, string(className))
		if err == nil {
			return kind.Thing, true
		}
	}

	if s.Actions != nil {
		_, err := GetClassByName(s.Actions, string(className))
		if err == nil {
			return kind.Action, true
		}
	}

	return "", false
}

func (s *Schema) GetProperty(kind kind.Kind, className ClassName, propName PropertyName) (*models.Property, error) {
	semSchemaClass, err := GetClassByName(s.SemanticSchemaFor(kind), string(className))
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
	var result []ClassAndProperty

	result = append(
		extractAllOfPropType(s.Actions.Classes, propType),
		extractAllOfPropType(s.Things.Classes, propType)...,
	)

	return result
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

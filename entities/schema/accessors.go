/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/semi-technologies/weaviate/blob/develop/LICENSE.md
 * DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@semi.technology
 */

package schema

import (
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
)

func (s *Schema) GetClass(k kind.Kind, className ClassName) *models.SemanticSchemaClass {
	schema := s.SemanticSchemaFor(k)
	class, err := GetClassByName(schema, string(className))

	if err != nil {
		return nil
	}

	return class
}

// FindClassByName will find either a Thing or Class by name.
func (s *Schema) FindClassByName(className ClassName) *models.SemanticSchemaClass {
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

func (s *Schema) GetProperty(kind kind.Kind, className ClassName, propName PropertyName) (error, *models.SemanticSchemaClassProperty) {
	semSchemaClass, err := GetClassByName(s.SemanticSchemaFor(kind), string(className))
	if err != nil {
		return err, nil
	}

	semProp, err := GetPropertyByName(semSchemaClass, string(propName))
	if err != nil {
		return err, nil
	}

	return nil, semProp
}

func (s *Schema) GetPropsOfType(propType string) []ClassAndProperty {
	var result []ClassAndProperty

	result = append(
		extractAllOfPropType(s.Actions.Classes, propType),
		extractAllOfPropType(s.Things.Classes, propType)...,
	)

	return result
}

func extractAllOfPropType(classes []*models.SemanticSchemaClass, propType string) []ClassAndProperty {
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

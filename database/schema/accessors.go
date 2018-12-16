/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2018 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * AUTHOR: Bob van Luijt (bob@kub.design)
 * See www.creativesoftwarefdn.org for details
 * Contact: @CreativeSofwFdn / bob@kub.design
 */
package schema

import (
	"github.com/creativesoftwarefdn/weaviate/database/schema/kind"
	"github.com/creativesoftwarefdn/weaviate/models"
)

func (s *Schema) GetClass(k kind.Kind, className ClassName) *models.SemanticSchemaClass {
	schema := s.SemanticSchemaFor(k)
	class, err := GetClassByName(schema, string(className))

	if err != nil {
		return nil
	}

	return class
}

// Find either a Thing or Class by name.
func (s *Schema) FindClassByName(className ClassName) *models.SemanticSchemaClass {
	semSchemaClass, err := GetClassByName(s.Things, string(className))
	if err == nil {
		return semSchemaClass
	}

	semSchemaClass, err = GetClassByName(s.Actions, string(className))
	if err == nil {
		return semSchemaClass
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

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
	"fmt"

	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
)

// Newtype to denote that this string is used as a Class name
type ClassName string

func (c ClassName) String() string {
	return string(c)
}

// Newtype to denote that this string is used as a Property name
type PropertyName string

func (p PropertyName) String() string {
	return string(p)
}

type ClassAndProperty struct {
	ClassName    ClassName
	PropertyName PropertyName
}

// Describes the schema that is used in Weaviate.
type Schema struct {
	Actions *models.Schema
	Things  *models.Schema
}

func Empty() Schema {
	return Schema{
		Actions: &models.Schema{
			Classes: []*models.Class{},
		},
		Things: &models.Schema{
			Classes: []*models.Class{},
		},
	}
}

// Return one of the semantic schema's
func (s *Schema) SemanticSchemaFor(k kind.Kind) *models.Schema {
	switch k {
	case kind.Thing:
		return s.Things
	case kind.Action:
		return s.Actions
	default:
		panic(fmt.Sprintf("No such kind '%s'", k))
	}
}

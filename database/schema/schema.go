/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@creativesoftwarefdn.org
 */
package schema

import (
	"fmt"

	"github.com/creativesoftwarefdn/weaviate/database/schema/kind"
	"github.com/creativesoftwarefdn/weaviate/entities/models"
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
	Actions *models.SemanticSchema
	Things  *models.SemanticSchema
}

func Empty() Schema {
	return Schema{
		Actions: &models.SemanticSchema{
			Classes: []*models.SemanticSchemaClass{},
		},
		Things: &models.SemanticSchema{
			Classes: []*models.SemanticSchemaClass{},
		},
	}
}

// Return one of the semantic schema's
func (s *Schema) SemanticSchemaFor(k kind.Kind) *models.SemanticSchema {
	switch k {
	case kind.THING_KIND:
		return s.Things
	case kind.ACTION_KIND:
		return s.Actions
	default:
		panic(fmt.Sprintf("No such kind '%s'", k))
	}
}

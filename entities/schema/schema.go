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

package schema

import (
	"github.com/semi-technologies/weaviate/entities/models"
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
	Objects *models.Schema
}

func Empty() Schema {
	return Schema{
		Objects: &models.Schema{
			Classes: []*models.Class{},
		},
	}
}

// Return one of the semantic schema's
func (s *Schema) SemanticSchemaFor() *models.Schema {
	return s.Objects
}

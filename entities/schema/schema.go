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
	"strings"

	"github.com/weaviate/weaviate/entities/models"
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

func UppercaseClassName(name string) string {
	if len(name) < 1 {
		return name
	}

	if len(name) == 1 {
		return strings.ToUpper(name)
	}

	return strings.ToUpper(string(name[0])) + name[1:]
}

func LowercaseAllPropertyNames(props []*models.Property) []*models.Property {
	for i, prop := range props {
		props[i].Name = LowercaseFirstLetter(prop.Name)
	}

	return props
}

func LowercaseFirstLetter(name string) string {
	if len(name) < 1 {
		return name
	}

	if len(name) == 1 {
		return strings.ToLower(name)
	}

	return strings.ToLower(string(name[0])) + name[1:]
}

func LowercaseFirstLetterOfStrings(in []string) []string {
	if len(in) < 1 {
		return in
	}
	out := make([]string, len(in))
	for i, str := range in {
		out[i] = LowercaseFirstLetter(str)
	}

	return out
}

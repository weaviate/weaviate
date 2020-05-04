//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Holding B.V. (registered @ Dutch Chamber of Commerce no 75221632). All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

package filters

import (
	"fmt"
	"strings"

	"github.com/semi-technologies/weaviate/entities/schema"
)

// Represents the path in a filter.
// Either RelationProperty or PrimitiveProperty must be empty (e.g. "").
type Path struct {
	Class    schema.ClassName
	Property schema.PropertyName

	// If nil, then this is the property we're interested in.
	// If a pointer to another Path, the constraint applies to that one.
	Child *Path
}

// GetInnerMost recursively searches for child paths, only when no more
// children can be found will the path be returned
func (p *Path) GetInnerMost() *Path {
	if p.Child == nil {
		return p
	}

	return p.Child.GetInnerMost()
}

// Slice flattens the nested path into a slice of segments
func (p *Path) Slice() []string {
	return appendNestedPath(p, true, true)
}

func (p *Path) SliceInterface() []interface{} {
	path := appendNestedPath(p, true, true)
	out := make([]interface{}, len(path), len(path))
	for i, element := range path {
		out[i] = element
	}
	return out
}

func (p *Path) SliceNonTitleized() []string {
	return appendNestedPath(p, true, false)
}

func appendNestedPath(p *Path, omitClass, titleizeProperty bool) []string {
	result := []string{}
	if !omitClass {
		result = append(result, string(p.Class))
	}

	if p.Child != nil {
		property := string(p.Property)
		if titleizeProperty {
			property = strings.Title(property)
		}
		result = append(result, property)
		result = append(result, appendNestedPath(p.Child, false, titleizeProperty)...)
	} else {
		result = append(result, string(p.Property))
	}

	return result
}

// ParsePath Parses the path
// It parses an array of strings in this format
// [0] ClassName -> The root class name we're drilling down from
// [1] propertyName -> The property name we're interested in.
func ParsePath(pathElements []interface{}, rootClass string) (*Path, error) {
	// we need to manually insert the root class, as that is omitted from the user
	pathElements = append([]interface{}{rootClass}, pathElements...)

	// The sentinel is used to bootstrap the inlined recursion.
	// we return sentinal.Child at the end.
	var sentinel Path

	// Keep track of where we are in the path (e.g. always points to latest Path segment)
	var current = &sentinel

	// Now go through the path elements, step over it in increments of two.
	// Simple case:      ClassName -> property
	// Nested path case: ClassName -> HasRef -> ClassOfRef -> Property
	for i := 0; i < len(pathElements); i += 2 {
		lengthRemaining := len(pathElements) - i
		if lengthRemaining < 2 {
			return nil, fmt.Errorf("missing an argument after '%s'", pathElements[i])
		}

		rawClassName, ok := pathElements[i].(string)
		if !ok {
			return nil, fmt.Errorf("element %v is not a string", i+1)
		}

		rawPropertyName, ok := pathElements[i+1].(string)
		if !ok {
			return nil, fmt.Errorf("element %v is not a string", i+2)
		}

		className, err := schema.ValidateClassName(rawClassName)
		if err != nil {
			return nil, fmt.Errorf("Expected a valid class name in 'path' field for the filter but got '%s'", rawClassName)
		}

		propertyName, err := schema.ValidatePropertyName(rawPropertyName)

		// Invalid property name?
		// Try to parse it as as a reference.
		if err != nil {
			untitlizedPropertyName := strings.ToLower(rawPropertyName[0:1]) + rawPropertyName[1:len(rawPropertyName)]
			propertyName, err = schema.ValidatePropertyName(untitlizedPropertyName)
			if err != nil {
				return nil, fmt.Errorf("Expected a valid property name in 'path' field for the filter, but got '%s'", rawPropertyName)
			}
		}

		current.Child = &Path{
			Class:    className,
			Property: propertyName,
		}

		// And down we go.
		current = current.Child
	}

	return sentinel.Child, nil
}

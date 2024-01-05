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

package traverser

import (
	"fmt"
)

// TODO: is this still used?

// SearchType to search for either class names or property names
type SearchType string

const (
	// SearchTypeClass to search the contextionary for class names
	SearchTypeClass SearchType = "class"
	// SearchTypeProperty to search the contextionary for property names
	SearchTypeProperty SearchType = "property"
)

// SearchParams to be used for a SchemaSearch. See individual properties for
// additional documentation on what they do
type SearchParams struct {
	// SearchType can be SearchTypeClass or SearchTypeProperty
	SearchType SearchType

	// Name is the string-representation of the class or property name
	Name string

	// Certainty must be a value between 0 and 1. The higher it is the narrower
	// is the search, the lower it is, the wider the search is
	Certainty float32
}

// Validate the feasibility of the specified arguments
func (p SearchParams) Validate() error {
	if p.Name == "" {
		return fmt.Errorf("Name cannot be empty")
	}

	if err := p.validateCertaintyOrWeight(p.Certainty); err != nil {
		return fmt.Errorf("invalid Certainty: %s", err)
	}

	if p.SearchType != SearchTypeClass && p.SearchType != SearchTypeProperty {
		return fmt.Errorf(
			"SearchType must be SearchTypeClass or SearchTypeProperty, but got '%s'", p.SearchType)
	}

	return nil
}

func (p SearchParams) validateCertaintyOrWeight(c float32) error {
	if c >= 0 && c <= 1 {
		return nil
	}

	return fmt.Errorf("must be between 0 and 1, but got '%f'", c)
}

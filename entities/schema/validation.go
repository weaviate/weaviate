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
	"fmt"
	"regexp"
)

var (
	validateClassNameRegex    *regexp.Regexp
	validatePropertyNameRegex *regexp.Regexp
	validateNetworkClassRegex *regexp.Regexp
	reservedPropertyNames     []string
)

func init() {
	validateClassNameRegex = regexp.MustCompile(`^[A-Z][_0-9A-Za-z]*$`)
	validatePropertyNameRegex = regexp.MustCompile(`^[_A-Za-z][_0-9A-Za-z]*$`)
	validateNetworkClassRegex = regexp.MustCompile(`^([A-Za-z]+)+/([A-Z][a-z]+)+$`)
	reservedPropertyNames = []string{"_additional", "_id", "id"}
}

// ValidateClassName validates that this string is a valid class name (formate
// wise)
func ValidateClassName(name string) (ClassName, error) {
	if validateClassNameRegex.MatchString(name) {
		return ClassName(name), nil
	}
	return "", fmt.Errorf("'%s' is not a valid class name", name)
}

// ValidatePropertyName validates that this string is a valid property name
func ValidatePropertyName(name string) (PropertyName, error) {
	if validatePropertyNameRegex.MatchString(name) {
		return PropertyName(name), nil
	}
	return "", fmt.Errorf("'%s' is not a valid property name. "+
		"Property names in Weaviate are restricted to valid GraphQL names, "+
		"which must be “/[_A-Za-z][_0-9A-Za-z]*/”.", name)
}

// ValidateReservedPropertyName validates that a string is not a reserved property name
func ValidateReservedPropertyName(name string) error {
	for i := range reservedPropertyNames {
		if name == reservedPropertyNames[i] {
			return fmt.Errorf("'%s' is a reserved property name", name)
		}
	}
	return nil
}

// ValidNetworkClassName verifies if the specified class is a valid
// crossReference name. This does not mean the class currently exists
// on the specified instance or that the instance exist, but simply
// that the name is valid.
// Receiving a false could also still mean the class is not network-ref, but
// simply a local-ref.
func ValidNetworkClassName(name string) bool {
	return validateNetworkClassRegex.MatchString(name)
}

// AssertValidClassName assert that this string is a valid class name or
// panics and should therefore most likely not be used
func AssertValidClassName(name string) ClassName {
	n, err := ValidateClassName(name)
	if err != nil {
		panic(fmt.Sprintf("Did not expect to be handled '%s', an invalid class name", name))
	}
	return n
}

// AssertValidPropertyName asserts that this string is a valid property name or
// panics and should therefore most likely never be used.
func AssertValidPropertyName(name string) PropertyName {
	n, err := ValidatePropertyName(name)
	if err != nil {
		panic(fmt.Sprintf("Did not expect to be handled '%s', an invalid property name", name))
	}
	return n
}

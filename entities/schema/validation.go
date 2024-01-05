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
	"fmt"
	"regexp"
)

var (
	validateClassNameRegex          *regexp.Regexp
	validatePropertyNameRegex       *regexp.Regexp
	validateNestedPropertyNameRegex *regexp.Regexp
	reservedPropertyNames           []string
)

const (
	ClassNameRegexCore      = `[A-Z][_0-9A-Za-z]*`
	ShardNameRegexCore      = `[A-Za-z0-9\-\_]{1,64}`
	PropertyNameRegex       = `[_A-Za-z][_0-9A-Za-z]*`
	NestedPropertyNameRegex = `[_A-Za-z][_0-9A-Za-z]*`
)

func init() {
	validateClassNameRegex = regexp.MustCompile(`^` + ClassNameRegexCore + `$`)
	validatePropertyNameRegex = regexp.MustCompile(`^` + PropertyNameRegex + `$`)
	validateNestedPropertyNameRegex = regexp.MustCompile(`^` + NestedPropertyNameRegex + `$`)
	reservedPropertyNames = []string{"_additional", "_id", "id"}
}

// ValidateClassName validates that this string is a valid class name (format wise)
func ValidateClassName(name string) (ClassName, error) {
	if validateClassNameRegex.MatchString(name) {
		return ClassName(name), nil
	}
	return "", fmt.Errorf("'%s' is not a valid class name", name)
}

// ValidatePropertyName validates that this string is a valid property name
func ValidatePropertyName(name string) (PropertyName, error) {
	if !validatePropertyNameRegex.MatchString(name) {
		return "", fmt.Errorf("'%s' is not a valid property name. "+
			"Property names in Weaviate are restricted to valid GraphQL names, "+
			"which must be “/%s/”.", name, PropertyNameRegex)
	}
	return PropertyName(name), nil
}

// ValidateNestedPropertyName validates that this string is a valid nested property name
func ValidateNestedPropertyName(name, prefix string) error {
	if !validateNestedPropertyNameRegex.MatchString(name) {
		return fmt.Errorf("'%s' is not a valid nested property name of '%s'. "+
			"NestedProperty names in Weaviate are restricted to valid GraphQL names, "+
			"which must be “/%s/”.", name, prefix, NestedPropertyNameRegex)
	}
	return nil
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

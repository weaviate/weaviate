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
	validateClassNameRegex          = regexp.MustCompile(`^` + ClassNameRegexCore + `$`)
	validateTenantNameRegex         = regexp.MustCompile(`^` + ShardNameRegexCore + `$`)
	validatePropertyNameRegex       = regexp.MustCompile(`^` + PropertyNameRegex + `$`)
	validateNestedPropertyNameRegex = regexp.MustCompile(`^` + NestedPropertyNameRegex + `$`)
	reservedPropertyNames           = []string{"_additional", "_id", "id"}
)

const (
	// Restricted by max length allowed for dir name (255 chars)
	// As dir containing class data is named after class, 255 chars are allowed
	classNameMaxLength = 255
	ClassNameRegexCore = `[A-Z][_0-9A-Za-z]{0,254}`
	// ClassNameRegexAllowRegex allowed chars in class name including regex patterns, 255 chars are allowed
	ClassNameRegexAllowRegex = `^(\*|[A-Z][_0-9A-Za-z\-.*+?^$()|{}\[\]\\]{0,254})$`
	// ShardNameRegexCore allowed chars in shard name, 64 chars are allowed
	ShardNameRegexCore = `[A-Za-z0-9\-\_]{1,64}`
	// ShardNameRegexAllowRegex allowed chars in shard name including regex patterns, 64 chars are allowed
	ShardNameRegexAllowRegex = `^[A-Za-z0-9\-_.*+?^$()|{}\[\]\\*]{1,64}$`
	// Restricted by max length allowed for dir name (255 chars)
	// Property name is used to build dir names of various purposes containing property
	// related data. Among them might be (depending on the settings):
	// - geo.{property_name}.hnsw.commitlog.d
	// - property_{property_name}__meta_count
	// - property_{property_name}_propertyLength
	// Last one seems to add the most additional characters (24) to property name,
	// therefore poperty max lentgh should not exceed 255 - 24 = 231 chars.
	propertyNameMaxLength = 231
	PropertyNameRegex     = `[_A-Za-z][_0-9A-Za-z]{0,230}`
	// Nested properties names are not used to build directory names (yet),
	// no max length restriction is imposed
	NestedPropertyNameRegex = `[_A-Za-z][_0-9A-Za-z]*`
	// Target vector names must be GraphQL compliant names no longer then 230 characters
	TargetVectorNameMaxLength = 230
	TargetVectorNameRegex     = `[_A-Za-z][_0-9A-Za-z]{0,229}`
)

// ValidateClassName validates that this string is a valid class name (format wise)
func ValidateClassName(name string) (ClassName, error) {
	if len(name) > classNameMaxLength {
		return "", fmt.Errorf("'%s' is not a valid class name. Name should not be longer than %d characters",
			name, classNameMaxLength)
	}
	if !validateClassNameRegex.MatchString(name) {
		return "", fmt.Errorf("'%s' is not a valid class name", name)
	}
	return ClassName(name), nil
}

// ValidateClassNameIncludesRegex validates that this string is a valid class name (format wise)
// can include regex pattern
func ValidateClassNameIncludesRegex(name string) (ClassName, error) {
	if len(name) > classNameMaxLength {
		return "", fmt.Errorf("'%s' is not a valid class name. Name should not be longer than %d characters",
			name, classNameMaxLength)
	}
	if !regexp.MustCompile(ClassNameRegexAllowRegex).MatchString(name) {
		return "", fmt.Errorf("'%s' is not a valid class name", name)
	}
	return ClassName(name), nil
}

// ValidateTenantName validates that this string is a valid tenant name (format wise)
func ValidateTenantName(name string) error {
	if !validateTenantNameRegex.MatchString(name) {
		var msg string
		if name == "" {
			msg = "empty tenant name"
		} else {
			msg = fmt.Sprintf(
				" '%s' is not a valid tenant name. should only contain alphanumeric characters (a-z, A-Z, 0-9), "+
					"underscore (_), and hyphen (-), with a length between 1 and 64 characters",
				name,
			)
		}
		return fmt.Errorf("%s", msg)
	}
	return nil
}

// ValidateTenantNameIncludesRegex validates that this string is a valid tenant name (format wise)
// can include regex pattern
func ValidateTenantNameIncludesRegex(name string) error {
	if !regexp.MustCompile(ShardNameRegexAllowRegex).MatchString(name) {
		var msg string
		if name == "" {
			msg = "empty tenant name"
		} else {
			msg = fmt.Sprintf(
				" '%s' is not a valid tenant name. should only contain alphanumeric characters (a-z, A-Z, 0-9), "+
					"underscore (_), and hyphen (-), with a length between 1 and 64 characters",
				name,
			)
		}
		return fmt.Errorf("%s", msg)
	}
	return nil
}

// ValidatePropertyName validates that this string is a valid property name
func ValidatePropertyName(name string) (PropertyName, error) {
	if len(name) > propertyNameMaxLength {
		return "", fmt.Errorf("'%s' is not a valid property name. Name should not be longer than %d characters",
			name, propertyNameMaxLength)
	}
	if !validatePropertyNameRegex.MatchString(name) {
		return "", fmt.Errorf("'%s' is not a valid property name. "+
			"Property names in Weaviate are restricted to valid GraphQL names, "+
			"which must be “/%s/”", name, PropertyNameRegex)
	}
	return PropertyName(name), nil
}

// ValidateNestedPropertyName validates that this string is a valid nested property name
func ValidateNestedPropertyName(name, prefix string) error {
	if !validateNestedPropertyNameRegex.MatchString(name) {
		return fmt.Errorf("'%s' is not a valid nested property name of '%s'. "+
			"NestedProperty names in Weaviate are restricted to valid GraphQL names, "+
			"which must be “/%s/”", name, prefix, NestedPropertyNameRegex)
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

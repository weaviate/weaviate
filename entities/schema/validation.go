//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2019 SeMI Holding B.V. (registered @ Dutch Chamber of Commerce no 75221632). All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

package schema

import (
	"fmt"
	"regexp"
)

var validateClassNameRegex *regexp.Regexp
var validatePropertyNameRegex *regexp.Regexp
var validateNetworkClassRegex *regexp.Regexp

func init() {
	validateClassNameRegex = regexp.MustCompile(`^([A-Z][a-z]+)+$`)
	validatePropertyNameRegex = regexp.MustCompile(`^[a-z]+([A-Z][a-z]+)*$`)
	validateNetworkClassRegex = regexp.MustCompile(`^([A-Za-z]+)+/([A-Z][a-z]+)+$`)
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

	return "", fmt.Errorf("'%s' is not a valid property name", name)
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

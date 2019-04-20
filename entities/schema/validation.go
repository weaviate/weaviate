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

// Validates that this string is a valid class name (formate wise)
func ValidateClassName(name string) (error, ClassName) {
	if validateClassNameRegex.MatchString(name) {
		return nil, ClassName(name)
	} else {
		return fmt.Errorf("'%s' is not a valid class name", name), ""
	}
}

// Validates that this string is a valid property name
func ValidatePropertyName(name string) (error, PropertyName) {
	if validatePropertyNameRegex.MatchString(name) {
		return nil, PropertyName(name)
	} else {
		return fmt.Errorf("'%s' is not a valid property name", name), ""
	}
}

// ValidClassName verifies if the specified class is a valid
// crossReference name. This does not mean the class currently exists
// on the specified instance or that the instance exist, but simply
// that the name is valid.
// Receiving a false could also still mean the class is not network-ref, but
// simply a local-ref.
func ValidNetworkClassName(name string) bool {
	return validateNetworkClassRegex.MatchString(name)
}

// Assert that this string is a valid class name
func AssertValidClassName(name string) ClassName {
	err, n := ValidateClassName(name)
	if err != nil {
		panic(fmt.Sprintf("Did not expect to be handled '%s', an invalid class name", name))
	}
	return n
}

// Assert that this string is a valid property name
func AssertValidPropertyName(name string) PropertyName {
	err, n := ValidatePropertyName(name)
	if err != nil {
		panic(fmt.Sprintf("Did not expect to be handled '%s', an invalid property name", name))
	}
	return n
}
